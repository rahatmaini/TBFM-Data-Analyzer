import gzip
import os.path
import pandas as pd
import subprocess
import time
import xml.etree.ElementTree as ET
import matplotlib.pyplot as plt
import numpy as np
from math import radians, cos, sin, asin, sqrt
from datetime import timezone
import datetime
from matplotlib import dates
import pytz
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()


root = ET.parse('airspacedata.xml') #for MFX location info

MLlist=[]
listOfTMAs=[]

dictionaryForUAIDandATA={}

timeBinAndFlightAmount={} #timeBin: num of flights

ATASTA0mins={} #timeBin: [data1, data2]
ATASTA5mins={}
ATASTA10mins={}
ATASTA15mins={}
ATASTA20mins={}
ATASTA25mins={}

ATAETA0mins={} #timeBin: [data1, data2]
ATAETA5mins={}
ATAETA10mins={}
ATAETA15mins={}
ATAETA20mins={}
ATAETA25mins={}


def stringInsert (source_str, insert_str, pos): #helper function inserting string into a string
    return source_str[:pos]+insert_str+source_str[pos:]

def distanceBetweenLatLong(lat1, lon1, lat2, lon2): #this will be used to find the distance between MFX and the flight location
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    # Radius of earth in kilometers is 6371
    km = 6371* c
    return km

print ("What is the airport name?")
airportName=input().upper()

print ("Which TRACON (clt is Charlotte, n90 is Newark) ?") #can get rid of this either by using USA IFF file or somehow finding a file that tells
#us which tracon is assigned to which airport, something like the xml file Bob gave me with the meter fixes at the beginning
tracon=input().lower()

print ("Which meter fix? (press enter to automatically use the meter fix with the most TBFM data)")
meterFixName=input().upper()
fileMeterFixName=meterFixName
mfxLocation=""

print ("For which date? YYYY/MM/DD format")
inputtedDate=input()




#gets a filepath to use in command, and string of the date to use for the 24 filenames
if ("/" not in inputtedDate):
    inputtedDate=stringInsert(inputtedDate,"/", 4)
    inputtedDate=stringInsert(inputtedDate,"/", 7)
dateOfData=inputtedDate.replace("/","")
splitInputtedDate=inputtedDate.split("/")
dateOfData="tbfm."+dateOfData+"T"
filepath="/home/data/swim/tbfm/"+splitInputtedDate[0]+"/"+splitInputtedDate[1]+"/"+splitInputtedDate[2]+"/"
inputtedDate=inputtedDate.replace("/","")

def findMFXLocation(mfx): #parse airspace xml file to get lat long of given mfx
    for waypoint in root.findall("./SkyViewAIS/Waypoints/Waypoint"):
        try:
            if (mfx==waypoint.find('Identifier').text):
                return (float(waypoint.find('Latitude').text),float(waypoint.find('Longitude').text))
        except:
            continue


def filterTMAtag(tbfmBlock): #just see if tma block has an eta or sta datapoint i.e. is it worth saving
    if ((tbfmBlock.find('apt="'+airportName+'"') != -1) and (tbfmBlock.find("eta_sfx")!=-1 or tbfmBlock.find("sta_sfx")!=-1)):
        listOfTMAs.append(tbfmBlock)

        
        
def get24HoursOfData(): #get all 24 hours of tbfm data based on date first
    x=0 
    while (x<24):
        print (int(x/24*100), end="% ", flush=True)
        if (x<10): #add try catch statements!!!
            filename=dateOfData+"0"+str(x)
        else:
            filename=dateOfData+str(x)
        x+=1
        #have filename, now read into string onto big data /user/rmaini1
        try:
            with gzip.open(filepath+filename+"00Z.xml.gz",'r') as tbfmFile:      
                for line in tbfmFile:        
                    filterTMAtag(line.decode('UTF-8'))
        except:
            pass
    print (100, end="% ", flush=True)
    print ("")

    
    
def findMostPopularMFX(): #construct a dictionary with keys=meter fixes, values=# of occurences
    meterFixDict={} #Returns the meter fix with the most occurrences in the tbfm data
    for item in listOfTMAs:
        if (item[item.find("<mfx>")+5:item.find("</mfx>")] in meterFixDict):
            meterFixDict[item[item.find("<mfx>")+5:item.find("</mfx>")]]+=1
        else:
            meterFixDict[item[item.find("<mfx>")+5:item.find("</mfx>")]]=0
    return max(meterFixDict, key=meterFixDict.get)


def createTBFMDataList(): #list of lists like [UAID, msgTime, ETAorSTA, value], and calls the function for ATA
    dataToConvertToDataFrame=[]
    global dictionaryForUAIDandATA
        
    for item in listOfTMAs:
        boolAdd1DontAdd0=True
        if (item.find("<mfx>"+meterFixName)!=-1): #data must be of the given meter fix

            if (item.find("<eta_sfx>")!=-1 or item.find("<sta_sfx>")!=-1): #so we have an eta reading
                msgTime=convertToEpochTime(item[item.find("msgTime=")+9:item.find('<air')-7])
                UAID=item[item.find('aid="')+5:item.find('dap')-2]
                
                MFX=item[item.find("<mfx>")+5:item.find("</mfx>")]
                
                if (item.find("<eta_sfx>")!=-1):
                    ETAorSTA="ETA."+MFX
                    value=convertToEpochTime(item[item.find("eta_sfx>")+8:item.find("</eta_sfx")-1])
                else:
                    ETAorSTA="STA."+MFX
                    value=convertToEpochTime(item[item.find("sta_sfx>")+8:item.find("</sta_sfx")-1])

                if (UAID+"."+str(int(str(value)[0:6])-1) in dictionaryForUAIDandATA or UAID+"."+str(int(str(value)[0:6])+1) in dictionaryForUAIDandATA or UAID+"."+str(value)[0:6] in dictionaryForUAIDandATA):

                    if (UAID+"."+str(int(str(value)[0:6])-1) in dictionaryForUAIDandATA): 
                        ATA=dictionaryForUAIDandATA.get(UAID+"."+str(int(str(value)[0:6])-1))
                        ataMinusVal=ATA-value
                        if (dictionaryForUAIDandATA.get(UAID+"."+str(int(str(value)[0:6])-1))==9999999):
                            boolAdd1DontAdd0=False
                    if (UAID+"."+str(int(str(value)[0:6])+1) in dictionaryForUAIDandATA): 
                        ATA=dictionaryForUAIDandATA.get(UAID+"."+str(int(str(value)[0:6])+1))
                        ataMinusVal=ATA-value
                        if (dictionaryForUAIDandATA.get(UAID+"."+str(int(str(value)[0:6])+1))==9999999):
                            boolAdd1DontAdd0=False
                    if (UAID+"."+str(int(str(value)[0:6])) in dictionaryForUAIDandATA): 
                        ATA=dictionaryForUAIDandATA.get(UAID+"."+str(int(str(value)[0:6])))
                        ataMinusVal=ATA-value
                        if (dictionaryForUAIDandATA.get(UAID+"."+str(int(str(value)[0:6])))==9999999):
                            boolAdd1DontAdd0=False
                    pass

                else: #find the ATA for this UAID AND TIME BIN
                    ATA=int(findATAforUAID(UAID,value))
                    if (ATA==9999999):
                        boolAdd1DontAdd0=False
                    else:
                        boolAdd1DontAdd0=True
                    ataMinusVal=ATA-value
                    dictionaryForUAIDandATA[UAID+"."+str(value)[0:6]]=ATA #now same UAIDs but different flights should be accounted for, since separated by time now
                if (msgTime<=value and boolAdd1DontAdd0 and msgTime<=ATA and msgTime>ATA-1800):
                    dataToConvertToDataFrame.append([UAID, str(value)[0:6], msgTime, ETAorSTA, ataMinusVal]) #timeBin separator for UAIDs
    return dataToConvertToDataFrame

def convertToEpochTime(dateTimeString): #have to take the complicated string and convert it
    year=dateTimeString[0:4]
    month=dateTimeString[5:7]
    day=dateTimeString[8:10]
    hour=dateTimeString[11:13]
    minute=dateTimeString[14:16]
    second=dateTimeString[17:20]
    timestamp = datetime.datetime(int(year), int(month), int(day), int(hour), int(minute), int(second)).strftime('%s')
    return int(timestamp)-25200 #matches UTC now

def readInIFF(): #open iff file as dataframe 
    #THIS WONT WORK FOR THE FIRST OF EVERY MONTH!!!
    listToTurnIntoDataFrame=[]
    var1=""

    filename2=os.listdir("/home/data/atac/SVDataLocal/IFF/"+tracon+"/"+splitInputtedDate[0]+"/"+splitInputtedDate[0]+splitInputtedDate[1]+"/"+splitInputtedDate[0]+splitInputtedDate[1]+splitInputtedDate[2])[0]

    var2="/home/data/atac/SVDataLocal/IFF/"+tracon+"/"+splitInputtedDate[0]+"/"+splitInputtedDate[0]+splitInputtedDate[1]+"/"+splitInputtedDate[0]+splitInputtedDate[1]+splitInputtedDate[2]+"/"+filename2
    
    #does not account for leap year where last day of february is 29th.
    #would just be better to convert input into datetime, subtract a day, and then convert back into string for this purpose
    theFirstOfEveryMonthAndTheirDateBefore={"0101":"1231","0201":"0131", "0301":"0228", "0401":"0331", "0501":"0430", "0601":"0531", "0701":"0630", "0801":"0731", "0901":"0831", "1001":"0930", "1101":"1031", "1201":"1130"}


    if (splitInputtedDate[1]+splitInputtedDate[2] in theFirstOfEveryMonthAndTheirDateBefore):
        if (splitInputtedDate[1]+splitInputtedDate[2]=="0101"):
            splitInputtedDate[0]=str(int(splitInputtedDate[0])-1)
        filename1=os.listdir("/home/data/atac/SVDataLocal/IFF/"+tracon+"/"+splitInputtedDate[0]+"/"+splitInputtedDate[0]+theFirstOfEveryMonthAndTheirDateBefore.get(splitInputtedDate[1]+splitInputtedDate[2])[0:2]+"/"+splitInputtedDate[0]+theFirstOfEveryMonthAndTheirDateBefore.get(splitInputtedDate[1]+splitInputtedDate[2]))
        var1="/home/data/atac/SVDataLocal/IFF/"+tracon+"/"+splitInputtedDate[0]+"/"+splitInputtedDate[0]+theFirstOfEveryMonthAndTheirDateBefore.get(splitInputtedDate[1]+splitInputtedDate[2])[0:2]+"/"+splitInputtedDate[0]+theFirstOfEveryMonthAndTheirDateBefore.get(splitInputtedDate[1]+splitInputtedDate[2])+"/"+filename1
    ##Code to subtract a day from the date if not the first of every month
    elif (int(splitInputtedDate[2])-1<10):
        filename1=os.listdir("/home/data/atac/SVDataLocal/IFF/"+tracon+"/"+splitInputtedDate[0]+"/"+splitInputtedDate[0]+splitInputtedDate[1]+"/"+splitInputtedDate[0]+splitInputtedDate[1]+"0"+str(int(splitInputtedDate[2])-1))[0]
        var1="/home/data/atac/SVDataLocal/IFF/"+tracon+"/"+splitInputtedDate[0]+"/"+splitInputtedDate[0]+splitInputtedDate[1]+"/"+splitInputtedDate[0]+splitInputtedDate[1]+"0"+str(int(splitInputtedDate[2])-1)+"/"+filename1
    else:
        filename1=os.listdir("/home/data/atac/SVDataLocal/IFF/"+tracon+"/"+splitInputtedDate[0]+"/"+splitInputtedDate[0]+splitInputtedDate[1]+"/"+splitInputtedDate[0]+splitInputtedDate[1]+str(int(splitInputtedDate[2])-1))[0]
        var1="/home/data/atac/SVDataLocal/IFF/"+tracon+"/"+splitInputtedDate[0]+"/"+splitInputtedDate[0]+splitInputtedDate[1]+"/"+splitInputtedDate[0]+splitInputtedDate[1]+splitInputtedDate[2]+"/"+filename1
    ##End of code to subtract a day from date

  
    
    with gzip.open(var1,'r') as iffFile:    #var1  
            for line in iffFile:     
                try:
                    line=line.decode('UTF-8').split(",") #split the csv
                    if (line[0]=="3"): #record type 3 in iff documentation, contains waypoint info
                        listToTurnIntoDataFrame.append( [ line[7],int(float(line[1])),float(line[9]),float(line[10]), float(line[11]) ] )
                except:
                    continue
    with gzip.open(var2,'r') as iffFile:    #var2  
            for line in iffFile:      
                try:
                    line=line.decode('UTF-8').split(",") #split the csv
                    if (line[0]=="3"): #record type 3 in iff documentation, contains waypoint info
                        listToTurnIntoDataFrame.append( [ line[7],int(float(line[1])),float(line[9]),float(line[10]),float(line[11]) ] )
                except:
                    continue
    df = pd.DataFrame(listToTurnIntoDataFrame,columns=['UAID','Time','Latitude','Longitude', 'Altitude']) 
    return df

def findATAforUAID(uaid, timeBin): #go through IFF waypoints for a UAID, find closest point to mfxLocation, return epoch time
    distValueToCompare=9999999
    actualTimeOfArrival=9999999
    df_filtered=iffDataFrame.query('UAID=="'+uaid+'"')
    for row in df_filtered.itertuples(index=True, name='Pandas'): #look through IFF for lat longs
        if (int(timeBin+"0000")+9999>=getattr(row, "Time") or int(timeBin+"9999")+3999>=getattr(row, "Time")):
            distance=distanceBetweenLatLong(getattr(row, "Latitude"), getattr(row, "Longitude"), mfxLocation[0], mfxLocation[1])
            if (distance<distValueToCompare):
                distValueToCompare=distance
                actualTimeOfArrival=getattr(row, 'Time')

    return actualTimeOfArrival
                

def convertToParquet(df, filename): #take the dataframe, turn into parquet
    df.to_parquet(filename+'.parquet')

def transferParquetToBigDataCluster(filename): #get that parquet from personal sherlock directory into big data cluster
    FILENAME1='/home/rmaini1/'
    FILENAME23=filename
    FILENAME4='.parquet'
    get_ipython().system('hdfs dfs -copyFromLocal -f $FILENAME1$FILENAME23$FILENAME4 /user/rmaini1/ ')
    
def fetchingDataFirstTime(): 
    global meterFixName, fileMeterFixName, mfxLocation
    #get the data from sherlock, if need be find the most popular mfx, then create dataframe out of data
    get24HoursOfData()
    if (meterFixName==""):
        meterFixName=findMostPopularMFX() 
        fileMeterFixName=""
    mfxLocation=findMFXLocation(meterFixName)#tuple (lat, long)
    df = pd.DataFrame(createTBFMDataList(),columns=['UAID', 'TimeBin', 'TimeOfReport','ETAorSTAandMFX','TimeForController'])
    return df

def createDataframeWithTraconInfo(): #makes dataframe to become parquet, [UAID, timeBin, MFX, ATA, rwyThresh, distFlown-distToApt, rwyName] 

    listOfDataframeData=[]
    
    for item in dictionaryForUAIDandATA:

        UAID=item[:item.find(".")]
        timeBin=item[item.find(".")+1:]
        MFX=meterFixName
        ATA=dictionaryForUAIDandATA.get(item)
        rwyThresh=0
        minimumAltitude=99999
        if (ATA != 9999999):
            df_filtered=iffDataFrame.query('UAID=="'+UAID+'"')
            for row in df_filtered.itertuples(index=True, name='Pandas'): #look through IFF for final time reading
                if (int(timeBin+"0000")+9999>=getattr(row, "Time") or int(timeBin+"9999")+3999>=getattr(row, "Time")):
                    if (getattr(row,"Altitude")<=minimumAltitude):
                        minimumAltitude=getattr(row,"Altitude")
                        rwyThresh=getattr(row, 'Time')

            distFlown=calculateMFXtoRWYDistanceTraveled(UAID,timeBin, ATA)
            if (distFlown!=0):
                listOfDataframeData.append([UAID, timeBin, MFX, ATA, rwyThresh, distFlown]) #rwyName])

    df = pd.DataFrame(listOfDataframeData,columns=['UAID', 'TimeBin', 'MFX','ATA','RunwayThreshold','DistanceFlown']) #landing runway
    return df

def perUAIDPlotting(UAID,ATA,timeBin): #pass in uaid, ata, timebin. Use UAID and timebin to find tbfm data to plot

    tbfmDataFrame=tbfmParquet.query('UAID=="'+UAID+'"')
    staXVals=[]
    staXValsDATETIME=[]
    staYVals=[]
    etaXVals=[]
    etaXValsDATETIME=[]
    etaYVals=[]

    if (timeBin not in ATASTA0mins): #oh God this is disgusting. Fix later
            ATASTA0mins[timeBin]=[]
    if (timeBin not in ATASTA5mins):
            ATASTA5mins[timeBin]=[]
    if (timeBin not in ATASTA10mins):
            ATASTA10mins[timeBin]=[]
    if (timeBin not in ATASTA15mins):
            ATASTA15mins[timeBin]=[]
    if (timeBin not in ATASTA20mins):
            ATASTA20mins[timeBin]=[]
    if (timeBin not in ATASTA25mins):
            ATASTA25mins[timeBin]=[]

    if (timeBin not in ATAETA0mins):
            ATAETA0mins[timeBin]=[]
    if (timeBin not in ATAETA5mins):
            ATAETA5mins[timeBin]=[]
    if (timeBin not in ATAETA10mins):
            ATAETA10mins[timeBin]=[]
    if (timeBin not in ATAETA15mins):
            ATAETA15mins[timeBin]=[]
    if (timeBin not in ATAETA20mins):
            ATAETA20mins[timeBin]=[]
    if (timeBin not in ATAETA25mins):
            ATAETA25mins[timeBin]=[]


    for row in tbfmDataFrame.itertuples(index=True, name='Pandas'): #look through IFF for lat longs
        if (int(timeBin)+1>=int(getattr(row, "TimeBin")) and int(timeBin)-1<=int(getattr(row, "TimeBin"))):
            dateTimeReportTime=datetime.datetime.fromtimestamp(getattr(row, "TimeOfReport"), pytz.utc)
            if (getattr(row, "ETAorSTAandMFX")[:getattr(row, "ETAorSTAandMFX").find(".")]=="ETA"):
                etaXVals.append(getattr(row, "TimeOfReport"))
                etaXValsDATETIME.append(dateTimeReportTime)
                etaYVals.append(abs(getattr(row, "TimeForController"))/60)
            if (getattr(row, "ETAorSTAandMFX")[:getattr(row, "ETAorSTAandMFX").find(".")]=="STA"):
                staXVals.append(getattr(row, "TimeOfReport"))
                staXValsDATETIME.append(dateTimeReportTime)
                staYVals.append(abs(getattr(row, "TimeForController"))/60)
    
    if (len(etaXVals)>0 and len(staXVals)>0 and len(etaYVals)>0 and len(staYVals)>0):
        plt.plot(staXValsDATETIME, staYVals, label = "ATA-STA", linestyle='-', marker='o', color='b')
        plt.plot(etaXValsDATETIME, etaYVals, label = "ATA-ETA", linestyle='-', marker='x', color='r')


        plt.axvline(datetime.datetime.fromtimestamp(ATA, pytz.utc),0,1, color='purple', label="Actual Time of Arrival: "+datetime.datetime.fromtimestamp(ATA, pytz.utc).strftime("%m/%d %H:%M"),linestyle="dotted")
        
        plt.xlabel('Time')
        plt.ylabel('Error (minutes)')
        plt.title('TBFM for '+UAID)
        plt.legend()

        plt.gcf().autofmt_xdate()
        plt.gca().xaxis.set_major_formatter(dates.DateFormatter('%m/%d %H:%M:%S'))
     
        plt.show()
        plt.clf()
        
        try:
            ATASTA0mins[timeBin].append(np.interp(ATA, staXVals, staYVals))
        except ValueError:
            pass
        try:
            ATASTA5mins[timeBin].append(np.interp(ATA-300, staXVals, staYVals))
        except ValueError:
            pass
        try:
            ATASTA10mins[timeBin].append(np.interp(ATA-600, staXVals, staYVals))
        except ValueError:
            pass
        try:
            ATASTA15mins[timeBin].append(np.interp(ATA-900, staXVals, staYVals))
        except ValueError:
            pass
        try:
            ATASTA20mins[timeBin].append(np.interp(ATA-1200, staXVals, staYVals))
        except ValueError:
            pass
        try:
            ATASTA25mins[timeBin].append(np.interp(ATA-1500, staXVals, staYVals))
        except ValueError:
            pass

        try:
            ATAETA0mins[timeBin].append(np.interp(ATA, etaXVals, etaYVals))
        except ValueError:
            pass
        try:
            ATAETA5mins[timeBin].append(np.interp(ATA-300, etaXVals, etaYVals))
        except ValueError:
            pass
        try:
            ATAETA10mins[timeBin].append(np.interp(ATA-600, etaXVals, etaYVals))
        except ValueError:
            pass
        try:
            ATAETA15mins[timeBin].append(np.interp(ATA-900, etaXVals, etaYVals))
        except ValueError:
            pass
        try:
            ATAETA20mins[timeBin].append(np.interp(ATA-1200, etaXVals, etaYVals))
        except ValueError:
            pass
        try:
            ATAETA25mins[timeBin].append(np.interp(ATA-1500, etaXVals, etaYVals))
        except ValueError:
            pass
                

def perTimeBinBoxPlot(timeBin):
    labels=["0 (ETA)","0 (STA)","5 (ETA)","5 (STA)","10 (ETA)","10 (STA)","15 (ETA)","15 (STA)","20 (ETA)","20 (STA)","25 (ETA)","25 (STA)"]
    boxList=[ATAETA0mins.get(timeBin), ATASTA0mins.get(timeBin), ATAETA5mins.get(timeBin), ATASTA5mins.get(timeBin), ATAETA10mins.get(timeBin), ATASTA10mins.get(timeBin),ATAETA15mins.get(timeBin), ATASTA15mins.get(timeBin), ATAETA20mins.get(timeBin), ATASTA20mins.get(timeBin), ATAETA25mins.get(timeBin), ATASTA25mins.get(timeBin)]
    bplot=plt.boxplot(boxList, vert=True, patch_artist=True, labels=labels)
    
    x=0
    for box in bplot['boxes']:
        if (x%2==0):
            box.set(color='red', linewidth=2)
        else:
            box.set(color='blue', linewidth=2)
        x+=1

    plt.xticks(rotation=45)
    timeBinEnd=datetime.datetime.fromtimestamp(int(timeBin+"0000")+9999, pytz.utc).strftime("%m/%d %H:%M")
    timeBinStart=datetime.datetime.fromtimestamp(int(timeBin+"0000"), pytz.utc).strftime("%m/%d %H:%M")
    plt.title("Data from: "+timeBinStart+" to "+timeBinEnd)
    plt.xlabel('Minutes out from ATA')
    plt.ylabel('Error (minutes)')
    #plt.savefig(str(x)+"->"+str(x+2)+".png")
    plt.show()
    plt.clf()

def calculateMFXtoRWYDistanceTraveled(UAID, timeBin, ATA): #read iff for a UAID and time bin, calculate distance traversed from lat lons
    previousLat=0
    previousLon=0
    distanceTraveled=0
    df_filtered=iffDataFrame.query('UAID=="'+UAID+'"')
    for row in df_filtered.itertuples(index=True, name='Pandas'): #look through IFF for final time reading
        if ((int(timeBin+"0000")+9999>=getattr(row, "Time") or int(timeBin+"9999")+3999>=getattr(row, "Time")) and ATA <=getattr(row, "Time")):
            if (previousLat != 0 and previousLon != 0):
                distanceTraveled+=distanceBetweenLatLong(float(getattr(row, "Latitude")),float(getattr(row, "Longitude")),previousLat,previousLon)
                previousLat=float(getattr(row, "Latitude"))
                previousLon=float(getattr(row, "Longitude"))
            else:
                previousLat=float(getattr(row, "Latitude"))
                previousLon=float(getattr(row, "Longitude"))
                        
    return distanceTraveled

def perTimeBinRunwayBoxPlot(timeBin): #single box and whisker for runway
    traconDataframe=traconParquet.query('TimeBin=="'+timeBin+'"')
    boxData=[]
    for row in traconDataframe.itertuples(index=True, name='Pandas'): #look through IFF for lat longs
        if (int(timeBin)+1>=int(getattr(row, "TimeBin")) and int(timeBin)-1<=int(getattr(row, "TimeBin"))):
            if (getattr(row, "DistanceFlown")!=0):
                boxData.append(getattr(row, "DistanceFlown"))            

    bplot=plt.boxplot(boxData)
    

    timeBinEnd=datetime.datetime.fromtimestamp(int(timeBin+"0000")+9999, pytz.utc).strftime("%m/%d %H:%M")
    timeBinStart=datetime.datetime.fromtimestamp(int(timeBin+"0000"), pytz.utc).strftime("%m/%d %H:%M")
    plt.title("Data from: "+timeBinStart+" to "+timeBinEnd)
    plt.ylabel('Distance Flown (km)')
    #plt.savefig(str(x)+"->"+str(x+2)+".png")
    plt.show()

    try:
        q75trac, q25trac = np.percentile(boxData, [75 ,25])
        iqrTRACON = q75trac - q25trac

        q75atasta, q25atasta = np.percentile(ATASTA25mins.get(timeBin), [75 ,25])
        iqratasta = q75atasta - q25atasta

        q75ataeta, q25ataeta = np.percentile(ATAETA25mins.get(timeBin), [75 ,25])
        iqrataeta = q75ataeta - q25ataeta

        if (iqrTRACON!=0.0):
            MLlist.append([airportName+" "+timeBinStart+"->"+timeBinEnd, iqrTRACON, iqratasta, iqrataeta, statistics.median(ATASTA25mins.get(timeBin)), statistics.median(ATAETA25mins.get(timeBin))])
    except:
        pass
    
    

    #ML list will be
    #['Timebin','TRACON_IQR', 'ATA-STA15_IQR', 'ATA-ETA15_IQR','ATA-STA15_MED', 'ATA-ETA15MED'])
    


    plt.clf()


#THIS IS READING FROM LOCAL HOME DIRECTORY, CHANGE TO HDFS!! MUST FIX!
if (not(os.path.exists('/home/rmaini1/'+airportName+inputtedDate+fileMeterFixName+'tbfm.parquet') and os.path.exists('/home/rmaini1/'+airportName+inputtedDate+fileMeterFixName+'tracon.parquet') and os.path.exists('/home/rmaini1/'+airportName+inputtedDate+fileMeterFixName+'iff.parquet'))): #fetch the data, make the dataframes, output the parquets, visualize
    
    print ("Fetching data from Sherlock", end=" ", flush=True)
    iffDataFrame=readInIFF()
    tbfmDataFrame=fetchingDataFirstTime()
    convertToParquet(tbfmDataFrame,airportName+inputtedDate+fileMeterFixName+"tbfm")
    convertToParquet(createDataframeWithTraconInfo(),airportName+inputtedDate+fileMeterFixName+"tracon")
    convertToParquet(iffDataFrame, airportName+inputtedDate+fileMeterFixName+"iff")
    transferParquetToBigDataCluster(airportName+inputtedDate+fileMeterFixName+"tbfm")
    transferParquetToBigDataCluster(airportName+inputtedDate+fileMeterFixName+"tracon")
    transferParquetToBigDataCluster(airportName+inputtedDate+fileMeterFixName+"iff")


#check if cache already exists
if (os.path.exists('/home/rmaini1/'+airportName+inputtedDate+fileMeterFixName+'tbfm.parquet') and os.path.exists('/home/rmaini1/'+airportName+inputtedDate+fileMeterFixName+'tracon.parquet') and os.path.exists('/home/rmaini1/'+airportName+inputtedDate+fileMeterFixName+'iff.parquet')): #open the parquets (iff, tbfm, tracon info), visualize): #cached
    
    print ("Using cached file") #turn parquet into dataframe here
    iffDataFrame=pd.read_parquet(airportName+inputtedDate+fileMeterFixName+'iff.parquet', engine='pyarrow')
    traconParquet=pd.read_parquet(airportName+inputtedDate+fileMeterFixName+'tracon.parquet', engine='pyarrow')
    tbfmParquet=pd.read_parquet(airportName+inputtedDate+fileMeterFixName+'tbfm.parquet', engine='pyarrow')
    for row in traconDataframe.itertuples(index=True, name='Pandas'): #look through IFF for lat longs 
        perUAIDPlotting(getattr(row, "UAID"),getattr(row, "ATA"),getattr(row, "TimeBin"))
    for item in ATAETA0mins:
        perTimeBinBoxPlot(item)
    for item in ATAETA0mins:
        perTimeBinRunwayBoxPlot(item)

    
    #for use for k means clustering, applicable when comparing airports and want to classify them to see which uses TBFM better. If you are reading this, contact me rahat@virginia.edu, I will set it up that tool for you    
    #df = pd.DataFrame(MLlist,columns=['Timebin','TRACON_IQR', 'ATA-STA25_IQR', 'ATA-ETA25_IQR','ATA-STA25_MED', 'ATA-ETA25MED'])
    #convertToParquet(df, "anExperiment") 
    
    

