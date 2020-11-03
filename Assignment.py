#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import sys

def readDataSets(light_levels,collision_data,flight_call):
    '''
    Reads the input .json files passed as arguments to this method
    '''
    lLevels=pd.read_json(light_levels)
    cData = pd.read_json(collision_data)
    fCall=pd.read_json(flight_call)
    print("Successfully read the data from json files\n")
    return lLevels,cData,fCall
   

def renameDataFrame(fCall,rename_columns):
    '''
    Renaming the Flight_call data according to the document 
    Species -> Genus
	Family -> Species
	Collisions -> Family
	Call -> Flight Call
    ["Genus","Species","Family","Flight","Flight Call","Habitat","Stratum"]
    '''
    if len(list(fCall.columns)) == len(rename_columns):
        fCall.columns=rename_columns
        print("Successfully renamed all the columns")
    else:
        print("Number of source columns and respective rename list lenghts are different\n")
        sys.exit(1)
    return fCall


def filterDataFrames(lLevels,cData,fCall):
    '''
    removes rows containing NaN values
    remove duplicates in lLevels Dataframe
    create Taxanomic Ranking on fCall
    '''
    print("Removing the NaN values\n")
    #Remove rows containing NaN values from all the DataFrames
    lLevels.dropna(subset=lLevels.columns[[0,1]], inplace=True)
    cData.dropna(subset=cData.columns[[0,1,2,3]], inplace=True)
    fCall.dropna(subset=fCall.columns[[0,1,2,3,4,5,6]], inplace=True)

    #Since the LightScore for a certain date should be unique. we should remove duplicates from the lLevels DataFrame
    lLevels.drop_duplicates(subset='Date', keep=False, inplace=True)

    print("Applying taxanomic ranking\n")
    #The Taxonomic Ranking is  Family > Genus > Species. So we will create a separate column in fCall and remove the duplicates in      that
    fCall['tax']=fCall.apply(lambda row: row.Family.lower().strip()+" "+row.Genus.lower().strip()+" "+row.Species.lower().strip(),axis=1)

    #since the Taxonomic name we made is unique, we can remove the duplicates accordingly.
    fCall.drop_duplicates(subset='tax', keep=False, inplace=True)
    print("Dropped Duplicates\n")
    return lLevels,cData,fCall


def mergeDataFrames(lLevels,cData,fCall):
    '''
    This method takes the filtered DataFrames and creates the tabular dataset which can be analyzed for bird collision
    Merge lLevels DataFrame and cData based on the Date column since Date column is unique in lLevles DataFrame
    returns the final dataframe without sorting on date
    '''
    #What this implies is the date and light score of that day when this bird if found.
    df1 = pd.merge(lLevels,cData,on='Date')

    #After merging that we need to merge df1 and fCall DataFrames. To do that we need to find a common column.
    #The common columns available in those DataFrames are Genus and Species.
    #So we will create a column called sName specifying "Specific Name"
    #Furthermore we will delete the Genus and Species columns in df1 DataFrame since it can create duplicate columns
    fCall['sName']=fCall.apply(lambda row: row.Genus.lower().strip()+" "+row.Species.lower().strip(),axis=1)
    df1['sName']=df1.apply(lambda row: row.Genus.lower().strip()+" "+row.Species.lower().strip(),axis=1)
    df1=df1.drop(columns=["Genus","Species"])

    #Now let's merge the fCall and df1 DataFrames
    #This implies the bird data when the bird is found on the particular date
    df2 = pd.merge(df1,fCall,on='sName')

    #We will remove the sName and tax columns we used for merging.
    df2=df2.drop(columns=["sName","tax"])
    return df2

def writeOutput(final_dataframe,output_file):
    '''
    sorts the DataFrame by "Date" and writes the output to a .csv file
    '''
    df2["Date"] = pd.to_datetime(df2["Date"])
    df2 = df2.sort_values(by="Date")
    #Let's send the DataFrame to a .csv file. Yo can open .csv file in Microsoft Excel or import it to MySQL database.
    df2.to_csv(output_file, index = False, header=True)
    print("Successfully written the data to an output file")

def main():
    try:
        light_levels = sys.argv[1]
        collision_data = sys.argv[2]
        flight_call = sys.argv[3]
        output_file = sys.argv[4]
        #read the input Datasets
        lLevels,cData,fCall = readDataSets(light_levels=light_levels,
                                            collision_data=collision_data,
                                            flight_call=flight_call)
        #Rename the datasets as per requirement 
        fCall = renameDataFrame(fCall=fCall,
                                rename_columns=["Genus","Species","Family","Flight","Flight Call","Habitat","Stratum"])
        #filter the data
        lLevels,cData,fCall = filterDataFrames(lLevels=lLevels,
                                                cData=cData,
                                                fCall=fCall)
        #identify the common columns and merge dataframes
        final_dataframe = mergeDataFrames(lLevels=lLevels,
                                            cData=cData,
                                            fCall=fCall)
        #write the output to a path in .csv
        writeOutput(final_dataframe=final_dataframe,    
                                output_file=output_file)
    except Exception as e:
        print(e)
        sys.exit(1)

if __name__ == "__main__":
    main()







