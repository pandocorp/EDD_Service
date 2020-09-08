#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
#import boto3
from pandas import read_csv,DataFrame,concat,Series
from datetime import datetime,timedelta
from geopy.distance import geodesic
import holidays
from tensorflow.keras.models import load_model
import os
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import LabelEncoder,MinMaxScaler
from sklearn.ensemble import RandomForestRegressor 
import pickle
import warnings
import operator
from collections import defaultdict
import math
import pickle
import requests
import warnings
warnings.filterwarnings("ignore")


# In[2]:


my_path=os.getcwd()
my_path


# In[3]:


diry='/'
for i,j in enumerate(my_path.split('/')[1:-1]):
    diry=diry+str(j)
    if i!=len( my_path.split('/')[1:-1] )-1:
        diry=diry+'/'


# In[4]:


data_path=os.path.join(diry,'Data_Lane_VT_Monthly')
support_file_path=os.path.join(diry,'EDD_API_Dev','SupportFiles')
processed_datafile_path=os.path.join(diry,'EDD_API_Dev','DataFiles')
model_file_path=os.path.join(diry,'EDD_API_Dev','ModelFiles')
model_out_path=os.path.join(diry,'EDD_API_Dev','OutputFiles') 

working_columns=['Quantity',
'Arrival Breached At',
'Vehicle Type',
'contract_start_date',
'contract_end_date',
'Loading End Date',
'Loading Start Date',
'Indent ID',
'Source_Depot_City',
'Arrived At',
'Dispatched At',
'sla_delay_charges',
'contract_source',
'depot_lat_long',
'Gross Weight',
'Transit Time',
'Delivery Date',
'Customer',
'contract_destination',
'contract_type',
'Transporter',
#'Created Date',
'Base Freight',
'consignee_lat_long',
'Actual Freight',
'distance',
'carton_damage_charges',
'damage_charges',
'consignee_pincode',
'contract_id',
'Destination',
'Indent Type',
'contract_validity',
'Gross Volume',
'shortage_charges']


# In[ ]:





# In[5]:


class DataCorrectImputeUpdate:   
    
    def data_pull():
        df_history=DataFrame()
        start='2019-01-01'
        updated_time=(datetime.strptime(start,'%Y-%m-%d'))
        now = datetime.now()
        today_date=(pd.to_datetime(now.strftime("%Y-%m-%d %H:%M:%S")))
        last_month=datetime.strptime(str(today_date.year)+'-'+str(today_date.month)+'-01','%Y-%m-%d')

        while updated_time <last_month: #pd.to_datetime(datetime.now().strftime("%Y-%m-%d")):


            if (updated_time.month==1):
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(31-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(31-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(31-1)




            if (updated_time.month==2) and (updated_time.year%4==0) :
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(29-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(29-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(29-1)

            if (updated_time.month==2) and (updated_time.year%4!=0) :
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(28-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(28-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(28-1)


            if (updated_time.month==3):
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(31-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(31-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(31-1)



            if (updated_time.month==4):
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(30-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(30-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(30-1)



            if (updated_time.month==5):
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(31-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(31-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(31-1)




            if (updated_time.month==6):
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(30-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(30-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(30-1)



            if (updated_time.month==7):
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(31-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(31-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(31-1)




            if (updated_time.month==8):
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(31-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(31-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(31-1)





            if (updated_time.month==9):
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(30-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(30-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(30-1)




            if (updated_time.month==10):
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(31-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(31-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(31-1)




            if (updated_time.month==11):
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(30-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(30-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(30-1)




            if (updated_time.month==12):
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(31-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(31-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(31-1)
                


            dfj = None

            try:
                dfj = pd.read_json(json_out_file, convert_dates=True)[working_columns]
            except ValueError:
                print(json_out_file)
                try:
                    dataset_url_check= 'http://35.154.192.224:8090/lane_vt_report?from_date=2019-07-01&to_date=2019-07-02'
                    response_check = requests.post(dataset_url_check, data={},timeout=20)
                    response = requests.post(dataset_url, data={})
                    if response.status_code == 200:
                        try:
                            #save Dataset to json
                            file = open(json_out_file, "w")
                            file.write(response.text) 
                            file.close() 
                        except OSError as err:
                            print("OS error: {0}".format(err))

                        #read json into dataframe
                        try:
                            dfj = pd.read_json(json_out_file, convert_dates=True)[working_columns]
                        
                        except ValueError:
                            print ('File not found:', json_out_file)
                            
                except:
                    print('Database Connection Error: Unable To Write New Data')

            
            df_history=df_history.append(dfj)


            updated_time=updated_time+timedelta(1)


        return df_history
        
        

    def Customer_name_correction(df):
        
        CustomerMap = {'Signify Innovations India Limited':'Signify',
                       'Marico Limited':'Marico',
                       'Orient Electric Ltd':'OEL'}
        df['Customer'] = df['Customer'].apply(lambda x: CustomerMap[x] if x in CustomerMap.keys() else x)
        df['Customer'] = df['Customer'].apply(lambda x: x.strip().capitalize())

        return df
    
    def Transporter_name_correction(df):
        df=df.dropna(subset=['Transporter'])
        
        TransMap = {'A.D. ROADLINES (INDIA) REGD.': 'A.D. Roadlines (INDIA) Regd',
                    'ABIZER CARRIERS':'Abizer Carriers',
                    'ANAND ROADLINES':'ANAND ROAD LINES',
                    'Anand Roadlines':'ANAND ROAD LINES',
                    'GATI-KINTETSU EXPRESS PVT LTD':'GATI KINTETSU EXPRESS',
                    'KAPOOR FREIGHT CARRIERS PVT LTD':'Kapoor Freight Carrier Pvt. Ltd',
                    'Om Logistics Limited':'OM Logistics',
                    'Om Logistics Ltd':'OM Logistics',
                    'OTS LIMITED':'OTS Limited',
                    'PANDEY ROAD LINES':'Pandey Roadlines',
                    'RCI LOGISTICS PVT LTD':'RCI Logistics Pvt Ltd',
                    'RIVIGO SERVICES PRIVATE LIMITED':'Rivigo Services Private Limited',
                    'SD CARGO PVT. LTD.':'SD CARGO PVT LTD',
                    'Sri Ramadas Motor Transpo':'Sri Ramadas Motor Transport Limited',
                    'STELLAR INNOVATIVE TRANSPORTATION':'Stellar Innovative Transportation',
                    'WHEELSEYE TECHNOLOGY INDIA PRIVATE LIMITED':'WHEELSEYE TECHNOLOGY INDIA PVT',
                    'ZINKA LOGISTICS SOLUTIONS PRIVATE':'ZINKA LOGISTICS SOLUTIONS',
                    'MIDDLETON LOGISTIC SOLUTIONS':'MIDDLETON LOGISTICS SOLUTIONS',
                    'SPOTON LOGISTICS PRIVATE LIMITED':'Spoton logistics Private Limited',
                    'Sri Ramadas Motor Transpo':'Sri Ramadas Motor Transport Limited',
                    'Sri Ramadas Motor Transport Ltd':'Sri Ramadas Motor Transport Limited'
        }
        
        
        df['Transporter'] = df['Transporter'].apply(lambda x: TransMap[x] if x in TransMap.keys() else x)
        df['Transporter'] = df['Transporter'].apply(lambda x: x.strip().capitalize())

        return df
        
        
    def sourcemap(df):    
        SourceMap = {
        'Guwahati (t)': 'Guwahati',
        'Jaipur-rajasthan': 'Jaipur',
        'Mil sc -ameya food': 'Coimbatore',
        'Mil- guwahati plant' : 'Guwahati',    
        'Mil- jalgaon plants' : 'Jalgaon',
        'Mil- kanjikode plant' : 'Kanjikode',
        'Mil- pondy plants' : 'Puducherry',
        'Tirumala-hyderabad' : 'Tirumala',
        'R598-coimbatore' : 'Coimbatore',
        'Snqz - khopoli' : 'Khopoli',
        'Snrq' : 'Coimbatore',
        'Jaipur-Rajasthan': 'Jaipur'
        }

        df['Source_Depot_City'] = df['Source_Depot_City'].apply(lambda x: x.strip().capitalize())
        df['Source_Depot_City'] = df['Source_Depot_City'].apply(lambda x: x.replace(',', '_'))
        df['Source'] = df['Source_Depot_City'].apply(lambda x: SourceMap[x] if x in SourceMap.keys() else x)
        df=df.drop(['Source_Depot_City'],1)
        return df

    def destmap(df):    
        DestMap = {
        'Balaosre': 'Balasore',
        'Zirakhpur': 'Zirakpur',
        'Dayalpura sodhian': 'Zirakpur',
        'Mubarikpur camp': 'Zirakpur',
        'Bhubaneswar': 'Bhubaneshwar',
        'Burdge town': 'Midnapore',
        'Chhoto mathkatpur': 'Kharagpur',
        'Vijayawada': 'Vijaywada',
        'Una-himachal pradesh': 'Una',
        'Guwahati (t)' : 'Guwahati',
        'Panskura town' : 'Panskura',
        'Rampura phul' : 'Kapurthala',
        'Raipur pachimbar': 'Contai',
        'Edapalayam , chennai': 'Chennai',
        'Kappalur , madurai': 'Madurai',
        'T c balam, vanur taluk': 'Puducherry',
        'Annur taluk, coimbatore': 'Coimbatore',
        'Renigunta mandal, chittoor dt': 'Renigunta',
        'Thane - bhiwandi.' : 'Thane',
        'Ramji mandir chowk': 'Sambalpur',
        'Village daowra (hawrah)': 'Amta',
        'Virar east, dist - palghar': 'Virar',
        'Sankrail, nh-6, howrah': 'Sankrail',
        'Quthbullapur, ida jeedimelta': 'Secunderabad',
        'Kanjikuzhy_ kottayam': 'kottayam',
        'Bestan,surat,gujarat': 'Surat',
        'Viralimalai (pudukkotai dt.)': 'Viralimalai',
        'Paramathi velur-(po), namakkal-(dis': 'Namakkal',
        'Valliyoor (kkdt)': 'Valliyoor',
        'Ongole , prakasam': 'Ongole',
        'Pallipalayam , namakkal': 'Namakkal',
        'Sheoganj dist-sirohi': 'Sirohi',
        'Barani, bhagalpur': 'Bhagalpur',
        'Pipraich ((gorakhpur)': 'Gorakhpur',
        'Titlagarh dist-bolangir': 'Balangir',
        'Ramnagar, varanasi': 'Varanasi',
        'Fhullbari -siliguri': 'Siliguri',
        'Mavelikara , kerala': 'Mavelikara',
        'Langtlai-mizoram': 'Langtlai',
        'Kavundampalayam_ coimbatore': 'Coimbatore',
        '24 pgs (north)': 'Habra',
        '24 parganas(south)': 'Dakshin Barasat',
        'Chengannur_ kerala': 'Chengannur',
        '846004drabhanga': 'Darbhanga',
        "'jhajjar":'Jhajjar',
        'Allahaabad':'Allahabad',
        'Allahabad (fafamau)':'Allahabad',
        "'karimganj":'Karimganj'}
        
        df['Destination'] = df['Destination'].apply(lambda x: x.strip().capitalize())
        df['Destination'] = df['Destination'].apply(lambda x: DestMap[x] if x in DestMap.keys() else x)
        df['Destination'] = df['Destination'].apply(lambda x: x.replace(',', '_'))
        df = df[~df['Destination'].str.contains('^\d+')]  # drop 110006 etc 
        return df

    
    def imp_dist(df):
        dfj=df.copy()
        dfj=dfj[(dfj.Customer!='Sandbox')]
        dfj['distance']=dfj['distance'].apply(lambda x: np.nan if x<=0 else x)
        dfj['distance']=dfj['distance'].fillna(0)
        dfj['distance']=dfj.apply(lambda x:int(geodesic((x['Source Lat'],x['Source Long']),
                                                    (x['Dest Lat'],x['Dest Long'])).km) if x['distance']==0 else int(x['distance']) ,axis=1)

        return dfj 

    
    def lat_lon_prep(df):
    
        df.dropna(subset = ["depot_lat_long"], inplace=True)
 
        df.dropna(subset = ["consignee_lat_long"], inplace=True)

        df['Source Lat'] = df['depot_lat_long'].apply (lambda x: float(x.split(',')[0]) if float(x.split(',')[0])!=0 else np.nan)  
        df['Source Long'] = df['depot_lat_long'].apply (lambda x: float(x.split(',')[1])if float(x.split(',')[0])!=0 else np.nan)
        df['Dest Lat'] = df['consignee_lat_long'].apply (lambda x: float(x.split(',')[0])if float(x.split(',')[0])!=0 else np.nan)
        df['Dest Long'] = df['consignee_lat_long'].apply (lambda x: float(x.split(',')[1])if float(x.split(',')[0])!=0 else np.nan)
        
        df = df.drop(['depot_lat_long', 'consignee_lat_long'], axis='columns')
        df.dropna(subset=['Source Lat'])
        df.dropna(subset=['Source Long'])
        df.dropna(subset=['Dest Lat'])
        df.dropna(subset=['Dest Long'])
        df=df[(df['Source Lat'] > 8)&
                (df['Source Lat'] <32)&
                (df['Source Long']> 67)&
                (df['Source Long']<97)&
                (df['Dest Lat']>8)&
                (df['Dest Lat']<32)&
                (df['Dest Long']>67)&
                (df['Dest Long']<97)]
        df=df.drop_duplicates()
        df=df.reset_index().drop(['index'],1)
        return df

    def date_format(date):

        try:
            date_time=datetime.strptime(date.split('.')[0],'%Y-%m-%d %H:%M:%S' )
        except:
            pass
        try:
            date_time=datetime.strptime(date.split('+')[0],'%Y-%m-%d %H:%M:%S' )
        except:
            pass

        return date_time#+timedelta(hours=5.5)    
    



    def update_hist_data(hist_data):
        data_from=str(max((hist_data['Loading Start Date'].apply(lambda x:DataCorrectImputeUpdate.date_format(x))))+timedelta(1)).split(' ')[0]
        print('Fetching Data\nFrom:',data_from)
        now = datetime.now()
        today_date=(pd.to_datetime(now.strftime("%Y-%m-%d %H:%M:%S")))
        data_to=str(today_date-timedelta(1)).split(' ')[0]

        print('To:' ,data_to)
        c=-3
        try:
            dataset_url= 'http://35.154.192.224:8090/lane_vt_report?from_date=2019-07-01&to_date=2019-07-02'
            response = requests.post(dataset_url, data={},timeout=20)
        except:
            c=-2
            
        if c!=-2:
            try:
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(data_from,data_to)
                print(dataset_url)
                response = requests.post(dataset_url, data={})
            except:
                c=-1
                print('Database Connection Error: Unable To Update the Data')
            if c!=-1:
                try:
                    dfj=DataFrame(response.json())[working_columns]

                except:
                    c=0
                    print('Empty dataframe from Database')
                if c!=0:
                    try:
                        dfj=DataCorrectImputeUpdate.sourcemap(dfj)
                    except:
                        c=1
                        print('Correction Error: Source Map')
                    if c!=1:
                        try:
                            dfj=DataCorrectImputeUpdate.destmap(dfj)
                        except:
                            c=2
                            print('Correction Error: Destination Map')
                        if c!=2:
                            try:
                                dfj=DataCorrectImputeUpdate.Customer_name_correction(dfj) 
                            except:
                                c=3
                                print('Correction Error: Customer Name ')
                            if c!=3:
                                try:
                                    dfj=DataCorrectImputeUpdate.Transporter_name_correction(dfj)

                                except:
                                    c=4
                                    print('Correction Error: Transporter Name ')
                                if c!=4:
                                    try:
                                        dfj=DataCorrectImputeUpdate.lat_lon_prep(dfj)
                                        
                                        hist_data=hist_data.append(dfj)
                                        c=5
                                    except:
                                        print('Correction Error: Lat Long')

                                    
                                    
        if c==5:
            print('Data Sucessfully Updated')
        if c!=5:
            print('Unable to Update the Data')#raise ValueError('Unable to Update Data')
            

                            
                

        return hist_data
    
class plots:
    
    def dens_box_plot(df1):
        for i, col in enumerate(df1):
            plt.figure(i)
            sns.distplot(df1[col], kde=True, color="b")
            #plt.savefig('DensityPlot_{}'.format(col))
            plt.show()
            sns.boxplot(df1[col], color="b")
            #plt.savefig('BoxPlot_{}'.format(col))
            print (df1.describe()[col])
    def Heat_Map(df1) :
        correlations = df1.corr()
        plt.figure(figsize=(10,10))
        sns.heatmap(round(correlations,2),vmin=-1,cmap='coolwarm',annot=True,)
        


# In[ ]:





# In[6]:


## write monthly data in Data_Lane_VT_Monthly Folder

hist_data=DataCorrectImputeUpdate.data_pull()

## check for availability of history data, if available update it else work with old data
try:
    hist_data=pd.read_csv(os.path.join(processed_datafile_path,'history_data.csv'))
    hist_data=DataCorrectImputeUpdate.update_hist_data(hist_data)
    hist_data.to_csv(os.path.join(processed_datafile_path,'history_data.csv'),index=False)
except:
    dfj=DataCorrectImputeUpdate.sourcemap(hist_data)
    dfj=DataCorrectImputeUpdate.destmap(dfj)
    dfj=DataCorrectImputeUpdate.Customer_name_correction(dfj)   
    dfj=DataCorrectImputeUpdate.Transporter_name_correction(dfj)
    dfj=DataCorrectImputeUpdate.lat_lon_prep(dfj)
    dfj=DataCorrectImputeUpdate.imp_dist(dfj)
    dfj.to_csv(os.path.join(processed_datafile_path,'history_data.csv'),index=False)
    hist_data=pd.read_csv(os.path.join(processed_datafile_path,'history_data.csv'))
    hist_data=DataCorrectImputeUpdate.update_hist_data(hist_data)
    hist_data.to_csv(os.path.join(processed_datafile_path,'history_data.csv'),index=False)
    


# In[7]:


hist_data['index']=Series(list(    range(0,len(hist_data))   )).apply(lambda x:str(x))


# In[8]:


hist_data['Indent ID']=hist_data['Indent ID']+hist_data['index']
hist_data=hist_data.drop(['index'],1)


# In[9]:


# data_from='2020-08-02'
# data_to='2020-08-22'
# dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(data_from,data_to)
# print(dataset_url)
# requests.post(dataset_url, data={})


# In[10]:


# dataset_url= 'http://35.154.192.224:8090/lane_vt_report?from_date=2019-07-01&to_date=2019-07-02'
# response = requests.post(dataset_url, data={},timeout=20)


# In[11]:


# os.path.join(processed_datafile_path,'history_data.csv')


# In[12]:


hist_data.columns


#  ### Feature Creation for Machine Learning Model

# In[13]:


class Common_Features:
    
    def correctdtypes(df):
        for i in df.columns:
            if operator.contains(i,'month') or operator.contains(i,'weekday') or operator.contains(i,'hour')or operator.contains(i,'day') or operator.contains(i,'year'):
                df[i+'_continous']=df[i].astype('int32')
                df[i]=df[i].astype(str)
            if operator.contains(i,'Target') or operator.contains(i,'distance') or operator.contains(i,'Transit Time'):
                df[i]=df[i].astype('int32')
            if operator.contains(i,'Lat') or operator.contains(i,'Long'):
                df[i]=df[i].astype('float32')

        return df



    def hdays(df,column_name):
        holiday_date=[]

        holis = holidays.CountryHoliday('IND')
        for i,j in holis.items():
             holiday_date.append(i)
        list_holidays=Series([holis.get(i)  for i in df[column_name]])
        list_holidays=Series([i if i !=None else 'Normal' for i in list_holidays])
        df['holi']=list_holidays
        return df


    def row_to_col(df,column_name):
        list_df=[]
        df_dict={}
        for i in range(len(df.columns)):
            for i in df.iloc[:,i].values:
                list_df.append(i)
        df_dict[column_name]=list_df
        return pd.DataFrame(df_dict)

    def calender_days(df,input_column_name,output_column_name):
        df['{}month'.format(output_column_name)]=df[input_column_name].dt.month
        df['{}year'.format(output_column_name)]=df[input_column_name].dt.year
        df['{}hour'.format(output_column_name)]=df[input_column_name].dt.hour
        df['{}weekday'.format(output_column_name)]=df[input_column_name].dt.weekday
        df['{}day'.format(output_column_name)]=df[input_column_name].dt.day
        return df

    def lag_var(data, n_in=1, n_out=1, dropnan=True):
        n_vars = 1 if type(data) is list else data.shape[1]
        columns_df=data.columns
        df = DataFrame(data)
        cols, names = list(), list()

        for i in (range(n_in, 0, -1)):
            cols.append(df.shift(i))
            names += [(k+'_var(t-%d)' % (i)) for j,k in zip(range(n_vars),columns_df)]

        agg = concat(cols, axis=1)
        agg.columns = names

        if dropnan:
            agg.dropna(inplace=True)
        return agg


    
    
class EDD:
    
    def select_columns(hist_data):
        df_data=hist_data.drop(['Quantity',
                          #'Arrival Breached At',
                          #'Vehicle Type',
                          'contract_start_date',
                          'contract_end_date',
                          #'Indent ID',
                          #'Arrived At',
                          #'Dispatched At',
                          #'sla_delay_charges',
                          'contract_source',
                          #'Source Lat',
                          #'Source Long',
                          #'Dest Lat',
                          #'Dest Long',
                          #'Gross Weight',
                          #'Transit Time',
                          #'Delivery Date',
                          #'Customer',
                          'contract_destination',
                          #'contract_type',
                          #'Transporter',
                          #'Created Date',
                          #'Base Freight',
                          #'Actual Freight',
                          #'distance',
                          'carton_damage_charges',
                          'damage_charges',
                          'consignee_pincode',
                          'contract_id',
                          #'Destination',
                          #'Indent Type',
                          #'Source'
                          'contract_validity',
                          'Gross Volume',
                          'shortage_charges'],1)


        return df_data
        

       
    def data_prep_ml(df_history):
       
        df_history['Arrival Breached At']=df_history['Arrival Breached At'].fillna(1)
        df_history['Placement_Status']=df_history['Arrival Breached At'].apply(lambda x : 0 if x!=1 else x)
        df_history=df_history.drop(['Arrival Breached At'],1)
         
 
        ## Target Variable
        df_history=df_history.drop(['Transit Time'],1) 
                       
            
        df_history= df_history[df_history['Transporter'] != 'Dummy']
       
        df_history=df_history[~df_history.isna().any(axis=1)]
         
         
        df_history['Transporter']=df_history['Transporter'].apply(lambda x: x.strip().capitalize())
      
        df_history = df_history[~df_history['Destination'].str.contains('^\d+')]
        
        df_history=df_history[(df_history.iloc[:, 0:] != 'NaT').all(axis=1)]
        
        for j in ['Dispatched At','Delivery Date','Arrived At','Loading Start Date','Loading End Date'] :
            try:
                df_history[j]=df_history[j].apply(lambda x:DataCorrectImputeUpdate.date_format(x))
            except:
                pass
            
        df_history['Target']=(df_history['Delivery Date']-df_history['Dispatched At']).dt.total_seconds()/60
        df_history=df_history[df_history.Target>0]
        print(min(df_history.Target))
#         Create input features

        ## Loadting time
#         df_history['Loading_Time']=(df_history['Loading End Date']-df_history['Loading Start Date']).dt.total_seconds()/60
    
 
        ## Indent Count total
        Total_indent_count=df_history.groupby(['Indent ID','Transporter']).count().reset_index().groupby(['Transporter']).count().reset_index()[['Transporter','Indent ID']]
        Total_indent_count.columns=['Transporter','Total_Indent_Count']

        ## Indent Count by route
        Indent_count_by_route=df_history.groupby(['Indent ID','Transporter','Source', 'Destination']).count().reset_index().groupby(['Transporter','Source', 'Destination']).count().reset_index()[['Transporter','Source', 'Destination','Indent ID']]
        Indent_count_by_route.columns=['Transporter','Source', 'Destination','Count_by_route']

        df_history=pd.merge(df_history,Total_indent_count, how='inner', on=None, left_on='Transporter', right_on='Transporter',
                 left_index=False, right_index=False, sort=False,
                 suffixes=('_x', '_y'), copy=True, indicator=False,
                 validate=None) 

        df_history=pd.merge(df_history,Indent_count_by_route, how='inner', on=None, left_on=['Transporter','Source', 'Destination'], right_on=['Transporter','Source', 'Destination'],
                 left_index=False, right_index=False, sort=False,
                 suffixes=('_x', '_y'), copy=True, indicator=False,
                 validate=None) 
        ## Special days
        df_history=Common_Features.hdays(df_history,'Loading Start Date')
        df_history=Common_Features.hdays(df_history,'Dispatched At')
        df_history=Common_Features.hdays(df_history,'Delivery Date')


        ## creating calender days
        df_history=Common_Features.calender_days(df_history,'Loading Start Date','Loading')
        df_history=Common_Features.calender_days(df_history,'Dispatched At','Dispatched')
        df_history=Common_Features.calender_days(df_history,'Delivery Date','Delivery')
        
        ## Dispatchedhour_category

        df_history['LoadingTime']=df_history['Loadinghour'].apply(EDD.func)
        
        
        ## Lat Long 3d cordinates
       
        df_history['src_x'] = df_history.apply(lambda x: math.cos(x['Source Lat'])*math.cos(x['Source Long']),axis=1)
        df_history['src_y'] = df_history.apply(lambda x: math.cos(x['Source Lat'])*math.sin(x['Source Long']),axis=1)
        df_history['src_z'] = df_history.apply(lambda x: math.sin(x['Source Lat']),axis=1)
        
                                               
        df_history['dest_x'] = df_history.apply(lambda x: math.cos(x['Dest Lat'])*math.cos(x['Dest Long']),axis=1)
        df_history['dest_y'] = df_history.apply(lambda x: math.cos(x['Dest Lat'])*math.sin(x['Dest Long']),axis=1)
        df_history['dest_z'] = df_history.apply(lambda x: math.sin(x['Dest Lat']),axis=1)
        

        
        ## variable to capture recency
        try:
            df_history['Loading Start Date']=df_history['Loading Start Date'].apply(lambda x: DataCorrectImputeUpdate.date_format(x))
        except:
            pass
        date_string = "21 January, 2018"
        refrence_date=datetime.strptime(date_string, "%d %B, %Y")
        df_history['creation_age']=round(( df_history['Loading Start Date']-refrence_date ).dt.total_seconds()/(3600*24))
        
        
        ## Vehicle type Count
        dict_count={}
        vc=[]
        tn=[]
        for i in pd.unique(df_history['Transporter']):
            vc.append(len(pd.unique(df_history[df_history['Transporter']==i]['Vehicle Type'])))
            tn.append(i)
        dict_count['vehicle_count']=vc
        dict_count['transporter_name']=tn
        vehicle_count_df=DataFrame(dict_count)
        df_history=pd.merge(df_history,vehicle_count_df, how='inner', on=None, left_on='Transporter', right_on='transporter_name',
                 left_index=False, right_index=False, sort=False,
                 suffixes=('_x', '_y'), copy=True, indicator=False,
                 validate=None).drop(['transporter_name'],1)
        
        return df_history
    

    def func(x):
        if int(x) in list(range(4,9)):
            return 'Early_Morning'
        if int(x) in list(range(9,12)):
            return 'Morning'
        if int(x) in list(range(12,16)):
            return "Afternoon"
        if int(x) in list(range(16,19)):
            return 'evening'
        if int(x) in list(range(19,24)):
            return 'night'
        if int(x) in list(range(0,4)):
            return 'latenight'     

    def data_prep_lag(df_data)   :
        # Lags Target
        df_arranged=df_data.sort_values(by='Loading Start Date')

     
        Target_lags=10

        lags_df=Common_Features.lag_var(df_arranged[['Target']],n_in=Target_lags)
        lags_df=lags_df.reset_index().drop(['index'],1)
   
        df_arranged=df_arranged.iloc[Target_lags:,:]
       
        df_arranged=df_arranged.reset_index().drop(['index'],1)

        df_lagged=pd.concat([df_arranged,lags_df],1).dropna()
     
 
        # Lags transportwise
        new_df=DataFrame()
#         for i in pd.unique(df_lagged['Transporter']):
#             #print(i)
#             df_sample=(df_lagged[df_lagged['Transporter']==i]).sort_values(by='Loading Start Date')

#             ## Auto regressive effect
#             lags=10

#             lags_trans=Common_Features.lag_var(df_sample[['Source','Destination','Vehicle Type','Target','sla_delay_charges','Placement_Status','Deliverymonth', 'Deliveryyear','Deliveryweekday', 'Deliveryhour', 'Deliveryday','Dispatchedmonth','Dispatchedyear','Dispatchedweekday', 'Dispatchedhour', 'Dispatchedday','Dispatched_time_category']],n_in=lags)  
            
#             lags_trans=lags_trans.reset_index().drop(['index'],1)
#             lags_trans.columns=lags_trans.columns+['_transporter_wise']
#             lags_trans=lags_trans.reset_index().drop(['index'],1)

#             df_sample=df_sample.iloc[lags:,:]
#             df_sample=df_sample.reset_index().drop(['index'],1)

#             df_laggedd=pd.concat([df_sample,lags_trans],1).dropna()
#             new_df=new_df.append(df_laggedd)

        return df_lagged#new_df
    
    
    
    def remove_outlier(df_in, col_name):
        q1 = df_in[col_name].quantile(0.25)
        q3 = df_in[col_name].quantile(0.75)
        iqr = q3-q1 #Interquartile range
        fence_low  = q1-1.5*iqr
        fence_high = q3+1.5*iqr
        print ('\nLower Limit :',fence_low, 'Upper Limit :', fence_high)
        df_out_lower = df_in.loc[ df_in[col_name] < fence_low]
        df_out_upper = df_in.loc[ df_in[col_name] > fence_high]


        if (df_out_lower.shape[0] > 0 and df_out_upper.shape[0] >0) :

            return concat([df_out_lower,df_out_upper],0)

        if (df_out_lower.shape[0] >0 and df_out_upper.shape[0] == 0) :
            return df_out_lower

        if (df_out_lower.shape[0]==0 and df_out_upper.shape[0] > 0) :
            return df_out_upper

        else:
            return pd.DataFrame(columns=['Nothing'])


# In[14]:


## Machine Learning Variables Creation for Ontime Delivery Probability 
df_data=EDD.select_columns(hist_data)
hist_data_ml_dlv_duration=EDD.data_prep_ml(df_data)
 
## Vehicle Master
vehicle_master_df=pd.read_excel(os.path.join(support_file_path,'VehicleMaster.xlsx')).drop(['client_id               ',
                                                            'height__do', 'length__do',
                                                            'breadth__do', 'weightage__do',
                                                            'exclude_sku_categories',
                                                            'created_at         '],1)
vehicle_master_df.columns=['Vehicle Type','Vehicle_Load_Type','CFT_capacity','KG_capacity','Customer']

## Add Geodesic distance
vehicle_master_df=DataCorrectImputeUpdate.Customer_name_correction(vehicle_master_df)
df_ml_dlv=pd.merge(hist_data_ml_dlv_duration,vehicle_master_df,how='inner',on=['Vehicle Type','Customer']).dropna()
df_ml_dlv['Geodesic_dist']=df_ml_dlv.apply(lambda x:int(geodesic((x['Source Lat'],x['Source Long']),(x['Dest Lat'],x['Dest Long'])).km),axis=1)
df_ml_dlv=df_ml_dlv[df_ml_dlv['Geodesic_dist']>0]

## select Non Stop journeys
indent_count=df_ml_dlv.groupby('Indent ID').size().reset_index(name='count')

for i in indent_count['count'].value_counts().index:
    if i==1:
        Non_stop_route=indent_count[indent_count['count']==i]
    break
#     if i==2:
#         one_stop_route=indent_count[indent_count['count']==i]
#     if i==3:
#         two_stop_route=indent_count[indent_count['count']==i]
#     if i==4:
#         three_stop_route=indent_count[indent_count['count']==i]
df_ml_dlv=df_ml_dlv[df_ml_dlv['Indent ID'].isin(Non_stop_route['Indent ID'].values)].reset_index().drop(['index'],1) 
 


## Remove outlier overall    
Outlier_df=DataFrame(columns=df_ml_dlv.columns)
a=EDD.remove_outlier(df_ml_dlv,'Target')
Outlier_df=concat([Outlier_df,a],0)

N0_outlier_rows=len(Outlier_df)
print ('Outlier Rows :',N0_outlier_rows,'\nOutlier Data Percentage :', (N0_outlier_rows/len(df_ml_dlv))*100)
print ('Final number of rows available after removing Outliers: ',len(df_ml_dlv)-N0_outlier_rows)


df=pd.merge(df_ml_dlv,Outlier_df,indicator=True,how='outer').query('_merge=="left_only"').drop('_merge', axis=1)
print(df.shape)


## Remove Outliers Lanewise
data_df=DataFrame(columns=df.columns)
for i in df.groupby(['Source','Destination']):
    print('group len:',len(i[1]))
    Outlier_df=DataFrame(columns=i[1].columns)
    a=EDD.remove_outlier(i[1],'Target')
    Outlier_df=concat([Outlier_df,a],0)
    N0_outlier_rows=len(Outlier_df)
    print ('Outlier Rows :',N0_outlier_rows,'\nOutlier Data Percentage :', (N0_outlier_rows/len(i[1]))*100)
    print ('Final number of rows available after removing Outliers: ',len(i[1])-N0_outlier_rows)
    df_OL_free=pd.merge(i[1],Outlier_df,indicator=True,how='outer').query('_merge=="left_only"').drop('_merge', axis=1)
   
    data_df=data_df.append(df_OL_free)

## Take on FTL trucks

data_df=data_df[data_df['Vehicle_Load_Type']=='FTL']

## Remove Outliers distancewise
incremental_start=0
step_size=10
incremental_end=10
stop_point=max(data_df['Geodesic_dist'])
data_df_final=DataFrame(columns=data_df.columns)
while incremental_end < stop_point:
    print(incremental_start,incremental_end)
    grp=data_df[(data_df['Geodesic_dist']>incremental_start)&(data_df['Geodesic_dist']<incremental_end)]
    
    
    if len(grp)>0:
        print('group len:',len(grp))
    
        Outlier_df=DataFrame(columns=grp.columns)
        a=EDD.remove_outlier(grp,'Target')
        Outlier_df=concat([Outlier_df,a],0)
        N0_outlier_rows=len(Outlier_df)
        print ('Outlier Rows :',N0_outlier_rows,'\nOutlier Data Percentage :', (N0_outlier_rows/len(grp))*100)
        print ('Final number of rows available after removing Outliers: ',len(grp)-N0_outlier_rows)
        df_OL_free=pd.merge(grp,Outlier_df,indicator=True,how='outer').query('_merge=="left_only"').drop('_merge', axis=1)

        data_df_final=data_df_final.append(df_OL_free)

        incremental_start=incremental_end
        incremental_end=incremental_end+step_size
    else:
        print('no group found:',len(grp))
        
        incremental_start=incremental_end
        incremental_end=incremental_end+step_size


print(data_df_final.shape)
data_df_final=data_df_final.reset_index().drop(['index','Nothing'],1)
data_df_final=data_df_final[data_df_final.Target>5]
data_df_final=data_df_final.drop(['distance'],1)

 


# In[15]:


#  data_df_final['Loading Start Date']=
# x=data_df_final['Dispatched At']#.apply(lambda x:DataCorrectImputeUpdate.date_format(x))
# y=data_df_final['Loading Start Date']#.apply(lambda x:DataCorrectImputeUpdate.date_format(x))
# z=(x-y).dt.total_seconds()/3600


# In[16]:


# (z[z<0])


# In[17]:


# (z[z>0])


# In[ ]:





# In[19]:


# data_df_final.columns


# In[20]:



data_df_final.to_csv(os.path.join(processed_datafile_path,'hist_data_ml_dlv_duration.csv'),index=False)
data_df_final=pd.read_csv(os.path.join(processed_datafile_path,'hist_data_ml_dlv_duration.csv'))


# In[21]:


data_df_final_=data_df_final[['Indent ID',
                              'Dispatched At',
                              'CFT_capacity',
                              'KG_capacity',
                              'Geodesic_dist',
                              'LoadingTime',
                              'creation_age',
                              'holi',
                              'Dispatchedmonth',
                              'Dispatchedyear',
                              'Dispatchedhour',
                              'Dispatchedweekday',
                              'Dispatchedday',
                              'Target',
                              'src_x',
                              'src_y',
                              'src_z',
                              'dest_x',
                              'dest_y',
                              'dest_z']]




# In[ ]:





# In[ ]:





# In[22]:




data_df_final_=Common_Features.correctdtypes(data_df_final_)
for j in ['Dispatched At'] :
    try:
        data_df_final_[j]=data_df_final_[j].apply(lambda x:DataCorrectImputeUpdate.date_format(x))
    except:
        pass


# In[23]:


data_df_final_.dtypes


# In[24]:


# data_df_final_['Total_Indent_Count']=data_df_final_['Total_Indent_Count'].apply(lambda x: int(x))
# data_df_final_['Count_by_route']=data_df_final_['Count_by_route'].apply(lambda x: int(x))
# data_df_final_['vehicle_count']=data_df_final_['vehicle_count'].apply(lambda x: int(x))
# data_df_final_['Geodesic_dist']=data_df_final_['Geodesic_dist'].apply(lambda x: int(x))


# In[25]:


data_df_final_=data_df_final_[~data_df_final_.isin([np.nan, np.inf, -np.inf]).any(1)]


# In[26]:


df=data_df_final_.drop(['Indent ID','Dispatched At'],1)
df_num=df[df.describe().columns]
df_obj=df.select_dtypes('object')


# In[27]:


df_obj


# ## Multicolinearity## 

# In[28]:


corr_matrix = df_num.corr().abs()

# Select upper triangle of correlation matrix
upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(np.bool))

# Find index of feature columns with correlation greater than 0.95
to_drop = [column for column in upper.columns if any(upper[column] > 0.85)]


# In[29]:


to_drop


# In[30]:


df_num=df_num.drop(to_drop,1)
df_num.drop(['Target'],1).columns


# In[31]:


DataFrame(df_num.drop(['Target'],1).columns,columns=['Num_Var']).to_csv(os.path.join(support_file_path,'Num_Var_EDD_dispatch.csv'),index=False)
DataFrame(df_obj.columns,columns=['Obj_Var']).to_csv(os.path.join(support_file_path,'Obj_Var_EDD_dispatch.csv'),index=False)


# In[32]:


## Label encoding
d = defaultdict(LabelEncoder)

# Encoding the variable

df_obj_coded=df_obj.apply(lambda x: d[x.name].fit_transform(x))

##Joinin dataframe to get complete dataframe once again
df_lab=pd.concat([df_obj_coded,df_num],1)
df_lab.insert(0, 'Indent ID',data_df_final_['Indent ID']) 
df_lab.shape


# In[33]:


df_lab


# In[34]:


pickle.dump(d, open(os.path.join(support_file_path,'label_coder_EDD_Dispatch.sav'), 'wb'))


# In[35]:


df_lab=pd.concat([data_df_final_['Dispatched At'],df_lab],1)


# In[36]:


df_lab


# In[37]:


#df_lab=EDD.data_prep_lag(df_lab)
 


# In[38]:


df_lab.to_csv(os.path.join(processed_datafile_path,'df_lab_EDD_Dispatched.csv'),index=False)


# In[ ]:





# In[39]:


df_lab_ml=df_lab


# In[40]:


df_lab_ml.columns


# In[41]:


# for i in df_lab_ml.drop(['Loading Start Date','Indent ID','Target'],1).columns:
#     print(i)
#     scaler_X= MinMaxScaler(copy=True, feature_range=(0, 1))
#     scaler_X.fit_transform(df_lab_ml.drop(['Loading Start Date','Indent ID','Target'],1)[i].values.reshape(-1,1))
    
    


# In[42]:


#Scaling

predictor_matrix=df_lab_ml.drop(['Dispatched At','Indent ID','Target'],1).values
response_vector=df_lab_ml['Target'].values.reshape(-1,1)
scaler_X= MinMaxScaler(copy=True, feature_range=(0, 1))
scaler_Y= MinMaxScaler(copy=True, feature_range=(0, 1))
predictor_scaled_matrix=scaler_X.fit_transform(predictor_matrix)
response_scaled_vector=scaler_Y.fit_transform(response_vector)

scaled_X_df=pd.DataFrame(predictor_scaled_matrix)
scaled_Y_df=DataFrame(response_scaled_vector,columns=['Target'])



# save the scaler to disk
pickle.dump(scaler_X, open(os.path.join(support_file_path,'scaler_X_EDD_Dispatched.sav'), 'wb'))
pickle.dump(scaler_Y, open(os.path.join(support_file_path,'scaler_Y_EDD_Dispatched.sav'), 'wb'))



columns_X=df_lab_ml.drop(['Dispatched At','Indent ID','Target'],1).columns
scaled_X_df.columns=columns_X


# In[43]:


Orders_column=df_lab_ml[['Indent ID']].reset_index().drop(['index'],1)
scaled_df=pd.concat([Orders_column,scaled_X_df,scaled_Y_df],1)#df_lab['Loading Start Date'].reset_index().drop(['index'],1)


DataFrame(scaled_df.drop(['Indent ID','Target',],1).columns.to_list(),columns=['variables']).to_csv(os.path.join(support_file_path,'variables_Dispatched.csv'),index=False)


# In[44]:


#  ## Train Test Split

import random 
#test_orders=random.sample(pd.unique(scaled_df['Indent ID']).tolist(),int(0.2*len(pd.unique(scaled_df['Indent ID']))))
test_orders=pd.read_csv('EDD_CLVT.csv')['Indent ID'].values
Test_df_=scaled_df.loc[scaled_df['Indent ID'].isin(test_orders)]
Train_df_=scaled_df[~scaled_df['Indent ID'].isin(test_orders)]


# In[45]:


Test_df=Test_df_.drop(['Indent ID'],1)
Train_df=Train_df_.drop(['Indent ID'],1)


stm=Train_df['Target'].values

spm=Train_df.drop(['Target'],1).values


# In[46]:


for i,j in zip(Train_df.columns,Train_df.dtypes):
    print(i,j)


# In[47]:


Train_df.columns


# 
# <hr>We will run the model on Creation date and then predict the EDD. Model will assume creation date as one day ahead of history data available<hr> 
# <hr>Drop variables which won’t pe available in real scenario<hr>

# In[48]:


# ## Select Best variable
import pandas as pd
import numpy as np
from pandas import read_csv,DataFrame,concat,Series
from sklearn.ensemble import RandomForestClassifier,RandomForestRegressor 
import pickle
import holidays
import warnings
import requests
from sklearn.linear_model import LogisticRegression
from tensorflow.keras import Sequential
from tensorflow.keras.layers import Dense,Dropout
from tensorflow.keras.callbacks import ModelCheckpoint
from tensorflow.keras.models import load_model
from sklearn.metrics import classification_report
warnings.filterwarnings("ignore")



class ml_model:
    def feat_select(x,y,Train_df,n_top):
        feat_select_model = RandomForestClassifier(n_estimators=200, 
                               bootstrap = True,
                               max_features = 'sqrt',verbose=1)
        feat_select_model.fit(x,y)
        feat_df=pd.concat([Series(feat_select_model.feature_importances_),Series(Train_df.drop(['Target'],1).columns)],1)
        feat_df.columns=['score','feature']
        feat_df=feat_df.sort_values(by='score',ascending=False)
        
        return feat_df.feature[0:n_top]
    
    def feat_select_reg(x,y,Train_df,n_top):
        feat_select_model = RandomForestRegressor(n_estimators=200, 
                               bootstrap = True,
                               max_features = 'sqrt',verbose=1)
        feat_select_model.fit(x,y)
        feat_df=pd.concat([Series(feat_select_model.feature_importances_),Series(Train_df.drop(['Target'],1).columns)],1)
        feat_df.columns=['score','feature']
        feat_df=feat_df.sort_values(by='score',ascending=False)
        
        return feat_df.feature[0:n_top]
    
    
    def RF_reg(x,y,filename):
        rf_reg_model=RandomForestRegressor(n_estimators=200,bootstrap=True,max_features='sqrt',verbose=1)
        rf_reg_model.fit(x,y)
        # save the model to disk
        pickle.dump(rf_reg_model, open(os.path.join(model_file_path,filename), 'wb'))
        
        # load the model from disk
        model= pickle.load(open(os.path.join(model_file_path,filename), 'rb'))
        
        return model
        
        return feat_select_model
    def DNN(x,y,filepath):

        model = Sequential()
        model.add(Dropout(0.5, input_shape=(x.shape[1],)))
        model.add(Dense(4, activation='relu'))
        #model.add(Dense(4, activation='relu'))
#         model.add(Dense(2, activation='relu'))
        model.add(Dense(y.shape[1], activation='sigmoid'))
        model.compile(loss='binary_crossentropy', optimizer='adam')

        # Monitor validation loss
        checkpoint = ModelCheckpoint(filepath, monitor='val_loss', verbose=1, save_best_only=True, mode='min')

        # can use early stop as well instead of checkpoint
        callbacks_list = [checkpoint]

        # fit model
        model.fit(x, y,
                  validation_split=0.33,
                  epochs=200,
                  batch_size=2**6,
                  verbose=1,callbacks=callbacks_list)
        # # # load the saved model
        saved_model = load_model(filepath)


        return saved_model
    
    def Logit(x,y,filename):
        
        logreg = LogisticRegression(C=1)
        model=logreg.fit(x,y)

        # save the model to disk
        pickle.dump(logreg, open(filename, 'wb'))
        
        # load the model from disk
        model= pickle.load(open(filename, 'rb'))
        
        return model

    def report(model,y,y_hat):
        
        return classification_report(y, y_hat)  


best_features=ml_model.feat_select_reg(spm,stm,Train_df,n_top=50).values
best_var_df=DataFrame(best_features).reset_index().drop(['index'],1)
best_var_df.columns=['Lab_variables']
best_var_df.to_csv(os.path.join(support_file_path,'variables_best_edd_Dipatched.csv'),index=False)

 


# In[49]:


for i in best_features:
    print(i)
    
 


# In[50]:


best_features_lst=best_features.tolist() 


best_features_lst.append('Target')

df_best_train=Train_df[best_features_lst]

df_best_test=Test_df[best_features_lst]


# In[51]:


predictor_scaled_matrix=df_best_train.drop(['Target'],1).values
response_scaled_vector=df_best_train['Target'].values.reshape(-1,1)

model_edd=ml_model.RF_reg(predictor_scaled_matrix,response_scaled_vector,filename ='edd_model_Dispatched.sav')


# In[52]:


model_edd


# In[53]:


predictor_matrix_best_test=df_best_test.drop(['Target'],1).values
response_matrix_test=df_best_test['Target'].values.reshape(-1,1)

model_LR=pickle.load(open(os.path.join(model_file_path,'edd_model_Dispatched.sav'), 'rb'))


# In[54]:


## Logistic Reg
y_hat = scaler_Y.inverse_transform(model_edd.predict(predictor_matrix_best_test).reshape(-1,1))
 
 


# In[55]:


y_act=scaler_Y.inverse_transform(response_matrix_test)
# plt.plot(y_act)


# In[56]:


# tsdf=Test_df_.copy()
# tsdf['Prediction_From_Dispatch']=y_hat
# tsdf['Prediction_From_Dispatch']=tsdf['Prediction_From_Dispatch'].apply(lambda x:int(x))
# tsdf['Actual']=y_act
# tsdf=tsdf[['Indent ID','Prediction_From_Dispatch']]


# In[57]:


# comp_df2=pd.read_csv('Comparison_EDD_OLD_NEW.csv')


# In[58]:


# comp_df1=data_df_final_.loc[data_df_final_['Indent ID'].isin(tsdf['Indent ID'])]


# In[59]:


# comp_df3=pd.merge(comp_df1,comp_df2,on=['Indent ID'],how='inner')


# In[60]:


# comp_df4=pd.merge(comp_df2,tsdf,on=['Indent ID'],how='inner')


# In[61]:


# comp_df4['error_from_Loadstart']=np.abs(comp_df4['Prediction_LoadStart']-comp_df4['Actual'])
# comp_df4['error_from_Dispatch']=np.abs(comp_df4['Prediction_From_Dispatch']-comp_df4['Actual'])


# In[62]:


# comp_df4.to_csv('Comparison_EDD_OLD_NEW.csv',index=False)


# ## Model Performance

# In[63]:


c=0
r=50
for i in range(len(y_hat)//r):     
    plt.plot(y_hat[c:r+c],color='b')
    plt.plot(y_act[c:r+c],color='r')
    plt.show()
    c+=r


# ## Metric

# In[64]:


from sklearn.metrics import mean_squared_error
np.sqrt(mean_squared_error(y_hat, y_act))


# ## Input Preparation to model

# In[65]:


#df_ml_dlv_final=pd.read_csv(os.path.join(processed_datafile_path,'hist_data_ml_dlv_duration.csv'))


# In[129]:


#df_ml_dlv_final


# In[130]:


# input_query={'data':
#  {'source': { 'lat' : (10.818357) , 'long' : (76.791356) },
#   'destination': { 'lat' : (30.414032) , 'long' : (76.664373)},
#   'vehicle_type': { 'cft': 1175.0, 'kg' : 20000.0 },
#   'Loading Start Date':'2020-07-25 20:47:14'}
# }
# input_dict={}
 
# input_dict['src_x']=[math.cos(input_query['data']['source']['lat'])*math.cos(input_query['data']['source']['long'])]
# input_dict['src_y']=[math.cos(input_query['data']['source']['lat'])*math.sin(input_query['data']['source']['long'])]
# input_dict['src_z'] =[math.sin(input_query['data']['source']['lat'])]

# input_dict['dest_x']=[math.cos(input_query['data']['destination']['lat'])*math.cos(input_query['data']['destination']['long'])]
# input_dict['dest_y']=[math.cos(input_query['data']['destination']['lat'])*math.sin(input_query['data']['destination']['long'])]
# input_dict['dest_z'] =[math.sin(input_query['data']['destination']['lat'])]


# input_dict['Source Lat']=[input_query['data']['source']['lat']]
# input_dict['Source Long']=[input_query['data']['source']['long']]


# input_dict['Dest Lat']=[input_query['data']['destination']['lat']]
# input_dict['Dest Long']=[input_query['data']['destination']['long']]

# input_dict['KG_capacity']=[input_query['data']['vehicle_type']['kg']]
# input_dict['CFT_capacity']=[input_query['data']['vehicle_type']['cft']]

# input_dict['Loading Start Date']=[input_query['data']['Loading Start Date']]


# In[131]:


# input_dict


# In[132]:



# input_df=DataFrame(input_dict)
# input_df['Loading Start Date']=input_df['Loading Start Date'].apply(lambda x:DataCorrectImputeUpdate.date_format(x))
# Common_Features.calender_days(input_df,'Loading Start Date','Loading')
# Common_Features.hdays(input_df,'Loading Start Date')
# Common_Features.correctdtypes(input_df)
# input_df['Geodesic_dist']=input_df.apply(lambda x:int(geodesic((x['Source Lat'],x['Source Long']),
#                                                     (x['Dest Lat'],x['Dest Long'])).km) ,axis=1)
# try:
#     input_df['Loading Start Date']=input_df['Loading Start Date'].apply(lambda x: DataCorrectImputeUpdate.date_format(x))
# except:
#     pass
# date_string = "21 January, 2018"
# refrence_date=datetime.strptime(date_string, "%d %B, %Y")
# input_df['creation_age']=592#round(( input_df['Loading Start Date']-refrence_date ).dt.total_seconds()/(3600*24))
# input_df['LoadingTime']=input_df['Loadinghour'].apply(EDD.func)


# In[133]:


# input_df


# In[134]:


# input_df.dtypes


# In[135]:


# pd.read_csv(os.path.join(support_file_path,'Num_Var_EDD.csv'))['Num_Var'].values


# In[136]:


# df_num_input=input_df[pd.read_csv(os.path.join(support_file_path,'Num_Var_EDD.csv'))['Num_Var'].values]
# df_obj_input=input_df[pd.read_csv(os.path.join(support_file_path,'Obj_Var_EDD.csv'))['Obj_Var'].values]
# ## 


# In[137]:


# ## Label encoding
# d=pickle.load(open(os.path.join(support_file_path,'label_coder_EDD.sav'),'rb'))


# # Encoding the variable

# df_obj_input_coded=df_obj_input.apply(lambda x: d[x.name].transform(x))

# ##Joinin dataframe to get complete dataframe once again
# Input_df_coded=pd.concat([df_obj_input_coded,df_num_input],1)


# In[138]:


# df_ml_dlv_final=pd.read_csv(os.path.join(processed_datafile_path,'hist_data_ml_dlv_duration.csv'))
# # try:
# #     df_ml_dlv_final['Loading Start Date']=df_ml_dlv_final['Loading Start Date'].apply(lambda x:DataCorrectImputeUpdate.date_format(x))
# # except:
# #     pass



# # Target_lags=10
# # lags_Data_all=(df_ml_dlv_final[df_ml_dlv_final['Loading Start Date']<input_df['Loading Start Date'].values[0]].sort_values(by='Loading Start Date')['Target'])[-Target_lags:]
 
# # for i,j in enumerate((lags_Data_all)):
# #     Input_df_coded['Target_var(t-%d)' % (Target_lags-i)]=[j]


# Input_df_coded=Input_df_coded[best_features]


# In[139]:


# Input_df_coded.columns


# In[140]:


# Input_df_coded


# In[141]:


# scaler_X=pickle.load(open(os.path.join(support_file_path,'scaler_X_EDD.sav'), 'rb'))
# scaler_Y=pickle.load(open(os.path.join(support_file_path,'scaler_Y_EDD.sav'), 'rb'))
# ## Scaling

# predictor_matrix_input=Input_df_coded.values
 
# predictor_scaled_matrix=scaler_X.transform(predictor_matrix_input)

 

# ## Logistic Reg
# model_edd=pickle.load(open(os.path.join(model_file_path,'edd_model.sav'), 'rb'))
# y_hat_output= scaler_Y.inverse_transform(model_edd.predict(predictor_scaled_matrix).reshape(-1,1))


# In[142]:


# y_hat_output


# In[143]:


# y_hat_output


# In[ ]:




