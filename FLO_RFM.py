
###############################################################
# RFM ile Müşteri Segmentasyonu (Customer Segmentation with RFM)
###############################################################

###############################################################
# İş Problemi (Business Problem)
###############################################################
# FLO müşterilerini segmentlere ayırıp bu segmentlere göre pazarlama stratejileri belirlemek istiyor.
# Buna yönelik olarak müşterilerin davranışları tanımlanacak ve bu davranış öbeklenmelerine göre gruplar oluşturulacak..

###############################################################
# Veri Seti Hikayesi
###############################################################

# Veri seti son alışverişlerini 2020 - 2021 yıllarında OmniChannel(hem online hem offline alışveriş yapan) olarak yapan müşterilerin geçmiş alışveriş davranışlarından
# elde edilen bilgilerden oluşmaktadır.

# master_id: Eşsiz müşteri numarası
# order_channel : Alışveriş yapılan platforma ait hangi kanalın kullanıldığı (Android, ios, Desktop, Mobile, Offline)
# last_order_channel : En son alışverişin yapıldığı kanal
# first_order_date : Müşterinin yaptığı ilk alışveriş tarihi
# last_order_date : Müşterinin yaptığı son alışveriş tarihi
# last_order_date_online : Muşterinin online platformda yaptığı son alışveriş tarihi
# last_order_date_offline : Muşterinin offline platformda yaptığı son alışveriş tarihi
# order_num_total_ever_online : Müşterinin online platformda yaptığı toplam alışveriş sayısı
# order_num_total_ever_offline : Müşterinin offline'da yaptığı toplam alışveriş sayısı
# customer_value_total_ever_offline : Müşterinin offline alışverişlerinde ödediği toplam ücret
# customer_value_total_ever_online : Müşterinin online alışverişlerinde ödediği toplam ücret
# interested_in_categories_12 : Müşterinin son 12 ayda alışveriş yaptığı kategorilerin listesi

###############################################################
# GÖREVLER
###############################################################

## GÖREV 1: Veriyi Anlama (Data Understanding) ve Hazırlama ##

import pandas as pd
import datetime as dt
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
pd.set_option('display.width', 1000)

# 1. flo_data_20K.csv verisini okuyunuz.

import csv
df_ = pd.read_csv("odevler_case1_flo_rfm_analizi/FLOMusteriSegmentasyonu/flo_data_20k.csv")
df = df_.copy()


# 2. Veri setinde

# a. İlk 10 gözlem,

df.head(10)
df.shape

# b. Değişken isimleri,
df.columns

# c. Betimsel istatistik,

df.describe().T

# d. Boş değer,

df.isnull().sum()
                     
# e. Değişken tipleri, incelemesi yapınız.
df.info()
df.dtypes
                     
# 3. Omnichannel müşterilerin hem online'dan hemde offline platformlardan alışveriş yaptığını ifade etmektedir. Herbir müşterinin toplam
# alışveriş sayısı ve harcaması için yeni değişkenler oluşturun.
df["order_num_total"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]

df["customer_value_total"] = df["customer_value_total_ever_offline"] + df["customer_value_total_ever_online"]

df.describe().T          

# 4. Değişken tiplerini inceleyiniz. Tarih ifade eden değişkenlerin tipini date'e çeviriniz.
df.dtypes

# tek tek işlem

df["first_order_date"] = pd.to_datetime(df["first_order_date"])
df["last_order_date"] = pd.to_datetime(df["last_order_date"])
df["last_order_date_online"] = pd.to_datetime(df["last_order_date_online"])
df["last_order_date_offline"] = pd.to_datetime(df["last_order_date_offline"])

# fonksiyon ile yapalım

def convert_columns_to_datetime(df, column_names):
    for column in column_names:
        df[column] = pd.to_datetime(df[column])
    return df

columns_to_convert = ["last_order_date_online", "last_order_date_offline", "last_order_date", "first_order_date"]
df = convert_columns_to_datetime(df, columns_to_convert)

# farklı yol 
date_columns = df.columns[df.columns.str.contains("date")]
df[date_columns] = df[date_columns].apply(pd.to_datetime)
df.info()

# 5. Alışveriş kanallarındaki müşteri sayısının, ortalama alınan ürün sayısının ve ortalama harcamaların dağılımına bakınız.

# Alışveriş kanallarına göre müşteri sayısı
customer_counts = df.groupby("order_channel")["master_id"].nunique()
print("Alişveriş Kanallarina Göre Müşteri Sayisi:")
print(customer_counts)

# Alışveriş kanallarına göre ortalama alınan ürün sayısı
avg_order_nums = df.groupby("order_channel")["order_num_total"].mean()
print("\nAlişveriş Kanallarina Göre Ortalama Alinan Ürün Sayisi:")
print(avg_order_nums)

# Alışveriş kanallarına göre ortalama harcamalar
avg_expenses = df.groupby("order_channel")["customer_value_total"].mean()
print("\nAlişveriş Kanallarina Göre Ortalama Harcamalar:")
print(avg_expenses)

# cozüm kısa yol
df.groupby("order_channel").agg({"master_id":"count",
                                 "order_num_total": "mean",
                                 "customer_value_total": "mean"})

# 6. En fazla kazancı getiren ilk 10 müşteriyi sıralayınız.
top_10_customers = df.groupby("master_id")["customer_value_total"].sum().sort_values(ascending=False).head(10)
top_10_customers = top_10_customers.reset_index()
print("En Fazla Kazanç Getiren İlk 10 Müşteri:")
print(top_10_customers)

#kısa ama dandik
df.sort_values("customer_value_total_ever_on_off", ascending=False)[:10]

#ikinci yol nlargest ile
top_10_customers = df.groupby("master_id")["customer_value_total"].sum().nlargest(10)
top_10_customers = top_10_customers.reset_index()
print("En Fazla Kazanç Getiren İlk 10 Müşteri:")
print(top_10_customers)


# 7. En fazla siparişi veren ilk 10 müşteriyi sıralayınız.
df.describe().T     

top_10_order_customers = df.groupby("master_id")["order_num_total"].sum().nlargest(10)

print("En Fazla Sipariş Veren İlk 10 Müşteri:")
print(top_10_order_customers)

# kısa ama dandik gösteren yol
df.sort_values("order_num_total", ascending=False)[:10]

# 8. Veri ön hazırlık sürecini fonksiyonlaştırınız.

# uzun fonksiyon

def data_preprocessing(data_frame):
    df_ = pd.read_csv("odevler_case1_flo_rfm_analizi/FLOMusteriSegmentasyonu/flo_data_20k.csv")
    df = df_.copy()

    # toplam sipariş ve toplam kazanç işlemleri
    df["order_num_total"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]
    df["customer_value_total"] = df["customer_value_total_ever_offline"] + df["customer_value_total_ever_online"]
    
    # tarih tipini değiştirmek
    df["first_order_date"] = pd.to_datetime(df["first_order_date"])
    df["last_order_date"] = pd.to_datetime(df["last_order_date"])
    df["last_order_date_online"] = pd.to_datetime(df["last_order_date_online"])
    df["last_order_date_offline"] = pd.to_datetime(df["last_order_date_offline"])

    return df

data_preprocessing(df).head(10)
df.dtypes

# kısa fonksiyon 

def data_prep(dataframe):
    dataframe["order_num_total"] = dataframe["order_num_total_ever_online"] + dataframe["order_num_total_ever_offline"]
    dataframe["customer_value_total"] = dataframe["customer_value_total_ever_offline"] + dataframe["customer_value_total_ever_online"]
    date_columns = dataframe.columns[dataframe.columns.str.contains("date")]
    dataframe[date_columns] = dataframe[date_columns].apply(pd.to_datetime)
    return df

data_prep(df).head(10)

## GÖREV 2: RFM Metriklerinin Hesaplanması ##
df.head()

df["last_order_date"].max()
today_date = dt.datetime(2021, 6, 2)
type(today_date)

#1 - 2 - 3 
# rfm = pd.DataFrame()
# rfm["customer_id"] = df["master_id"]


rfm = df.groupby('master_id').agg({'last_order_date': lambda last_order_date: (today_date - last_order_date.max()).days,
                                     'order_num_total': lambda order_num_total: order_num_total.nunique(),
                                     'customer_value_total': lambda customer_value_total: customer_value_total.sum()})
rfm.head()

# 4
rfm.columns = ['recency', 'frequency', 'monetary']

rfm.describe().T

# rfm = rfm[rfm["monetary"] > 0]
# rfm = rfm[rfm["frequency"] > 0]
# rfm.shape


## GÖREV 3: RF ve RFM Skorlarının Hesaplanması ##
rfm["recency_score"] = pd.qcut(rfm['recency'], 5, labels=[5, 4, 3, 2, 1])

rfm["frequency_score"] = pd.qcut(rfm['frequency'].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])

rfm["monetary_score"] = pd.qcut(rfm['monetary'], 5, labels=[1, 2, 3, 4, 5])

rfm["RF_SCORE"] = (rfm['recency_score'].astype(str) +
                    rfm['frequency_score'].astype(str))

rfm.describe().T
rfm.head(10)

## GÖREV 4: RF Skorlarının Segment Olarak Tanımlanması ##

seg_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at_Risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',
    r'51': 'new_customers',
    r'[4-5][2-3]': 'potential_loyalists',
    r'5[4-5]': 'champions'
}

rfm['segment'] = rfm['RF_SCORE'].replace(seg_map, regex=True)

# kontrol
rfm[rfm["segment"] == "loyal_customers"].head()

## GÖREV 5: Aksiyon zamanı! ##
# 1. Segmentlerin recency, frequnecy ve monetary ortalamalarını inceleyiniz.

rfm[["segment", "recency", "frequency", "monetary"]].groupby("segment").agg(["mean", "count"])

rfm.head(10)

# 2. RFM analizi yardımı ile 2 case için ilgili profildeki müşterileri bulun ve müşteri id'lerini csv ye kaydediniz.
# a. FLO bünyesine yeni bir kadın ayakkabı markası dahil ediyor. Dahil ettiği markanın ürün fiyatları genel müşteri tercihlerinin üstünde. Bu nedenle markanın
# tanıtımı ve ürün satışları için ilgilenecek profildeki müşterilerle özel olarak iletişime geçeilmek isteniliyor. Sadık müşterilerinden(champions,loyal_customers),
# ortalama 250 TL üzeri ve kadın kategorisinden alışveriş yapan kişiler özel olarak iletişim kuralacak müşteriler. Bu müşterilerin id numaralarını csv dosyasına
# yeni_marka_hedef_müşteri_id.cvs olarak kaydediniz.

# hedeflenen segmentlere göre müşteri id'si bulmak için 


rfm.head()

# master id index olarak kalmış hatası aldım o yüzden reset index yaptım

# rfm[rfm["segment"].isin(["champions","loyal_customers"])].reset_index().head()

target_segments_customer_ids = rfm[rfm["segment"].isin(["champions","loyal_customers"])].reset_index()["master_id"]

# en alttaki fonksiyondan sonra reset indexe gerek kalmıyor 
# target_segments_customer_ids = rfm_df[rfm_df["segment"].isin(["champions","loyal_customers"])]["master_id"]

# hefelenen segmenti ve kadın kategorisini seçmek için 

cust_ids = df[(df["master_id"].isin(target_segments_customer_ids)) &(df["interested_in_categories_12"].str.contains("KADIN"))]["master_id"]

# csv oluşturma

cust_ids.to_csv("yeni_marka_hedef_müşteri_id.csv", index=False)
cust_ids.shape




# b. Erkek ve Çoçuk ürünlerinde %40'a yakın indirim planlanmaktadır. Bu indirimle ilgili kategorilerle ilgilenen geçmişte iyi müşteri olan ama uzun süredir
# alışveriş yapmayan kaybedilmemesi gereken müşteriler, uykuda olanlar ve yeni gelen müşteriler özel olarak hedef alınmak isteniliyor. Uygun profildeki müşterilerin id'lerini csv dosyasına indirim_hedef_müşteri_ids.csv
# olarak kaydediniz.

target_segments_customer_ids = rfm[rfm["segment"].isin(["cant_loose","about_to_sleep","new_customers"])]["master_id"]
cust_ids = df[(df["master_id"].isin(target_segments_customer_ids)) & ((df["interested_in_categories_12"].str.contains("ERKEK"))|(df["interested_in_categories_12"].str.contains("COCUK")))]["master_id"]
cust_ids.to_csv("indirim_hedef_müşteri_ids.csv", index=False)


# GÖREV 6: Tüm süreci fonksiyonlaştırınız.

def create_rfm(dataframe):
    # Veriyi Hazırlma
    dataframe["order_num_total"] = dataframe["order_num_total_ever_online"] + dataframe["order_num_total_ever_offline"]
    dataframe["customer_value_total"] = dataframe["customer_value_total_ever_offline"] + dataframe["customer_value_total_ever_online"]
    date_columns = dataframe.columns[dataframe.columns.str.contains("date")]
    dataframe[date_columns] = dataframe[date_columns].apply(pd.to_datetime)


    # RFM METRIKLERININ HESAPLANMASI
    dataframe["last_order_date"].max()  # 2021-05-30
    today_date = dt.datetime(2021, 6, 2)
    rfm = df.groupby('master_id').agg({'last_order_date': lambda last_order_date: (today_date - last_order_date.max()).days,
                                     'order_num_total': lambda order_num_total: order_num_total.nunique(),
                                     'customer_value_total': lambda customer_value_total: customer_value_total.sum()})
    rfm.columns = ['recency', 'frequency', 'monetary']


    # RF ve RFM SKORLARININ HESAPLANMASI
    rfm["recency_score"] = pd.qcut(rfm['recency'], 5, labels=[5, 4, 3, 2, 1])
    rfm["frequency_score"] = pd.qcut(rfm['frequency'].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])
    rfm["monetary_score"] = pd.qcut(rfm['monetary'], 5, labels=[1, 2, 3, 4, 5])
    rfm["RF_SCORE"] = (rfm['recency_score'].astype(str) + rfm['frequency_score'].astype(str))
    rfm["RFM_SCORE"] = (rfm['recency_score'].astype(str) + rfm['frequency_score'].astype(str) + rfm['monetary_score'].astype(str))


    # SEGMENTLERIN ISIMLENDIRILMESI
    seg_map = {
        r'[1-2][1-2]': 'hibernating',
        r'[1-2][3-4]': 'at_Risk',
        r'[1-2]5': 'cant_loose',
        r'3[1-2]': 'about_to_sleep',
        r'33': 'need_attention',
        r'[3-4][4-5]': 'loyal_customers',
        r'41': 'promising',
        r'51': 'new_customers',
        r'[4-5][2-3]': 'potential_loyalists',
        r'5[4-5]': 'champions'
    }
    rfm['segment'] = rfm['RF_SCORE'].replace(seg_map, regex=True)

    #index resetle master id yüzünden

    rfm = rfm.reset_index()

    return rfm[["master_id", "recency","frequency","monetary","RF_SCORE","RFM_SCORE","segment"]]


rfm_df = create_rfm(df)

rfm_df.head()
