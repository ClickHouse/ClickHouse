---
slug: /ja/getting-started/example-datasets/tw-weather
sidebar_label: 台湾の歴史的気象データセット
sidebar_position: 1
description: 過去128年間の気象観測データ1億3100万行
---

# 台湾の歴史的気象データセット

このデータセットには、過去128年間の歴史的な気象観測の測定値が含まれています。それぞれの行は、特定の日時と気象観測所の観測値です。

このデータセットの出典は[こちら](https://github.com/Raingel/historical_weather)で、気象観測所番号のリストは[こちら](https://github.com/Raingel/weather_station_list)から参照できます。

> 気象データの出典には、中央気象局が設置した気象観測所（観測所コードがC0、C1、4で始まる）や、農業委員会所属の農業気象観測所（上記以外の観測所コード）を含みます：

- StationId
- MeasuredDate（観測時刻）
- StnPres（観測所気圧）
- SeaPres（海面気圧）
- Td（露点温度）
- RH（相対湿度）
- 他の利用可能な要素

## データのダウンロード

- ClickHouse用にクリーニング、再構造化、およびエンリッチメントされた[事前処理済みバージョン](#pre-processed-data)のデータ。このデータセットは1896年から2023年までの情報をカバーしています。
- [オリジナルの生データをダウンロード](#original-raw-data)し、ClickHouseが必要とするフォーマットに変換します。独自にカラムを追加したいユーザーは、独自のアプローチを検討したり、完成させたりすることができます。

### 事前処理済みデータ

データセットは1行ごとの測定値から、観測所IDと測定日の行ごとに再構造化されています。例：

```csv
StationId,MeasuredDate,StnPres,Tx,RH,WS,WD,WSGust,WDGust,Precp,GloblRad,TxSoil0cm,TxSoil5cm,TxSoil20cm,TxSoil50cm,TxSoil100cm,SeaPres,Td,PrecpHour,SunShine,TxSoil10cm,EvapA,Visb,UVI,Cloud Amount,TxSoil30cm,TxSoil200cm,TxSoil300cm,TxSoil500cm,VaporPressure
C0X100,2016-01-01 01:00:00,1022.1,16.1,72,1.1,8.0,,,,,,,,,,,,,,,,,,,,,,,
C0X100,2016-01-01 02:00:00,1021.6,16.0,73,1.2,358.0,,,,,,,,,,,,,,,,,,,,,,,
C0X100,2016-01-01 03:00:00,1021.3,15.8,74,1.5,353.0,,,,,,,,,,,,,,,,,,,,,,,
C0X100,2016-01-01 04:00:00,1021.2,15.8,74,1.7,8.0,,,,,,,,,,,,,,,,,,,,,,,
```

このデータをクエリするのが容易で、結果のテーブルに対して、不足している要素をnullとして管理することができ、各気象観測所で利用可能な測定項目が異なることがあります。

このデータセットは、以下のGoogle CloudStorageの場所にあります。データセットをローカルのファイルシステムにダウンロードして（そしてClickHouseクライアントを使用して挿入する）も、直接ClickHouseに挿入しても構いません（[URLから挿入](#inserting-from-url)を参照）。

ダウンロードするには：

```bash
wget https://storage.googleapis.com/taiwan-weather-observaiton-datasets/preprocessed_weather_daily_1896_2023.tar.gz

# オプション: チェックサムを検証
md5sum preprocessed_weather_daily_1896_2023.tar.gz
# チェックサムは次のとおりである必要があります: 11b484f5bd9ddafec5cfb131eb2dd008

tar -xzvf preprocessed_weather_daily_1896_2023.tar.gz
daily_weather_preprocessed_1896_2023.csv

# オプション: チェックサムを検証
md5sum daily_weather_preprocessed_1896_2023.csv
# チェックサムは次のとおりである必要があります: 1132248c78195c43d93f843753881754
```

### オリジナルの生データ

以下は、変換および独自の目的に合わせて変更するためのオリジナルの生データをダウンロードする手順に関する詳細です。

#### ダウンロード

オリジナルの生データをダウンロードするには：

```bash
mkdir tw_raw_weather_data && cd tw_raw_weather_data

wget https://storage.googleapis.com/taiwan-weather-observaiton-datasets/raw_data_weather_daily_1896_2023.tar.gz

# オプション: チェックサムを検証
md5sum raw_data_weather_daily_1896_2023.tar.gz
# チェックサムは次のとおりである必要があります: b66b9f137217454d655e3004d7d1b51a

tar -xzvf raw_data_weather_daily_1896_2023.tar.gz
466920_1928.csv
466920_1929.csv
466920_1930.csv
466920_1931.csv
...

# オプション: チェックサムを検証
cat *.csv | md5sum
# チェックサムは次のとおりである必要があります: b26db404bf84d4063fac42e576464ce1
```

#### 台湾の気象観測所を取得

```bash
wget -O weather_sta_list.csv https://github.com/Raingel/weather_station_list/raw/main/data/weather_sta_list.csv

# オプション: UTF-8-BOMからUTF-8エンコーディングに変換
sed -i '1s/^\xEF\xBB\xBF//' weather_sta_list.csv
```

## テーブルスキーマの作成

ClickHouseにMergeTreeテーブルを作成します（ClickHouseクライアントから）。

```bash
CREATE TABLE tw_weather_data (
    StationId String null,
    MeasuredDate DateTime64,
    StnPres Float64 null,
    SeaPres Float64 null,
    Tx Float64 null,
    Td Float64 null,
    RH Float64 null,
    WS Float64 null,
    WD Float64 null,
    WSGust Float64 null,
    WDGust Float64 null,
    Precp Float64 null,
    PrecpHour Float64 null,
    SunShine Float64 null,
    GloblRad Float64 null,
    TxSoil0cm Float64 null,
    TxSoil5cm Float64 null,
    TxSoil10cm Float64 null,
    TxSoil20cm Float64 null,
    TxSoil50cm Float64 null,
    TxSoil100cm Float64 null,
    TxSoil30cm Float64 null,
    TxSoil200cm Float64 null,
    TxSoil300cm Float64 null,
    TxSoil500cm Float64 null,
    VaporPressure Float64 null,
    UVI Float64 null,
    "Cloud Amount" Float64 null,
    EvapA Float64 null,
    Visb Float64 null
)
ENGINE = MergeTree
ORDER BY (MeasuredDate);
```

## データのClickHouseへの挿入

### ローカルファイルからの挿入

データはローカルファイルから次のように挿入できます（ClickHouseクライアントから）：

```sql
INSERT INTO tw_weather_data FROM INFILE '/path/to/daily_weather_preprocessed_1896_2023.csv'
```

ここで `/path/to` は、ディスク上の特定のユーザーのローカルファイルへのパスを表します。

ClickHouseへのデータの挿入後のサンプル応答出力は次のとおりです：

```response
Query id: 90e4b524-6e14-4855-817c-7e6f98fbeabb

Ok.
131985329 行がセット内にあります。Elapsed: 71.770 秒。処理された1億3198.5万行、10.06 GB（1.84百万行／秒、140.14 MB／秒）。
ピークメモリ使用量: 583.23 MiB。
```

### URLからの挿入

```sql
INSERT INTO tw_weather_data SELECT *
FROM url('https://storage.googleapis.com/taiwan-weather-observaiton-datasets/daily_weather_preprocessed_1896_2023.csv', 'CSVWithNames')
```

データのロードを高速化する方法については、[大規模データロードの調整に関するブログ投稿](https://clickhouse.com/blog/supercharge-your-clickhouse-data-loads-part2)をご覧ください。

## データ行とサイズを確認

1. 挿入された行数を確認：

```sql
SELECT formatReadableQuantity(count())
FROM tw_weather_data;
```

```response
┌─formatReadableQuantity(count())─┐
│ 1億3199万                        │
└─────────────────────────────────┘
```

2. このテーブルに使用されているディスク容量を確認：

```sql
SELECT
    formatReadableSize(sum(bytes)) AS disk_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size
FROM system.parts
WHERE (`table` = 'tw_weather_data') AND active
```

```response
┌─disk_size─┬─uncompressed_size─┐
│ 2.13 GiB  │ 32.94 GiB         │
└───────────┴───────────────────┘
```

## サンプルクエリ

### Q1: 特定の年における各気象観測所の最高露点温度を取得

```sql
SELECT
    StationId,
    max(Td) AS max_td
FROM tw_weather_data
WHERE (year(MeasuredDate) = 2023) AND (Td IS NOT NULL)
GROUP BY StationId

┌─StationId─┬─max_td─┐
│ 466940    │      1 │
│ 467300    │      1 │
│ 467540    │      1 │
│ 467490    │      1 │
│ 467080    │      1 │
│ 466910    │      1 │
│ 467660    │      1 │
│ 467270    │      1 │
│ 467350    │      1 │
│ 467571    │      1 │
│ 466920    │      1 │
│ 467650    │      1 │
│ 467550    │      1 │
│ 467480    │      1 │
│ 467610    │      1 │
│ 467050    │      1 │
│ 467590    │      1 │
│ 466990    │      1 │
│ 467060    │      1 │
│ 466950    │      1 │
│ 467620    │      1 │
│ 467990    │      1 │
│ 466930    │      1 │
│ 467110    │      1 │
│ 466881    │      1 │
│ 467410    │      1 │
│ 467441    │      1 │
│ 467420    │      1 │
│ 467530    │      1 │
│ 466900    │      1 │
└───────────┴────────┘

30 行がセット内にあります。Elapsed: 0.045 秒。処理された641万行、187.33 MB（143.92百万行／秒、4.21 GB／秒）。
```

### Q2: 特定の期間、フィールド、および気象観測所での生データ取得

```sql
SELECT
    StnPres,
    SeaPres,
    Tx,
    Td,
    RH,
    WS,
    WD,
    WSGust,
    WDGust,
    Precp,
    PrecpHour
FROM tw_weather_data
WHERE (StationId = 'C0UB10') AND (MeasuredDate >= '2023-12-23') AND (MeasuredDate < '2023-12-24')
ORDER BY MeasuredDate ASC
LIMIT 10
```

```response
┌─StnPres─┬─SeaPres─┬───Tx─┬───Td─┬─RH─┬──WS─┬──WD─┬─WSGust─┬─WDGust─┬─Precp─┬─PrecpHour─┐
│  1029.5 │    ᴺᵁᴸᴸ │ 11.8 │ ᴺᵁᴸᴸ │ 78 │ 2.7 │ 271 │    5.5 │    275 │ -99.8 │     -99.8 │
│  1029.8 │    ᴺᵁᴸᴸ │ 12.3 │ ᴺᵁᴸᴸ │ 78 │ 2.7 │ 289 │    5.5 │    308 │ -99.8 │     -99.8 │
│  1028.6 │    ᴺᵁᴸᴸ │ 12.3 │ ᴺᵁᴸᴸ │ 79 │ 2.3 │ 251 │    6.1 │    289 │ -99.8 │     -99.8 │
│  1028.2 │    ᴺᵁᴸᴸ │   13 │ ᴺᵁᴸᴸ │ 75 │ 4.3 │ 312 │    7.5 │    316 │ -99.8 │     -99.8 │
│  1027.8 │    ᴺᵁᴸᴸ │ 11.1 │ ᴺᵁᴸᴸ │ 89 │ 7.1 │ 310 │   11.6 │    322 │ -99.8 │     -99.8 │
│  1027.8 │    ᴺᵁᴸᴸ │ 11.6 │ ᴺᵁᴸᴸ │ 90 │ 3.1 │ 269 │   10.7 │    295 │ -99.8 │     -99.8 │
│  1027.9 │    ᴺᵁᴸᴸ │ 12.3 │ ᴺᵁᴸᴸ │ 89 │ 4.7 │ 296 │    8.1 │    310 │ -99.8 │     -99.8 │
│  1028.2 │    ᴺᵁᴸᴸ │ 12.2 │ ᴺᵁᴸᴸ │ 94 │ 2.5 │ 246 │    7.1 │    283 │ -99.8 │     -99.8 │
│  1028.4 │    ᴺᵁᴸᴸ │ 12.5 │ ᴺᵁᴸᴸ │ 94 │ 3.1 │ 265 │    4.8 │    297 │ -99.8 │     -99.8 │
│  1028.3 │    ᴺᵁᴸᴸ │ 13.6 │ ᴺᵁᴸᴸ │ 91 │ 1.2 │ 273 │    4.4 │    256 │ -99.8 │     -99.8 │
└─────────┴─────────┴──────┴──────┴────┴─────┴─────┴────────┴────────┴───────┴───────────┘

10 行がセット内にあります。Elapsed: 0.009 秒。処理された9万1700行、2.33 MB（967万行／秒、245.31 MB／秒）。
```

## クレジット

データセットの準備、クリーニング、および配信に対する中央気象局と農業委員会の農業気象観測ネットワーク（観測所）の努力を認めたいと思います。感謝いたします。

Ou, J.-H., Kuo, C.-H., Wu, Y.-F., Lin, G.-C., Lee, M.-H., Chen, R.-K., Chou, H.-P., Wu, H.-Y., Chu, S.-C., Lai, Q.-J., Tsai, Y.-C., Lin, C.-C., Kuo, C.-C., Liao, C.-T., Chen, Y.-N., Chu, Y.-W., Chen, C.-Y., 2023. 台湾におけるイネいもち病の早期警報のためのアプリケーション指向の深層学習モデル。Ecological Informatics 73, 101950. https://doi.org/10.1016/j.ecoinf.2022.101950 [13/12/2022]
