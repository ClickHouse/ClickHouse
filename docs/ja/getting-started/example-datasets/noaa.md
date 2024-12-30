---
slug: /ja/getting-started/example-datasets/noaa
sidebar_label: NOAA グローバル気象データネットワーク
sidebar_position: 1
description: 過去120年間の気候データ25億行
---

# NOAA グローバル気象データネットワーク

このデータセットには、過去120年間の気象観測データが含まれています。各行は時間と観測所地点の測定値です。

このデータの[由来](https://github.com/awslabs/open-data-docs/tree/main/docs/noaa/noaa-ghcn)によると、より正確には次のとおりです。

> GHCN-Daily は、世界の陸域における日別観測を含むデータセットです。 それは世界中の陸地に基づく観測所からの測定を含み、その約3分の2が降水量測定のみのためのものです (Menne et al., 2012)。 GHCN-Daily は、様々なソースからの気候記録の合成であり、それらは結合され、共通の品質保証レビューを受けています (Durre et al., 2010)。アーカイブには、以下の気象要素が含まれています。

    - 日最高気温
    - 日最低気温
    - 観測時の気温
    - 降水量 (例：雨、溶けた雪)
    - 降雪量
    - 積雪深
    - 利用可能な場合、他の要素

## データのダウンロード

- ClickHouse 用に[準備されたバージョン](#pre-prepared-data)のデータで、データのクレンジング、再構築、および拡張を行っています。 このデータは1900年から2022年のものを対象としています。
- [オリジナルデータをダウンロード](#original-data)し、ClickHouse が必要とする形式に変換します。独自のカラムを追加したいユーザーはこの方法を試すと良いでしょう。

### 準備されたデータ

より具体的には、Noaa による品質保証チェックに不合格となった行を削除しました。データはまた、1行ごとの測定を観測所IDと日付ごとの行に再構築しています。つまり、

```csv
"station_id","date","tempAvg","tempMax","tempMin","precipitation","snowfall","snowDepth","percentDailySun","averageWindSpeed","maxWindSpeed","weatherType"
"AEM00041194","2022-07-30",347,0,308,0,0,0,0,0,0,0
"AEM00041194","2022-07-31",371,413,329,0,0,0,0,0,0,0
"AEM00041194","2022-08-01",384,427,357,0,0,0,0,0,0,0
"AEM00041194","2022-08-02",381,424,352,0,0,0,0,0,0,0
```

これはクエリが簡単であり、結果のテーブルがスパースにならないことを保証します。最後に、データには緯度と経度が追加されています。

このデータは以下の S3 ロケーションにあります。データをローカルファイルシステムにダウンロードして (ClickHouse クライアントを使用して挿入する)、またはClickHouseに直接挿入してください ([S3からの挿入](#inserting-from-s3)を参照)。

ダウンロード方法:

```bash
wget https://datasets-documentation.s3.eu-west-3.amazonaws.com/noaa/noaa_enriched.parquet
```

### オリジナルデータ

以下に、ClickHouseへのロード準備のためにオリジナルデータをダウンロードして変換する手順を示します。

#### ダウンロード

オリジナルデータをダウンロードするには:

```bash
for i in {1900..2023}; do wget https://noaa-ghcn-pds.s3.amazonaws.com/csv.gz/${i}.csv.gz; done
```

#### サンプリングデータ

```bash
$ clickhouse-local --query "SELECT * FROM '2021.csv.gz' LIMIT 10" --format PrettyCompact
┌─c1──────────┬───────c2─┬─c3───┬──c4─┬─c5───┬─c6───┬─c7─┬───c8─┐
│ AE000041196 │ 20210101 │ TMAX │ 278 │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │ S  │ ᴺᵁᴸᴸ │
│ AE000041196 │ 20210101 │ PRCP │   0 │ D    │ ᴺᵁᴸᴸ │ S  │ ᴺᵁᴸᴸ │
│ AE000041196 │ 20210101 │ TAVG │ 214 │ H    │ ᴺᵁᴸᴸ │ S  │ ᴺᵁᴸᴸ │
│ AEM00041194 │ 20210101 │ TMAX │ 266 │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │ S  │ ᴺᵁᴸᴸ │
│ AEM00041194 │ 20210101 │ TMIN │ 178 │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │ S  │ ᴺᵁᴸᴸ │
│ AEM00041194 │ 20210101 │ PRCP │   0 │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │ S  │ ᴺᵁᴸᴸ │
│ AEM00041194 │ 20210101 │ TAVG │ 217 │ H    │ ᴺᵁᴸᴸ │ S  │ ᴺᵁᴸᴸ │
│ AEM00041217 │ 20210101 │ TMAX │ 262 │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │ S  │ ᴺᵁᴸᴸ │
│ AEM00041217 │ 20210101 │ TMIN │ 155 │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │ S  │ ᴺᵁᴸᴸ │
│ AEM00041217 │ 20210101 │ TAVG │ 202 │ H    │ ᴺᵁᴸᴸ │ S  │ ᴺᵁᴸᴸ │
└─────────────┴──────────┴──────┴─────┴──────┴──────┴────┴──────┘
```

[フォーマットドキュメンテーション](https://github.com/awslabs/open-data-docs/tree/main/docs/noaa/noaa-ghcn)の要約とカラムの順序:

 - 11文字の観測所識別コード。これは実際にはいくつかの有用な情報をエンコードしています。
 - YEAR/MONTH/DAY = YYYYMMDDフォーマットの8文字の日付（例：19860529 = 1986年5月29日）
 - ELEMENT = 要素タイプの4文字のインジケーター。実際には測定タイプ。利用可能な多くの測定があるが、ここでは以下のものを選択：
   - PRCP - 降水量（10分の1mm）
   - SNOW - 降雪量（mm）
   - SNWD - 積雪深（mm）
   - TMAX - 最高気温（摂氏10分の1度）
   - TAVG - 平均気温（摂氏10分の1度）
   - TMIN - 最低気温（摂氏10分の1度）
   - PSUN - 1日の可能な日照の割合（％）
   - AWND - 日平均風速（10分の1メートル毎秒）
   - WSFG - 突風最大風速（10分の1メートル毎秒）
   - WT** = 天候タイプ（**は具体的な天候タイプ明義）。天候タイプの完全なリストはこちら。
- DATA VALUE = ELEMENTの5文字データ値、すなわち測定の値。
- M-FLAG = 測定旗の1文字。10の可能性のある値があります。これらの値のいくつかは疑わしいデータの正確性を示します。PRCP、SNOW、およびSNWD測定のみに関連するため、"P"に設定されているデータを受け入れます。
- Q-FLAGは品質チェックを通過した場合の空の値。問題なく通過したもののみに興味があります。
- S-FLAG は観察のソース旗。不必要とされるため、無視します。
- OBS-TIME = 4文字で時間を表し、時分フォーマットです（例：0700 = 午前7時）。古いデータには通常存在しません。当面は無視します。

1行ごとの測定はClickHouseにおいてスパースなテーブル構造をもたらします。時間と観測所ごとの行に変換し、測定値がカラムになるように変換します。まず、`qFlag` が空文字列であることを条件に、問題のない行にデータセットを制限します。

#### データのクレンジング

[ClickHouse local](https://clickhouse.com/blog/extracting-converting-querying-local-files-with-sql-clickhouse-local)を使用し、関心のある測定を表し、品質要件を満たす行をフィルタリングできます。

```bash
clickhouse local --query "SELECT count() 
FROM file('*.csv.gz', CSV, 'station_id String, date String, measurement String, value Int64, mFlag String, qFlag String, sFlag String, obsTime String') WHERE qFlag = '' AND (measurement IN ('PRCP', 'SNOW', 'SNWD', 'TMAX', 'TAVG', 'TMIN', 'PSUN', 'AWND', 'WSFG') OR startsWith(measurement, 'WT'))"

2679264563
```

26億行以上を超えるため、すべてのファイルを解析するので高速ではありません。私たちの8コアマシンで約160秒かかります。

### データのピボット

1行当たりの測定構造はClickHouseで使用可能ですが、今後のクエリを不必要に複雑化します。観測所IDと日付ごとの行で、各測定タイプとその関連する値がカラムである必要があります。

```csv
"station_id","date","tempAvg","tempMax","tempMin","precipitation","snowfall","snowDepth","percentDailySun","averageWindSpeed","maxWindSpeed","weatherType"
"AEM00041194","2022-07-30",347,0,308,0,0,0,0,0,0,0
"AEM00041194","2022-07-31",371,413,329,0,0,0,0,0,0,0
"AEM00041194","2022-08-01",384,427,357,0,0,0,0,0,0,0
"AEM00041194","2022-08-02",381,424,352,0,0,0,0,0,0,0
```

ClickHouse local とシンプルな `GROUP BY` を使って、この構造にデータを再ピボットできます。メモリのオーバーヘッドを制限するため、1ファイルずつこれを行います。

```bash
for i in {1900..2022}
do
clickhouse-local --query "SELECT station_id,
       toDate32(date) as date,
       anyIf(value, measurement = 'TAVG') as tempAvg,
       anyIf(value, measurement = 'TMAX') as tempMax,
       anyIf(value, measurement = 'TMIN') as tempMin,
       anyIf(value, measurement = 'PRCP') as precipitation,
       anyIf(value, measurement = 'SNOW') as snowfall,
       anyIf(value, measurement = 'SNWD') as snowDepth,
       anyIf(value, measurement = 'PSUN') as percentDailySun,
       anyIf(value, measurement = 'AWND') as averageWindSpeed,
       anyIf(value, measurement = 'WSFG') as maxWindSpeed,
       toUInt8OrZero(replaceOne(anyIf(measurement, startsWith(measurement, 'WT') AND value = 1), 'WT', '')) as weatherType
FROM file('$i.csv.gz', CSV, 'station_id String, date String, measurement String, value Int64, mFlag String, qFlag String, sFlag String, obsTime String')
 WHERE qFlag = '' AND (measurement IN ('PRCP', 'SNOW', 'SNWD', 'TMAX', 'TAVG', 'TMIN', 'PSUN', 'AWND', 'WSFG') OR startsWith(measurement, 'WT'))
GROUP BY station_id, date
ORDER BY station_id, date FORMAT CSV" >> "noaa.csv";
done
```

このクエリは1つの50GBファイル `noaa.csv` を生成します。

### データの拡張

データには場所を示すものが観測所IDしかないが、このIDには国コードのプレフィックスが含まれています。理想的には、各観測所に緯度と経度を関連付けるべきです。これを実現するために、NOAAは便利に各観測所の詳細を別の[ghcnd-stations.txt](https://github.com/awslabs/open-data-docs/tree/main/docs/noaa/noaa-ghcn#format-of-ghcnd-stationstxt-file)として提供しています。このファイルにはいくつかのカラムがありますが、私たちの分析に役立つのは5つ、id、緯度、経度、高度、および名前です。

```bash
wget http://noaa-ghcn-pds.s3.amazonaws.com/ghcnd-stations.txt
```

```bash
clickhouse local --query "WITH stations AS (SELECT id, lat, lon, elevation, splitByString(' GSN ',name)[1] as name FROM file('ghcnd-stations.txt', Regexp, 'id String, lat Float64, lon Float64, elevation Float32, name String'))
SELECT station_id,
       date,
       tempAvg,
       tempMax,
       tempMin,
       precipitation,
       snowfall,
       snowDepth,
       percentDailySun,
       averageWindSpeed,
       maxWindSpeed,
       weatherType,
       tuple(lon, lat) as location,
       elevation,
       name
FROM file('noaa.csv', CSV,
          'station_id String, date Date32, tempAvg Int32, tempMax Int32, tempMin Int32, precipitation Int32, snowfall Int32, snowDepth Int32, percentDailySun Int8, averageWindSpeed Int32, maxWindSpeed Int32, weatherType UInt8') as noaa LEFT OUTER
         JOIN stations ON noaa.station_id = stations.id INTO OUTFILE 'noaa_enriched.parquet' FORMAT Parquet SETTINGS format_regexp='^(.{11})\s+(\-?\d{1,2}\.\d{4})\s+(\-?\d{1,3}\.\d{1,4})\s+(\-?\d*\.\d*)\s+(.*)\s+(?:[\d]*)'"
```
このクエリは数分かかり、6.4GBのファイル `noaa_enriched.parquet` を生成します。

## テーブルの作成

ClickHouseでMergeTreeテーブルを作成します (ClickHouseクライアントから)。

```sql
CREATE TABLE noaa
(
   `station_id` LowCardinality(String),
   `date` Date32,
   `tempAvg` Int32 COMMENT '平均気温（摂氏10分の1度）',
   `tempMax` Int32 COMMENT '最高気温（摂氏10分の1度）',
   `tempMin` Int32 COMMENT '最低気温（摂氏10分の1度）',
   `precipitation` UInt32 COMMENT '降水量（10分の1mm）',
   `snowfall` UInt32 COMMENT '降雪量（mm）',
   `snowDepth` UInt32 COMMENT '積雪深（mm）',
   `percentDailySun` UInt8 COMMENT '1日の可能な日照の割合（％）',
   `averageWindSpeed` UInt32 COMMENT '日平均風速（10分の1メートル毎秒）',
   `maxWindSpeed` UInt32 COMMENT '突風最大風速（10分の1メートル毎秒）',
   `weatherType` Enum8('Normal' = 0, 'Fog' = 1, 'Heavy Fog' = 2, 'Thunder' = 3, 'Small Hail' = 4, 'Hail' = 5, 'Glaze' = 6, 'Dust/Ash' = 7, 'Smoke/Haze' = 8, 'Blowing/Drifting Snow' = 9, 'Tornado' = 10, 'High Winds' = 11, 'Blowing Spray' = 12, 'Mist' = 13, 'Drizzle' = 14, 'Freezing Drizzle' = 15, 'Rain' = 16, 'Freezing Rain' = 17, 'Snow' = 18, 'Unknown Precipitation' = 19, 'Ground Fog' = 21, 'Freezing Fog' = 22),
   `location` Point,
   `elevation` Float32,
   `name` LowCardinality(String)
) ENGINE = MergeTree() ORDER BY (station_id, date);

```

## ClickHouseへの挿入

### ローカルファイルからの挿入

ローカルファイルからデータを挿入するには以下のようにします（ClickHouseクライアントから）：

```sql
INSERT INTO noaa FROM INFILE '<path>/noaa_enriched.parquet'
```

ここで `<path>` はディスク上のローカルファイルのフルパスです。

このロードの速度を向上する方法については[こちら](https://clickhouse.com/blog/real-world-data-noaa-climate-data#load-the-data)を参照してください。

### S3からの挿入

```sql
INSERT INTO noaa SELECT *
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/noaa/noaa_enriched.parquet')
```

これを高速化する方法については、大量データロードのチューニングに関する[ブログ投稿](https://clickhouse.com/blog/supercharge-your-clickhouse-data-loads-part2)を参照してください。

## サンプルクエリ

### これまでの最高気温

```sql
SELECT
    tempMax / 10 AS maxTemp,
    location,
    name,
    date
FROM blogs.noaa
WHERE tempMax > 500
ORDER BY
    tempMax DESC,
    date ASC
LIMIT 5

┌─maxTemp─┬─location──────────┬─name───────────────────────────────────────────┬───────date─┐
│    56.7 │ (-116.8667,36.45) │ CA GREENLAND RCH                               │ 1913-07-10 │
│    56.7 │ (-115.4667,32.55) │ MEXICALI (SMN)                                 │ 1949-08-20 │
│    56.7 │ (-115.4667,32.55) │ MEXICALI (SMN)                                 │ 1949-09-18 │
│    56.7 │ (-115.4667,32.55) │ MEXICALI (SMN)                                 │ 1952-07-17 │
│    56.7 │ (-115.4667,32.55) │ MEXICALI (SMN)                                 │ 1952-09-04 │
└─────────┴───────────────────┴────────────────────────────────────────────────┴────────────┘

5 rows in set. Elapsed: 0.514 sec. Processed 1.06 billion rows, 4.27 GB (2.06 billion rows/s., 8.29 GB/s.)
```

[記録の文書](https://en.wikipedia.org/wiki/List_of_weather_records#Highest_temperatures_ever_recorded)されている[Furnace Creek](https://www.google.com/maps/place/36%C2%B027'00.0%22N+116%C2%B052'00.1%22W/@36.1329666,-116.1104099,8.95z/data=!4m5!3m4!1s0x0:0xf2ed901b860f4446!8m2!3d36.45!4d-116.8667)と2023年の状態として矛盾なく一致しています。

### ベストスキーリゾート

屋内雪の少ない[スキーリゾートのリスト](https://gist.githubusercontent.com/gingerwizard/dd022f754fd128fdaf270e58fa052e35/raw/622e03c37460f17ef72907afe554cb1c07f91f23/ski_resort_stats.csv)を利用し、それと過去5年間の雪の多い上位1000気象台を結びつけます。この結びつけを[geoDistance](https://clickhouse.com/docs/ja/sql-reference/functions/geo/coordinates/#geodistance)でソートし、結果を距離が20km未満のものに絞り込み、各リゾートの上位結果を選び、総雪量でソートします。また、高度が1800m以上のリゾートに制限を設けて、優れたスキー条件の広範な指標を示します。

```sql
SELECT
   resort_name,
   total_snow / 1000 AS total_snow_m,
   resort_location,
   month_year
FROM
(
   WITH resorts AS
       (
           SELECT
               resort_name,
               state,
               (lon, lat) AS resort_location,
               'US' AS code
           FROM url('https://gist.githubusercontent.com/gingerwizard/dd022f754fd128fdaf270e58fa052e35/raw/622e03c37460f17ef72907afe554cb1c07f91f23/ski_resort_stats.csv', CSVWithNames)
       )
   SELECT
       resort_name,
       highest_snow.station_id,
       geoDistance(resort_location.1, resort_location.2, station_location.1, station_location.2) / 1000 AS distance_km,
       highest_snow.total_snow,
       resort_location,
       station_location,
       month_year
   FROM
   (
       SELECT
           sum(snowfall) AS total_snow,
           station_id,
           any(location) AS station_location,
           month_year,
           substring(station_id, 1, 2) AS code
       FROM noaa
       WHERE (date > '2017-01-01') AND (code = 'US') AND (elevation > 1800)
       GROUP BY
           station_id,
           toYYYYMM(date) AS month_year
       ORDER BY total_snow DESC
       LIMIT 1000
   ) AS highest_snow
   INNER JOIN resorts ON highest_snow.code = resorts.code
   WHERE distance_km < 20
   ORDER BY
       resort_name ASC,
       total_snow DESC
   LIMIT 1 BY
       resort_name,
       station_id
)
ORDER BY total_snow DESC
LIMIT 5

┌─resort_name──────────┬─total_snow_m─┬─resort_location─┬─month_year─┐
│ Sugar Bowl, CA       │        7.799 │ (-120.3,39.27)  │     201902 │
│ Donner Ski Ranch, CA │        7.799 │ (-120.34,39.31) │     201902 │
│ Boreal, CA           │        7.799 │ (-120.35,39.33) │     201902 │
│ Homewood, CA         │        4.926 │ (-120.17,39.08) │     201902 │
│ Alpine Meadows, CA   │        4.926 │ (-120.22,39.17) │     201902 │
└──────────────────────┴──────────────┴─────────────────┴────────────┘

5 rows in set. Elapsed: 0.750 sec. Processed 689.10 million rows, 3.20 GB (918.20 million rows/s., 4.26 GB/s.)
Peak memory usage: 67.66 MiB.
```

## 謝辞

このデータを準備し、クレンジングし、配布する努力をしてきたグローバル気象データネットワークの方々に感謝いたします。このデータを利用することができ、感謝しております。

Menne, M.J., I. Durre, B. Korzeniewski, S. McNeal, K. Thomas, X. Yin, S. Anthony, R. Ray, R.S. Vose, B.E.Gleason, and T.G. Houston, 2012: Global Historical Climatology Network - Daily (GHCN-Daily), Version 3. [使用したサブセットの指示、例：Version 3.25]。NOAA National Centers for Environmental Information http://doi.org/10.7289/V5D21VHZ [17/08/2020]
