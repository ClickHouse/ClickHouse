---
slug: /ja/getting-started/example-datasets/nypd_complaint_data
sidebar_label: NYPD Complaint Data
description: "5つのステップでタブ区切りデータを取り込み、クエリする"
title: NYPD Complaint Data
---

タブ区切り値ファイル (TSV) は一般的で、ファイルの最初の行にフィールド見出しが含まれる場合があります。ClickHouseはTSVを取り込むことができ、また、ファイルを取り込まずにTSVにクエリを実行することも可能です。このガイドではこれらのケースを扱います。CSVファイルにクエリを実行または取り込む必要がある場合、フォーマット引数の`TSV`を`CSV`に置き換えるだけで同様の方法が適用できます。

このガイドを通じて、以下のことを行います:
- **調査**: TSVファイルの構造と内容をクエリします。
- **ターゲットとするClickHouseスキーマを決定**: 適切なデータ型を選び、既存のデータをそれらの型にマッピングします。
- **ClickHouseテーブルを作成**します。
- データを**前処理してClickHouseにストリーム**します。
- ClickHouseに対して**いくつかのクエリを実行**します。

このガイドで使用するデータセットはニューヨーク市のオープンデータチームから提供され、"ニューヨーク市警察 (NYPD) に報告されたすべての有効な重罪、軽犯罪、違反に関するデータ"が含まれています。執筆時点でデータファイルのサイズは166MBですが、定期的に更新されています。

**ソース**: [data.cityofnewyork.us](https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Current-Year-To-Date-/5uac-w243)
**利用規約**: https://www1.nyc.gov/home/terms-of-use.page

## 前提条件
- データセットは[NYPD Complaint Data Current (Year To Date)](https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Current-Year-To-Date-/5uac-w243)ページからダウンロードしてください。エクスポートボタンをクリックして**TSV for Excel**を選びます。
- [ClickHouseサーバーとクライアント](../../getting-started/install.md)をインストールします。
- [起動](../../getting-started/install.md#launch)し、`clickhouse-client`で接続します

### このガイドで説明されるコマンドについての注意
このガイドには2種類のコマンドがあります:
- 一部のコマンドはTSVファイルにクエリを実行するもので、これらはコマンドプロンプトで実行されます。
- その他のコマンドはClickHouseにクエリを実行するもので、これらは`clickhouse-client`またはPlay UIで実行されます。

:::note
このガイドの例は、TSVファイルを`${HOME}/NYPD_Complaint_Data_Current__Year_To_Date_.tsv`に保存したと仮定しています。必要に応じてコマンドを調整してください。
:::

## TSVファイルに慣れ親しむ
ClickHouseデータベースを使用する前に、データに慣れ親しんでください。

### ソースTSVファイルのフィールドを確認する
これがTSVファイルにクエリを実行するコマンドの例です。ただし、まだ実行しないでください。
```sh
clickhouse-local --query \
"describe file('${HOME}/NYPD_Complaint_Data_Current__Year_To_Date_.tsv', 'TSVWithNames')"
```

サンプルレスポンス
```response
CMPLNT_NUM                  Nullable(Float64)
ADDR_PCT_CD                 Nullable(Float64)
BORO_NM                     Nullable(String)
CMPLNT_FR_DT                Nullable(String)
CMPLNT_FR_TM                Nullable(String)
```

:::tip
多くの場合、上記のコマンドは入力データの中で数値フィールドと文字列フィールド、およびタプルであるフィールドを教えてくれます。これは常にそうではありません。ClickHouseは何億ものレコードを含むデータセットでルーチンに使用されるため、スキーマを推測するために既定の行数（100）が調べられますが、これはスキーマを推測するために何十億もの行を解析することを避けるためです。以下のレスポンスは、データセットが毎年数回更新されるため、お客様が見る内容とは一致しないかもしれません。データディクショナリーを見ると、CMPLNT_NUMがテキストとして指定されており、数値ではありません。スキーマ推測のための入力行数を100から設定`SETTINGS input_format_max_rows_to_read_for_schema_inference=2000`にオーバーライドすると、より良いコンテンツの理解ができます。

バージョン22.5以降、推測のためのデフォルト行数が25,000行に変更されているため、古いバージョンの場合や25,000行以上のサンプルが必要な場合にのみ設定を変更してください。
:::

このコマンドをコマンドプロンプトで実行してください。`clickhouse-local`を使用してダウンロードしたTSVファイル内のデータにクエリを実行します。
```sh
clickhouse-local --input_format_max_rows_to_read_for_schema_inference=2000 \
--query \
"describe file('${HOME}/NYPD_Complaint_Data_Current__Year_To_Date_.tsv', 'TSVWithNames')"
```

結果:
```response
CMPLNT_NUM        Nullable(String)
ADDR_PCT_CD       Nullable(Float64)
BORO_NM           Nullable(String)
CMPLNT_FR_DT      Nullable(String)
CMPLNT_FR_TM      Nullable(String)
CMPLNT_TO_DT      Nullable(String)
CMPLNT_TO_TM      Nullable(String)
CRM_ATPT_CPTD_CD  Nullable(String)
HADEVELOPT        Nullable(String)
HOUSING_PSA       Nullable(Float64)
JURISDICTION_CODE Nullable(Float64)
JURIS_DESC        Nullable(String)
KY_CD             Nullable(Float64)
LAW_CAT_CD        Nullable(String)
LOC_OF_OCCUR_DESC Nullable(String)
OFNS_DESC         Nullable(String)
PARKS_NM          Nullable(String)
PATROL_BORO       Nullable(String)
PD_CD             Nullable(Float64)
PD_DESC           Nullable(String)
PREM_TYP_DESC     Nullable(String)
RPT_DT            Nullable(String)
STATION_NAME      Nullable(String)
SUSP_AGE_GROUP    Nullable(String)
SUSP_RACE         Nullable(String)
SUSP_SEX          Nullable(String)
TRANSIT_DISTRICT  Nullable(Float64)
VIC_AGE_GROUP     Nullable(String)
VIC_RACE          Nullable(String)
VIC_SEX           Nullable(String)
X_COORD_CD        Nullable(Float64)
Y_COORD_CD        Nullable(Float64)
Latitude          Nullable(Float64)
Longitude         Nullable(Float64)
Lat_Lon           Tuple(Nullable(Float64), Nullable(Float64))
New Georeferenced Column Nullable(String)
```

この時点で、TSVファイルのカラムが[データセットのウェブページ](https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Current-Year-To-Date-/5uac-w243)の**このデータセットのカラム**セクションで指定されている名前と型に一致するか確認してください。データ型は非常に具体的ではなく、すべての数値フィールドは`Nullable(Float64)`に設定され、その他のフィールドはすべて`Nullable(String)`となっています。データを保存するClickHouseテーブルを作成する際に、より適切でパフォーマンスに優れた型を指定できます。

### 適切なスキーマを決定する

フィールドに使用すべき型を判断するには、データがどのように見えるかを知る必要があります。例えば、`JURISDICTION_CODE`フィールドは数値ですが、`UInt8`にすべきか、`Enum`にすべきか、それとも`Float64`が適切かを判断します。

```sql
clickhouse-local --input_format_max_rows_to_read_for_schema_inference=2000 \
--query \
"select JURISDICTION_CODE, count() FROM
 file('${HOME}/NYPD_Complaint_Data_Current__Year_To_Date_.tsv', 'TSVWithNames')
 GROUP BY JURISDICTION_CODE
 ORDER BY JURISDICTION_CODE
 FORMAT PrettyCompact"
```

結果:
```response
┌─JURISDICTION_CODE─┬─count()─┐
│                 0 │  188875 │
│                 1 │    4799 │
│                 2 │   13833 │
│                 3 │     656 │
│                 4 │      51 │
│                 6 │       5 │
│                 7 │       2 │
│                 9 │      13 │
│                11 │      14 │
│                12 │       5 │
│                13 │       2 │
│                14 │      70 │
│                15 │      20 │
│                72 │     159 │
│                87 │       9 │
│                88 │      75 │
│                97 │     405 │
└───────────────────┴─────────┘
```

このクエリのレスポンスは、`JURISDICTION_CODE`が`UInt8`に適していることを示しています。

同様に、一部の`String`フィールドを確認して、それらが`DateTime`または[`LowCardinality(String)`](../../sql-reference/data-types/lowcardinality.md)フィールドに適しているかを確認します。

例えば、`PARKS_NM`フィールドは"発生場所の適用がある場合はNYCの公園、遊び場、またはグリーンスペースの名前（州立公園は含まれません）"と説明されています。ニューヨーク市の公園の名前は、`LowCardinality(String)`に適している可能性があります。

```sh
clickhouse-local --input_format_max_rows_to_read_for_schema_inference=2000 \
--query \
"select count(distinct PARKS_NM) FROM
 file('${HOME}/NYPD_Complaint_Data_Current__Year_To_Date_.tsv', 'TSVWithNames')
 FORMAT PrettyCompact"
```

結果:
```response
┌─uniqExact(PARKS_NM)─┐
│                 319 │
└─────────────────────┘
```

いくつかの公園名を見てみましょう:
```sql
clickhouse-local --input_format_max_rows_to_read_for_schema_inference=2000 \
--query \
"select distinct PARKS_NM FROM
 file('${HOME}/NYPD_Complaint_Data_Current__Year_To_Date_.tsv', 'TSVWithNames')
 LIMIT 10
 FORMAT PrettyCompact"
```

結果:
```response
┌─PARKS_NM───────────────────┐
│ (null)                     │
│ ASSER LEVY PARK            │
│ JAMES J WALKER PARK        │
│ BELT PARKWAY/SHORE PARKWAY │
│ PROSPECT PARK              │
│ MONTEFIORE SQUARE          │
│ SUTTON PLACE PARK          │
│ JOYCE KILMER PARK          │
│ ALLEY ATHLETIC PLAYGROUND  │
│ ASTORIA PARK               │
└────────────────────────────┘
```

執筆時のデータセットには、`PARK_NM`カラムに数百の異なる公園や遊び場しか含まれていません。これは、`LowCardinality`推奨の[`LowCardinality`](../../sql-reference/data-types/lowcardinality.md#lowcardinality-dscr)フィールドにおける 10,000未満の異なる文字列を保つための小さな数値です。

### DateTimeフィールド
[データセットのウェブページ](https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Current-Year-To-Date-/5uac-w243)の**このデータセットのカラム**セクションに基づき、報告されたイベントの開始と終了の日付と時間フィールドがあります。CMPLNT_FR_DTとCMPLT_TO_DTの最小値と最大値を確認すると、フィールドが常に入力されているかどうか分かります。

```sh title="CMPLNT_FR_DT"
clickhouse-local --input_format_max_rows_to_read_for_schema_inference=2000 \
--query \
"select min(CMPLNT_FR_DT), max(CMPLNT_FR_DT) FROM
file('${HOME}/NYPD_Complaint_Data_Current__Year_To_Date_.tsv', 'TSVWithNames')
FORMAT PrettyCompact"
```

結果:
```response
┌─min(CMPLNT_FR_DT)─┬─max(CMPLNT_FR_DT)─┐
│ 01/01/1973        │ 12/31/2021        │
└───────────────────┴───────────────────┘
```

```sh title="CMPLNT_TO_DT"
clickhouse-local --input_format_max_rows_to_read_for_schema_inference=2000 \
--query \
"select min(CMPLNT_TO_DT), max(CMPLNT_TO_DT) FROM
file('${HOME}/NYPD_Complaint_Data_Current__Year_To_Date_.tsv', 'TSVWithNames')
FORMAT PrettyCompact"
```

結果:
```response
┌─min(CMPLNT_TO_DT)─┬─max(CMPLNT_TO_DT)─┐
│                   │ 12/31/2021        │
└───────────────────┴───────────────────┘
```

```sh title="CMPLNT_FR_TM"
clickhouse-local --input_format_max_rows_to_read_for_schema_inference=2000 \
--query \
"select min(CMPLNT_FR_TM), max(CMPLNT_FR_TM) FROM
file('${HOME}/NYPD_Complaint_Data_Current__Year_To_Date_.tsv', 'TSVWithNames')
FORMAT PrettyCompact"
```

結果:
```response
┌─min(CMPLNT_FR_TM)─┬─max(CMPLNT_FR_TM)─┐
│ 00:00:00          │ 23:59:00          │
└───────────────────┴───────────────────┘
```

```sh title="CMPLNT_TO_TM"
clickhouse-local --input_format_max_rows_to_read_for_schema_inference=2000 \
--query \
"select min(CMPLNT_TO_TM), max(CMPLNT_TO_TM) FROM
file('${HOME}/NYPD_Complaint_Data_Current__Year_To_Date_.tsv', 'TSVWithNames')
FORMAT PrettyCompact"
```

結果:
```response
┌─min(CMPLNT_TO_TM)─┬─max(CMPLNT_TO_TM)─┐
│ (null)            │ 23:59:00          │
└───────────────────┴───────────────────┘
```

## プランを立てる

上記の調査結果に基づき:
- `JURISDICTION_CODE`は`UInt8`としてキャストする必要があります。
- `PARKS_NM`は`LowCardinality(String)`にキャストするべきです。
- `CMPLNT_FR_DT`と`CMPLNT_FR_TM`は常に入力されています（おそらくデフォルトの時間`00:00:00`で）
- `CMPLNT_TO_DT`と`CMPLNT_TO_TM`は空欄の可能性があります
- ソースでは、日付と時刻が分割されたフィールドに保存されています
- 日付は`mm/dd/yyyy`形式です
- 時間は`hh:mm:ss`形式です
- 日付と時間は`DateTime`型に連結できます
- 1970年1月1日以前のデータが含まれるため、64ビットのDateTimeが必要です

:::note
型に加えるべき変更は多数ありますが、すべて同じ調査手順に従って決定できます。フィールドの低位数文字列の数や、最小値と最大値を確認して決定します。このガイドの後のテーブルスキーマは、`LowCardinality`の文字列と符号なし整数フィールドが多く、浮動小数点数値フィールドは非常に少数です。
:::

## 日付と時間のフィールドを連結する

日時フィールド`CMPLNT_FR_DT`と`CMPLNT_FR_TM`を1つの`String`に連結して`DateTime`型にキャストできるようにします。まず2つのフィールドを連結演算子で結合します: `CMPLNT_FR_DT || ' ' || CMPLNT_FR_TM`。`CMPLNT_TO_DT`と`CMPLNT_TO_TM`フィールドも同様に処理されます。

```sh
clickhouse-local --input_format_max_rows_to_read_for_schema_inference=2000 \
--query \
"select CMPLNT_FR_DT || ' ' || CMPLNT_FR_TM AS complaint_begin FROM
file('${HOME}/NYPD_Complaint_Data_Current__Year_To_Date_.tsv', 'TSVWithNames')
LIMIT 10
FORMAT PrettyCompact"
```

結果:
```response
┌─complaint_begin─────┐
│ 07/29/2010 00:01:00 │
│ 12/01/2011 12:00:00 │
│ 04/01/2017 15:00:00 │
│ 03/26/2018 17:20:00 │
│ 01/01/2019 00:00:00 │
│ 06/14/2019 00:00:00 │
│ 11/29/2021 20:00:00 │
│ 12/04/2021 00:35:00 │
│ 12/05/2021 12:50:00 │
│ 12/07/2021 20:30:00 │
└─────────────────────┘
```

## 日時文字列をDateTime64型に変換する

ガイドの早い段階で、TSVファイル内に1970年1月1日以前の日付が存在することが判明したため、日付には64ビットDateTime型が必要です。また、日付は`MM/DD/YYYY`から`YYYY/MM/DD`形式に変換する必要があります。これらはすべて[`parseDateTime64BestEffort()`](../../sql-reference/functions/type-conversion-functions.md#parsedatetime64besteffort)で処理できます。

```sh
clickhouse-local --input_format_max_rows_to_read_for_schema_inference=2000 \
--query \
"WITH (CMPLNT_FR_DT || ' ' || CMPLNT_FR_TM) AS CMPLNT_START,
      (CMPLNT_TO_DT || ' ' || CMPLNT_TO_TM) AS CMPLNT_END
select parseDateTime64BestEffort(CMPLNT_START) AS complaint_begin,
       parseDateTime64BestEffortOrNull(CMPLNT_END) AS complaint_end
FROM file('${HOME}/NYPD_Complaint_Data_Current__Year_To_Date_.tsv', 'TSVWithNames')
ORDER BY complaint_begin ASC
LIMIT 25
FORMAT PrettyCompact"
```

上記の2行目と3行目には、前のステップでの連結があります。4行目と5行目で`String`を`DateTime64`に解析します。苦情の終了時間が存在するとは限らないため、`parseDateTime64BestEffortOrNull`を使用しています。

結果:
```response
┌─────────complaint_begin─┬───────────complaint_end─┐
│ 1925-01-01 10:00:00.000 │ 2021-02-12 09:30:00.000 │
│ 1925-01-01 11:37:00.000 │ 2022-01-16 11:49:00.000 │
│ 1925-01-01 15:00:00.000 │ 2021-12-31 00:00:00.000 │
│ 1925-01-01 15:00:00.000 │ 2022-02-02 22:00:00.000 │
│ 1925-01-01 19:00:00.000 │ 2022-04-14 05:00:00.000 │
│ 1955-09-01 19:55:00.000 │ 2022-08-01 00:45:00.000 │
│ 1972-03-17 11:40:00.000 │ 2022-03-17 11:43:00.000 │
│ 1972-05-23 22:00:00.000 │ 2022-05-24 09:00:00.000 │
│ 1972-05-30 23:37:00.000 │ 2022-05-30 23:50:00.000 │
│ 1972-07-04 02:17:00.000 │                    ᴺᵁᴸᴸ │
│ 1973-01-01 00:00:00.000 │                    ᴺᵁᴸᴸ │
│ 1975-01-01 00:00:00.000 │                    ᴺᵁᴸᴸ │
│ 1976-11-05 00:01:00.000 │ 1988-10-05 23:59:00.000 │
│ 1977-01-01 00:00:00.000 │ 1977-01-01 23:59:00.000 │
│ 1977-12-20 00:01:00.000 │                    ᴺᵁᴸᴸ │
│ 1981-01-01 00:01:00.000 │                    ᴺᵁᴸᴸ │
│ 1981-08-14 00:00:00.000 │ 1987-08-13 23:59:00.000 │
│ 1983-01-07 00:00:00.000 │ 1990-01-06 00:00:00.000 │
│ 1984-01-01 00:01:00.000 │ 1984-12-31 23:59:00.000 │
│ 1985-01-01 12:00:00.000 │ 1987-12-31 15:00:00.000 │
│ 1985-01-11 09:00:00.000 │ 1985-12-31 12:00:00.000 │
│ 1986-03-16 00:05:00.000 │ 2022-03-16 00:45:00.000 │
│ 1987-01-07 00:00:00.000 │ 1987-01-09 00:00:00.000 │
│ 1988-04-03 18:30:00.000 │ 2022-08-03 09:45:00.000 │
│ 1988-07-29 12:00:00.000 │ 1990-07-27 22:00:00.000 │
└─────────────────────────┴─────────────────────────┘
```
:::note
上記の`1925`年はデータ中のエラーに由来します。オリジナルデータには、`1019`年から`1022`年の日付のまま入力された数件のレコードが含まれており、これらは`2019`年から`2022`年であるべきです。これらは64ビットDateTime型の最初の日である1925年1月1日として格納されています。
:::

## テーブルを作成する

フィールドに対するデータ型の変更に関する前の決定を以下のテーブルスキーマに反映します。また、テーブルの`ORDER BY`と`PRIMARY KEY`を決定します。少なくとも1つの`ORDER BY`または`PRIMARY KEY`を指定する必要があります。このドキュメントの最後にある*次のステップ*セクションで、コラムを`ORDER BY`に含めるためのガイドラインと、より詳しい情報が提供されています。

### Order ByとPrimary Keyの句

- `ORDER BY`タプルにはクエリフィルタに使用されるフィールドを含めるべきです
- ディスクでの圧縮を最大化するため、`ORDER BY`タプルはカーディナリティの昇順にするべきです
- `PRIMARY KEY`タプルが存在する場合は、`ORDER BY`タプルの部分集合でなければなりません
- `ORDER BY`が指定されている場合、同じタプルが`PRIMARY KEY`として使用されます
- `PRIMARY KEY`インデックスは指定されている`PRIMARY KEY`タプルを用いて作成されるか、`ORDER BY`タプルを用いて作成されます
- `PRIMARY KEY`インデックスはメインメモリに保持されます

データセットを見て、ニューヨーク市の5区における時間経過に応じた報告された犯罪の種類を調べることに決めたと仮定します。これらのフィールドを`ORDER BY`に含めることになります。

| カラム      | データ辞書の説明                 |
| ----------- | ----------------------------- |
| OFNS_DESC   | キーコードに対応する犯罪の説明     |
| RPT_DT      | 警察に報告された日                 |
| BORO_NM     | 発生場所の地区名                   |

TSVファイルをクエリして3つの候補カラムのカーディナリティを確認します：

```bash
clickhouse-local --input_format_max_rows_to_read_for_schema_inference=2000 \
--query \
"select formatReadableQuantity(uniq(OFNS_DESC)) as cardinality_OFNS_DESC,
        formatReadableQuantity(uniq(RPT_DT)) as cardinality_RPT_DT,
        formatReadableQuantity(uniq(BORO_NM)) as cardinality_BORO_NM
  FROM
  file('${HOME}/NYPD_Complaint_Data_Current__Year_To_Date_.tsv', 'TSVWithNames')
  FORMAT PrettyCompact"
```

結果:
```response
┌─cardinality_OFNS_DESC─┬─cardinality_RPT_DT─┬─cardinality_BORO_NM─┐
│ 60.00                 │ 306.00             │ 6.00                │
└───────────────────────┴────────────────────┴─────────────────────┘
```
カードリナリティを考慮すると、`ORDER BY`は次のようになります：

```
ORDER BY ( BORO_NM, OFNS_DESC, RPT_DT )
```
:::note
ここでは読みやすいカラム名を使用しており、上記の名前は次のようにマッピングされます。
```
ORDER BY ( borough, offense_description, date_reported )
```
:::

データ型の変更と`ORDER BY`タプルを合わせたテーブル構造は以下の通りです:

```sql
CREATE TABLE NYPD_Complaint (
    complaint_number     String,
    precinct             UInt8,
    borough              LowCardinality(String),
    complaint_begin      DateTime64(0,'America/New_York'),
    complaint_end        DateTime64(0,'America/New_York'),
    was_crime_completed  String,
    housing_authority    String,
    housing_level_code   UInt32,
    jurisdiction_code    UInt8,
    jurisdiction         LowCardinality(String),
    offense_code         UInt8,
    offense_level        LowCardinality(String),
    location_descriptor  LowCardinality(String),
    offense_description  LowCardinality(String),
    park_name            LowCardinality(String),
    patrol_borough       LowCardinality(String),
    PD_CD                UInt16,
    PD_DESC              String,
    location_type        LowCardinality(String),
    date_reported        Date,
    transit_station      LowCardinality(String),
    suspect_age_group    LowCardinality(String),
    suspect_race         LowCardinality(String),
    suspect_sex          LowCardinality(String),
    transit_district     UInt8,
    victim_age_group     LowCardinality(String),
    victim_race          LowCardinality(String),
    victim_sex           LowCardinality(String),
    NY_x_coordinate      UInt32,
    NY_y_coordinate      UInt32,
    Latitude             Float64,
    Longitude            Float64
) ENGINE = MergeTree
  ORDER BY ( borough, offense_description, date_reported )
```

### テーブルの主キーを見つける

ClickHouseの`system`データベース、特に`system.table`には、今作成したテーブルに関するすべての情報が含まれています。このクエリは`ORDER BY`（ソートキー）と`PRIMARY KEY`を表示します。
```sql
SELECT
    partition_key,
    sorting_key,
    primary_key,
    table
FROM system.tables
WHERE table = 'NYPD_Complaint'
FORMAT Vertical
```

レスポンス:
```response
Query id: 6a5b10bf-9333-4090-b36e-c7f08b1d9e01

Row 1:
──────
partition_key:
sorting_key:   borough, offense_description, date_reported
primary_key:   borough, offense_description, date_reported
table:         NYPD_Complaint

1 row in set. Elapsed: 0.001 sec.
```

## データの前処理とインポート {#preprocess-import-data}

データ前処理に`clickhouse-local`ツールを使用し、アップロードには`clickhouse-client`を使います。

### 使用する`clickhouse-local`引数

:::tip
`clickhouse-local`の引数に`table='input'`が含まれています。clickhouse-localは提供された入力（`cat ${HOME}/NYPD_Complaint_Data_Current__Year_To_Date_.tsv`）を受け取り、テーブルに挿入します。デフォルトではそのテーブル名は`table`です。このガイドでは、データフローを明確にするためにテーブル名を`input`に設定しています。clickhouse-localへの最終引数はテーブルから選択するクエリ（`FROM input`）で、これを`clickhouse-client`にパイプしてテーブル`NYPD_Complaint`を入力します。
:::

```sql
cat ${HOME}/NYPD_Complaint_Data_Current__Year_To_Date_.tsv \
  | clickhouse-local --table='input' --input-format='TSVWithNames' \
  --input_format_max_rows_to_read_for_schema_inference=2000 \
  --query "
    WITH (CMPLNT_FR_DT || ' ' || CMPLNT_FR_TM) AS CMPLNT_START,
     (CMPLNT_TO_DT || ' ' || CMPLNT_TO_TM) AS CMPLNT_END
    SELECT
      CMPLNT_NUM                                  AS complaint_number,
      ADDR_PCT_CD                                 AS precinct,
      BORO_NM                                     AS borough,
      parseDateTime64BestEffort(CMPLNT_START)     AS complaint_begin,
      parseDateTime64BestEffortOrNull(CMPLNT_END) AS complaint_end,
      CRM_ATPT_CPTD_CD                            AS was_crime_completed,
      HADEVELOPT                                  AS housing_authority_development,
      HOUSING_PSA                                 AS housing_level_code,
      JURISDICTION_CODE                           AS jurisdiction_code,
      JURIS_DESC                                  AS jurisdiction,
      KY_CD                                       AS offense_code,
      LAW_CAT_CD                                  AS offense_level,
      LOC_OF_OCCUR_DESC                           AS location_descriptor,
      OFNS_DESC                                   AS offense_description,
      PARKS_NM                                    AS park_name,
      PATROL_BORO                                 AS patrol_borough,
      PD_CD,
      PD_DESC,
      PREM_TYP_DESC                               AS location_type,
      toDate(parseDateTimeBestEffort(RPT_DT))     AS date_reported,
      STATION_NAME                                AS transit_station,
      SUSP_AGE_GROUP                              AS suspect_age_group,
      SUSP_RACE                                   AS suspect_race,
      SUSP_SEX                                    AS suspect_sex,
      TRANSIT_DISTRICT                            AS transit_district,
      VIC_AGE_GROUP                               AS victim_age_group,
      VIC_RACE                                    AS victim_race,
      VIC_SEX                                     AS victim_sex,
      X_COORD_CD                                  AS NY_x_coordinate,
      Y_COORD_CD                                  AS NY_y_coordinate,
      Latitude,
      Longitude
    FROM input" \
  | clickhouse-client --query='INSERT INTO NYPD_Complaint FORMAT TSV'
```

## データの検証 {#validate-data}

:::note
データセットは年に一度以上変わりますので、カウントがこのドキュメントと一致しない場合があります。
:::

クエリ:

```sql
SELECT count()
FROM NYPD_Complaint
```

結果:

```text
┌─count()─┐
│  208993 │
└─────────┘

1 row in set. Elapsed: 0.001 sec.
```

ClickHouse内のデータセットのサイズは、元のTSVファイルのわずか12%です。元のTSVファイルのサイズとテーブルのサイズを比較します。

クエリ:

```sql
SELECT formatReadableSize(total_bytes)
FROM system.tables
WHERE name = 'NYPD_Complaint'
```

結果:
```text
┌─formatReadableSize(total_bytes)─┐
│ 8.63 MiB                        │
└─────────────────────────────────┘
```

## クエリを実行する {#run-queries}

### クエリ1. 月ごとの苦情数を比較する

クエリ:

```sql
SELECT
    dateName('month', date_reported) AS month,
    count() AS complaints,
    bar(complaints, 0, 50000, 80)
FROM NYPD_Complaint
GROUP BY month
ORDER BY complaints DESC
```

結果:
```response
Query id: 7fbd4244-b32a-4acf-b1f3-c3aa198e74d9

┌─month─────┬─complaints─┬─bar(count(), 0, 50000, 80)───────────────────────────────┐
│ March     │      34536 │ ███████████████████████████████████████████████████████▎ │
│ May       │      34250 │ ██████████████████████████████████████████████████████▋  │
│ April     │      32541 │ ████████████████████████████████████████████████████     │
│ January   │      30806 │ █████████████████████████████████████████████████▎       │
│ February  │      28118 │ ████████████████████████████████████████████▊            │
│ November  │       7474 │ ███████████▊                                             │
│ December  │       7223 │ ███████████▌                                             │
│ October   │       7070 │ ███████████▎                                             │
│ September │       6910 │ ███████████                                              │
│ August    │       6801 │ ██████████▊                                              │
│ June      │       6779 │ ██████████▋                                              │
│ July      │       6485 │ ██████████▍                                              │
└───────────┴────────────┴──────────────────────────────────────────────────────────┘

12 rows in set. Elapsed: 0.006 sec. Processed 208.99 thousand rows, 417.99 KB (37.48 million rows/s., 74.96 MB/s.)
```

### クエリ2. 地区ごとの総苦情数を比較する

クエリ:

```sql
SELECT
    borough,
    count() AS complaints,
    bar(complaints, 0, 125000, 60)
FROM NYPD_Complaint
GROUP BY borough
ORDER BY complaints DESC
```

結果:
```response
Query id: 8cdcdfd4-908f-4be0-99e3-265722a2ab8d

┌─borough───────┬─complaints─┬─bar(count(), 0, 125000, 60)──┐
│ BROOKLYN      │      57947 │ ███████████████████████████▋ │
│ MANHATTAN     │      53025 │ █████████████████████████▍   │
│ QUEENS        │      44875 │ █████████████████████▌       │
│ BRONX         │      44260 │ █████████████████████▏       │
│ STATEN ISLAND │       8503 │ ████                         │
│ (null)        │        383 │ ▏                            │
└───────────────┴────────────┴──────────────────────────────┘

6 rows in set. Elapsed: 0.008 sec. Processed 208.99 thousand rows, 209.43 KB (27.14 million rows/s., 27.20 MB/s.)
```

## 次のステップ

[A Practical Introduction to Sparse Primary Indexes in ClickHouse](/docs/ja/guides/best-practices/sparse-primary-indexes.md)は、ClickHouseのインデックスが従来のリレーショナルデータベースとどのように異なるか、ClickHouseがスパースなプライマリインデックスをどのように作成し使用するか、そしてインデックスのベストプラクティスについて解説します。
