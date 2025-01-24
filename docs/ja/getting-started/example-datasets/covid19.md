---
slug: /ja/getting-started/example-datasets/covid19
sidebar_label: COVID-19 オープンデータ
---

# COVID-19 オープンデータ

COVID-19 オープンデータは、最大のCovid-19疫学データベースを構築し、広範な共変量の強力なセットを提供することを試みています。人口統計、経済、疫学、地理、健康、入院、移動、政府の対応、天候などに関するオープンで公開されたデータが含まれています。

詳細はGitHubの[こちら](https://github.com/GoogleCloudPlatform/covid-19-open-data)にあります。

このデータをClickHouseに挿入するのは簡単です...

:::note
以下のコマンドは [ClickHouse Cloud](https://clickhouse.cloud) の**本番環境**で実行されました。ローカルインストールでも簡単に実行できます。
:::

1. データの形を見てみましょう:

```sql
DESCRIBE url(
    'https://storage.googleapis.com/covid19-open-data/v3/epidemiology.csv',
    'CSVWithNames'
);
```

このCSVファイルには10のカラムがあります:

```response
┌─name─────────────────┬─type─────────────┐
│ date                 │ Nullable(Date)   │
│ location_key         │ Nullable(String) │
│ new_confirmed        │ Nullable(Int64)  │
│ new_deceased         │ Nullable(Int64)  │
│ new_recovered        │ Nullable(Int64)  │
│ new_tested           │ Nullable(Int64)  │
│ cumulative_confirmed │ Nullable(Int64)  │
│ cumulative_deceased  │ Nullable(Int64)  │
│ cumulative_recovered │ Nullable(Int64)  │
│ cumulative_tested    │ Nullable(Int64)  │
└──────────────────────┴──────────────────┘

10 rows in set. Elapsed: 0.745 sec.
```

2. 次にいくつかの行を見てみましょう:

```sql
SELECT *
FROM url('https://storage.googleapis.com/covid19-open-data/v3/epidemiology.csv')
LIMIT 100;
```

`url`関数はCSVファイルからデータを簡単に読み取ることができます:

```response
┌─c1─────────┬─c2───────────┬─c3────────────┬─c4───────────┬─c5────────────┬─c6─────────┬─c7───────────────────┬─c8──────────────────┬─c9───────────────────┬─c10───────────────┐
│ date       │ location_key │ new_confirmed │ new_deceased │ new_recovered │ new_tested │ cumulative_confirmed │ cumulative_deceased │ cumulative_recovered │ cumulative_tested │
│ 2020-04-03 │ AD           │ 24            │ 1            │ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ       │ 466                  │ 17                  │ ᴺᵁᴸᴸ                 │ ᴺᵁᴸᴸ              │
│ 2020-04-04 │ AD           │ 57            │ 0            │ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ       │ 523                  │ 17                  │ ᴺᵁᴸᴸ                 │ ᴺᵁᴸᴸ              │
│ 2020-04-05 │ AD           │ 17            │ 4            │ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ       │ 540                  │ 21                  │ ᴺᵁᴸᴸ                 │ ᴺᵁᴸᴸ              │
│ 2020-04-06 │ AD           │ 11            │ 1            │ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ       │ 551                  │ 22                  │ ᴺᵁᴸᴸ                 │ ᴺᵁᴸᴸ              │
│ 2020-04-07 │ AD           │ 15            │ 2            │ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ       │ 566                  │ 24                  │ ᴺᵁᴸᴸ                 │ ᴺᵁᴸᴸ              │
│ 2020-04-08 │ AD           │ 23            │ 2            │ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ       │ 589                  │ 26                  │ ᴺᵁᴸᴸ                 │ ᴺᵁᴸᴸ              │
└────────────┴──────────────┴───────────────┴──────────────┴───────────────┴────────────┴──────────────────────┴─────────────────────┴──────────────────────┴───────────────────┘
```

3. データの形がわかったので、テーブルを作成します:

```sql
CREATE TABLE covid19 (
    date Date,
    location_key LowCardinality(String),
    new_confirmed Int32,
    new_deceased Int32,
    new_recovered Int32,
    new_tested Int32,
    cumulative_confirmed Int32,
    cumulative_deceased Int32,
    cumulative_recovered Int32,
    cumulative_tested Int32
)
ENGINE = MergeTree
ORDER BY (location_key, date);
```

4. 以下のコマンドは、`covid19`テーブルに全データセットを挿入します:

```sql
INSERT INTO covid19
   SELECT *
   FROM
      url(
        'https://storage.googleapis.com/covid19-open-data/v3/epidemiology.csv',
        CSVWithNames,
        'date Date,
        location_key LowCardinality(String),
        new_confirmed Int32,
        new_deceased Int32,
        new_recovered Int32,
        new_tested Int32,
        cumulative_confirmed Int32,
        cumulative_deceased Int32,
        cumulative_recovered Int32,
        cumulative_tested Int32'
    );
```

5. 処理は非常に速く進行します - 挿入された行数を確認してみましょう:

```sql
SELECT formatReadableQuantity(count())
FROM covid19;
```

```response
┌─formatReadableQuantity(count())─┐
│ 12.53 million                   │
└─────────────────────────────────┘
```

6. Covid-19に関して記録された総症例数を見てみましょう:

```sql
SELECT formatReadableQuantity(sum(new_confirmed))
FROM covid19;
```

```response
┌─formatReadableQuantity(sum(new_confirmed))─┐
│ 1.39 billion                               │
└────────────────────────────────────────────┘
```

7. データには0が多く含まれていることに気づくでしょう - 週末や日々の報告がされなかった日です。ウィンドウ関数を使用して新しい症例の毎日の平均をスムーズにすることができます:

```sql
SELECT
   AVG(new_confirmed) OVER (PARTITION BY location_key ORDER BY date ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS cases_smoothed,
   new_confirmed,
   location_key,
   date
FROM covid19;
```

8. このクエリは、各場所の最新値を決定します。すべての国が毎日報告したわけではないため、`max(date)`を使用することはできません。そのため、`ROW_NUMBER`を使用して最後の行を取得します:

```sql
WITH latest_deaths_data AS
   ( SELECT location_key,
            date,
            new_deceased,
            new_confirmed,
            ROW_NUMBER() OVER (PARTITION BY location_key ORDER BY date DESC) as rn
     FROM covid19)
SELECT location_key,
       date,
       new_deceased,
       new_confirmed,
       rn
FROM latest_deaths_data
WHERE rn=1;
```

9. `lagInFrame`を使用して、各日の新しい症例の遅延を決定できます。このクエリでは`US_DC`の場所でフィルタリングします:

```sql
SELECT
   new_confirmed - lagInFrame(new_confirmed,1) OVER (PARTITION BY location_key ORDER BY date) AS confirmed_cases_delta,
   new_confirmed,
   location_key,
   date
FROM covid19
WHERE location_key = 'US_DC';
```

出力は以下のようになります:

```response
┌─confirmed_cases_delta─┬─new_confirmed─┬─location_key─┬───────date─┐
│                     0 │             0 │ US_DC        │ 2020-03-08 │
│                     2 │             2 │ US_DC        │ 2020-03-09 │
│                    -2 │             0 │ US_DC        │ 2020-03-10 │
│                     6 │             6 │ US_DC        │ 2020-03-11 │
│                    -6 │             0 │ US_DC        │ 2020-03-12 │
│                     0 │             0 │ US_DC        │ 2020-03-13 │
│                     6 │             6 │ US_DC        │ 2020-03-14 │
│                    -5 │             1 │ US_DC        │ 2020-03-15 │
│                     4 │             5 │ US_DC        │ 2020-03-16 │
│                     4 │             9 │ US_DC        │ 2020-03-17 │
│                    -1 │             8 │ US_DC        │ 2020-03-18 │
│                    24 │            32 │ US_DC        │ 2020-03-19 │
│                   -26 │             6 │ US_DC        │ 2020-03-20 │
│                    15 │            21 │ US_DC        │ 2020-03-21 │
│                    -3 │            18 │ US_DC        │ 2020-03-22 │
│                     3 │            21 │ US_DC        │ 2020-03-23 │
```

10. このクエリは、各日の新しい症例の変化率を計算し、結果セットに単純な`増加`または`減少`の列を含めます:

```sql
WITH confirmed_lag AS (
  SELECT
    *,
    lagInFrame(new_confirmed) OVER(
      PARTITION BY location_key
      ORDER BY date
    ) AS confirmed_previous_day
  FROM covid19
),
confirmed_percent_change AS (
  SELECT
    *,
    COALESCE(ROUND((new_confirmed - confirmed_previous_day) / confirmed_previous_day * 100), 0) AS percent_change
  FROM confirmed_lag
)
SELECT
  date,
  new_confirmed,
  percent_change,
  CASE
    WHEN percent_change > 0 THEN 'increase'
    WHEN percent_change = 0 THEN 'no change'
    ELSE 'decrease'
  END AS trend
FROM confirmed_percent_change
WHERE location_key = 'US_DC';
```

結果は次のようになります

```response
┌───────date─┬─new_confirmed─┬─percent_change─┬─trend─────┐
│ 2020-03-08 │             0 │            nan │ decrease  │
│ 2020-03-09 │             2 │            inf │ increase  │
│ 2020-03-10 │             0 │           -100 │ decrease  │
│ 2020-03-11 │             6 │            inf │ increase  │
│ 2020-03-12 │             0 │           -100 │ decrease  │
│ 2020-03-13 │             0 │            nan │ decrease  │
│ 2020-03-14 │             6 │            inf │ increase  │
│ 2020-03-15 │             1 │            -83 │ decrease  │
│ 2020-03-16 │             5 │            400 │ increase  │
│ 2020-03-17 │             9 │             80 │ increase  │
│ 2020-03-18 │             8 │            -11 │ decrease  │
│ 2020-03-19 │            32 │            300 │ increase  │
│ 2020-03-20 │             6 │            -81 │ decrease  │
│ 2020-03-21 │            21 │            250 │ increase  │
│ 2020-03-22 │            18 │            -14 │ decrease  │
│ 2020-03-23 │            21 │             17 │ increase  │
│ 2020-03-24 │            46 │            119 │ increase  │
│ 2020-03-25 │            48 │              4 │ increase  │
│ 2020-03-26 │            36 │            -25 │ decrease  │
│ 2020-03-27 │            37 │              3 │ increase  │
│ 2020-03-28 │            38 │              3 │ increase  │
│ 2020-03-29 │            59 │             55 │ increase  │
│ 2020-03-30 │            94 │             59 │ increase  │
│ 2020-03-31 │            91 │             -3 │ decrease  │
│ 2020-04-01 │            67 │            -26 │ decrease  │
│ 2020-04-02 │           104 │             55 │ increase  │
│ 2020-04-03 │           145 │             39 │ increase  │
```

:::note
[GitHubリポジトリ](https://github.com/GoogleCloudPlatform/covid-19-open-data)に記載されているように、このデータセットは2022年9月15日をもって更新が終了しています。
:::
