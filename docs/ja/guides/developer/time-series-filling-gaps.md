---
slug: /ja/guides/developer/time-series-filling-gaps
sidebar_label: タイムシリーズ - ギャップの埋め方
sidebar_position: 10
description: タイムシリーズデータのギャップを埋める。
keywords: [time series, gap fill]
---

# タイムシリーズデータのギャップを埋める

タイムシリーズデータを扱う際、データが欠落しているか、非アクティブのためにギャップが発生することがあります。通常、データをクエリする際に、そのようなギャップを残したくありません。この場合、`WITH FILL`句が便利です。このガイドでは、`WITH FILL`を使用してタイムシリーズデータのギャップを埋める方法を説明します。

## セットアップ

GenAIイメージサービスによって生成された画像のメタデータを格納する以下のようなテーブルがあるとします：

```sql
CREATE TABLE images
(
    `id` String,
    `timestamp` DateTime64(3),
    `height` Int64,
    `width` Int64,
    `size` Int64
)
ENGINE = MergeTree
ORDER BY (size, height, width);
```

いくつかのレコードをインポートします：

```sql
INSERT INTO images VALUES (1088619203512250448, '2023-03-24 00:24:03.684', 1536, 1536, 2207289);
INSERT INTO images VALUES (1088619204040736859, '2023-03-24 00:24:03.810', 1024, 1024, 1928974);
INSERT INTO images VALUES (1088619204749561989, '2023-03-24 00:24:03.979', 1024, 1024, 1275619);
INSERT INTO images VALUES (1088619206431477862, '2023-03-24 00:24:04.380', 2048, 2048, 5985703);
INSERT INTO images VALUES (1088619206905434213, '2023-03-24 00:24:04.493', 1024, 1024, 1558455);
INSERT INTO images VALUES (1088619208524431510, '2023-03-24 00:24:04.879', 1024, 1024, 1494869);
INSERT INTO images VALUES (1088619208425437515, '2023-03-24 00:24:05.160', 1024, 1024, 1538451);
```

## バケット単位のクエリ

2023年3月24日の`00:24:03`から`00:24:04`の間に作成された画像を調べるために、これらの時点のパラメータを作成します：

```sql
SET param_start = '2023-03-24 00:24:03',
    param_end = '2023-03-24 00:24:04';
```

次に、データを100ミリ秒のバケットにグループ化し、そのバケットで作成された画像の数を返すクエリを書きます：

```sql
SELECT
    toStartOfInterval(timestamp, toIntervalMillisecond(100)) AS bucket,
    count() AS count
FROM MidJourney.images
WHERE (timestamp >= {start:String}) AND (timestamp <= {end:String})
GROUP BY ALL
ORDER BY bucket ASC
```

```response
┌──────────────────bucket─┬─count─┐
│ 2023-03-24 00:24:03.600 │     1 │
│ 2023-03-24 00:24:03.800 │     1 │
│ 2023-03-24 00:24:03.900 │     1 │
│ 2023-03-24 00:24:04.300 │     1 │
│ 2023-03-24 00:24:04.400 │     1 │
│ 2023-03-24 00:24:04.800 │     1 │
└─────────────────────────┴───────┘
```

結果セットは画像が作成されたバケットのみを含んでいますが、タイムシリーズ分析では、エントリがなくても各100msのバケットを返したい場合があります。

## WITH FILL

これらのギャップを埋めるために`WITH FILL`句を使用できます。また、ギャップを埋めるサイズである`STEP`も指定します。`DateTime`タイプの場合のデフォルトは1秒ですが、100msのギャップを埋めたいので、ステップ値として100msの間隔を指定します：

```sql
SELECT
    toStartOfInterval(timestamp, toIntervalMillisecond(100)) AS bucket,
    count() AS count
FROM MidJourney.images
WHERE (timestamp >= {start:String}) AND (timestamp <= {end:String})
GROUP BY ALL
ORDER BY bucket ASC
WITH FILL
STEP toIntervalMillisecond(100);
```

```response
┌──────────────────bucket─┬─count─┐
│ 2023-03-24 00:24:03.600 │     1 │
│ 2023-03-24 00:24:03.700 │     0 │
│ 2023-03-24 00:24:03.800 │     1 │
│ 2023-03-24 00:24:03.900 │     1 │
│ 2023-03-24 00:24:04.000 │     0 │
│ 2023-03-24 00:24:04.100 │     0 │
│ 2023-03-24 00:24:04.200 │     0 │
│ 2023-03-24 00:24:04.300 │     1 │
│ 2023-03-24 00:24:04.400 │     1 │
│ 2023-03-24 00:24:04.500 │     0 │
│ 2023-03-24 00:24:04.600 │     0 │
│ 2023-03-24 00:24:04.700 │     0 │
│ 2023-03-24 00:24:04.800 │     1 │
└─────────────────────────┴───────┘
```

`count`カラムには0の値でギャップが埋められています。

## WITH FILL...FROM

しかし、時間範囲の最初にまだギャップがあります。これを修正するには`FROM`を指定します：

```sql
SELECT
    toStartOfInterval(timestamp, toIntervalMillisecond(100)) AS bucket,
    count() AS count
FROM MidJourney.images
WHERE (timestamp >= {start:String}) AND (timestamp <= {end:String})
GROUP BY ALL
ORDER BY bucket ASC
WITH FILL
FROM toDateTime64({start:String}, 3)
STEP toIntervalMillisecond(100);
```

```response
┌──────────────────bucket─┬─count─┐
│ 2023-03-24 00:24:03.000 │     0 │
│ 2023-03-24 00:24:03.100 │     0 │
│ 2023-03-24 00:24:03.200 │     0 │
│ 2023-03-24 00:24:03.300 │     0 │
│ 2023-03-24 00:24:03.400 │     0 │
│ 2023-03-24 00:24:03.500 │     0 │
│ 2023-03-24 00:24:03.600 │     1 │
│ 2023-03-24 00:24:03.700 │     0 │
│ 2023-03-24 00:24:03.800 │     1 │
│ 2023-03-24 00:24:03.900 │     1 │
│ 2023-03-24 00:24:04.000 │     0 │
│ 2023-03-24 00:24:04.100 │     0 │
│ 2023-03-24 00:24:04.200 │     0 │
│ 2023-03-24 00:24:04.300 │     1 │
│ 2023-03-24 00:24:04.400 │     1 │
│ 2023-03-24 00:24:04.500 │     0 │
│ 2023-03-24 00:24:04.600 │     0 │
│ 2023-03-24 00:24:04.700 │     0 │
│ 2023-03-24 00:24:04.800 │     1 │
└─────────────────────────┴───────┘
```

結果から`00:24:03.000`から`00:24:03.500`までのバケットがすべて出現しています。

## WITH FILL...TO

しかしまだ、時間範囲の終わりからいくつかのバケットが欠けています。これを埋めるために`TO`値を指定します。`TO`は含まれないので、終わりの時間に少しだけ追加します：

```sql
SELECT
    toStartOfInterval(timestamp, toIntervalMillisecond(100)) AS bucket,
    count() AS count
FROM MidJourney.images
WHERE (timestamp >= {start:String}) AND (timestamp <= {end:String})
GROUP BY ALL
ORDER BY bucket ASC
WITH FILL
FROM toDateTime64({start:String}, 3)
TO toDateTime64({end:String}, 3) + INTERVAL 1 millisecond
STEP toIntervalMillisecond(100);
```

```response
┌──────────────────bucket─┬─count─┐
│ 2023-03-24 00:24:03.000 │     0 │
│ 2023-03-24 00:24:03.100 │     0 │
│ 2023-03-24 00:24:03.200 │     0 │
│ 2023-03-24 00:24:03.300 │     0 │
│ 2023-03-24 00:24:03.400 │     0 │
│ 2023-03-24 00:24:03.500 │     0 │
│ 2023-03-24 00:24:03.600 │     1 │
│ 2023-03-24 00:24:03.700 │     0 │
│ 2023-03-24 00:24:03.800 │     1 │
│ 2023-03-24 00:24:03.900 │     1 │
│ 2023-03-24 00:24:04.000 │     0 │
│ 2023-03-24 00:24:04.100 │     0 │
│ 2023-03-24 00:24:04.200 │     0 │
│ 2023-03-24 00:24:04.300 │     1 │
│ 2023-03-24 00:24:04.400 │     1 │
│ 2023-03-24 00:24:04.500 │     0 │
│ 2023-03-24 00:24:04.600 │     0 │
│ 2023-03-24 00:24:04.700 │     0 │
│ 2023-03-24 00:24:04.800 │     1 │
│ 2023-03-24 00:24:04.900 │     0 │
│ 2023-03-24 00:24:05.000 │     0 │
└─────────────────────────┴───────┘
```

ギャップがすべて埋められ、`00:24:03.000`から`00:24:05.000`までのすべての100msのエントリがあります。

## 累積カウント

次に、バケット全体で作成された画像の累積カウントを保持したいとします。以下のように`cumulative`カラムを追加できます：

```sql
SELECT
    toStartOfInterval(timestamp, toIntervalMillisecond(100)) AS bucket,
    count() AS count,
    sum(count) OVER (ORDER BY bucket) AS cumulative
FROM MidJourney.images
WHERE (timestamp >= {start:String}) AND (timestamp <= {end:String})
GROUP BY ALL
ORDER BY bucket ASC
WITH FILL
FROM toDateTime64({start:String}, 3)
TO toDateTime64({end:String}, 3) + INTERVAL 1 millisecond
STEP toIntervalMillisecond(100);
```

```response
┌──────────────────bucket─┬─count─┬─cumulative─┐
│ 2023-03-24 00:24:03.000 │     0 │          0 │
│ 2023-03-24 00:24:03.100 │     0 │          0 │
│ 2023-03-24 00:24:03.200 │     0 │          0 │
│ 2023-03-24 00:24:03.300 │     0 │          0 │
│ 2023-03-24 00:24:03.400 │     0 │          0 │
│ 2023-03-24 00:24:03.500 │     0 │          0 │
│ 2023-03-24 00:24:03.600 │     1 │          1 │
│ 2023-03-24 00:24:03.700 │     0 │          0 │
│ 2023-03-24 00:24:03.800 │     1 │          2 │
│ 2023-03-24 00:24:03.900 │     1 │          3 │
│ 2023-03-24 00:24:04.000 │     0 │          0 │
│ 2023-03-24 00:24:04.100 │     0 │          0 │
│ 2023-03-24 00:24:04.200 │     0 │          0 │
│ 2023-03-24 00:24:04.300 │     1 │          4 │
│ 2023-03-24 00:24:04.400 │     1 │          5 │
│ 2023-03-24 00:24:04.500 │     0 │          0 │
│ 2023-03-24 00:24:04.600 │     0 │          0 │
│ 2023-03-24 00:24:04.700 │     0 │          0 │
│ 2023-03-24 00:24:04.800 │     1 │          6 │
│ 2023-03-24 00:24:04.900 │     0 │          0 │
│ 2023-03-24 00:24:05.000 │     0 │          0 │
└─────────────────────────┴───────┴────────────┘
```

`cumulative`カラムの値は意図した通りには機能していません。

## WITH FILL...INTERPOLATE

`count`カラムが0の行は`cumulative`カラムも0のままですが、`cumulative`カラムの前の値を使用させたいです。以下のように`INTERPOLATE`句を使用してこれを実現できます：

```sql
SELECT
    toStartOfInterval(timestamp, toIntervalMillisecond(100)) AS bucket,
    count() AS count,
    sum(count) OVER (ORDER BY bucket) AS cumulative
FROM MidJourney.images
WHERE (timestamp >= {start:String}) AND (timestamp <= {end:String})
GROUP BY ALL
ORDER BY bucket ASC
WITH FILL
FROM toDateTime64({start:String}, 3)
TO toDateTime64({end:String}, 3) + INTERVAL 100 millisecond
STEP toIntervalMillisecond(100)
INTERPOLATE (cumulative);
```

```response
┌──────────────────bucket─┬─count─┬─cumulative─┐
│ 2023-03-24 00:24:03.000 │     0 │          0 │
│ 2023-03-24 00:24:03.100 │     0 │          0 │
│ 2023-03-24 00:24:03.200 │     0 │          0 │
│ 2023-03-24 00:24:03.300 │     0 │          0 │
│ 2023-03-24 00:24:03.400 │     0 │          0 │
│ 2023-03-24 00:24:03.500 │     0 │          0 │
│ 2023-03-24 00:24:03.600 │     1 │          1 │
│ 2023-03-24 00:24:03.700 │     0 │          1 │
│ 2023-03-24 00:24:03.800 │     1 │          2 │
│ 2023-03-24 00:24:03.900 │     1 │          3 │
│ 2023-03-24 00:24:04.000 │     0 │          3 │
│ 2023-03-24 00:24:04.100 │     0 │          3 │
│ 2023-03-24 00:24:04.200 │     0 │          3 │
│ 2023-03-24 00:24:04.300 │     1 │          4 │
│ 2023-03-24 00:24:04.400 │     1 │          5 │
│ 2023-03-24 00:24:04.500 │     0 │          5 │
│ 2023-03-24 00:24:04.600 │     0 │          5 │
│ 2023-03-24 00:24:04.700 │     0 │          5 │
│ 2023-03-24 00:24:04.800 │     1 │          6 │
│ 2023-03-24 00:24:04.900 │     0 │          6 │
│ 2023-03-24 00:24:05.000 │     0 │          6 │
└─────────────────────────┴───────┴────────────┘
```

これでかなり良くなりました。そして最後に、`bar`関数を使用して棒グラフを追加します。新しいカラムを`INTERPPOLATE`句に追加するのも忘れないようにします。

```sql
SELECT
    toStartOfInterval(timestamp, toIntervalMillisecond(100)) AS bucket,
    count() AS count,
    sum(count) OVER (ORDER BY bucket) AS cumulative,
    bar(cumulative, 0, 10, 10) AS barChart
FROM MidJourney.images
WHERE (timestamp >= {start:String}) AND (timestamp <= {end:String})
GROUP BY ALL
ORDER BY bucket ASC
WITH FILL
FROM toDateTime64({start:String}, 3)
TO toDateTime64({end:String}, 3) + INTERVAL 100 millisecond
STEP toIntervalMillisecond(100)
INTERPOLATE (cumulative, barChart);
```

```response
┌──────────────────bucket─┬─count─┬─cumulative─┬─barChart─┐
│ 2023-03-24 00:24:03.000 │     0 │          0 │          │
│ 2023-03-24 00:24:03.100 │     0 │          0 │          │
│ 2023-03-24 00:24:03.200 │     0 │          0 │          │
│ 2023-03-24 00:24:03.300 │     0 │          0 │          │
│ 2023-03-24 00:24:03.400 │     0 │          0 │          │
│ 2023-03-24 00:24:03.500 │     0 │          0 │          │
│ 2023-03-24 00:24:03.600 │     1 │          1 │ █        │
│ 2023-03-24 00:24:03.700 │     0 │          1 │ █        │
│ 2023-03-24 00:24:03.800 │     1 │          2 │ ██       │
│ 2023-03-24 00:24:03.900 │     1 │          3 │ ███      │
│ 2023-03-24 00:24:04.000 │     0 │          3 │ ███      │
│ 2023-03-24 00:24:04.100 │     0 │          3 │ ███      │
│ 2023-03-24 00:24:04.200 │     0 │          3 │ ███      │
│ 2023-03-24 00:24:04.300 │     1 │          4 │ ████     │
│ 2023-03-24 00:24:04.400 │     1 │          5 │ █████    │
│ 2023-03-24 00:24:04.500 │     0 │          5 │ █████    │
│ 2023-03-24 00:24:04.600 │     0 │          5 │ █████    │
│ 2023-03-24 00:24:04.700 │     0 │          5 │ █████    │
│ 2023-03-24 00:24:04.800 │     1 │          6 │ ██████   │
│ 2023-03-24 00:24:04.900 │     0 │          6 │ ██████   │
│ 2023-03-24 00:24:05.000 │     0 │          6 │ ██████   │
└─────────────────────────┴───────┴────────────┴──────────┘
```
