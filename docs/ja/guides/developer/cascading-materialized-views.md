---
slug: /ja/guides/developer/cascading-materialized-views
title: カスケードされたMaterialized View
description: ソーステーブルから複数のMaterialized Viewを使用する方法。
keywords: [materialized view, 集計]
---

# カスケードされたMaterialized View

この例では、まずMaterialized Viewを作成し、次にそのMaterialized ViewにさらにカスケードさせたMaterialized Viewを作成する方法を示します。このページでは、その方法、多くの可能性、および制限を確認できます。異なるユースケースは、2番目のMaterialized Viewをソースとして使用してMaterialized Viewを作成することで解決できます。

<div style={{width:'640px', height: '360px'}}>
  <iframe src="//www.youtube.com/embed/QDAJTKZT8y4"
    width="640"
    height="360"
    frameborder="0"
    allow="autoplay;
    fullscreen;
    picture-in-picture"
    allowfullscreen>
  </iframe>
</div>

<br />

例:

ドメイン名ごとの毎時のビュー数という架空のデータセットを使用します。

### 目的

1. 各ドメイン名ごとに月ごとに集計されたデータを必要とする。
2. 各ドメイン名ごとに年ごとに集計されたデータも必要とする。

以下のオプションから選択できます：

- SELECTリクエスト中にデータを読み取り、集計するクエリを書く
- データを新しい形式で取り込み時に準備する
- データを特定の集計形式で取り込み時に準備する

Materialized Viewを使用してデータを準備すると、ClickHouseが必要とするデータ量と計算を制限できるため、SELECTリクエストがより高速になります。

## Materialized Viewのソーステーブル

ソーステーブルを作成します。目標が個々の行ではなく集計データのレポートであるため、データを解析し、その情報をMaterialized Viewに渡し、実際の受信データを破棄できます。これにより目的が達成され、ストレージが節約できるため、`Null`テーブルエンジンを使用します。

```sql
CREATE DATABASE IF NOT EXISTS analytics;
```

```sql
CREATE TABLE analytics.hourly_data
(
    `domain_name` String,
    `event_time` DateTime,
    `count_views` UInt64
)
ENGINE = Null
```

:::note
Nullテーブルに対してMaterialized Viewを作成できます。したがって、テーブルに書き込まれるデータはビューに影響を与えますが、元の生データは破棄されます。
:::

## 月次集計テーブルとMaterialized View

最初のMaterialized Viewについては、`Target`テーブルを作成する必要があります。この例では、`analytics.monthly_aggregated_data`というテーブルを作成し、月別およびドメイン名別にビューの合計を保存します。

```sql
CREATE TABLE analytics.monthly_aggregated_data
(
    `domain_name` String,
    `month` Date,
    `sumCountViews` AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (domain_name, month)
```

ターゲットテーブルにデータを転送するMaterialized Viewは次のようになります：

```sql
CREATE MATERIALIZED VIEW analytics.monthly_aggregated_data_mv
TO analytics.monthly_aggregated_data
AS
SELECT
    toDate(toStartOfMonth(event_time)) AS month,
    domain_name,
    sumState(count_views) AS sumCountViews
FROM analytics.hourly_data
GROUP BY
    domain_name,
    month
```

## 年次集計テーブルとMaterialized View

次に、前のターゲットテーブル`monthly_aggregated_data`にリンクされる2番目のMaterialized Viewを作成します。

まず、ドメイン名ごとに年別に集計されたビューの合計を格納する新しいターゲットテーブルを作成します。

```sql
CREATE TABLE analytics.year_aggregated_data
(
    `domain_name` String,
    `year` UInt16,
    `sumCountViews` UInt64
)
ENGINE = SummingMergeTree()
ORDER BY (domain_name, year)
```

このステップでカスケードを定義します。`FROM`文は`monthly_aggregated_data`テーブルを使用します。これは、データフローが次のようになることを意味します：

1. データは`hourly_data`テーブルに到着します。
2. ClickHouseは、受信したデータを最初のMaterialized Viewである`monthly_aggregated_data`テーブルに転送します。
3. 最後に、ステップ2で受信したデータが`year_aggregated_data`に転送されます。

```sql
CREATE MATERIALIZED VIEW analytics.year_aggregated_data_mv
TO analytics.year_aggregated_data
AS
SELECT
    toYear(toStartOfYear(month)) AS year,
    domain_name,
    sumMerge(sumCountViews) as sumCountViews
FROM analytics.monthly_aggregated_data
GROUP BY
    domain_name,
    year
```

:::note
Materialized Viewを使用する際の一般的な誤解は、データがテーブルから読み取られるということです。これは`Materialized View`の動作ではありません。転送されるデータはテーブルの最終結果ではなく、挿入されたブロックです。

この例で`monthly_aggregated_data`で使用されるエンジンがCollapsingMergeTreeであると仮定した場合、私たちの2番目のMaterialized View`year_aggregated_data_mv`に転送されるデータは、圧縮されたテーブルの最終結果ではなく、`SELECT ... GROUP BY`で定義されたフィールドを持つデータブロックが転送されます。

CollapsingMergeTree、ReplacingMergeTree、またはSummingMergeTreeを使用してカスケードMaterialized Viewを作成する予定がある場合は、ここで説明されている制限を理解する必要があります。
:::

## サンプルデータ

カスケードMaterialized Viewをテストするために、データを挿入します：

```sql
INSERT INTO analytics.hourly_data (domain_name, event_time, count_views)
VALUES ('clickhouse.com', '2019-01-01 10:00:00', 1),
       ('clickhouse.com', '2019-02-02 00:00:00', 2),
       ('clickhouse.com', '2019-02-01 00:00:00', 3),
       ('clickhouse.com', '2020-01-01 00:00:00', 6);
```

`analytics.hourly_data`の内容をSELECTすると、テーブルエンジンが`Null`であるため、次のようになりますが、データは処理されます。

```sql
SELECT * FROM analytics.hourly_data
```

```response
Ok.

0 rows in set. Elapsed: 0.002 sec.
```

少量のデータセットを使用することで、予想される結果を確認し、比較することができます。少量のデータセットでフローが正しいことを確認したら、大量のデータに移行することができます。

## 結果

ターゲットテーブルをクエリして`sumCountViews`フィールドを選択しようとすると、バイナリ表現（いくつかのターミナルで）を目にすることがあります。これは、値が数値としてではなく、`AggregateFunction`型として格納されているためです。集計の最終結果を得るには、`-Merge`サフィックスを使用する必要があります。

AggregateFunctionに格納された特殊文字を見るためのクエリ：

```sql
SELECT sumCountViews FROM analytics.monthly_aggregated_data
```

```response
┌─sumCountViews─┐
│               │
│               │
│               │
└───────────────┘

3 rows in set. Elapsed: 0.003 sec.
```

代わりに、`Merge`サフィックスを使用して`sumCountViews`の値を取得してみましょう：

```sql
SELECT
   sumMerge(sumCountViews) as sumCountViews
FROM analytics.monthly_aggregated_data;
```

```response
┌─sumCountViews─┐
│            12 │
└───────────────┘

1 row in set. Elapsed: 0.003 sec.
```

`AggregatingMergeTree`で`AggregateFunction`が`sum`として定義されているため、`sumMerge`を使用できます。`AggregateFunction`で`avg`を使用すると、`avgMerge`を使用します。

```sql
SELECT
    month,
    domain_name,
    sumMerge(sumCountViews) as sumCountViews
FROM analytics.monthly_aggregated_data
GROUP BY
    domain_name,
    month
```

これで、Materialized Viewが定義した目標に答えていることを確認できます。

ターゲットテーブル`monthly_aggregated_data`にデータが保存されているため、各ドメイン名ごとに月ごとに集計されたデータを取得できます：

```sql
SELECT
   month,
   domain_name,
   sumMerge(sumCountViews) as sumCountViews
FROM analytics.monthly_aggregated_data
GROUP BY
   domain_name,
   month
```

```response
┌──────month─┬─domain_name────┬─sumCountViews─┐
│ 2020-01-01 │ clickhouse.com │             6 │
│ 2019-01-01 │ clickhouse.com │             1 │
│ 2019-02-01 │ clickhouse.com │             5 │
└────────────┴────────────────┴───────────────┘

3 rows in set. Elapsed: 0.004 sec.
```

各ドメイン名ごとの年ごとに集計されたデータ：

```sql
SELECT
   year,
   domain_name,
   sum(sumCountViews)
FROM analytics.year_aggregated_data
GROUP BY
   domain_name,
   year
```

```response
┌─year─┬─domain_name────┬─sum(sumCountViews)─┐
│ 2019 │ clickhouse.com │                  6 │
│ 2020 │ clickhouse.com │                  6 │
└──────┴────────────────┴────────────────────┘

2 rows in set. Elapsed: 0.004 sec.
```

## 複数のソーステーブルを単一のターゲットテーブルに結合する

Materialized Viewは、複数のソーステーブルを同じ宛先テーブルに結合するためにも使用できます。これは、`UNION ALL`と同様のロジックを持つMaterialized Viewを作成するのに役立ちます。

まず、異なるメトリックセットを表す2つのソーステーブルを作成します：

```sql
CREATE TABLE analytics.impressions
(
    `event_time` DateTime,
    `domain_name` String
) ENGINE = MergeTree ORDER BY (domain_name, event_time)
;

CREATE TABLE analytics.clicks
(
    `event_time` DateTime,
    `domain_name` String
) ENGINE = MergeTree ORDER BY (domain_name, event_time)
;
```

次に、結合されたメトリックセットを持つ`Target`テーブルを作成します：

```sql
CREATE TABLE analytics.daily_overview
(
    `on_date` Date,
    `domain_name` String,
    `impressions` SimpleAggregateFunction(sum, UInt64),
    `clicks` SimpleAggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree ORDER BY (on_date, domain_name)
```

同じ`Target`テーブルを指す2つのMaterialized Viewを作成します。不足しているカラムを明示的に含める必要はありません：

```sql
CREATE MATERIALIZED VIEW analytics.daily_impressions_mv
TO analytics.daily_overview
AS                                                
SELECT
    toDate(event_time) AS on_date,
    domain_name,
    count() AS impressions,
    0 clicks         ---<<<--- これを省略すると、デフォルトで0になります
FROM                                              
    analytics.impressions
GROUP BY
    toDate(event_time) AS on_date,
    domain_name
;

CREATE MATERIALIZED VIEW analytics.daily_clicks_mv
TO analytics.daily_overview
AS
SELECT
    toDate(event_time) AS on_date,
    domain_name,
    count() AS clicks,
    0 impressions    ---<<<--- これを省略すると、デフォルトで0になります
FROM
    analytics.clicks
GROUP BY
    toDate(event_time) AS on_date,
    domain_name
;
```

これで、挿入された値は`Target`テーブルのそれぞれのカラムに集計されます：

```sql
INSERT INTO analytics.impressions (domain_name, event_time)
VALUES ('clickhouse.com', '2019-01-01 00:00:00'),
       ('clickhouse.com', '2019-01-01 12:00:00'),
       ('clickhouse.com', '2019-02-01 00:00:00'),
       ('clickhouse.com', '2019-03-01 00:00:00')
;

INSERT INTO analytics.clicks (domain_name, event_time)
VALUES ('clickhouse.com', '2019-01-01 00:00:00'),
       ('clickhouse.com', '2019-01-01 12:00:00'),
       ('clickhouse.com', '2019-03-01 00:00:00')
;
```

`Target`テーブルで統合されたインプレッションとクリック：

```sql
SELECT
    on_date,
    domain_name,
    sum(impressions) AS impressions,
    sum(clicks) AS clicks
FROM
    analytics.daily_overview
GROUP BY
    on_date,
    domain_name
;
```

このクエリは次のような出力になります：

```
┌────on_date─┬─domain_name────┬─impressions─┬─clicks─┐
│ 2019-01-01 │ clickhouse.com │           2 │      2 │
│ 2019-03-01 │ clickhouse.com │           1 │      1 │
│ 2019-02-01 │ clickhouse.com │           1 │      0 │
└────────────┴────────────────┴─────────────┴────────┘

3 rows in set. Elapsed: 0.018 sec.
```
