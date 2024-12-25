---
slug: /ja/sql-reference/statements/alter/view
sidebar_position: 50
sidebar_label: VIEW
---

# ALTER TABLE ... MODIFY QUERY ステートメント

`ALTER TABLE ... MODIFY QUERY` ステートメントを使用して、[マテリアライズドビュー](../create/view.md#materialized)が作成されたときに指定された `SELECT` クエリを、データの取り込みプロセスを中断することなく変更できます。

このコマンドは、`TO [db.]name` 句で作成されたマテリアライズドビューを変更するために作成されました。このコマンドでは、基となるストレージテーブルの構造や、マテリアライズドビューのカラム定義を変更しません。そのため、`TO [db.]name` 句を使用せずに作成されたマテリアライズドビューに対してこのコマンドの適用は非常に限定されています。

**TO テーブルを使用した例**

```sql
CREATE TABLE events (ts DateTime, event_type String)
ENGINE = MergeTree ORDER BY (event_type, ts);

CREATE TABLE events_by_day (ts DateTime, event_type String, events_cnt UInt64)
ENGINE = SummingMergeTree ORDER BY (event_type, ts);

CREATE MATERIALIZED VIEW mv TO events_by_day AS
SELECT toStartOfDay(ts) ts, event_type, count() events_cnt
FROM events
GROUP BY ts, event_type;

INSERT INTO events
SELECT Date '2020-01-01' + interval number * 900 second,
       ['imp', 'click'][number%2+1]
FROM numbers(100);

SELECT ts, event_type, sum(events_cnt)
FROM events_by_day
GROUP BY ts, event_type
ORDER BY ts, event_type;

┌──────────────────ts─┬─event_type─┬─sum(events_cnt)─┐
│ 2020-01-01 00:00:00 │ click      │              48 │
│ 2020-01-01 00:00:00 │ imp        │              48 │
│ 2020-01-02 00:00:00 │ click      │               2 │
│ 2020-01-02 00:00:00 │ imp        │               2 │
└─────────────────────┴────────────┴─────────────────┘

-- 新しい測定項目 `cost` と新しい次元 `browser` を追加します。

ALTER TABLE events
  ADD COLUMN browser String,
  ADD COLUMN cost Float64;

-- カラムはマテリアライズドビューとTO
-- （移動先テーブル）で一致する必要はないため、次のアルターは挿入を壊しません。

ALTER TABLE events_by_day
    ADD COLUMN cost Float64,
    ADD COLUMN browser String after event_type,
    MODIFY ORDER BY (event_type, ts, browser);

INSERT INTO events
SELECT Date '2020-01-02' + interval number * 900 second,
       ['imp', 'click'][number%2+1],
       ['firefox', 'safary', 'chrome'][number%3+1],
       10/(number+1)%33
FROM numbers(100);

-- 新しいカラム `browser` と `cost` はまだマテリアライズドビューで変更していないため空です。

SELECT ts, event_type, browser, sum(events_cnt) events_cnt, round(sum(cost),2) cost
FROM events_by_day
GROUP BY ts, event_type, browser
ORDER BY ts, event_type;

┌──────────────────ts─┬─event_type─┬─browser─┬─events_cnt─┬─cost─┐
│ 2020-01-01 00:00:00 │ click      │         │         48 │    0 │
│ 2020-01-01 00:00:00 │ imp        │         │         48 │    0 │
│ 2020-01-02 00:00:00 │ click      │         │         50 │    0 │
│ 2020-01-02 00:00:00 │ imp        │         │         50 │    0 │
│ 2020-01-03 00:00:00 │ click      │         │          2 │    0 │
│ 2020-01-03 00:00:00 │ imp        │         │          2 │    0 │
└─────────────────────┴────────────┴─────────┴────────────┴──────┘

ALTER TABLE mv MODIFY QUERY
  SELECT toStartOfDay(ts) ts, event_type, browser,
  count() events_cnt,
  sum(cost) cost
  FROM events
  GROUP BY ts, event_type, browser;

INSERT INTO events
SELECT Date '2020-01-03' + interval number * 900 second,
       ['imp', 'click'][number%2+1],
       ['firefox', 'safary', 'chrome'][number%3+1],
       10/(number+1)%33
FROM numbers(100);

SELECT ts, event_type, browser, sum(events_cnt) events_cnt, round(sum(cost),2) cost
FROM events_by_day
GROUP BY ts, event_type, browser
ORDER BY ts, event_type;

┌──────────────────ts─┬─event_type─┬─browser─┬─events_cnt─┬──cost─┐
│ 2020-01-01 00:00:00 │ click      │         │         48 │     0 │
│ 2020-01-01 00:00:00 │ imp        │         │         48 │     0 │
│ 2020-01-02 00:00:00 │ click      │         │         50 │     0 │
│ 2020-01-02 00:00:00 │ imp        │         │         50 │     0 │
│ 2020-01-03 00:00:00 │ click      │ firefox │         16 │  6.84 │
│ 2020-01-03 00:00:00 │ click      │         │          2 │     0 │
│ 2020-01-03 00:00:00 │ click      │ safary  │         16 │  9.82 │
│ 2020-01-03 00:00:00 │ click      │ chrome  │         16 │  5.63 │
│ 2020-01-03 00:00:00 │ imp        │         │          2 │     0 │
│ 2020-01-03 00:00:00 │ imp        │ firefox │         16 │ 15.14 │
│ 2020-01-03 00:00:00 │ imp        │ safary  │         16 │  6.14 │
│ 2020-01-03 00:00:00 │ imp        │ chrome  │         16 │  7.89 │
│ 2020-01-04 00:00:00 │ click      │ safary  │          1 │   0.1 │
│ 2020-01-04 00:00:00 │ click      │ firefox │          1 │   0.1 │
│ 2020-01-04 00:00:00 │ imp        │ firefox │          1 │   0.1 │
│ 2020-01-04 00:00:00 │ imp        │ chrome  │          1 │   0.1 │
└─────────────────────┴────────────┴─────────┴────────────┴───────┘

-- !!! `MODIFY ORDER BY` によって、主キーが暗黙的に導入されました。

SHOW CREATE TABLE events_by_day FORMAT TSVRaw

CREATE TABLE test.events_by_day
(
    `ts` DateTime,
    `event_type` String,
    `browser` String,
    `events_cnt` UInt64,
    `cost` Float64
)
ENGINE = SummingMergeTree
PRIMARY KEY (event_type, ts)
ORDER BY (event_type, ts, browser)
SETTINGS index_granularity = 8192

-- !!! カラムの定義は変更されていませんが、それは問題ではありません。クエリを発行するのは
-- マテリアライズドビューではなく、TO（ストレージ）テーブルです。
-- SELECT セクションが更新されました。

SHOW CREATE TABLE mv FORMAT TSVRaw;

CREATE MATERIALIZED VIEW test.mv TO test.events_by_day
(
    `ts` DateTime,
    `event_type` String,
    `events_cnt` UInt64
) AS
SELECT
    toStartOfDay(ts) AS ts,
    event_type,
    browser,
    count() AS events_cnt,
    sum(cost) AS cost
FROM test.events
GROUP BY
    ts,
    event_type,
    browser
```

**TO テーブルを使用しない場合の例**

使用は非常に限定的であり、新しいカラムを追加することなく `SELECT` セクションだけを変更できます。

```sql
CREATE TABLE src_table (`a` UInt32) ENGINE = MergeTree ORDER BY a;
CREATE MATERIALIZED VIEW mv (`a` UInt32) ENGINE = MergeTree ORDER BY a AS SELECT a FROM src_table;
INSERT INTO src_table (a) VALUES (1), (2);
SELECT * FROM mv;
```
```text
┌─a─┐
│ 1 │
│ 2 │
└───┘
```
```sql
ALTER TABLE mv MODIFY QUERY SELECT a * 2 as a FROM src_table;
INSERT INTO src_table (a) VALUES (3), (4);
SELECT * FROM mv;
```
```text
┌─a─┐
│ 6 │
│ 8 │
└───┘
┌─a─┐
│ 1 │
│ 2 │
└───┘
```

## ALTER LIVE VIEW ステートメント

`ALTER LIVE VIEW ... REFRESH` ステートメントは、[ライブビュー](../create/view.md#live-view)を更新します。詳細は[ライブビューの強制更新](../create/view.md#live-view-alter-refresh)を参照してください。

## ALTER TABLE ... MODIFY REFRESH ステートメント

`ALTER TABLE ... MODIFY REFRESH` ステートメントは、[リフレッシュ可能なマテリアライズドビュー](../create/view.md#refreshable-materialized-view)のリフレッシュパラメータを変更します。詳細は[リフレッシュパラメータの変更](../create/view.md#changing-refresh-parameters)を参照してください。
