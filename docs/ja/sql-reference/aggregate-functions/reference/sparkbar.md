---
slug: /ja/sql-reference/aggregate-functions/reference/sparkbar
sidebar_position: 187
sidebar_label: sparkbar
---

# sparkbar

この関数は、値 `x` とこれらの値の繰り返し頻度 `y` に基づいて、区間 `[min_x, max_x]` の頻度ヒストグラムをプロットします。
同じバケットに入るすべての `x` の繰り返しは平均されるため、事前にデータを集約しておく必要があります。
負の繰り返しは無視されます。

もし区間が指定されていない場合、最小の `x` が区間の開始として使われ、最大の `x` が区間の終了として使われます。
それ以外の場合、区間外の値は無視されます。

**構文**

``` sql
sparkbar(buckets[, min_x, max_x])(x, y)
```

**パラメータ**

- `buckets` — セグメント数。型: [Integer](../../../sql-reference/data-types/int-uint.md)。
- `min_x` — 区間の開始。オプションのパラメータ。
- `max_x` — 区間の終了。オプションのパラメータ。

**引数**

- `x` — 値のフィールド。
- `y` — 値の頻度のフィールド。

**戻り値**

- 頻度ヒストグラム。

**例**

クエリ:

``` sql
CREATE TABLE spark_bar_data (`value` Int64, `event_date` Date) ENGINE = MergeTree ORDER BY event_date;

INSERT INTO spark_bar_data VALUES (1,'2020-01-01'), (3,'2020-01-02'), (4,'2020-01-02'), (-3,'2020-01-02'), (5,'2020-01-03'), (2,'2020-01-04'), (3,'2020-01-05'), (7,'2020-01-06'), (6,'2020-01-07'), (8,'2020-01-08'), (2,'2020-01-11');

SELECT sparkbar(9)(event_date,cnt) FROM (SELECT sum(value) as cnt, event_date FROM spark_bar_data GROUP BY event_date);

SELECT sparkbar(9, toDate('2020-01-01'), toDate('2020-01-10'))(event_date,cnt) FROM (SELECT sum(value) as cnt, event_date FROM spark_bar_data GROUP BY event_date);
```

結果:

``` text
┌─sparkbar(9)(event_date, cnt)─┐
│ ▂▅▂▃▆█  ▂                    │
└──────────────────────────────┘

┌─sparkbar(9, toDate('2020-01-01'), toDate('2020-01-10'))(event_date, cnt)─┐
│ ▂▅▂▃▇▆█                                                                  │
└──────────────────────────────────────────────────────────────────────────┘
```

この関数の別名は sparkBar です。
