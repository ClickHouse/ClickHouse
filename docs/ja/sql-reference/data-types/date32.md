---
slug: /ja/sql-reference/data-types/date32
sidebar_position: 14
sidebar_label: Date32
---

# Date32

日付を表すデータ型です。[DateTime64](../../sql-reference/data-types/datetime64.md)と同じ範囲の日時をサポートします。データはネイティブバイトオーダーで符号付き32ビット整数として保存され、1970-01-01からの日数を表現しています（0は1970-01-01を表し、負の値は1970年以前の日数を表します）。

**例**

`Date32`型のカラムを持つテーブルを作成し、データを挿入する例:

``` sql
CREATE TABLE dt32
(
    `timestamp` Date32,
    `event_id` UInt8
)
ENGINE = TinyLog;
```

``` sql
-- Dateを解析する
-- - 文字列から、
-- - 1970-01-01からの日数を解釈する「小さな」整数から、
-- - 1970-01-01からの秒数を解釈する「大きな」整数から。
INSERT INTO dt32 VALUES ('2100-01-01', 1), (47482, 2), (4102444800, 3);

SELECT * FROM dt32;
```

``` text
┌──timestamp─┬─event_id─┐
│ 2100-01-01 │        1 │
│ 2100-01-01 │        2 │
└────────────┴──────────┘
```

**関連項目**

- [toDate32](../../sql-reference/functions/type-conversion-functions.md#todate32)
- [toDate32OrZero](../../sql-reference/functions/type-conversion-functions.md#todate32-or-zero)
- [toDate32OrNull](../../sql-reference/functions/type-conversion-functions.md#todate32-or-null)
