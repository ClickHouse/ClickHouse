---
slug: /ja/sql-reference/statements/undrop
sidebar_label: UNDROP
---

# UNDROP TABLE

テーブルの削除を取り消します。

ClickHouseバージョン23.3から、DROP TABLE文を発行してから`database_atomic_delay_before_drop_table_sec`（デフォルトで8分）以内に、AtomicデータベースでテーブルをUNDROPすることができます。削除されたテーブルは`system.dropped_tables`というシステムテーブルに一覧表示されます。

`TO`句が関連付けられていないMaterialized Viewが削除されたテーブルに関連している場合、そのビューの内部テーブルもUNDROPする必要があります。

:::tip
[DROP TABLE](/docs/ja/sql-reference/statements/drop.md)も参照してください
:::

構文:

``` sql
UNDROP TABLE [db.]name [UUID '<uuid>'] [ON CLUSTER cluster]
```

**例**

```sql
CREATE TABLE tab
(
    `id` UInt8
)
ENGINE = MergeTree
ORDER BY id;

DROP TABLE tab;

SELECT *
FROM system.dropped_tables
FORMAT Vertical;
```

```response
Row 1:
──────
index:                 0
database:              default
table:                 tab
uuid:                  aa696a1a-1d70-4e60-a841-4c80827706cc
engine:                MergeTree
metadata_dropped_path: /var/lib/clickhouse/metadata_dropped/default.tab.aa696a1a-1d70-4e60-a841-4c80827706cc.sql
table_dropped_time:    2023-04-05 14:12:12

1 row in set. Elapsed: 0.001 sec. 
```

```sql
UNDROP TABLE tab;

SELECT *
FROM system.dropped_tables
FORMAT Vertical;
```

```response
Ok.

0 rows in set. Elapsed: 0.001 sec. 
```

```sql
DESCRIBE TABLE tab
FORMAT Vertical;
```

```response
Row 1:
──────
name:               id
type:               UInt8
default_type:       
default_expression: 
comment:            
codec_expression:   
ttl_expression:     
```
