---
slug: /ja/operations/system-tables/one
---
# one

このテーブルは、値が0の`dummy`という単一のUInt8型のカラムを持つ、1つの行を含んでいます。

このテーブルは、`SELECT`クエリが`FROM`句を指定しない場合に使用されます。

これは、他のDBMSに見られる`DUAL`テーブルと似ています。

**例**

```sql
SELECT * FROM system.one LIMIT 10;
```

```response
┌─dummy─┐
│     0 │
└───────┘

1 rows in set. Elapsed: 0.001 sec.
```
