---
slug: /ja/operations/system-tables/numbers_mt
---
# numbers_mt

[system.numbers](../../operations/system-tables/numbers.md) と同様ですが、読み込みが並列化されています。数値は任意の順序で返される可能性があります。

テストに使用されます。

**例**

```sql
SELECT * FROM system.numbers_mt LIMIT 10;
```

```response
┌─number─┐
│      0 │
│      1 │
│      2 │
│      3 │
│      4 │
│      5 │
│      6 │
│      7 │
│      8 │
│      9 │
└────────┘

10 rows in set. Elapsed: 0.001 sec.
```
