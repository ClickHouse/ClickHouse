---
slug: /en/operations/system-tables/numbers_mt
---
# numbers_mt

The same as [system.numbers](../../operations/system-tables/numbers.md) but reads are parallelized. The numbers can be returned in any order.

Used for tests.

**Example**

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
