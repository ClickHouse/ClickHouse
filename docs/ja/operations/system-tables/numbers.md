---
slug: /ja/operations/system-tables/numbers
---
# numbers

このテーブルは、ゼロから始まるほぼすべての自然数を含む単一のUInt64カラム`number`を含んでいます。

このテーブルはテスト用、またはブルートフォース検索が必要な場合に使用できます。

このテーブルからの読み取りは並列化されていません。

**例**

```sql
SELECT * FROM system.numbers LIMIT 10;
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

出力を条件で制限することもできます。

```sql
SELECT * FROM system.numbers < 10;
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

