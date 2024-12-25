---
slug: /ja/sql-reference/statements/select/all
sidebar_label: ALL
---

# ALL句

テーブル内に複数の一致する行がある場合、`ALL`はそれらすべてを返します。`SELECT ALL`は、`DISTINCT`なしの`SELECT`と同一です。`ALL`と`DISTINCT`の両方が指定されると、例外がスローされます。

`ALL`は集計関数内で同じ効果（実質的には無意味）で指定することもできます。例えば：

```sql
SELECT sum(ALL number) FROM numbers(10);
```
は次と同じです：

```sql
SELECT sum(number) FROM numbers(10);
```

