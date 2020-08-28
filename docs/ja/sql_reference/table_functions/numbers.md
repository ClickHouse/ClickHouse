---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 39
toc_title: "\u6570\u5B57"
---

# 数字 {#numbers}

`numbers(N)` – Returns a table with the single ‘number’ 0からN-1までの整数を含む列(UInt64)。
`numbers(N, M)` -単一のテーブルを返す ‘number’ nから(N+M-1)までの整数を含む列(UInt64)。

に似て `system.numbers` テーブルに使用でき試験および発生連続値, `numbers(N, M)` より有効より `system.numbers`.

次のクエリは同等です:

``` sql
SELECT * FROM numbers(10);
SELECT * FROM numbers(0, 10);
SELECT * FROM system.numbers LIMIT 10;
```

例:

``` sql
-- Generate a sequence of dates from 2010-01-01 to 2010-12-31
select toDate('2010-01-01') + number as d FROM numbers(365);
```

[元の記事](https://clickhouse.tech/docs/en/query_language/table_functions/numbers/) <!--hide-->
