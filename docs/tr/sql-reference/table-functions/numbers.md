---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 39
toc_title: "\u015Fiir"
---

# şiir {#numbers}

`numbers(N)` – Returns a table with the single ‘number’ 0'dan n-1'e kadar tamsayılar içeren sütun (Uİnt64).
`numbers(N, M)` - Tek bir tablo döndürür ‘number’ n'den (n + M - 1) tamsayıları içeren sütun (Uİnt64).

Benzer `system.numbers` tablo, ardışık değerleri test etmek ve üretmek için kullanılabilir, `numbers(N, M)` daha verimli `system.numbers`.

Aşağıdaki sorgular eşdeğerdir:

``` sql
SELECT * FROM numbers(10);
SELECT * FROM numbers(0, 10);
SELECT * FROM system.numbers LIMIT 10;
```

Örnekler:

``` sql
-- Generate a sequence of dates from 2010-01-01 to 2010-12-31
select toDate('2010-01-01') + number as d FROM numbers(365);
```

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/table_functions/numbers/) <!--hide-->
