---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 45
toc_title: FixedString(N)
---

# Fixedstring {#fixedstring}

Sabit uzunlukta bir dize `N` bayt (ne karakter ne de kod noktaları).

Bir sütun bildirmek için `FixedString` yazın, aşağıdaki sözdizimini kullanın:

``` sql
<column_name> FixedString(N)
```

Nerede `N` doğal bir sayıdır.

Bu `FixedString` veri tam olarak uzunluğa sahip olduğunda tür etkilidir `N` baytlar. Diğer tüm durumlarda, verimliliği düşürmesi muhtemeldir.

Verimli bir şekilde depolan theabilen değerlere örnekler `FixedString`- yazılan sütunlar:

-   IP adreslerinin ikili gösterimi (`FixedString(16)` IPv6 için).
-   Language codes (ru_RU, en_US … ).
-   Currency codes (USD, RUB … ).
-   Karma ikili gösterimi (`FixedString(16)` MD5 için, `FixedString(32)` SHA256 için).

UUID değerlerini depolamak için [UUID](uuid.md) veri türü.

Verileri eklerken, ClickHouse:

-   Dize daha az içeriyorsa, boş bayt ile bir dize tamamlar `N` baytlar.
-   Atar `Too large value for FixedString(N)` dize birden fazla içeriyorsa, özel durum `N` baytlar.

Verileri seçerken, ClickHouse dize sonunda boş bayt kaldırmaz. Eğer kullanıyorsanız `WHERE` yan tümcesi, null bayt el ile eşleştirmek için eklemelisiniz `FixedString` değer. Kullanımı için aşağıdaki örnek, nasıl gösterir `WHERE` fık withra ile `FixedString`.

Aşağıdaki tabloyu tek ile düşünelim `FixedString(2)` sütun:

``` text
┌─name──┐
│ b     │
└───────┘
```

Sorgu `SELECT * FROM FixedStringTable WHERE a = 'b'` sonuç olarak herhangi bir veri döndürmez. Filtre desenini boş baytlarla tamamlamalıyız.

``` sql
SELECT * FROM FixedStringTable
WHERE a = 'b\0'
```

``` text
┌─a─┐
│ b │
└───┘
```

Bu davranış için MySQL farklıdır `CHAR` tür (burada dizeler boşluklarla doldurulur ve boşluklar çıktı için kaldırılır).

Not uzunluğu `FixedString(N)` değer sabittir. Bu [uzunluk](../../sql-reference/functions/array-functions.md#array_functions-length) fonksiyon döndürür `N` hatta eğer `FixedString(N)` değer yalnızca boş baytlarla doldurulur, ancak [boş](../../sql-reference/functions/string-functions.md#empty) fonksiyon döndürür `1` bu durumda.

[Orijinal makale](https://clickhouse.tech/docs/en/data_types/fixedstring/) <!--hide-->
