---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 51
toc_title: Dizi(T)
---

# Dizi(t) {#data-type-array}

Bir dizi `T`- tip öğeleri. `T` herhangi bir veri türü, bir dizi dahil edilebilir.

## Bir dizi oluşturma {#creating-an-array}

Bir dizi oluşturmak için bir işlev kullanabilirsiniz:

``` sql
array(T)
```

Köşeli parantez de kullanabilirsiniz.

``` sql
[]
```

Bir dizi oluşturma örneği:

``` sql
SELECT array(1, 2) AS x, toTypeName(x)
```

``` text
┌─x─────┬─toTypeName(array(1, 2))─┐
│ [1,2] │ Array(UInt8)            │
└───────┴─────────────────────────┘
```

``` sql
SELECT [1, 2] AS x, toTypeName(x)
```

``` text
┌─x─────┬─toTypeName([1, 2])─┐
│ [1,2] │ Array(UInt8)       │
└───────┴────────────────────┘
```

## Veri türleri ile çalışma {#working-with-data-types}

Anında bir dizi oluştururken, ClickHouse bağımsız değişken türünü otomatik olarak listelenen tüm bağımsız değişkenleri depolayabilen en dar veri türü olarak tanımlar. Eğer herhangi bir [Nullable](nullable.md#data_type-nullable) veya edebi [NULL](../../sql-reference/syntax.md#null-literal) değerler, bir dizi öğesinin türü de olur [Nullable](nullable.md).

ClickHouse veri türünü belirleyemedi, bir özel durum oluşturur. Örneğin, aynı anda dizeler ve sayılarla bir dizi oluşturmaya çalışırken bu olur (`SELECT array(1, 'a')`).

Otomatik veri türü algılama örnekleri:

``` sql
SELECT array(1, 2, NULL) AS x, toTypeName(x)
```

``` text
┌─x──────────┬─toTypeName(array(1, 2, NULL))─┐
│ [1,2,NULL] │ Array(Nullable(UInt8))        │
└────────────┴───────────────────────────────┘
```

Uyumsuz veri türleri dizisi oluşturmaya çalışırsanız, ClickHouse bir özel durum atar:

``` sql
SELECT array(1, 'a')
```

``` text
Received exception from server (version 1.1.54388):
Code: 386. DB::Exception: Received from localhost:9000, 127.0.0.1. DB::Exception: There is no supertype for types UInt8, String because some of them are String/FixedString and some of them are not.
```

[Orijinal makale](https://clickhouse.tech/docs/en/data_types/array/) <!--hide-->
