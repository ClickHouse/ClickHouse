---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 53
toc_title: Tuple (T1, T2, ...)
---

# Tuple(t1, T2, …) {#tuplet1-t2}

Elemanlarının bir demet, her bir birey olması [tür](index.md#data_types).

Tuples geçici sütun gruplama için kullanılır. Sütunlar, bir sorguda bir In ifadesi kullanıldığında ve lambda işlevlerinin belirli resmi parametrelerini belirtmek için gruplandırılabilir. Daha fazla bilgi için bölümlere bakın [Operatör İNLERDE](../../sql-reference/operators/in.md) ve [Yüksek mertebeden fonksiyonlar](../../sql-reference/functions/higher-order-functions.md).

Tuples bir sorgunun sonucu olabilir. Bu durumda, json dışındaki metin formatları için değerler köşeli parantez içinde virgülle ayrılır. JSON formatlarında, tuples diziler olarak çıktılanır (köşeli parantez içinde).

## Bir Tuple oluşturma {#creating-a-tuple}

Bir tuple oluşturmak için bir işlev kullanabilirsiniz:

``` sql
tuple(T1, T2, ...)
```

Bir tuple oluşturma örneği:

``` sql
SELECT tuple(1,'a') AS x, toTypeName(x)
```

``` text
┌─x───────┬─toTypeName(tuple(1, 'a'))─┐
│ (1,'a') │ Tuple(UInt8, String)      │
└─────────┴───────────────────────────┘
```

## Veri türleri ile çalışma {#working-with-data-types}

Anında bir tuple oluştururken, ClickHouse her bağımsız değişkenin türünü bağımsız değişken değerini depolayabilen türlerin en azı olarak otomatik olarak algılar. Argüman ise [NULL](../../sql-reference/syntax.md#null-literal), tuple elemanının türü [Nullable](nullable.md).

Otomatik veri türü algılama örneği:

``` sql
SELECT tuple(1, NULL) AS x, toTypeName(x)
```

``` text
┌─x────────┬─toTypeName(tuple(1, NULL))──────┐
│ (1,NULL) │ Tuple(UInt8, Nullable(Nothing)) │
└──────────┴─────────────────────────────────┘
```

[Orijinal makale](https://clickhouse.tech/docs/en/data_types/tuple/) <!--hide-->
