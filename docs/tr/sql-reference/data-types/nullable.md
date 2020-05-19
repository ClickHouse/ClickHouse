---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 54
toc_title: Nullable
---

# Nullable (typename) {#data_type-nullable}

Özel işaretleyici saklamak için izin verir ([NULL](../../sql-reference/syntax.md)) bu ifade eder “missing value” tarafından izin verilen normal değerlerin yanında `TypeName`. Örneğin, bir `Nullable(Int8)` tipi sütun saklayabilirsiniz `Int8` değerleri yazın ve değeri olmayan satırlar depolayacaktır `NULL`.

İçin... `TypeName`, bileşik veri türlerini kullanamazsınız [Dizi](array.md) ve [Demet](tuple.md). Bileşik veri türleri şunları içerebilir `Nullable` gibi tür değerleri `Array(Nullable(Int8))`.

A `Nullable` tür alanı tablo dizinlerine dahil edilemez.

`NULL` herhangi biri için varsayılan değer mi `Nullable` ClickHouse sunucu yapılandırmasında aksi belirtilmediği sürece yazın.

## Depolama Özellikleri {#storage-features}

İçermek `Nullable` bir tablo sütunundaki değerleri yazın, ClickHouse ile ayrı bir dosya kullanır `NULL` değerleri ile normal dosyaya ek olarak Maskeler. Maskeli girişleri ClickHouse ayırt izin dosyası `NULL` ve her tablo satırı için karşılık gelen veri türünün varsayılan değeri. Ek bir dosya nedeniyle, `Nullable` sütun, benzer bir normal olana kıyasla ek depolama alanı tüketir.

!!! info "Not"
    Kullanım `Nullable` neredeyse her zaman performansı olumsuz etkiler, veritabanlarınızı tasarlarken bunu aklınızda bulundurun.

## Kullanım Örneği {#usage-example}

``` sql
CREATE TABLE t_null(x Int8, y Nullable(Int8)) ENGINE TinyLog
```

``` sql
INSERT INTO t_null VALUES (1, NULL), (2, 3)
```

``` sql
SELECT x + y FROM t_null
```

``` text
┌─plus(x, y)─┐
│       ᴺᵁᴸᴸ │
│          5 │
└────────────┘
```

[Orijinal makale](https://clickhouse.tech/docs/en/data_types/nullable/) <!--hide-->
