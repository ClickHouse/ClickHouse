---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 46
toc_title: GenerateRandom
---

# Generaterandom {#table_engines-generate}

GenerateRandom tablo motoru, verilen tablo şeması için rasgele veri üretir.

Kullanım örnekleri:

-   Tekrarlanabilir büyük tabloyu doldurmak için testte kullanın.
-   Fuzzing testleri için rastgele girdi oluşturun.

## ClickHouse sunucusunda kullanım {#usage-in-clickhouse-server}

``` sql
ENGINE = GenerateRandom(random_seed, max_string_length, max_array_length)
```

Bu `max_array_length` ve `max_string_length` parametreler tüm maksimum uzunluğu belirtin
oluşturulan verilerde dizi sütunları ve dizeleri.

Tablo motoru oluşturmak yalnızca destekler `SELECT` sorgular.

Tüm destekler [Veri türleri](../../../sql-reference/data-types/index.md) dışında bir tabloda saklanabilir `LowCardinality` ve `AggregateFunction`.

**Örnek:**

**1.** Set up the `generate_engine_table` Tablo:

``` sql
CREATE TABLE generate_engine_table (name String, value UInt32) ENGINE = GenerateRandom(1, 5, 3)
```

**2.** Verileri sorgula:

``` sql
SELECT * FROM generate_engine_table LIMIT 3
```

``` text
┌─name─┬──────value─┐
│ c4xJ │ 1412771199 │
│ r    │ 1791099446 │
│ 7#$  │  124312908 │
└──────┴────────────┘
```

## Uygulama Detayları {#details-of-implementation}

-   Desteklenmiyor:
    -   `ALTER`
    -   `SELECT ... SAMPLE`
    -   `INSERT`
    -   Dizinler
    -   Çoğalma

[Orijinal makale](https://clickhouse.tech/docs/en/operations/table_engines/generate/) <!--hide-->
