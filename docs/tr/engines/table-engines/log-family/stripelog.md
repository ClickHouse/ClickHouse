---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 32
toc_title: StripeLog
---

# Stripelog {#stripelog}

Bu motor günlük motor ailesine aittir. Günlük motorlarının ortak özelliklerini ve farklılıklarını görün [Log Engine Ailesi](index.md) Makale.

Az miktarda veri içeren (1 milyondan az satır) birçok tablo yazmanız gerektiğinde, bu altyapıyı senaryolarda kullanın.

## Tablo oluşturma {#table_engines-stripelog-creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    column1_name [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    column2_name [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = StripeLog
```

Ayrıntılı açıklamasına bakın [CREATE TABLE](../../../sql-reference/statements/create.md#create-table-query) sorgu.

## Veri yazma {#table_engines-stripelog-writing-the-data}

Bu `StripeLog` motor tüm sütunları tek bir dosyada saklar. Her biri için `INSERT` sorgu, ClickHouse veri bloğunu bir tablo dosyasının sonuna ekler, sütunları tek tek yazar.

Her tablo için ClickHouse dosyaları yazar:

-   `data.bin` — Data file.
-   `index.mrk` — File with marks. Marks contain offsets for each column of each data block inserted.

Bu `StripeLog` motor desteklemiyor `ALTER UPDATE` ve `ALTER DELETE` harekat.

## Verileri okuma {#table_engines-stripelog-reading-the-data}

İşaretli dosya, Clickhouse'un verilerin okunmasını paralelleştirmesine izin verir. Bu demektir `SELECT` sorgu satırları öngörülemeyen bir sırayla döndürür. Kullan... `ORDER BY` satırları sıralamak için yan tümce.

## Kullanım örneği {#table_engines-stripelog-example-of-use}

Tablo oluşturma:

``` sql
CREATE TABLE stripe_log_table
(
    timestamp DateTime,
    message_type String,
    message String
)
ENGINE = StripeLog
```

Veri ekleme:

``` sql
INSERT INTO stripe_log_table VALUES (now(),'REGULAR','The first regular message')
INSERT INTO stripe_log_table VALUES (now(),'REGULAR','The second regular message'),(now(),'WARNING','The first warning message')
```

İki kullandık `INSERT` içinde iki veri bloğu oluşturmak için sorgular `data.bin` Dosya.

ClickHouse veri seçerken birden çok iş parçacığı kullanır. Her iş parçacığı ayrı bir veri bloğu okur ve sonuç olarak satırları bağımsız olarak döndürür. Sonuç olarak, çıktıdaki satır bloklarının sırası, çoğu durumda girişteki aynı blokların sırasına uymuyor. Mesela:

``` sql
SELECT * FROM stripe_log_table
```

``` text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
┌───────────timestamp─┬─message_type─┬─message───────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message │
└─────────────────────┴──────────────┴───────────────────────────┘
```

Sonuçları sıralama (varsayılan olarak artan sipariş):

``` sql
SELECT * FROM stripe_log_table ORDER BY timestamp
```

``` text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message  │
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
```

[Orijinal makale](https://clickhouse.tech/docs/en/operations/table_engines/stripelog/) <!--hide-->
