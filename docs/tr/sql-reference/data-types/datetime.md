---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 48
toc_title: DateTime
---

# Datetime {#data_type-datetime}

Bir takvim tarih ve bir günün bir saat olarak ifade edilebilir, zaman içinde bir anlık saklamak için izin verir.

Sözdizimi:

``` sql
DateTime([timezone])
```

Desteklenen değerler aralığı: \[1970-01-01 00:00:00, 2105-12-31 23:59:59\].

Çözünürlük: 1 saniye.

## Kullanım Açıklamaları {#usage-remarks}

Zaman içindeki nokta bir [Unix zaman damgası](https://en.wikipedia.org/wiki/Unix_time), ne olursa olsun saat dilimi veya gün ışığından yararlanma saati. Ayrıca, `DateTime` tür, tüm sütun için aynı olan saat dilimini depolayabilir, bu da `DateTime` tür değerleri metin biçiminde görüntülenir ve dizeler olarak belirtilen değerlerin nasıl ayrıştırılır (‘2020-01-01 05:00:01’). Saat dilimi tablo (veya resultset) satırlarında depolanır, ancak sütun meta verileri depolanır.
Desteklenen saat dilimlerinin bir listesi şu adreste bulunabilir: [IANA Saat Dilimi veritabanı](https://www.iana.org/time-zones).
Bu `tzdata` paket, içeren [IANA Saat Dilimi veritabanı](https://www.iana.org/time-zones), sisteme Kurul .malıdır. Kullan... `timedatectl list-timezones` yerel bir sistem tarafından bilinen zaman dilimlerini listelemek için komut.

İçin bir saat dilimi açıkça ayarlayabilirsiniz `DateTime`- bir tablo oluştururken sütunları yazın. Saat dilimi ayarlanmamışsa, ClickHouse değerini kullanır [saat dilimi](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) sunucu ayarlarında veya ClickHouse sunucusunun başlatıldığı anda işletim sistemi ayarlarında parametre.

Bu [clickhouse-müşteri](../../interfaces/cli.md) veri türünü başlatırken bir saat dilimi açıkça ayarlanmamışsa, sunucu saat dilimini varsayılan olarak uygular. İstemci saat dilimini kullanmak için `clickhouse-client` ile... `--use_client_time_zone` parametre.

ClickHouse çıkış değerleri `YYYY-MM-DD hh:mm:ss` varsayılan olarak metin biçimi. Çıkış ile değiştirebilirsiniz [formatDateTime](../../sql-reference/functions/date-time-functions.md#formatdatetime) İşlev.

Clickhouse'a veri eklerken, Tarih ve saat dizelerinin farklı biçimlerini kullanabilirsiniz. [date\_time\_input\_format](../../operations/settings/settings.md#settings-date_time_input_format) ayar.

## Örnekler {#examples}

**1.** Bir tablo ile bir tablo oluşturma `DateTime`- sütun yazın ve içine veri ekleme:

``` sql
CREATE TABLE dt
(
    `timestamp` DateTime('Europe/Moscow'),
    `event_id` UInt8
)
ENGINE = TinyLog;
```

``` sql
INSERT INTO dt Values (1546300800, 1), ('2019-01-01 00:00:00', 2);
```

``` sql
SELECT * FROM dt;
```

``` text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00 │        1 │
│ 2019-01-01 00:00:00 │        2 │
└─────────────────────┴──────────┘
```

-   Bir tamsayı olarak datetime eklerken, Unıx Zaman Damgası (UTC) olarak kabul edilir. `1546300800` temsil etmek `'2019-01-01 00:00:00'` UTC. Ancak, `timestamp` sütun vardır `Europe/Moscow` (UTC+3) belirtilen zaman dilimi, dize olarak çıkış yaparken değer olarak gösterilecektir `'2019-01-01 03:00:00'`
-   Dize değerini datetime olarak eklerken, sütun saat diliminde olduğu kabul edilir. `'2019-01-01 00:00:00'` will gibi muamele `Europe/Moscow` saat dilimi ve farklı kaydedildi `1546290000`.

**2.** Üzerinde filtreleme `DateTime` değerler

``` sql
SELECT * FROM dt WHERE timestamp = toDateTime('2019-01-01 00:00:00', 'Europe/Moscow')
```

``` text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00 │        2 │
└─────────────────────┴──────────┘
```

`DateTime` sütun değerleri, bir dize değeri kullanılarak filtrelenebilir `WHERE` yüklem. Dönüştürül willecektir `DateTime` otomatik olarak:

``` sql
SELECT * FROM dt WHERE timestamp = '2019-01-01 00:00:00'
```

``` text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00 │        1 │
└─────────────────────┴──────────┘
```

**3.** Bir saat dilimi almak `DateTime`- type Col columnum columnn:

``` sql
SELECT toDateTime(now(), 'Europe/Moscow') AS column, toTypeName(column) AS x
```

``` text
┌──────────────column─┬─x─────────────────────────┐
│ 2019-10-16 04:12:04 │ DateTime('Europe/Moscow') │
└─────────────────────┴───────────────────────────┘
```

**4.** Zaman dilimi dönüştürme

``` sql
SELECT
toDateTime(timestamp, 'Europe/London') as lon_time,
toDateTime(timestamp, 'Europe/Moscow') as mos_time
FROM dt
```

``` text
┌───────────lon_time──┬────────────mos_time─┐
│ 2019-01-01 00:00:00 │ 2019-01-01 03:00:00 │
│ 2018-12-31 21:00:00 │ 2019-01-01 00:00:00 │
└─────────────────────┴─────────────────────┘
```

## Ayrıca Bakınız {#see-also}

-   [Tip dönüştürme fonksiyonları](../../sql-reference/functions/type-conversion-functions.md)
-   [Tarih ve saatlerle çalışmak için işlevler](../../sql-reference/functions/date-time-functions.md)
-   [Dizilerle çalışmak için işlevler](../../sql-reference/functions/array-functions.md)
-   [Bu `date_time_input_format` ayar](../../operations/settings/settings.md#settings-date_time_input_format)
-   [Bu `timezone` sunucu yapılandırma parametresi](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone)
-   [Tarih ve saatlerle çalışmak için operatörler](../../sql-reference/operators/index.md#operators-datetime)
-   [Bu `Date` veri türü](date.md)

[Orijinal makale](https://clickhouse.tech/docs/en/data_types/datetime/) <!--hide-->
