---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 49
toc_title: DateTime64
---

# Datetime64 {#data_type-datetime64}

Tanımlanmış alt saniye hassasiyetle, bir takvim tarihi ve bir günün saati olarak ifade edilebilir, zaman içinde bir anlık saklamak için izin verir

Kene boyutu (hassas): 10<sup>-hassaslık</sup> ikincilikler

Sözdizimi:

``` sql
DateTime64(precision, [timezone])
```

DAHİLİ olarak, verileri bir dizi olarak saklar ‘ticks’ epoch başlangıçtan beri (1970-01-01 00:00:00 UTC) Int64 olarak. Kene çözünürlüğü hassasiyet parametresi tarafından belirlenir. Ayrıca, `DateTime64` tür, tüm sütun için aynı olan saat dilimini depolayabilir, bu da `DateTime64` tür değerleri metin biçiminde görüntülenir ve dizeler olarak belirtilen değerlerin nasıl ayrıştırılır (‘2020-01-01 05:00:01.000’). Saat dilimi tablo (veya resultset) satırlarında depolanır, ancak sütun meta verileri depolanır. Ayrıntıları görün [DateTime](datetime.md).

## Örnekler {#examples}

**1.** İle bir tablo oluşturma `DateTime64`- sütun yazın ve içine veri ekleme:

``` sql
CREATE TABLE dt
(
    `timestamp` DateTime64(3, 'Europe/Moscow'),
    `event_id` UInt8
)
ENGINE = TinyLog
```

``` sql
INSERT INTO dt Values (1546300800000, 1), ('2019-01-01 00:00:00', 2)
```

``` sql
SELECT * FROM dt
```

``` text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00.000 │        1 │
│ 2019-01-01 00:00:00.000 │        2 │
└─────────────────────────┴──────────┘
```

-   Bir tamsayı olarak datetime eklerken, uygun şekilde ölçeklendirilmiş bir Unıx Zaman Damgası (UTC) olarak kabul edilir. `1546300800000` (hassas 3 ile) temsil eder `'2019-01-01 00:00:00'` UTC. Ancak, `timestamp` sütun vardır `Europe/Moscow` (UTC+3) belirtilen zaman dilimi, bir dize olarak çıkış yaparken değer olarak gösterilir `'2019-01-01 03:00:00'`
-   Dize değerini datetime olarak eklerken, sütun saat diliminde olduğu kabul edilir. `'2019-01-01 00:00:00'` will gibi muamele `Europe/Moscow` saat dilimi ve olarak saklanır `1546290000000`.

**2.** Üzerinde filtreleme `DateTime64` değerler

``` sql
SELECT * FROM dt WHERE timestamp = toDateTime64('2019-01-01 00:00:00', 3, 'Europe/Moscow')
```

``` text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00.000 │        2 │
└─────────────────────────┴──────────┘
```

Aksine `DateTime`, `DateTime64` değerler dönüştürülmez `String` otomatik olarak

**3.** Bir saat dilimi almak `DateTime64`- tip değeri:

``` sql
SELECT toDateTime64(now(), 3, 'Europe/Moscow') AS column, toTypeName(column) AS x
```

``` text
┌──────────────────column─┬─x──────────────────────────────┐
│ 2019-10-16 04:12:04.000 │ DateTime64(3, 'Europe/Moscow') │
└─────────────────────────┴────────────────────────────────┘
```

**4.** Zaman dilimi dönüştürme

``` sql
SELECT
toDateTime64(timestamp, 3, 'Europe/London') as lon_time,
toDateTime64(timestamp, 3, 'Europe/Moscow') as mos_time
FROM dt
```

``` text
┌───────────────lon_time──┬────────────────mos_time─┐
│ 2019-01-01 00:00:00.000 │ 2019-01-01 03:00:00.000 │
│ 2018-12-31 21:00:00.000 │ 2019-01-01 00:00:00.000 │
└─────────────────────────┴─────────────────────────┘
```

## Ayrıca Bakınız {#see-also}

-   [Tip dönüştürme fonksiyonları](../../sql-reference/functions/type-conversion-functions.md)
-   [Tarih ve saatlerle çalışmak için işlevler](../../sql-reference/functions/date-time-functions.md)
-   [Dizilerle çalışmak için işlevler](../../sql-reference/functions/array-functions.md)
-   [Bu `date_time_input_format` ayar](../../operations/settings/settings.md#settings-date_time_input_format)
-   [Bu `timezone` sunucu yapılandırma parametresi](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone)
-   [Tarih ve saatlerle çalışmak için operatörler](../../sql-reference/operators/index.md#operators-datetime)
-   [`Date` veri türü](date.md)
-   [`DateTime` veri türü](datetime.md)
