---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 61
toc_title: "Aral\u0131kl\u0131"
---

# Aralıklı {#data-type-interval}

Zaman ve Tarih aralıklarını temsil eden veri türleri ailesi. Ortaya çıkan türleri [INTERVAL](../../../sql-reference/operators/index.md#operator-interval) operatör.

!!! warning "Uyarıcı"
    `Interval` veri türü değerleri tablolarda saklanamaz.

Yapılı:

-   İmzasız bir tamsayı değeri olarak zaman aralığı.
-   Bir aralık türü.

Desteklenen Aralık türleri:

-   `SECOND`
-   `MINUTE`
-   `HOUR`
-   `DAY`
-   `WEEK`
-   `MONTH`
-   `QUARTER`
-   `YEAR`

Her Aralık türü için ayrı bir veri türü vardır. Örneğin, `DAY` Aralık karşılık gelir `IntervalDay` veri türü:

``` sql
SELECT toTypeName(INTERVAL 4 DAY)
```

``` text
┌─toTypeName(toIntervalDay(4))─┐
│ IntervalDay                  │
└──────────────────────────────┘
```

## Kullanım Açıklamaları {#data-type-interval-usage-remarks}

Kullanabilirsiniz `Interval`- aritmetik işlemlerde değerler yazın [Tarihli](../../../sql-reference/data-types/date.md) ve [DateTime](../../../sql-reference/data-types/datetime.md)- tip değerleri. Örneğin, geçerli saate 4 gün ekleyebilirsiniz:

``` sql
SELECT now() as current_date_time, current_date_time + INTERVAL 4 DAY
```

``` text
┌───current_date_time─┬─plus(now(), toIntervalDay(4))─┐
│ 2019-10-23 10:58:45 │           2019-10-27 10:58:45 │
└─────────────────────┴───────────────────────────────┘
```

Farklı tiplere sahip aralıklar birleştirilemez. Gibi aralıklarla kullanamazsınız `4 DAY 1 HOUR`. Aralıkların, örneğin aralığın en küçük birimine eşit veya daha küçük olan birimlerdeki aralıkları belirtin `1 day and an hour` aralık olarak ifade edilebilir `25 HOUR` veya `90000 SECOND`.

İle aritmetik işlemler yapamazsınız `Interval`- değerleri yazın, ancak farklı türde aralıklar ekleyebilirsiniz. `Date` veya `DateTime` veri türleri. Mesela:

``` sql
SELECT now() AS current_date_time, current_date_time + INTERVAL 4 DAY + INTERVAL 3 HOUR
```

``` text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay(4)), toIntervalHour(3))─┐
│ 2019-10-23 11:16:28 │                                    2019-10-27 14:16:28 │
└─────────────────────┴────────────────────────────────────────────────────────┘
```

Aşağıdaki sorgu bir özel duruma neden olur:

``` sql
select now() AS current_date_time, current_date_time + (INTERVAL 4 DAY + INTERVAL 3 HOUR)
```

``` text
Received exception from server (version 19.14.1):
Code: 43. DB::Exception: Received from localhost:9000. DB::Exception: Wrong argument types for function plus: if one argument is Interval, then another must be Date or DateTime..
```

## Ayrıca Bakınız {#see-also}

-   [INTERVAL](../../../sql-reference/operators/index.md#operator-interval) operatör
-   [toİnterval](../../../sql-reference/functions/type-conversion-functions.md#function-tointerval) tip dönüştürme işlevleri
