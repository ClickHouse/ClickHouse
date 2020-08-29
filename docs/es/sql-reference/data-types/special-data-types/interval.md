---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 61
toc_title: Intervalo
---

# Intervalo {#data-type-interval}

La familia de tipos de datos que representan intervalos de fecha y hora. Los tipos resultantes del [INTERVAL](../../../sql-reference/operators/index.md#operator-interval) operador.

!!! warning "Advertencia"
    `Interval` los valores de tipo de datos no se pueden almacenar en tablas.

Estructura:

-   Intervalo de tiempo como un valor entero sin signo.
-   Tipo de intervalo.

Tipos de intervalo admitidos:

-   `SECOND`
-   `MINUTE`
-   `HOUR`
-   `DAY`
-   `WEEK`
-   `MONTH`
-   `QUARTER`
-   `YEAR`

Para cada tipo de intervalo, hay un tipo de datos independiente. Por ejemplo, el `DAY` el intervalo corresponde a la `IntervalDay` tipo de datos:

``` sql
SELECT toTypeName(INTERVAL 4 DAY)
```

``` text
┌─toTypeName(toIntervalDay(4))─┐
│ IntervalDay                  │
└──────────────────────────────┘
```

## Observaciones de uso {#data-type-interval-usage-remarks}

Usted puede utilizar `Interval`-type valores en operaciones aritméticas con [Fecha](../../../sql-reference/data-types/date.md) y [FechaHora](../../../sql-reference/data-types/datetime.md)-type valores. Por ejemplo, puede agregar 4 días a la hora actual:

``` sql
SELECT now() as current_date_time, current_date_time + INTERVAL 4 DAY
```

``` text
┌───current_date_time─┬─plus(now(), toIntervalDay(4))─┐
│ 2019-10-23 10:58:45 │           2019-10-27 10:58:45 │
└─────────────────────┴───────────────────────────────┘
```

Los intervalos con diferentes tipos no se pueden combinar. No puedes usar intervalos como `4 DAY 1 HOUR`. Especifique los intervalos en unidades que son más pequeñas o iguales que la unidad más pequeña del intervalo, por ejemplo, el intervalo `1 day and an hour` se puede expresar como `25 HOUR` o `90000 SECOND`.

No puede realizar operaciones aritméticas con `Interval`-type valores, pero puede agregar intervalos de diferentes tipos en consecuencia a los valores en `Date` o `DateTime` tipos de datos. Por ejemplo:

``` sql
SELECT now() AS current_date_time, current_date_time + INTERVAL 4 DAY + INTERVAL 3 HOUR
```

``` text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay(4)), toIntervalHour(3))─┐
│ 2019-10-23 11:16:28 │                                    2019-10-27 14:16:28 │
└─────────────────────┴────────────────────────────────────────────────────────┘
```

La siguiente consulta provoca una excepción:

``` sql
select now() AS current_date_time, current_date_time + (INTERVAL 4 DAY + INTERVAL 3 HOUR)
```

``` text
Received exception from server (version 19.14.1):
Code: 43. DB::Exception: Received from localhost:9000. DB::Exception: Wrong argument types for function plus: if one argument is Interval, then another must be Date or DateTime..
```

## Ver también {#see-also}

-   [INTERVAL](../../../sql-reference/operators/index.md#operator-interval) operador
-   [ToInterval](../../../sql-reference/functions/type-conversion-functions.md#function-tointerval) funciones de conversión de tipo
