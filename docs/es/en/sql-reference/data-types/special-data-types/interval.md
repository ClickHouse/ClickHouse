---
description: 'Documentación del tipo de dato especial Interval'
sidebar_label: 'Interval'
sidebar_position: 61
slug: /sql-reference/data-types/special-data-types/interval
title: 'Interval'
doc_type: 'reference'
---

La familia de tipos de datos que representa intervalos de tiempo y fecha. Los tipos resultantes del operador [INTERVAL](/es/sql-reference/operators#interval).

Estructura:

* Intervalo de tiempo como valor entero sin signo.
* Tipo de intervalo.

Tipos de intervalo compatibles:

* `NANOSECOND`
* `MICROSECOND`
* `MILLISECOND`
* `SECOND`
* `MINUTE`
* `HOUR`
* `DAY`
* `WEEK`
* `MONTH`
* `QUARTER`
* `YEAR`

Para cada tipo de intervalo, existe un tipo de dato independiente. Por ejemplo, el intervalo `DAY` corresponde al tipo de dato `IntervalDay`:

```sql
SELECT toTypeName(INTERVAL 4 DAY)
```

```text
┌─toTypeName(toIntervalDay(4))─┐
│ IntervalDay                  │
└──────────────────────────────┘
```

<div id="usage-remarks">
  ## Observaciones sobre el uso
</div>

Puede usar valores del tipo `Interval` en operaciones aritméticas con valores de tipo [Date](../../../sql-reference/data-types/date.md) y [DateTime](../../../sql-reference/data-types/datetime.md). Por ejemplo, puede sumar 4 días a la fecha y hora actuales:

```sql
SELECT now() AS current_date_time, current_date_time + INTERVAL 4 DAY
```

```text
┌───current_date_time─┬─plus(now(), toIntervalDay(4))─┐
│ 2019-10-23 10:58:45 │           2019-10-27 10:58:45 │
└─────────────────────┴───────────────────────────────┘
```

También es posible utilizar varios intervalos a la vez:

```sql
SELECT now() AS current_date_time, current_date_time + (INTERVAL 4 DAY + INTERVAL 3 HOUR)
```

```text
┌───current_date_time─┬─plus(current_date_time, plus(toIntervalDay(4), toIntervalHour(3)))─┐
│ 2024-08-08 18:31:39 │                                                2024-08-12 21:31:39 │
└─────────────────────┴────────────────────────────────────────────────────────────────────┘
```

Y para comparar valores con diferentes intervalos:

```sql
SELECT toIntervalMicrosecond(179999999) < toIntervalMinute(3);
```

```text
┌─less(toIntervalMicrosecond(179999999), toIntervalMinute(3))─┐
│                                                           1 │
└─────────────────────────────────────────────────────────────┘
```

```sql
SELECT toIntervalMicrosecond(3600000000) = toIntervalHour(1);
```

```text
┌─equals(toIntervalMicrosecond(3600000000), toIntervalHour(1))─┐
│                                                            1 │
└──────────────────────────────────────────────────────────────┘
```

<div id="mixed-type-intervals">
  ## Intervalos de tipo mixto
</div>

Los intervalos de tipo mixto, por ejemplo, varias horas y varios minutos, pueden crearse con la sintaxis `INTERVAL 'value' <from_kind> TO <to_kind>`.
El resultado es una tupla de dos o más intervalos.

Combinaciones admitidas:

| Sintaxis           | Formato de cadena | Ejemplo                               |
| ------------------ | ----------------- | ------------------------------------- |
| `YEAR TO MONTH`    | `Y-M`             | `INTERVAL '2-6' YEAR TO MONTH`        |
| `DAY TO HOUR`      | `D H`             | `INTERVAL '5 12' DAY TO HOUR`         |
| `DAY TO MINUTE`    | `D H:M`           | `INTERVAL '5 12:30' DAY TO MINUTE`    |
| `DAY TO SECOND`    | `D H:M:S`         | `INTERVAL '5 12:30:45' DAY TO SECOND` |
| `HOUR TO MINUTE`   | `H:M`             | `INTERVAL '1:30' HOUR TO MINUTE`      |
| `HOUR TO SECOND`   | `H:M:S`           | `INTERVAL '1:30:45' HOUR TO SECOND`   |
| `MINUTE TO SECOND` | `M:S`             | `INTERVAL '5:30' MINUTE TO SECOND`    |

Los campos que no son el inicial se validan según el estándar SQL: `MONTH` 0-11, `HOUR` 0-23, `MINUTE` 0-59, `SECOND` 0-59.

```sql
SELECT INTERVAL '1:30' HOUR TO MINUTE;
```

```text
┌─(toIntervalHour(1), toIntervalMinute(30))─┐
│ (1,30)                                     │
└────────────────────────────────────────────┘
```

Un signo opcional `+` o `-` al principio se aplica a todos los componentes:

```sql
SELECT INTERVAL '+1:30' HOUR TO MINUTE;
-- this is equivalent to:
-- SELECT INTERVAL '1:30' HOUR TO MINUTE;
```

```text
┌─(toIntervalHour(1), toIntervalMinute(30))─┐
│ (1,30)                                     │
└────────────────────────────────────────────┘
```

<div id="see-also">
  ## Véase también
</div>

* operador [INTERVAL](/es/sql-reference/operators#interval)
* funciones de conversión de tipo [toInterval](/es/sql-reference/functions/type-conversion-functions#toIntervalYear)