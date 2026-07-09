---
description: 'Documentación del tipo de dato Time en ClickHouse, que almacena
  una hora con precisión de segundos'
slug: /sql-reference/data-types/time
sidebar_position: 15
sidebar_label: 'Time'
title: 'Time'
doc_type: 'reference'
---

El tipo de dato `Time` representa una hora con componentes de horas, minutos y segundos.
Es independiente de cualquier fecha del calendario y resulta adecuado para valores que no requieren componentes de día, mes ni año.

Sintaxis:

```sql
Time
```

Rango de representación textual: [-999:59:59, 999:59:59].

Resolución: 1 segundo.

<div id="implementation-details">
  ## Detalles de implementación
</div>

**Representación y rendimiento**.
El tipo de dato `Time` almacena internamente un entero con signo de 32 bits que codifica los segundos.
Los valores de tipo `Time` y `DateTime` tienen el mismo tamaño en bytes y, por tanto, un rendimiento comparable.

**Normalización**.
Al convertir cadenas a `Time`, los componentes de la hora se normalizan, pero no se validan.
Por ejemplo, `25:70:70` se interpreta como `26:11:10`.

**Valores negativos**.
Se admiten y conservan los signos menos iniciales.
Los valores negativos suelen surgir de operaciones aritméticas sobre valores `Time`.
Para el tipo `Time`, las entradas negativas se conservan tanto en entradas de texto (p. ej., `'-01:02:03'`) como numéricas (p. ej., `-3723`).

**Saturación**.
El componente de hora del día se limita al rango [-999:59:59, 999:59:59].
Los valores con horas superiores a 999 (o inferiores a -999) se representan y pueden convertirse de ida y vuelta mediante texto como `999:59:59` (o `-999:59:59`).

**Zonas horarias**.
`Time` no admite zonas horarias; es decir, los valores `Time` se interpretan sin contexto regional.
Especificar una zona horaria para `Time` como parámetro de tipo o durante la creación del valor genera un error.
Del mismo modo, no se admiten los intentos de aplicar o cambiar la zona horaria en columnas `Time`, y generan un error.
Los valores `Time` no se reinterpretan silenciosamente con distintas zonas horarias.

<div id="examples">
  ## Ejemplos
</div>

**1.** Creación de una tabla con una columna de tipo `Time` e inserción de datos:

```sql
CREATE TABLE tab
(
    `event_id` UInt8,
    `time` Time
)
ENGINE = TinyLog;
```

```sql
-- Parse Time
-- - from string,
-- - from integer interpreted as number of seconds since 00:00:00.
INSERT INTO tab VALUES (1, '14:30:25'), (2, 52225);

SELECT * FROM tab ORDER BY event_id;
```

```text
   ┌─event_id─┬──────time─┐
1. │        1 │ 14:30:25 │
2. │        2 │ 14:30:25 │
   └──────────┴───────────┘
```

**2.** Filtrado por valores de `Time`

```sql
SET use_legacy_to_time = 0;
SELECT * FROM tab WHERE time = toTime('14:30:25')
```

```text
   ┌─event_id─┬──────time─┐
1. │        1 │ 14:30:25 │
2. │        2 │ 14:30:25 │
   └──────────┴───────────┘
```

Los valores de la columna `Time` se pueden filtrar mediante una cadena de texto en el predicado `WHERE`. Esta se convertirá automáticamente a `Time`:

```sql
SELECT * FROM tab WHERE time = '14:30:25'
```

```text
   ┌─event_id─┬──────time─┐
1. │        1 │ 14:30:25 │
2. │        2 │ 14:30:25 │
   └──────────┴───────────┘
```

**3.** Inspección del tipo resultante:

```sql
SELECT CAST('14:30:25' AS Time) AS column, toTypeName(column) AS type
```

```text
   ┌────column─┬─type─┐
1. │ 14:30:25 │ Time │
   └───────────┴──────┘
```

<div id="addition-with-date">
  ## Suma con Date
</div>

Se puede sumar un valor [Time](time.md) a un valor [Date](date.md) o [Date32](date32.md) para obtener un [DateTime](datetime.md) o [DateTime64](datetime64.md):

```sql
SET use_legacy_to_time = 0;
SELECT toDate('2024-07-15') + toTime('14:30:25') as datetime;
```

```text
   ┌────────────datetime─┐
1. │ 2024-07-15 14:30:25 │
   └─────────────────────┘
```

Consulte [Suma de fecha y hora](../operators/index.md#date-time-addition) para obtener más detalles sobre todas las combinaciones compatibles y los tipos de resultado.

<div id="see-also">
  ## Véase también
</div>

* [Funciones de conversión de tipos](../functions/type-conversion-functions.md)
* [Funciones para trabajar con fechas y horas](../functions/date-time-functions.md)
* [Funciones para trabajar con arrays](../functions/array-functions.md)
* [La opción `date_time_input_format`](../../operations/settings/settings-formats.md#date_time_input_format)
* [La opción `date_time_output_format`](../../operations/settings/settings-formats.md#date_time_output_format)
* [El parámetro de configuración del servidor `timezone`](../../operations/server-configuration-parameters/settings.md#timezone)
* [La opción `session_timezone`](../../operations/settings/settings.md#session_timezone)
* [El tipo de dato `DateTime`](datetime.md)
* [El tipo de dato `Date`](date.md)