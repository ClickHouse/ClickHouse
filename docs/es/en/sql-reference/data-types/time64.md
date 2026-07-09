---
description: 'Documentación del tipo de dato Time64 en ClickHouse, que almacena
  la hora con precisión de fracciones de segundo'
slug: /sql-reference/data-types/time64
sidebar_position: 17
sidebar_label: 'Time64'
title: 'Time64'
doc_type: 'reference'
---

El tipo de dato `Time64` representa una hora del día con segundos fraccionarios.
No tiene componentes de fecha del calendario (día, mes, año).
El parámetro `precision` define el número de dígitos fraccionarios y, por lo tanto, el tamaño del tick.

Tamaño del tick (precisión): 10<sup>-precision</sup> segundos. Rango válido: 0..9. Las opciones más comunes son 3 (milisegundos), 6 (microsegundos) y 9 (nanosegundos).

**Sintaxis:**

```sql
Time64(precision)
```

Internamente, `Time64` almacena un número decimal con signo de 64 bits (Decimal64) de segundos fraccionarios.
La resolución del tick viene determinada por el parámetro `precision`.
Las zonas horarias no son compatibles: especificar una zona horaria con `Time64` generará un error.

A diferencia de `DateTime64`, `Time64` no almacena un componente de fecha.
Véase también [`Time`](../../sql-reference/data-types/time.md).

Rango de representación textual: [-999:59:59.000, 999:59:59.999] para `precision = 3`. En general, el mínimo es `-999:59:59` y el máximo es `999:59:59`, con hasta `precision` dígitos fraccionarios (para `precision = 9`, el mínimo es `-999:59:59.999999999`).

<div id="implementation-details">
  ## Detalles de implementación
</div>

**Representación**.
Valor `Decimal64` con signo que cuenta fracciones de segundo con `precision` dígitos fraccionarios.

**Normalización**.
Al interpretar cadenas como `Time64`, los componentes de tiempo se normalizan y no se validan.
Por ejemplo, `25:70:70` se interpreta como `26:11:10`.

**Valores negativos**.
Se admiten y se conservan los signos menos iniciales.
Los valores negativos suelen surgir de operaciones aritméticas con valores `Time64`.
En `Time64`, las entradas negativas se conservan tanto en entradas de texto (p. ej., `'-01:02:03.123'`) como numéricas (p. ej., `-3723.123`).

**Saturación**.
El componente de hora del día se limita al rango [-999:59:59.xxx, 999:59:59.xxx] al convertirlo en componentes o serializarlo como texto.
El valor numérico almacenado puede exceder este rango; sin embargo, cualquier extracción de componentes (horas, minutos, segundos) y la representación textual usan el valor saturado.

**Zonas horarias**.
`Time64` no admite zonas horarias.
Especificar una zona horaria al crear un tipo o valor `Time64` produce un error.
Del mismo modo, intentar aplicar o cambiar la zona horaria en columnas `Time64` no es compatible y produce un error.

<div id="examples">
  ## Ejemplos
</div>

1. Crear una tabla con una columna de tipo `Time64` e insertar datos en ella:

```sql
CREATE TABLE tab64
(
    `event_id` UInt8,
    `time` Time64(3)
)
ENGINE = TinyLog;
```

```sql
-- Parse Time64
-- - from string,
-- - from a number of seconds since 00:00:00 (fractional part according to precision).
INSERT INTO tab64 VALUES (1, '14:30:25'), (2, 52225.123), (3, '14:30:25');

SELECT * FROM tab64 ORDER BY event_id;
```

```text
   ┌─event_id─┬────────time─┐
1. │        1 │ 14:30:25.000 │
2. │        2 │ 14:30:25.123 │
3. │        3 │ 14:30:25.000 │
   └──────────┴──────────────┘
```

2. Filtrado por valores `Time64`

```sql
SELECT * FROM tab64 WHERE time = toTime64('14:30:25', 3);
```

```text
   ┌─event_id─┬────────time─┐
1. │        1 │ 14:30:25.000 │
2. │        3 │ 14:30:25.000 │
   └──────────┴──────────────┘
```

```sql
SELECT * FROM tab64 WHERE time = toTime64(52225.123, 3);
```

```text
   ┌─event_id─┬────────time─┐
1. │        2 │ 14:30:25.123 │
   └──────────┴──────────────┘
```

Nota: `toTime64` interpreta los literales numéricos como segundos con una parte fraccionaria según la precisión especificada, así que indique explícitamente los dígitos fraccionarios deseados.

3. Inspección del tipo resultante:

```sql
SELECT CAST('14:30:25.250' AS Time64(3)) AS column, toTypeName(column) AS type;
```

```text
   ┌────────column─┬─type──────┐
1. │ 14:30:25.250 │ Time64(3) │
   └───────────────┴───────────┘
```

<div id="addition-with-date">
  ## Suma con Date
</div>

Se puede sumar un valor [Time64](time64.md) a un valor [Date](date.md) o [Date32](date32.md) para obtener un [DateTime64](datetime64.md) con la misma escala que `Time64`:

```sql
SET use_legacy_to_time = 0;
SELECT toDate('2024-07-15') + toTime64('14:30:25.123456', 6) AS dt, toTypeName(dt);
```

```text
   ┌─────────────────────────dt─┬─toTypeName(dt)─┐
1. │ 2024-07-15 14:30:25.123456 │ DateTime64(6)  │
   └────────────────────────────┴────────────────┘
```

Consulta [Suma de fecha y hora](../operators/index.md#date-time-addition) para ver detalles sobre todas las combinaciones admitidas y los tipos de resultado.

**Véase también**

* [Funciones de conversión de tipos](../../sql-reference/functions/type-conversion-functions.md)
* [Funciones para trabajar con fechas y horas](../../sql-reference/functions/date-time-functions.md)
* [La configuración `date_time_input_format`](../../operations/settings/settings-formats.md#date_time_input_format)
* [La configuración `date_time_output_format`](../../operations/settings/settings-formats.md#date_time_output_format)
* [El parámetro de configuración del servidor `timezone`](../../operations/server-configuration-parameters/settings.md#timezone)
* [La configuración `session_timezone`](../../operations/settings/settings.md#session_timezone)
* [Operadores para trabajar con fechas y horas](../../sql-reference/operators/index.md#operators-for-working-with-dates-and-times)
* [Tipo de dato `Date`](../../sql-reference/data-types/date.md)
* [Tipo de dato `Time`](../../sql-reference/data-types/time.md)
* [Tipo de dato `DateTime`](../../sql-reference/data-types/datetime.md)