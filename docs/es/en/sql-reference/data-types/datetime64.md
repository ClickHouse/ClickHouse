---
description: 'Documentación del tipo de dato DateTime64 en ClickHouse, que almacena
  marcas de tiempo con precisión de subsegundos'
sidebar_label: 'DateTime64'
sidebar_position: 18
slug: /sql-reference/data-types/datetime64
title: 'DateTime64'
doc_type: 'reference'
---

Permite almacenar un instante que puede expresarse como una fecha del calendario y una hora del día, con una precisión de subsegundos definida.

Tamaño del tick (precisión): 10<sup>-precision</sup> segundos. Rango válido: [ 0 : 9 ].
Normalmente se usan 3 (milisegundos), 6 (microsegundos) y 9 (nanosegundos).

Valor predeterminado: 3 (milisegundos).

**Sintaxis:**

```sql
DateTime64(precision, [timezone])
```

Internamente, almacena los datos como un número de &#39;ticks&#39; desde el inicio de la época (1970-01-01 00:00:00 UTC) como Int64. La resolución de los ticks viene determinada por el parámetro de precisión. Además, el tipo `DateTime64` puede almacenar una zona horaria común para toda la columna, lo que afecta tanto a cómo se muestran en formato de texto los valores del tipo `DateTime64` como a cómo se interpretan los valores especificados como cadenas (&#39;2020-01-01 05:00:01.000&#39;). La zona horaria no se almacena en las filas de la tabla (ni en el conjunto de resultados), sino en los metadatos de la columna. Consulte los detalles en [DateTime](../../sql-reference/data-types/datetime.md).

Rango de valores admitido: [1900-01-01 00:00:00, 2299-12-31 23:59:59.999999999]

El número de dígitos después del punto decimal depende del parámetro de precisión.

Nota: La precisión del valor máximo es 8. Si se utiliza la precisión máxima de 9 dígitos (nanosegundos), el valor máximo admitido es `2262-04-11 23:47:16` en UTC.

<div id="examples">
  ## Ejemplos
</div>

1. Crear una tabla con una columna de tipo `DateTime64` e insertar datos en ella:

```sql
CREATE TABLE dt64
(
    `timestamp` DateTime64(3, 'Asia/Istanbul'),
    `event_id` UInt8
)
ENGINE = MergeTree;
```

```sql
-- Parse DateTime
-- - from an integer interpreted as the number of milliseconds (because of precision 3) since 1970-01-01,
-- - from a decimal interpreted as the number of seconds before the decimal part, and based on the precision after the decimal point,
-- - from a string.

INSERT INTO dt64
VALUES
(1546300800123, 1),
(1546300800.123, 2),
('2019-01-01 00:00:00', 3);

SELECT * FROM dt64;
```

```text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00.123 │        1 │
│ 2019-01-01 03:00:00.123 │        2 │
│ 2019-01-01 00:00:00.000 │        3 │
└─────────────────────────┴──────────┘
```

* Al insertar un datetime como entero, se trata como un Unix Timestamp (UTC) con la escala adecuada. `1546300800000` (con precisión 3) representa `'2019-01-01 00:00:00'` UTC. Sin embargo, como la columna `timestamp` tiene especificada la zona horaria `Asia/Istanbul` (UTC+3), al mostrarse como cadena, el valor aparecerá como `'2019-01-01 03:00:00'`. Al insertar un datetime como decimal, se trata de forma similar a un entero, salvo que el valor antes del punto decimal es el Unix Timestamp hasta los segundos inclusive, y lo que va después del punto decimal se interpreta como la precisión.
* Al insertar un valor de cadena como datetime, se trata como si estuviera en la zona horaria de la columna. `'2019-01-01 00:00:00'` se interpretará como si estuviera en la zona horaria `Asia/Istanbul` y se almacenará como `1546290000000`.

2. Filtrado de valores `DateTime64`

```sql
SELECT * FROM dt64 WHERE timestamp = toDateTime64('2019-01-01 00:00:00', 3, 'Asia/Istanbul');
```

```text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00.000 │        3 │
└─────────────────────────┴──────────┘
```

A diferencia de `DateTime`, los valores de `DateTime64` no se convierten automáticamente a partir de `String`.

```sql
SELECT * FROM dt64 WHERE timestamp = toDateTime64(1546300800.123, 3);
```

```text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00.123 │        1 │
│ 2019-01-01 03:00:00.123 │        2 │
└─────────────────────────┴──────────┘
```

A diferencia de lo que ocurre al insertar, la función `toDateTime64` tratará todos los valores como la variante decimal, por lo que la precisión debe
indicarse después del punto decimal.

3. Obtener una zona horaria para un valor de tipo `DateTime64`:

```sql
SELECT toDateTime64(now(), 3, 'Asia/Istanbul') AS column, toTypeName(column) AS x;
```

```text
┌──────────────────column─┬─x──────────────────────────────┐
│ 2023-06-05 00:09:52.000 │ DateTime64(3, 'Asia/Istanbul') │
└─────────────────────────┴────────────────────────────────┘
```

4. Conversión de zona horaria

```sql
SELECT
toDateTime64(timestamp, 3, 'Europe/London') AS lon_time,
toDateTime64(timestamp, 3, 'Asia/Istanbul') AS istanbul_time
FROM dt64;
```

```text
┌────────────────lon_time─┬───────────istanbul_time─┐
│ 2019-01-01 00:00:00.123 │ 2019-01-01 03:00:00.123 │
│ 2019-01-01 00:00:00.123 │ 2019-01-01 03:00:00.123 │
│ 2018-12-31 21:00:00.000 │ 2019-01-01 00:00:00.000 │
└─────────────────────────┴─────────────────────────┘
```

**Véase también**

* [Funciones de conversión de tipos](../../sql-reference/functions/type-conversion-functions.md)
* [Funciones para trabajar con fechas y horas](../../sql-reference/functions/date-time-functions.md)
* [La configuración `date_time_input_format`](../../operations/settings/settings-formats.md#date_time_input_format)
* [La configuración `date_time_output_format`](../../operations/settings/settings-formats.md#date_time_output_format)
* [El parámetro `timezone` de la configuración del servidor](../../operations/server-configuration-parameters/settings.md#timezone)
* [La configuración `session_timezone`](../../operations/settings/settings.md#session_timezone)
* [Operadores para trabajar con fechas y horas](../../sql-reference/operators/index.md#operators-for-working-with-dates-and-times)
* [Tipo de dato `Date`](../../sql-reference/data-types/date.md)
* [Tipo de dato `DateTime`](../../sql-reference/data-types/datetime.md)