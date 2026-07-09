---
description: 'Documentación del tipo de dato DateTime en ClickHouse, que almacena
  marcas de tiempo con precisión de segundos'
sidebar_label: 'DateTime'
sidebar_position: 16
slug: /sql-reference/data-types/datetime
title: 'DateTime'
doc_type: 'reference'
---

Permite almacenar un instante temporal que puede expresarse como una fecha del calendario y una hora del día.

Sintaxis:

```sql
DateTime([timezone])
```

Rango de valores admitido: [1970-01-01 00:00:00, 2106-02-07 06:28:15].

Resolución: 1 segundo.

<div id="speed">
  ## Velocidad
</div>

El tipo de dato `Date` es más rápido que `DateTime` en *la mayoría* de los casos.

El tipo `Date` requiere 2 bytes de almacenamiento, mientras que `DateTime` requiere 4. Sin embargo, durante la compresión, la diferencia de tamaño entre `Date` y `DateTime` se vuelve más significativa. Esto se debe a que los minutos y los segundos de `DateTime` son menos compresibles. Filtrar y agregar `Date` en lugar de `DateTime` también es más rápido.

<div id="usage-remarks">
  ## Observaciones de uso
</div>

El momento temporal se guarda como un [Unix timestamp](https://en.wikipedia.org/wiki/Unix_time), independientemente de la zona horaria o del horario de verano. La zona horaria afecta a cómo se muestran en formato de texto los valores del tipo `DateTime` y a cómo se interpretan los valores especificados como cadenas (`'2020-01-01 05:00:01'`).

En las tablas se almacena un Unix timestamp independiente de la zona horaria, y la zona horaria se usa para convertirlo a formato de texto o viceversa durante la importación/exportación de datos, o para realizar cálculos de calendario sobre los valores (por ejemplo, las funciones `toDate`, `toHour`, etc.). La zona horaria no se almacena en las filas de la tabla (ni en el resultset), sino en los metadatos de la columna.

Puede encontrarse una lista de las zonas horarias admitidas en la [IANA Time Zone Database](https://www.iana.org/time-zones) y también puede consultarse con `SELECT * FROM system.time_zones`. [La lista](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones) también está disponible en Wikipedia.

Puede establecer explícitamente una zona horaria para las columnas de tipo `DateTime` al crear una tabla. Ejemplo: `DateTime('UTC')`. Si no se establece la zona horaria, ClickHouse usa el valor del parámetro [timezone](../../operations/server-configuration-parameters/settings.md#timezone) en la configuración del servidor o la configuración del sistema operativo en el momento en que se inicia el servidor ClickHouse.

El [clickhouse-client](../../interfaces/client.md) aplica de forma predeterminada la zona horaria del servidor si no se establece explícitamente una zona horaria al inicializar el tipo de dato. Para usar la zona horaria del client, ejecute `clickhouse-client` con el parámetro `--use_client_time_zone`.

ClickHouse genera los valores según el valor de la configuración [date&#95;time&#95;output&#95;format](../../operations/settings/settings-formats.md#date_time_output_format). De forma predeterminada, usa el formato de texto `YYYY-MM-DD hh:mm:ss`. Además, puede cambiar el formato de salida con la función [formatDateTime](../../sql-reference/functions/date-time-functions.md#formatDateTime).

Al insertar datos en ClickHouse, puede usar distintos formatos de cadenas de fecha y hora, según el valor de la configuración [date&#95;time&#95;input&#95;format](../../operations/settings/settings-formats.md#date_time_input_format).

<div id="examples">
  ## Ejemplos
</div>

**1.** Crear una tabla con una columna de tipo `DateTime` e insertar datos en ella:

```sql
CREATE TABLE dt
(
    `timestamp` DateTime('Asia/Istanbul'),
    `event_id` UInt8
)
ENGINE = TinyLog;
```

```sql
-- Parse DateTime
-- - from string,
-- - from integer interpreted as number of seconds since 1970-01-01.
INSERT INTO dt VALUES ('2019-01-01 00:00:00', 1), (1546300800, 2);

SELECT * FROM dt;
```

```text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00 │        1 │
│ 2019-01-01 03:00:00 │        2 │
└─────────────────────┴──────────┘
```

* Al insertar un datetime como un entero, se trata como un Unix timestamp (UTC). `1546300800` representa `'2019-01-01 00:00:00'` UTC. Sin embargo, como la columna `timestamp` tiene especificada la zona horaria `Asia/Istanbul` (UTC+3), al mostrarse como cadena el valor aparecerá como `'2019-01-01 03:00:00'`
* Al insertar un valor de cadena como datetime, se trata como si estuviera en la zona horaria de la columna. `'2019-01-01 00:00:00'` se tratará como si estuviera en la zona horaria `Asia/Istanbul` y se guardará como `1546290000`.

**2.** Filtrado de valores `DateTime`

```sql
SELECT * FROM dt WHERE timestamp = toDateTime('2019-01-01 00:00:00', 'Asia/Istanbul')
```

```text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00 │        1 │
└─────────────────────┴──────────┘
```

Los valores de la columna `DateTime` se pueden filtrar mediante un valor de cadena en el predicado `WHERE`. Este se convertirá automáticamente a `DateTime`:

```sql
SELECT * FROM dt WHERE timestamp = '2019-01-01 00:00:00'
```

```text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00 │        1 │
└─────────────────────┴──────────┘
```

**3.** Obtener la zona horaria de una columna de tipo `DateTime`:

```sql
SELECT toDateTime(now(), 'Asia/Istanbul') AS column, toTypeName(column) AS x
```

```text
┌──────────────column─┬─x─────────────────────────┐
│ 2019-10-16 04:12:04 │ DateTime('Asia/Istanbul') │
└─────────────────────┴───────────────────────────┘
```

**4.** Conversión de zonas horarias

```sql
SELECT
toDateTime(timestamp, 'Europe/London') AS lon_time,
toDateTime(timestamp, 'Asia/Istanbul') AS istanbul_time
FROM dt
```

```text
┌───────────lon_time──┬───────istanbul_time─┐
│ 2019-01-01 00:00:00 │ 2019-01-01 03:00:00 │
│ 2018-12-31 21:00:00 │ 2019-01-01 00:00:00 │
└─────────────────────┴─────────────────────┘
```

Como la conversión de zona horaria solo cambia los metadatos, la operación no tiene coste de procesamiento.

<div id="limitations-on-time-zones-support">
  ## Limitaciones del soporte de zonas horarias
</div>

Es posible que algunas zonas horarias no sean totalmente compatibles. Hay algunos casos:

Si el desfase con respecto a UTC no es múltiplo de 15 minutos, el cálculo de horas y minutos puede ser incorrecto. Por ejemplo, la zona horaria de Monrovia, Liberia, tuvo un desfase de UTC -0:44:30 antes del 7 de ene. de 1972. Si realiza cálculos con la hora histórica de la zona horaria de Monrovia, las funciones de procesamiento de tiempo pueden dar resultados incorrectos. No obstante, los resultados posteriores al 7 de ene. de 1972 serán correctos.

Si el cambio de hora (debido al horario de verano o por otras razones) se realizó en un momento que no es múltiplo de 15 minutos, también puede obtener resultados incorrectos en ese día concreto.

Fechas de calendario no monótonas. Por ejemplo, en Happy Valley - Goose Bay, la hora se retrasó una hora a las 00:01:00 del 7 de nov. de 2010 (un minuto después de la medianoche). Así, después de que terminara el 6 de nov., se vivió un minuto completo del 7 de nov.; luego la hora volvió a las 23:01 del 6 de nov. y, tras otros 59 minutos, el 7 de nov. comenzó de nuevo. ClickHouse no admite (todavía) este tipo de situaciones curiosas. Durante esos días, los resultados de las funciones de procesamiento de tiempo pueden ser ligeramente incorrectos.

Existe un issue similar para la station antártica Casey en 2010. Allí retrasaron la hora tres horas el 5 de mar., a las 02:00. Si trabaja en una station antártica, no tema usar ClickHouse. Solo asegúrese de establecer la zona horaria en UTC o tenga en cuenta estas imprecisiones.

Desplazamientos de hora de varios días. Algunas islas del Pacífico cambiaron el desfase de su zona horaria de UTC+14 a UTC-12. Esto no supone un problema, pero pueden producirse algunas imprecisiones si realiza cálculos con su zona horaria para momentos históricos en los días de la conversión.

<div id="handling-daylight-saving-time-dst">
  ## Gestión del horario de verano (DST)
</div>

El tipo DateTime de ClickHouse con zonas horarias puede presentar comportamientos inesperados durante las transiciones del horario de verano (DST), especialmente cuando:

* [`date_time_output_format`](../../operations/settings/settings-formats.md#date_time_output_format) está configurado como `simple`.
* Los relojes se atrasan (&quot;Fall Back&quot;), lo que provoca un solapamiento de una hora.
* Los relojes se adelantan (&quot;Spring Forward&quot;), lo que provoca un hueco de una hora.

De forma predeterminada, ClickHouse siempre elige la primera ocurrencia de una hora solapada y puede interpretar horas inexistentes durante los adelantos.

Por ejemplo, considere la siguiente transición del horario de verano (DST) al horario estándar.

* El 29 de octubre de 2023, a las 02:00:00, los relojes se atrasan hasta las 01:00:00 (BST → GMT).
* La franja 01:00:00 – 01:59:59 aparece dos veces (una vez en BST y otra en GMT)
* ClickHouse siempre elige la primera ocurrencia (BST), lo que provoca resultados inesperados al sumar intervalos de tiempo.

```sql
SELECT '2023-10-29 01:30:00'::DateTime('Europe/London') AS time, time + toIntervalHour(1) AS one_hour_later

┌────────────────time─┬──────one_hour_later─┐
│ 2023-10-29 01:30:00 │ 2023-10-29 01:30:00 │
└─────────────────────┴─────────────────────┘
```

Del mismo modo, durante la transición de la hora estándar al horario de verano, puede parecer que se salta una hora.

Por ejemplo:

* El 26 de marzo de 2023, a las `00:59:59`, los relojes se adelantan hasta las 02:00:00 (GMT → BST).
* La hora `01:00:00` – `01:59:59` no existe.

```sql
SELECT '2023-03-26 01:30:00'::DateTime('Europe/London') AS time, time + toIntervalHour(1) AS one_hour_later

┌────────────────time─┬──────one_hour_later─┐
│ 2023-03-26 00:30:00 │ 2023-03-26 02:30:00 │
└─────────────────────┴─────────────────────┘
```

En este caso, ClickHouse retrasa la hora inexistente `2023-03-26 01:30:00` a `2023-03-26 00:30:00`.

<div id="see-also">
  ## Véase también
</div>

* [Funciones de conversión de tipos](../../sql-reference/functions/type-conversion-functions.md)
* [Funciones para trabajar con fechas y horas](../../sql-reference/functions/date-time-functions.md)
* [Funciones para trabajar con arrays](../../sql-reference/functions/array-functions.md)
* [El ajuste `date_time_input_format`](../../operations/settings/settings-formats.md#date_time_input_format)
* [El ajuste `date_time_output_format`](../../operations/settings/settings-formats.md#date_time_output_format)
* [El parámetro de configuración del servidor `timezone`](../../operations/server-configuration-parameters/settings.md#timezone)
* [El ajuste `session_timezone`](../../operations/settings/settings.md#session_timezone)
* [Operadores para trabajar con fechas y horas](../../sql-reference/operators#operators-for-working-with-dates-and-times)
* [El tipo de datos `Date`](../../sql-reference/data-types/date.md)