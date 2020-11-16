---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 48
toc_title: FechaHora
---

# Datetime {#data_type-datetime}

Permite almacenar un instante en el tiempo, que se puede expresar como una fecha del calendario y una hora de un día.

Sintaxis:

``` sql
DateTime([timezone])
```

Rango de valores: \[1970-01-01 00:00:00, 2105-12-31 23:59:59\].

Resolución: 1 segundo.

## Observaciones de uso {#usage-remarks}

El punto en el tiempo se guarda como un [Timestamp de Unix](https://en.wikipedia.org/wiki/Unix_time), independientemente de la zona horaria o el horario de verano. Además, el `DateTime` tipo puede almacenar zona horaria que es la misma para toda la columna, que afecta a cómo los valores de la `DateTime` Los valores de tipo se muestran en formato de texto y cómo se analizan los valores especificados como cadenas (‘2020-01-01 05:00:01’). La zona horaria no se almacena en las filas de la tabla (o en el conjunto de resultados), sino que se almacena en los metadatos de la columna.
Se puede encontrar una lista de zonas horarias compatibles en el [Base de datos de zonas horarias de IANA](https://www.iana.org/time-zones).
El `tzdata` paquete, que contiene [Base de datos de zonas horarias de IANA](https://www.iana.org/time-zones), debe instalarse en el sistema. Utilice el `timedatectl list-timezones` comando para listar zonas horarias conocidas por un sistema local.

Puede establecer explícitamente una zona horaria para `DateTime`-type columnas al crear una tabla. Si la zona horaria no está establecida, ClickHouse usa el valor [Zona horaria](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) parámetro en la configuración del servidor o la configuración del sistema operativo en el momento del inicio del servidor ClickHouse.

El [Casa de clics-cliente](../../interfaces/cli.md) aplica la zona horaria del servidor de forma predeterminada si una zona horaria no se establece explícitamente al inicializar el tipo de datos. Para utilizar la zona horaria del cliente, ejecute `clickhouse-client` con el `--use_client_time_zone` parámetro.

ClickHouse genera valores en `YYYY-MM-DD hh:mm:ss` formato de texto por defecto. Puede cambiar la salida con el [formatDateTime](../../sql-reference/functions/date-time-functions.md#formatdatetime) función.

Al insertar datos en ClickHouse, puede usar diferentes formatos de cadenas de fecha y hora, dependiendo del valor de la [Date_time_input_format](../../operations/settings/settings.md#settings-date_time_input_format) configuración.

## Ejemplos {#examples}

**1.** Creación de una tabla con un `DateTime`-tipo de columna e insertar datos en ella:

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

-   Al insertar datetime como un entero, se trata como Unix Timestamp (UTC). `1546300800` representar `'2019-01-01 00:00:00'` UTC. Sin embargo, como `timestamp` columna tiene `Europe/Moscow` (UTC + 3) zona horaria especificada, al emitir como cadena, el valor se mostrará como `'2019-01-01 03:00:00'`
-   Al insertar el valor de cadena como fecha y hora, se trata como si estuviera en la zona horaria de la columna. `'2019-01-01 00:00:00'` será tratado como estar en `Europe/Moscow` zona horaria y guardado como `1546290000`.

**2.** Filtrado en `DateTime` valor

``` sql
SELECT * FROM dt WHERE timestamp = toDateTime('2019-01-01 00:00:00', 'Europe/Moscow')
```

``` text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00 │        2 │
└─────────────────────┴──────────┘
```

`DateTime` se pueden filtrar usando un valor de cadena en `WHERE` predicado. Se convertirá a `DateTime` automática:

``` sql
SELECT * FROM dt WHERE timestamp = '2019-01-01 00:00:00'
```

``` text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00 │        1 │
└─────────────────────┴──────────┘
```

**3.** Obtener una zona horaria para un `DateTime`-tipo columna:

``` sql
SELECT toDateTime(now(), 'Europe/Moscow') AS column, toTypeName(column) AS x
```

``` text
┌──────────────column─┬─x─────────────────────────┐
│ 2019-10-16 04:12:04 │ DateTime('Europe/Moscow') │
└─────────────────────┴───────────────────────────┘
```

**4.** Conversión de zona horaria

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

## Ver también {#see-also}

-   [Funciones de conversión de tipos](../../sql-reference/functions/type-conversion-functions.md)
-   [Funciones para trabajar con fechas y horas](../../sql-reference/functions/date-time-functions.md)
-   [Funciones para trabajar con matrices](../../sql-reference/functions/array-functions.md)
-   [El `date_time_input_format` configuración](../../operations/settings/settings.md#settings-date_time_input_format)
-   [El `timezone` parámetro de configuración del servidor](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone)
-   [Operadores para trabajar con fechas y horas](../../sql-reference/operators/index.md#operators-datetime)
-   [El `Date` tipo de datos](date.md)

[Artículo Original](https://clickhouse.tech/docs/en/data_types/datetime/) <!--hide-->
