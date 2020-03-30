---
machine_translated: true
---

# DateTime64 {#data_type-datetime64}

Permite almacenar un instante en el tiempo, que se puede expresar como una fecha de calendario y una hora de un día, con una precisión de subsegundo definida

Tamaño de la marca (precisión): 10<sup>-Precision</sup> segundo

Sintaxis:

``` sql
DateTime64(precision, [timezone])
```

Internamente, almacena datos como un número de ‘ticks’ desde el inicio de la época (1970-01-01 00:00:00 UTC) como Int64. La resolución de tick está determinada por el parámetro precision. Además, el `DateTime64` tipo puede almacenar zona horaria que es la misma para toda la columna, que afecta a cómo los valores de la `DateTime64` Los valores de tipo se muestran en formato de texto y cómo se analizan los valores especificados como cadenas (‘2020-01-01 05:00:01.000’). La zona horaria no se almacena en las filas de la tabla (o en el conjunto de resultados), sino que se almacena en los metadatos de la columna. Ver detalles en [FechaHora](datetime.md).

## Ejemplos {#examples}

**1.** Creación de una tabla con `DateTime64`-tipo de columna e insertar datos en ella:

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

-   Al insertar datetime como un entero, se trata como una marca de tiempo Unix (UTC) apropiadamente escalada. `1546300800000` (con precisión 3) representa `'2019-01-01 00:00:00'` UTC. Sin embargo, como `timestamp` tetas grandes `Europe/Moscow` (UTC + 3) zona horaria especificada, al emitir como una cadena, el valor se mostrará como `'2019-01-01 03:00:00'`
-   Al insertar el valor de cadena como fecha y hora, se trata como si estuviera en la zona horaria de la columna. `'2019-01-01 00:00:00'` será tratado como estar en `Europe/Moscow` Zona horaria y se almacena como `1546290000000`.

**2.** Filtrado en `DateTime64` valor

``` sql
SELECT * FROM dt WHERE timestamp = toDateTime64('2019-01-01 00:00:00', 3, 'Europe/Moscow')
```

``` text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00.000 │        2 │
└─────────────────────────┴──────────┘
```

A diferencia de `DateTime`, `DateTime64` los valores no se convierten desde `String` automática

**3.** Obtener una zona horaria para un `DateTime64`-tipo de valor:

``` sql
SELECT toDateTime64(now(), 3, 'Europe/Moscow') AS column, toTypeName(column) AS x
```

``` text
┌──────────────────column─┬─x──────────────────────────────┐
│ 2019-10-16 04:12:04.000 │ DateTime64(3, 'Europe/Moscow') │
└─────────────────────────┴────────────────────────────────┘
```

**4.** Conversión de zona horaria

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

## Ver también {#see-also}

-   [Funciones de conversión de tipos](../query_language/functions/type_conversion_functions.md)
-   [Funciones para trabajar con fechas y horas](../query_language/functions/date_time_functions.md)
-   [Funciones para trabajar con matrices](../query_language/functions/array_functions.md)
-   [El `date_time_input_format` configuración](../operations/settings/settings.md#settings-date_time_input_format)
-   [El `timezone` parámetro de configuración del servidor](../operations/server_settings/settings.md#server_settings-timezone)
-   [Operadores para trabajar con fechas y horas](../query_language/operators.md#operators-datetime)
-   [`Date` Tipo de datos](date.md)
-   [`DateTime` Tipo de datos](datetime.md)
