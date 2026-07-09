---
description: 'Documentación sobre las funciones de conversión de tipos'
sidebar_label: 'Conversión de tipos'
slug: /sql-reference/functions/type-conversion-functions
title: 'Funciones de conversión de tipos'
doc_type: 'reference'
---

<div id="common-issues-with-data-conversion">
  ## Problemas comunes con la conversión de datos
</div>

ClickHouse generalmente utiliza el [mismo comportamiento que los programas de C++](https://en.cppreference.com/w/cpp/language/implicit_conversion).

Las funciones `to<type>` y [cast](#CAST) se comportan de forma diferente en algunos casos; por ejemplo, en el caso de [LowCardinality](../data-types/lowcardinality.md), [cast](#CAST) elimina el atributo [LowCardinality](../data-types/lowcardinality.md), mientras que las funciones `to<type>` no. Lo mismo ocurre con [Nullable](../data-types/nullable.md); este comportamiento no es compatible con el estándar SQL y puede modificarse mediante la configuración [cast&#95;keep&#95;nullable](../../operations/settings/settings.md/#cast_keep_nullable).

:::note
Tenga en cuenta la posible pérdida de datos si los valores de un tipo de dato se convierten a un tipo de dato más pequeño (por ejemplo, de `Int64` a `Int32`) o entre
tipos de datos incompatibles (por ejemplo, de `String` a `Int`). Asegúrese de comprobar cuidadosamente si el resultado es el esperado.
:::

Ejemplo:

```sql
SELECT
    toTypeName(toLowCardinality('') AS val) AS source_type,
    toTypeName(toString(val)) AS to_type_result_type,
    toTypeName(CAST(val, 'String')) AS cast_result_type

┌─source_type────────────┬─to_type_result_type────┬─cast_result_type─┐
│ LowCardinality(String) │ LowCardinality(String) │ String           │
└────────────────────────┴────────────────────────┴──────────────────┘

SELECT
    toTypeName(toNullable('') AS val) AS source_type,
    toTypeName(toString(val)) AS to_type_result_type,
    toTypeName(CAST(val, 'String')) AS cast_result_type

┌─source_type──────┬─to_type_result_type─┬─cast_result_type─┐
│ Nullable(String) │ Nullable(String)    │ String           │
└──────────────────┴─────────────────────┴──────────────────┘

SELECT
    toTypeName(toNullable('') AS val) AS source_type,
    toTypeName(toString(val)) AS to_type_result_type,
    toTypeName(CAST(val, 'String')) AS cast_result_type
SETTINGS cast_keep_nullable = 1

┌─source_type──────┬─to_type_result_type─┬─cast_result_type─┐
│ Nullable(String) │ Nullable(String)    │ Nullable(String) │
└──────────────────┴─────────────────────┴──────────────────┘
```

<div id="to-string-functions">
  ## Notas sobre las funciones `toString`
</div>

La familia de funciones `toString` permite realizar conversiones entre números, cadenas (pero no `Fixed String`), fechas y fechas con hora.
Todas estas funciones aceptan un argumento.

* Al convertir un valor a una cadena o desde una cadena, el valor se formatea o se analiza siguiendo las mismas reglas que en el formato TabSeparated (y en casi todos los demás formatos de texto). Si la cadena no puede analizarse, se lanza una excepción y la solicitud se cancela.
* Al convertir fechas en números o viceversa, la fecha corresponde al número de días desde el inicio del Unix epoch.
* Al convertir fechas con hora en números o viceversa, la fecha con hora corresponde al número de segundos desde el inicio del Unix epoch.
* La función `toString` del argumento `DateTime` puede aceptar un segundo argumento String que contenga el nombre de la zona horaria, por ejemplo: `Europe/Amsterdam`. En este caso, la hora se formatea según la zona horaria especificada.

<div id="to-date-and-date-time-functions">
  ## Notas sobre las funciones `toDate`/`toDateTime`
</div>

Los formatos de fecha y fecha con hora de las funciones `toDate`/`toDateTime` se definen de la siguiente manera:

```response
YYYY-MM-DD
YYYY-MM-DD hh:mm:ss
```

Como excepción, al convertir tipos numéricos UInt32, Int32, UInt64 o Int64 a Date, si el número es mayor o igual que 65536, se interpreta como un timestamp Unix (y no como un número de días) y se redondea a una fecha.
Esto permite admitir el caso habitual de escribir `toDate(unix_timestamp)`, que de otro modo daría error y obligaría a escribir la forma más engorrosa `toDate(toDateTime(unix_timestamp))`.

La conversión entre una fecha y una fecha con hora se realiza de forma natural: añadiendo una hora nula o eliminando la parte horaria.

La conversión entre tipos numéricos utiliza las mismas reglas que las asignaciones entre distintos tipos numéricos en C++.

**Ejemplo**

```sql title="Query"
SELECT
    now() AS ts,
    time_zone,
    toString(ts, time_zone) AS str_tz_datetime
FROM system.time_zones
WHERE time_zone LIKE 'Europe%'
LIMIT 10
```

```response title="Response"
┌──────────────────ts─┬─time_zone─────────┬─str_tz_datetime─────┐
│ 2023-09-08 19:14:59 │ Europe/Amsterdam  │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Andorra    │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Astrakhan  │ 2023-09-08 23:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Athens     │ 2023-09-08 22:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Belfast    │ 2023-09-08 20:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Belgrade   │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Berlin     │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Bratislava │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Brussels   │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Bucharest  │ 2023-09-08 22:14:59 │
└─────────────────────┴───────────────────┴─────────────────────┘
```

Véase también la función [`toUnixTimestamp`](/es/sql-reference/functions/date-time-functions#toUnixTimestamp).

{/* 
  El contenido interno de las etiquetas siguientes se sustituye durante la compilación del framework de documentación por 
  documentación generada a partir de system.functions. Por favor, no modifique ni elimine las etiquetas.
  Consulte: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }