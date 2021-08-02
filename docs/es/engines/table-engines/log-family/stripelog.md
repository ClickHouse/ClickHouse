---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 32
toc_title: StripeLog
---

# Lista de Stripelog {#stripelog}

Este motor pertenece a la familia de motores de registro. Consulte las propiedades comunes de los motores de registro y sus diferencias en [Familia del motor de registro](index.md) artículo.

Utilice este motor en escenarios en los que necesite escribir muchas tablas con una pequeña cantidad de datos (menos de 1 millón de filas).

## Creación de una tabla {#table_engines-stripelog-creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    column1_name [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    column2_name [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = StripeLog
```

Vea la descripción detallada del [CREATE TABLE](../../../sql-reference/statements/create.md#create-table-query) consulta.

## Escribir los datos {#table_engines-stripelog-writing-the-data}

El `StripeLog` el motor almacena todas las columnas en un archivo. Para cada `INSERT` consulta, ClickHouse agrega el bloque de datos al final de un archivo de tabla, escribiendo columnas una por una.

Para cada tabla, ClickHouse escribe los archivos:

-   `data.bin` — Data file.
-   `index.mrk` — File with marks. Marks contain offsets for each column of each data block inserted.

El `StripeLog` el motor no soporta el `ALTER UPDATE` y `ALTER DELETE` operación.

## Lectura de los datos {#table_engines-stripelog-reading-the-data}

El archivo con marcas permite ClickHouse paralelizar la lectura de datos. Esto significa que un `SELECT` query devuelve filas en un orden impredecible. Utilice el `ORDER BY` cláusula para ordenar filas.

## Ejemplo de uso {#table_engines-stripelog-example-of-use}

Creación de una tabla:

``` sql
CREATE TABLE stripe_log_table
(
    timestamp DateTime,
    message_type String,
    message String
)
ENGINE = StripeLog
```

Insertar datos:

``` sql
INSERT INTO stripe_log_table VALUES (now(),'REGULAR','The first regular message')
INSERT INTO stripe_log_table VALUES (now(),'REGULAR','The second regular message'),(now(),'WARNING','The first warning message')
```

Se utilizaron dos `INSERT` consultas para crear dos bloques de datos dentro del `data.bin` file.

ClickHouse usa múltiples subprocesos al seleccionar datos. Cada subproceso lee un bloque de datos separado y devuelve las filas resultantes de forma independiente a medida que termina. Como resultado, el orden de los bloques de filas en la salida no coincide con el orden de los mismos bloques en la entrada en la mayoría de los casos. Por ejemplo:

``` sql
SELECT * FROM stripe_log_table
```

``` text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
┌───────────timestamp─┬─message_type─┬─message───────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message │
└─────────────────────┴──────────────┴───────────────────────────┘
```

Ordenación de los resultados (orden ascendente por defecto):

``` sql
SELECT * FROM stripe_log_table ORDER BY timestamp
```

``` text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message  │
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
```

[Artículo Original](https://clickhouse.tech/docs/en/operations/table_engines/stripelog/) <!--hide-->
