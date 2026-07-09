---
description: 'Documentación sobre el motor de tabla StripeLog'
slug: /engines/table-engines/log-family/stripelog
toc_priority: 32
toc_title: 'StripeLog'
title: 'Motor de tabla StripeLog'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="stripelog-table-engine">
  # Motor de tabla StripeLog
</div>

<CloudNotSupportedBadge />

Este motor pertenece a la familia de motores log. Consulte las propiedades comunes de los motores log y sus diferencias en el artículo [Familia de motores Log](../../../engines/table-engines/log-family/index.md).

Use este motor en escenarios en los que necesite escribir en muchas tablas con una cantidad pequeña de datos (menos de 1 millón de filas). Por ejemplo, esta tabla puede usarse para almacenar batches de datos entrantes para su transformación cuando se requiere un procesamiento atómico. Es posible tener 100k instancias de este tipo de tabla en un servidor ClickHouse. Debe preferirse este motor de tabla frente a [Log](./log.md) cuando se requiere un gran número de tablas. Esto es a costa de la eficiencia de lectura.

<div id="table_engines-stripelog-creating-a-table">
  ## Crear una tabla
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    column1_name [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    column2_name [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = StripeLog
```

Consulte la descripción detallada de la consulta [CREATE TABLE](/es/sql-reference/statements/create/table).

<div id="table_engines-stripelog-writing-the-data">
  ## Escritura de los datos
</div>

El motor `StripeLog` almacena todas las columnas en un único archivo. Para cada consulta `INSERT`, ClickHouse añade el bloque de datos al final del archivo de la tabla y escribe las columnas una por una.

Para cada tabla, ClickHouse escribe los archivos siguientes:

* `data.bin` — Archivo de datos.
* `index.mrk` — Archivo con marcas. Las marcas contienen desplazamientos para cada columna de cada bloque de datos insertado.

El motor `StripeLog` no admite las operaciones `ALTER UPDATE` ni `ALTER DELETE`.

<div id="table_engines-stripelog-reading-the-data">
  ## Lectura de los datos
</div>

El archivo de marcas permite a ClickHouse paralelizar la lectura de datos. Esto significa que una consulta `SELECT` devuelve las filas en un orden no predecible. Use la cláusula `ORDER BY` para ordenar las filas.

<div id="table_engines-stripelog-example-of-use">
  ## Ejemplo de uso
</div>

Crear una tabla:

```sql
CREATE TABLE stripe_log_table
(
    timestamp DateTime,
    message_type String,
    message String
)
ENGINE = StripeLog
```

Inserción de datos:

```sql
INSERT INTO stripe_log_table VALUES (now(),'REGULAR','The first regular message')
INSERT INTO stripe_log_table VALUES (now(),'REGULAR','The second regular message'),(now(),'WARNING','The first warning message')
```

Usamos dos consultas `INSERT` para crear dos bloques de datos dentro del archivo `data.bin`.

ClickHouse usa varios hilos al consultar datos. Cada hilo lee un bloque de datos distinto y devuelve las filas resultantes en cuanto termina. Como resultado, el orden de los bloques de filas en la salida no coincide, en la mayoría de los casos, con el orden de esos mismos bloques en la entrada. Por ejemplo:

```sql
SELECT * FROM stripe_log_table
```

```text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
┌───────────timestamp─┬─message_type─┬─message───────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message │
└─────────────────────┴──────────────┴───────────────────────────┘
```

Ordenación de los resultados (orden ascendente de forma predeterminada):

```sql
SELECT * FROM stripe_log_table ORDER BY timestamp
```

```text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message  │
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
```