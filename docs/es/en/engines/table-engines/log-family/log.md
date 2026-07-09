---
description: 'Documentación del motor de tabla Log'
slug: /engines/table-engines/log-family/log
toc_priority: 33
toc_title: 'Log'
title: 'Motor de tabla Log'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="log-table-engine">
  # Motor de tabla Log
</div>

<CloudNotSupportedBadge />

El motor pertenece a la familia de motores `Log`. Consulte las propiedades comunes de los motores `Log` y sus diferencias en el artículo sobre la [familia de motores Log](../../../engines/table-engines/log-family/index.md).

`Log` se diferencia de [TinyLog](../../../engines/table-engines/log-family/tinylog.md) en que incorpora un pequeño archivo de &quot;marcas&quot; junto con los archivos de columnas. Estas marcas se escriben en cada bloque de datos y contienen desplazamientos que indican desde dónde empezar a leer el archivo para omitir el número especificado de filas. Esto permite leer los datos de la tabla en varios hilos.
Para el acceso concurrente a los datos, las operaciones de lectura pueden realizarse simultáneamente, mientras que las operaciones de escritura bloquean las lecturas y también se bloquean entre sí.
El motor `Log` no admite índices. Del mismo modo, si falla la escritura en una tabla, la tabla queda dañada y su lectura devuelve un error. El motor `Log` es adecuado para datos temporales, tablas de una sola escritura y para fines de prueba o demostración.

<div id="table_engines-log-creating-a-table">
  ## Crear una tabla
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    column1_name [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    column2_name [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = Log
```

Consulte la descripción detallada de la consulta [CREATE TABLE](/es/sql-reference/statements/create/table).

<div id="table_engines-log-writing-the-data">
  ## Escritura de los datos
</div>

El motor `Log` almacena los datos de forma eficiente escribiendo cada columna en su propio archivo. Para cada tabla, el motor `Log` escribe los siguientes archivos en la ruta de almacenamiento especificada:

* `<column>.bin`: un archivo de datos para cada columna que contiene los datos serializados y comprimidos.
  `__marks.mrk`: un archivo de marcas que almacena los desplazamientos y el recuento de filas de cada bloque de datos insertado. Las marcas se utilizan para agilizar la ejecución de consultas, ya que permiten al motor omitir bloques de datos irrelevantes durante la lectura.

<div id="writing-process">
  ### Proceso de escritura
</div>

Cuando se escriben datos en una tabla `Log`:

1. Los datos se serializan y se comprimen en bloques.
2. Para cada columna, los datos comprimidos se agregan a su archivo `<column>.bin` correspondiente.
3. Se añaden las entradas correspondientes al archivo `__marks.mrk` para registrar el desplazamiento y el número de filas de los datos recién insertados.

<div id="table_engines-log-reading-the-data">
  ## Lectura de los datos
</div>

El archivo de marcas permite a ClickHouse paralelizar la lectura de los datos. Esto significa que una consulta `SELECT` devuelve las filas en un orden impredecible. Utilice la cláusula `ORDER BY` para ordenar las filas.

<div id="table_engines-log-example-of-use">
  ## Ejemplo de uso
</div>

Creación de una tabla:

```sql
CREATE TABLE log_table
(
    timestamp DateTime,
    message_type String,
    message String
)
ENGINE = Log
```

Inserción de datos:

```sql
INSERT INTO log_table VALUES (now(),'REGULAR','The first regular message')
INSERT INTO log_table VALUES (now(),'REGULAR','The second regular message'),(now(),'WARNING','The first warning message')
```

Usamos dos consultas `INSERT` para crear dos bloques de datos en los archivos `<column>.bin`.

ClickHouse usa varios hilos al seleccionar datos. Cada hilo lee un bloque de datos distinto y devuelve las filas resultantes por separado a medida que finaliza. Como resultado, el orden de los bloques de filas en la salida puede no coincidir con el orden de esos mismos bloques en la entrada. Por ejemplo:

```sql
SELECT * FROM log_table
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

Ordenar los resultados (en orden ascendente de forma predeterminada):

```sql
SELECT * FROM log_table ORDER BY timestamp
```

```text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message  │
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
```