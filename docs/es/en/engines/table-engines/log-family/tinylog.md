---
description: 'Documentación del motor de tabla TinyLog'
slug: /engines/table-engines/log-family/tinylog
toc_priority: 34
toc_title: 'TinyLog'
title: 'Motor de tabla TinyLog'
doc_type: 'referencia'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="tinylog-table-engine">
  # Motor de tabla TinyLog
</div>

<CloudNotSupportedBadge />

El motor pertenece a la familia de motores Log. Consulte [Familia de motores Log](../../../engines/table-engines/log-family/index.md) para conocer las propiedades comunes de los motores Log y sus diferencias.

Este motor de tabla suele usarse con el método de escritura única: escribir datos una sola vez y luego leerlos tantas veces como sea necesario. Por ejemplo, puede usar tablas de tipo `TinyLog` para datos intermedios que se procesan en lotes pequeños. Tenga en cuenta que almacenar datos en una gran cantidad de tablas pequeñas no es eficiente.

Las consultas se ejecutan en un único flujo. En otras palabras, este motor está pensado para tablas relativamente pequeñas (hasta aproximadamente 1.000.000 de filas). Tiene sentido usar este motor de tabla si tiene muchas tablas pequeñas, ya que es más simple que el motor [Log](../../../engines/table-engines/log-family/log.md) (hay que abrir menos archivos).

<div id="characteristics">
  ## Características
</div>

* **Estructura más simple**: A diferencia del motor Log, TinyLog no utiliza archivos de marcas. Esto reduce la complejidad, pero también limita las optimizaciones de rendimiento para conjuntos de datos grandes.
* **Consultas en un único flujo**: Las consultas en tablas TinyLog se ejecutan en un único flujo, lo que lo hace adecuado para tablas relativamente pequeñas, normalmente de hasta 1.000.000 de filas.
* **Eficiente para tablas pequeñas**: La simplicidad del motor TinyLog lo hace ventajoso al gestionar muchas tablas pequeñas, ya que requiere menos operaciones con archivos en comparación con el motor Log.

A diferencia del motor Log, TinyLog no utiliza archivos de marcas. Esto reduce la complejidad, pero también limita las optimizaciones de rendimiento para conjuntos de datos más grandes.

<div id="table_engines-tinylog-creating-a-table">
  ## Crear una tabla
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    column1_name [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    column2_name [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = TinyLog
```

Consulte la descripción detallada de la consulta [CREATE TABLE](/es/sql-reference/statements/create/table).

<div id="table_engines-tinylog-writing-the-data">
  ## Escritura de los datos
</div>

El motor `TinyLog` almacena todas las columnas en un único archivo. Para cada consulta `INSERT`, ClickHouse añade el bloque de datos al final del archivo de la tabla y escribe las columnas una por una.

Para cada tabla, ClickHouse escribe los siguientes archivos:

* `<column>.bin`: un archivo de datos para cada columna, que contiene los datos serializados y comprimidos.

El motor `TinyLog` no admite las operaciones `ALTER UPDATE` y `ALTER DELETE`.

<div id="table_engines-tinylog-example-of-use">
  ## Ejemplo de uso
</div>

Creación de una tabla:

```sql
CREATE TABLE tiny_log_table
(
    timestamp DateTime,
    message_type String,
    message String
)
ENGINE = TinyLog
```

Inserción de datos:

```sql
INSERT INTO tiny_log_table VALUES (now(),'REGULAR','The first regular message')
INSERT INTO tiny_log_table VALUES (now(),'REGULAR','The second regular message'),(now(),'WARNING','The first warning message')
```

Usamos dos consultas `INSERT` para crear dos bloques de datos dentro de los archivos `<column>.bin`.

ClickHouse utiliza un único flujo para seleccionar datos. Como resultado, el orden de los bloques de filas en la salida coincide con el orden de esos mismos bloques en la entrada. Por ejemplo:

```sql
SELECT * FROM tiny_log_table
```

```text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2024-12-10 13:11:58 │ REGULAR      │ The first regular message  │
│ 2024-12-10 13:12:12 │ REGULAR      │ The second regular message │
│ 2024-12-10 13:12:12 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
```