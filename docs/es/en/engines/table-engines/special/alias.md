---
description: 'El motor de tabla Alias crea un proxy transparente hacia otra tabla. Todas las operaciones se redirigen a la tabla de destino, mientras que el propio alias no almacena datos.'
sidebar_label: 'Alias'
sidebar_position: 5
slug: /engines/table-engines/special/alias
title: 'Motor de tabla Alias'
doc_type: 'referencia'
---

<div id="alias-table-engine">
  # Motor de tabla Alias
</div>

El motor `Alias` crea un proxy a otra tabla. Todas las operaciones de lectura y escritura se redirigen a la tabla de destino, mientras que el alias en sí no almacena datos, sino que solo mantiene una referencia a la tabla de destino.

<div id="creating-a-table">
  ## Crear una tabla
</div>

```sql
CREATE TABLE [db_name.]alias_name
ENGINE = Alias(target_table)
```

O bien, con el nombre explícito de la base de datos:

```sql
CREATE TABLE [db_name.]alias_name
ENGINE = Alias(target_db, target_table)
```

:::note
La tabla `Alias` no admite definiciones explícitas de columnas. Las columnas se heredan automáticamente de la tabla de destino. Esto garantiza que el alias siempre coincida con el esquema de la tabla de destino.
:::

<div id="engine-parameters">
  ## Parámetros del motor
</div>

* **`target_db (optional)`** — Nombre de la base de datos que contiene la tabla de destino.
* **`target_table`** — Nombre de la tabla de destino.

:::note
Cuando se omite `target_db` y `target_table` no está totalmente calificado (por ejemplo, `Alias('my_table')`), el destino se resuelve en la misma base de datos que el propio alias, no en la base de datos actual de la sesión.
:::

<div id="supported-operations">
  ## Operaciones admitidas
</div>

El motor de tabla `Alias` admite todas las operaciones principales. 

<div id="operations-on-target">
  ### Operaciones en la tabla de destino
</div>

Estas operaciones se redirigen a la tabla de destino:

| Operación                    | Compatibilidad | Descripción                                                 |
| ---------------------------- | -------------- | ----------------------------------------------------------- |
| `SELECT`                     | ✅              | Leer datos de la tabla de destino                           |
| `INSERT`                     | ✅              | Escribir datos en la tabla de destino                       |
| `INSERT SELECT`              | ✅              | Inserción por lotes en la tabla de destino                  |
| `ALTER TABLE ADD COLUMN`     | ✅              | Agregar columnas a la tabla de destino                      |
| `ALTER TABLE MODIFY SETTING` | ✅              | Modificar la configuración de la tabla de destino           |
| `ALTER TABLE PARTITION`      | ✅              | Operaciones de partición (DETACH/ATTACH/DROP) en el destino |
| `ALTER TABLE UPDATE`         | ✅              | Actualizar filas en la tabla de destino (mutación)          |
| `ALTER TABLE DELETE`         | ✅              | Eliminar filas de la tabla de destino (mutación)            |
| `OPTIMIZE TABLE`             | ✅              | Optimizar la tabla de destino (fusionar partes)             |
| `TRUNCATE TABLE`             | ✅              | Truncar la tabla de destino                                 |

<div id="operations-on-alias">
  ### Operaciones sobre el propio alias
</div>

Estas operaciones solo afectan al alias, **no** a la tabla de destino:

| Operación      | Compatibilidad | Descripción                                                       |
| -------------- | -------------- | ----------------------------------------------------------------- |
| `DROP TABLE`   | ✅              | Solo se elimina el alias; la tabla de destino no cambia           |
| `RENAME TABLE` | ✅              | Solo se cambia el nombre del alias; la tabla de destino no cambia |

<div id="usage-examples">
  ## Ejemplos de uso
</div>

<div id="basic-alias-creation">
  ### Creación de un alias básico
</div>

Cree un alias sencillo en la misma base de datos:

```sql
-- Create source table
CREATE TABLE source_data (
    id UInt32,
    name String,
    value Float64
) ENGINE = MergeTree
ORDER BY id;

-- Insert some data
INSERT INTO source_data VALUES (1, 'one', 10.1), (2, 'two', 20.2);

-- Create alias
CREATE TABLE data_alias ENGINE = Alias('source_data');

-- Query through alias
SELECT * FROM data_alias;
```

```text
┌─id─┬─name─┬─value─┐
│  1 │ one  │  10.1 │
│  2 │ two  │  20.2 │
└────┴──────┴───────┘
```

<div id="cross-database-alias">
  ### Alias entre bases de datos
</div>

Cree un alias que apunte a una tabla de otra base de datos:

```sql
-- Create databases
CREATE DATABASE db1;
CREATE DATABASE db2;

-- Create source table in db1
CREATE TABLE db1.events (
    timestamp DateTime,
    event_type String,
    user_id UInt32
) ENGINE = MergeTree
ORDER BY timestamp;

-- Create alias in db2 pointing to db1.events
CREATE TABLE db2.events_alias ENGINE = Alias('db1', 'events');

-- Or using database.table format
CREATE TABLE db2.events_alias2 ENGINE = Alias('db1.events');

-- Both aliases work identically
INSERT INTO db2.events_alias VALUES (now(), 'click', 100);
SELECT * FROM db2.events_alias2;
```

<div id="write-operations">
  ### Operaciones de escritura a través del alias
</div>

Todas las operaciones de escritura se redirigen a la tabla de destino:

```sql
CREATE TABLE metrics (
    ts DateTime,
    metric_name String,
    value Float64
) ENGINE = MergeTree
ORDER BY ts;

CREATE TABLE metrics_alias ENGINE = Alias('metrics');

-- Insert through alias
INSERT INTO metrics_alias VALUES 
    (now(), 'cpu_usage', 45.2),
    (now(), 'memory_usage', 78.5);

-- Insert with SELECT
INSERT INTO metrics_alias 
SELECT now(), 'disk_usage', number * 10 
FROM system.numbers 
LIMIT 5;

-- Verify data is in the target table
SELECT count() FROM metrics;  -- Returns 7
SELECT count() FROM metrics_alias;  -- Returns 7
```

<div id="schema-modification">
  ### Modificación del esquema
</div>

Las operaciones ALTER modifican el esquema de la tabla de destino:

```sql
CREATE TABLE users (
    id UInt32,
    name String
) ENGINE = MergeTree
ORDER BY id;

CREATE TABLE users_alias ENGINE = Alias('users');

-- Add column through alias
ALTER TABLE users_alias ADD COLUMN email String DEFAULT '';

-- Column is added to target table
DESCRIBE users;
```

```text
┌─name──┬─type───┬─default_type─┬─default_expression─┐
│ id    │ UInt32 │              │                    │
│ name  │ String │              │                    │
│ email │ String │ DEFAULT      │ ''                 │
└───────┴────────┴──────────────┴────────────────────┘
```

<div id="data-mutations">
  ### Mutaciones de datos
</div>

Se admiten operaciones UPDATE y DELETE:

```sql
CREATE TABLE products (
    id UInt32,
    name String,
    price Float64,
    status String DEFAULT 'active'
) ENGINE = MergeTree
ORDER BY id;

CREATE TABLE products_alias ENGINE = Alias('products');

INSERT INTO products_alias VALUES 
    (1, 'item_one', 100.0, 'active'),
    (2, 'item_two', 200.0, 'active'),
    (3, 'item_three', 300.0, 'inactive');

-- Update through alias
ALTER TABLE products_alias UPDATE price = price * 1.1 WHERE status = 'active';

-- Delete through alias
ALTER TABLE products_alias DELETE WHERE status = 'inactive';

-- Changes are applied to target table
SELECT * FROM products ORDER BY id;
```

```text
┌─id─┬─name─────┬─price─┬─status─┐
│  1 │ item_one │ 110.0 │ active │
│  2 │ item_two │ 220.0 │ active │
└────┴──────────┴───────┴────────┘
```

<div id="partition-operations">
  ### Operaciones sobre particiones
</div>

En las tablas particionadas, las operaciones sobre particiones se redirigen:

```sql
CREATE TABLE logs (
    date Date,
    level String,
    message String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY date;

CREATE TABLE logs_alias ENGINE = Alias('logs');

INSERT INTO logs_alias VALUES 
    ('2024-01-15', 'INFO', 'message1'),
    ('2024-02-15', 'ERROR', 'message2'),
    ('2024-03-15', 'INFO', 'message3');

-- Detach partition through alias
ALTER TABLE logs_alias DETACH PARTITION '202402';

SELECT count() FROM logs_alias;  -- Returns 2 (partition 202402 detached)

-- Attach partition back
ALTER TABLE logs_alias ATTACH PARTITION '202402';

SELECT count() FROM logs_alias;  -- Returns 3
```

<div id="table-optimization">
  ### Optimización de la tabla
</div>

Optimiza las operaciones que fusionan partes en la tabla de destino:

```sql
CREATE TABLE events (
    id UInt32,
    data String
) ENGINE = MergeTree
ORDER BY id;

CREATE TABLE events_alias ENGINE = Alias('events');

-- Multiple inserts create multiple parts
INSERT INTO events_alias VALUES (1, 'data1');
INSERT INTO events_alias VALUES (2, 'data2');
INSERT INTO events_alias VALUES (3, 'data3');

-- Check parts count
SELECT count() FROM system.parts 
WHERE database = currentDatabase() 
  AND table = 'events' 
  AND active;

-- Optimize through alias
OPTIMIZE TABLE events_alias FINAL;

-- Parts are merged in target table
SELECT count() FROM system.parts 
WHERE database = currentDatabase() 
  AND table = 'events' 
  AND active;  -- Returns 1
```

<div id="alias-management">
  ### Gestión de alias
</div>

Los alias pueden renombrarse o eliminarse de forma independiente:

```sql
CREATE TABLE important_data (
    id UInt32,
    value String
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO important_data VALUES (1, 'critical'), (2, 'important');

CREATE TABLE old_alias ENGINE = Alias('important_data');

-- Rename alias (target table unchanged)
RENAME TABLE old_alias TO new_alias;

-- Create another alias to same table
CREATE TABLE another_alias ENGINE = Alias('important_data');

-- Drop one alias (target table and other aliases unchanged)
DROP TABLE new_alias;

SELECT * FROM another_alias;  -- Still works
SELECT count() FROM important_data;  -- Data intact, returns 2
```