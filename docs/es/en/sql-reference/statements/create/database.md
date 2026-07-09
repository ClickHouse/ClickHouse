---
description: 'Documentación sobre CREATE DATABASE'
sidebar_label: 'DATABASE'
sidebar_position: 35
slug: /sql-reference/statements/create/database
title: 'CREATE DATABASE'
doc_type: 'reference'
---

Crea una base de datos nueva.

```sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster] [ENGINE = engine(...)] [SETTINGS ...] [COMMENT 'Comment']
```

<div id="clauses">
  ## Cláusulas
</div>

<div id="if-not-exists">
  ### IF NOT EXISTS
</div>

Si la base de datos `db_name` ya existe, ClickHouse no crea una nueva base de datos y:

* No lanza una excepción si se especifica esta cláusula.
* Lanza una excepción si no se especifica esta cláusula.

<div id="on-cluster">
  ### ON CLUSTER
</div>

ClickHouse crea la base de datos `db_name` en todos los servidores del clúster especificado. Encontrará más detalles en el artículo sobre [DDL distribuido](../../../sql-reference/distributed-ddl.md).

<div id="engine">
  ### ENGINE
</div>

De forma predeterminada, ClickHouse usa su propio [motor de base de datos Atomic](../../../engines/database-engines/atomic.md). También están [MySQL](../../../engines/database-engines/mysql.md), [PostgresSQL](../../../engines/database-engines/postgresql.md), [MaterializedPostgreSQL](../../../engines/database-engines/materialized-postgresql.md), [Replicated](../../../engines/database-engines/replicated.md) y [SQLite](../../../engines/database-engines/sqlite.md).

<div id="comment">
  ### COMMENT
</div>

Puede añadir un comentario a la base de datos al crearla.

Se admiten comentarios en todos los motores de base de datos.

**Sintaxis**

```sql
CREATE DATABASE db_name ENGINE = engine(...) COMMENT 'Comment'
```

**Ejemplo**

```sql title="Query"
CREATE DATABASE db_comment ENGINE = Memory COMMENT 'The temporary database';
SELECT name, comment FROM system.databases WHERE name = 'db_comment';
```

```text title="Response"
┌─name───────┬─comment────────────────┐
│ db_comment │ The temporary database │
└────────────┴────────────────────────┘
```

<div id="settings">
  ### SETTINGS
</div>

<div id="lazy-load-tables">
  #### lazy_load_tables
</div>

Cuando está activado, las tablas no se cargan por completo al iniciar la base de datos. En su lugar, se crea un proxy ligero para cada tabla y el motor de tabla real se materializa en el primer acceso. Esto reduce el tiempo de inicio y el uso de memoria en bases de datos con muchas tablas en las que solo se consulta activamente un subconjunto.

```sql
CREATE DATABASE db_name ENGINE = Atomic SETTINGS lazy_load_tables = 1;
```

Se aplica a los motores de base de datos que almacenan los metadatos de las tablas en disco (p. ej., `Atomic`, `Ordinary`). Las vistas, las vistas materializadas, los diccionarios y las tablas respaldadas por funciones de tabla siempre se cargan inmediatamente, independientemente de esta configuración.

**Cuándo usarlo:** Esta configuración es útil para bases de datos con una gran cantidad de tablas (cientos o miles) en las que solo se consulta activamente un subconjunto. Reduce el tiempo de inicio del servidor y el uso de memoria al posponer la creación de objetos del motor de tabla, el escaneo de las partes de datos y la inicialización de hilos en segundo plano hasta el primer acceso.

**Impacto en `system.tables`:**

* Antes de acceder a una tabla, `system.tables` muestra su motor como `TableProxy`. Después del primer acceso, muestra el nombre real del motor (p. ej., `MergeTree`).
* Columnas como `total_rows` y `total_bytes` devuelven `NULL` para las tablas no cargadas porque el almacenamiento real aún no se ha creado.

**Interacción con operaciones DDL:**

* `SELECT`, `INSERT`, `ALTER`, `DROP` activan de forma transparente la carga del motor de tabla real en el primer uso.
* `RENAME TABLE` funciona sin activar la carga.
* Una vez cargada una tabla, permanece cargada durante toda la vida útil del proceso del servidor.

**Limitaciones:**

* Las herramientas de monitorización que dependen de los metadatos de `system.tables` (p. ej., `total_rows`, `engine`) pueden mostrar información incompleta para las tablas no cargadas.
* La primera consulta a una tabla no cargada conlleva un coste de carga puntual (analizar la instrucción `CREATE TABLE` almacenada e inicializar el motor).

Valor predeterminado: `0` (deshabilitado).