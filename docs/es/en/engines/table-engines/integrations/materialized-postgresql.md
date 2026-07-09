---
description: 'Crea una tabla de ClickHouse a partir de un volcado inicial de datos de una tabla de PostgreSQL
  e inicia el proceso de replicación.'
sidebar_label: 'MaterializedPostgreSQL'
sidebar_position: 130
slug: /engines/table-engines/integrations/materialized-postgresql
title: 'Motor de tabla MaterializedPostgreSQL'
doc_type: 'guide'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="materializedpostgresql-table-engine">
  # Motor de tabla MaterializedPostgreSQL
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

:::note
Se recomienda a los usuarios de ClickHouse Cloud usar [ClickPipes](/es/integrations/clickpipes) para la replicación de PostgreSQL a ClickHouse. Esto admite de forma nativa Change Data Capture (CDC) de alto rendimiento para PostgreSQL.
:::

Crea una tabla de ClickHouse con un volcado inicial de datos de una tabla de PostgreSQL e inicia el proceso de replicación; es decir, ejecuta una tarea en segundo plano para aplicar los cambios nuevos a medida que se producen en la tabla de PostgreSQL de la base de datos PostgreSQL remota.

:::note
Este motor de tabla es experimental. Para usarlo, establezca `allow_experimental_materialized_postgresql_table` en 1 en los archivos de configuración o mediante el comando `SET`:

```sql
SET allow_experimental_materialized_postgresql_table=1
```

:::

Si se requiere más de una tabla, se recomienda encarecidamente usar el motor de base de datos [MaterializedPostgreSQL](../../../engines/database-engines/materialized-postgresql.md) en lugar del motor de tabla, y usar la configuración `materialized_postgresql_tables_list`, que especifica las tablas que se replicarán (también será posible añadir el `schema` de la base de datos). Será mucho mejor en términos de CPU, con menos conexiones y menos slots de replicación en la base de datos PostgreSQL remota.

<div id="creating-a-table">
  ## Crear una tabla
</div>

```sql
CREATE TABLE postgresql_db.postgresql_replica (key UInt64, value UInt64)
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_table', 'postgres_user', 'postgres_password')
PRIMARY KEY key;
```

**Parámetros del motor**

* `host:port` — Dirección del servidor PostgreSQL.
* `database` — Nombre de la base de datos remota.
* `table` — Nombre de la tabla remota.
* `user` — Usuario de PostgreSQL.
* `password` — Contraseña del usuario.

<div id="requirements">
  ## Requisitos
</div>

1. La configuración [wal&#95;level](https://www.postgresql.org/docs/current/runtime-config-wal.html) debe tener el valor `logical`, y el parámetro `max_replication_slots` debe tener un valor de al menos `2` en el archivo de configuración de PostgreSQL.

2. Una tabla con el motor `MaterializedPostgreSQL` debe tener una clave primaria, que debe ser la misma que el índice de identidad de réplica (de forma predeterminada, la clave primaria) de una tabla de PostgreSQL (consulte [los detalles sobre el índice de identidad de réplica](../../../engines/database-engines/materialized-postgresql.md#requirements)).

3. Solo se permite la base de datos [Atomic](https://en.wikipedia.org/wiki/Atomicity_\(database_systems\)).

4. El motor de tabla `MaterializedPostgreSQL` solo funciona con versiones de PostgreSQL &gt;= 11, ya que la implementación requiere la función de PostgreSQL [pg&#95;replication&#95;slot&#95;advance](https://pgpedia.info/p/pg_replication_slot_advance.html).

<div id="virtual-columns">
  ## Columnas virtuales
</div>

* `_version` — Contador de transacciones. Tipo: [UInt64](../../../sql-reference/data-types/int-uint.md).

* `_sign` — Marca de eliminación. Tipo: [Int8](../../../sql-reference/data-types/int-uint.md). Valores posibles:
  * `1` — La fila no está eliminada,
  * `-1` — La fila está eliminada.

No es necesario añadir estas columnas al crear una tabla. Siempre están accesibles en la consulta `SELECT`.
La columna `_version` equivale a la posición de `LSN` en `WAL`, por lo que puede usarse para comprobar lo actualizada que está la replicación.

```sql
CREATE TABLE postgresql_db.postgresql_replica (key UInt64, value UInt64)
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_replica', 'postgres_user', 'postgres_password')
PRIMARY KEY key;

SELECT key, value, _version FROM postgresql_db.postgresql_replica;
```

:::note
No se admite la replicación de los valores [**TOAST**](https://www.postgresql.org/docs/9.5/storage-toast.html). Se usará el valor predeterminado del tipo de dato.
:::