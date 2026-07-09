---
description: 'Crea una base de datos de ClickHouse con tablas de una base de datos PostgreSQL.'
sidebar_label: 'MaterializedPostgreSQL'
sidebar_position: 60
slug: /engines/database-engines/materialized-postgresql
title: 'MaterializedPostgreSQL'
doc_type: 'referencia'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="materializedpostgresql">
  # MaterializedPostgreSQL
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

:::note
Se recomienda a los usuarios de ClickHouse Cloud que utilicen [ClickPipes](/es/integrations/clickpipes) para la replicación de PostgreSQL a ClickHouse. Esto ofrece compatibilidad nativa con Change Data Capture (CDC) de alto rendimiento para PostgreSQL.
:::

Crea una base de datos de ClickHouse con tablas de una base de datos PostgreSQL. En primer lugar, una base de datos con el motor `MaterializedPostgreSQL` crea una instantánea de la base de datos PostgreSQL y carga las tablas necesarias. Las tablas necesarias pueden incluir cualquier subconjunto de tablas de cualquier subconjunto de esquemas de la base de datos especificada. Junto con la instantánea, el motor de base de datos adquiere el LSN y, una vez realizado el initial dump de las tablas, empieza a extraer actualizaciones del WAL. Después de crear la base de datos, las tablas que se añadan posteriormente a la base de datos PostgreSQL no se incorporan automáticamente a la replicación. Deben añadirse manualmente con la consulta `ATTACH TABLE db.table`.

La replicación se implementa mediante PostgreSQL Logical Replication Protocol, que no permite replicar DDL, pero sí detectar si se han producido breaking changes en la replicación (cambios en el tipo de columna, adición o eliminación de columnas). Estos cambios se detectan y las tablas correspondientes dejan de recibir actualizaciones. En este caso, debe usar las consultas `ATTACH`/ `DETACH PERMANENTLY` para recargar por completo la tabla. Si el DDL no rompe la replicación (por ejemplo, al cambiar el nombre de una columna), la tabla seguirá recibiendo actualizaciones (la inserción se realiza por posición).

:::note
Este motor de base de datos es experimental. Para usarlo, establezca `allow_experimental_database_materialized_postgresql` en 1 en sus archivos de configuración o mediante el comando `SET`:

```sql
SET allow_experimental_database_materialized_postgresql=1
```

:::

<div id="creating-a-database">
  ## Crear una base de datos
</div>

```sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster]
ENGINE = MaterializedPostgreSQL('host:port', 'database', 'user', 'password') [SETTINGS ...]
```

**Parámetros del motor**

* `host:port` — endpoint del servidor PostgreSQL.
* `database` — nombre de la base de datos de PostgreSQL.
* `user` — usuario de PostgreSQL.
* `password` — contraseña del usuario.

<div id="example-of-use">
  ## Ejemplo de uso
</div>

```sql
CREATE DATABASE postgres_db
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password');

SHOW TABLES FROM postgres_db;

┌─name───┐
│ table1 │
└────────┘

SELECT * FROM postgres_db.postgres_table;
```

<div id="dynamically-adding-table-to-replication">
  ## Añadir dinámicamente nuevas tablas a la replicación
</div>

Una vez creada la base de datos `MaterializedPostgreSQL`, no detecta automáticamente las tablas nuevas de la base de datos PostgreSQL correspondiente. Estas tablas pueden añadirse manualmente:

```sql
ATTACH TABLE postgres_database.new_table;
```

:::warning
Antes de la versión 22.1, al añadir una tabla a la replicación quedaba un slot de replicación temporal sin eliminar (llamado `{db_name}_ch_replication_slot_tmp`). Si adjunta tablas en una versión de ClickHouse anterior a la 22.1, asegúrese de eliminarlo manualmente (`SELECT pg_drop_replication_slot('{db_name}_ch_replication_slot_tmp')`). De lo contrario, el uso de disco crecerá. Este problema se corrigió en la versión 22.1.
:::

<div id="dynamically-removing-table-from-replication">
  ## Cómo eliminar dinámicamente tablas de la replicación
</div>

Es posible eliminar tablas específicas de la replicación:

```sql
DETACH TABLE postgres_database.table_to_remove PERMANENTLY;
```

<div id="schema">
  ## Esquema de PostgreSQL
</div>

El [esquema](https://www.postgresql.org/docs/9.1/ddl-schemas.html) de PostgreSQL se puede configurar de 3 maneras (a partir de la versión 21.12).

1. Un esquema para un motor de base de datos `MaterializedPostgreSQL`. Requiere usar el parámetro `materialized_postgresql_schema`.
   Se accede a las tablas únicamente mediante el nombre de la tabla:

```sql
CREATE DATABASE postgres_database
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
SETTINGS materialized_postgresql_schema = 'postgres_schema';

SELECT * FROM postgres_database.table1;
```

2. Cualquier número de esquemas con un conjunto especificado de tablas para un motor de database `MaterializedPostgreSQL`. Requiere usar el parámetro `materialized_postgresql_tables_list`. Cada table se especifica junto con su esquema.
   Se accede a las tables mediante el nombre del esquema y el nombre de la table al mismo tiempo:

```sql
CREATE DATABASE database1
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
SETTINGS materialized_postgresql_tables_list = 'schema1.table1,schema2.table2,schema1.table3',
         materialized_postgresql_tables_list_with_schema = 1;

SELECT * FROM database1.`schema1.table1`;
SELECT * FROM database1.`schema2.table2`;
```

Pero, en este caso, todas las tablas de `materialized_postgresql_tables_list` deben escribirse con su nombre de esquema.
Requiere `materialized_postgresql_tables_list_with_schema = 1`.

Advertencia: en este caso no se permiten puntos en el nombre de la tabla.

3. Cualquier número de esquemas con el conjunto completo de tablas para un motor de base de datos `MaterializedPostgreSQL`. Requiere usar la configuración `materialized_postgresql_schema_list`.

```sql
CREATE DATABASE database1
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
SETTINGS materialized_postgresql_schema_list = 'schema1,schema2,schema3';

SELECT * FROM database1.`schema1.table1`;
SELECT * FROM database1.`schema1.table2`;
SELECT * FROM database1.`schema2.table2`;
```

Advertencia: en este caso no se permiten puntos en el nombre de la tabla.

<div id="requirements">
  ## Requisitos
</div>

1. La configuración [wal&#95;level](https://www.postgresql.org/docs/current/runtime-config-wal.html) debe tener el valor `logical`, y el parámetro `max_replication_slots` debe tener un valor de al menos `2` en el archivo de configuración de PostgreSQL.

2. Cada tabla replicada debe tener una de las siguientes [identidades de réplica](https://www.postgresql.org/docs/10/sql-altertable.html#SQL-CREATETABLE-REPLICA-IDENTITY):

* clave primaria (de forma predeterminada)

* índice

```bash
postgres# CREATE TABLE postgres_table (a Integer NOT NULL, b Integer, c Integer NOT NULL, d Integer, e Integer NOT NULL);
postgres# CREATE unique INDEX postgres_table_index on postgres_table(a, c, e);
postgres# ALTER TABLE postgres_table REPLICA IDENTITY USING INDEX postgres_table_index;
```

La clave primaria siempre se comprueba primero. Si no está presente, se comprueba el índice definido como replica identity index.
Si el índice se usa como replica identity, solo puede haber un índice de este tipo en una tabla.
Puede comprobar qué tipo se usa para una tabla concreta con el siguiente comando:

```bash
postgres# SELECT CASE relreplident
          WHEN 'd' THEN 'default'
          WHEN 'n' THEN 'nothing'
          WHEN 'f' THEN 'full'
          WHEN 'i' THEN 'index'
       END AS replica_identity
FROM pg_class
WHERE oid = 'postgres_table'::regclass;
```

:::note
No se admite la replicación de los valores de [**TOAST**](https://www.postgresql.org/docs/9.5/storage-toast.html). Se usará el valor predeterminado del tipo de dato.
:::

<div id="settings">
  ## Configuración
</div>

<div id="materialized-postgresql-tables-list">
  ### `materialized_postgresql_tables_list`
</div>

Establece una lista de tablas de la base de datos PostgreSQL separadas por comas, que se replicarán mediante el motor de base de datos [MaterializedPostgreSQL](../../engines/database-engines/materialized-postgresql.md).

Cada tabla puede tener un subconjunto de columnas replicadas entre corchetes. Si se omite ese subconjunto de columnas, se replicarán todas las columnas de la tabla.

```sql
    materialized_postgresql_tables_list = 'table1(co1, col2),table2,table3(co3, col5, col7)
```

Valor predeterminado: lista vacía — significa que se replicará toda la base de datos PostgreSQL.

<div id="materialized-postgresql-schema">
  ### `materialized_postgresql_schema`
</div>

Valor por defecto: cadena vacía. (Se usa el esquema por defecto)

<div id="materialized-postgresql-schema-list">
  ### `materialized_postgresql_schema_list`
</div>

Valor predeterminado: lista vacía. (Se usa el esquema por defecto)

<div id="materialized-postgresql-max-block-size">
  ### `materialized_postgresql_max_block_size`
</div>

Establece el número de filas que se recopilan en memoria antes de volcar los datos en la tabla de la base de datos PostgreSQL.

Valores posibles:

* Entero positivo.

Valor predeterminado: `65536`.

<div id="materialized-postgresql-replication-slot">
  ### `materialized_postgresql_replication_slot`
</div>

Un slot de replicación creado por el usuario. Debe usarse junto con `materialized_postgresql_snapshot`.

<div id="materialized-postgresql-snapshot">
  ### `materialized_postgresql_snapshot`
</div>

Cadena de texto que identifica una instantánea a partir de la cual se realizará el [volcado inicial de las tablas de PostgreSQL](../../engines/database-engines/materialized-postgresql.md). Debe usarse junto con `materialized_postgresql_replication_slot`.

```sql
    CREATE DATABASE database1
    ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
    SETTINGS materialized_postgresql_tables_list = 'table1,table2,table3';

    SELECT * FROM database1.table1;
```

La configuración puede modificarse, si es necesario, mediante una consulta DDL. Sin embargo, no es posible cambiar la configuración `materialized_postgresql_tables_list`. Para actualizar la lista de tablas de esta configuración, use la consulta `ATTACH TABLE`.

```sql
    ALTER DATABASE postgres_database MODIFY SETTING materialized_postgresql_max_block_size = <new_size>;
```

<div id="materialized_postgresql_use_unique_replication_consumer_identifier">
  ### `materialized_postgresql_use_unique_replication_consumer_identifier`
</div>

Utiliza un identificador único para el consumer de replicación. Valor predeterminado: `0`.
Si se establece en `1`, permite configurar varias tablas `MaterializedPostgreSQL` que apunten a la misma tabla `PostgreSQL`.

<div id="materialized-postgresql-use-extended-date-and-time-types">
  ### `materialized_postgresql_use_extended_date_and_time_types`
</div>

Asigna los tipos `date` y `timestamp`/`timestamptz` de PostgreSQL a `Date32` y `DateTime64` de ClickHouse, que abarcan el rango de valores más amplio de los tipos de PostgreSQL. Valor predeterminado: `1`.
Si se establece en `0`, se usan en su lugar los tipos más limitados `Date` y `DateTime` (los valores fuera de su rango o con precisión de subsegundos no pueden representarse).

Esta configuración solo controla los tipos de columna que elige la inferencia de tipos cuando se crean las tablas anidadas, por lo que debe especificarse al ejecutar `CREATE DATABASE`. No puede cambiarse después con `ALTER DATABASE ... MODIFY SETTING` (las tablas anidadas ya creadas conservan sus tipos de columna fijos y ese cambio se rechaza); para cambiarlo, vuelva a crear la base de datos. No se aplica al motor de tabla `MaterializedPostgreSQL`, donde los tipos de columna se declaran explícitamente.

<div id="notes">
  ## Notas
</div>

<div id="logical-replication-slot-failover">
  ### Failover del slot de replicación lógica
</div>

Los slot de replicación lógica que existen en la instancia primaria no están disponibles en las réplicas en espera.
Por lo tanto, si se produce un failover, la nueva primaria (la antigua réplica física en espera) no tendrá conocimiento de ningún slot que existiera en la primaria anterior. Esto provocará una interrupción de la replicación desde PostgreSQL.
Una solución para esto es administrar los slot de replicación usted mismo y definir un slot de replicación permanente (puede encontrar más información [aquí](https://patroni.readthedocs.io/en/latest/SETTINGS.html)). Tendrá que pasar el nombre del slot mediante la configuración `materialized_postgresql_replication_slot`, y este debe exportarse con la opción `EXPORT SNAPSHOT`. El identificador de la instantánea debe pasarse mediante la configuración `materialized_postgresql_snapshot`.

Tenga en cuenta que esto debe usarse solo si realmente es necesario. Si no hay una necesidad real de hacerlo o no se entiende bien por qué, es mejor dejar que el motor de tabla cree y administre su propio slot de replicación.

**Ejemplo (de [@bchrobot](https://github.com/bchrobot))**

1. Configure el slot de replicación en PostgreSQL.

   ```yaml
   apiVersion: "acid.zalan.do/v1"
   kind: postgresql
   metadata:
     name: acid-demo-cluster
   spec:
     numberOfInstances: 2
     postgresql:
       parameters:
         wal_level: logical
     patroni:
       slots:
         clickhouse_sync:
           type: logical
           database: demodb
           plugin: pgoutput
   ```

2. Espere a que el slot de replicación esté listo y, a continuación, inicie una transacción y exporte el identificador de la instantánea de la transacción:

   ```sql
   BEGIN;
   SELECT pg_export_snapshot();
   ```

3. En ClickHouse, cree la base de datos:

   ```sql
   CREATE DATABASE demodb
   ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
   SETTINGS
     materialized_postgresql_replication_slot = 'clickhouse_sync',
     materialized_postgresql_snapshot = '0000000A-0000023F-3',
     materialized_postgresql_tables_list = 'table1,table2,table3';
   ```

4. Finalice la transacción de PostgreSQL una vez que se confirme la replicación hacia la base de datos de ClickHouse. Verifique que la replicación continúe después del failover:

   ```bash
   kubectl exec acid-demo-cluster-0 -c postgres -- su postgres -c 'patronictl failover --candidate acid-demo-cluster-1 --force'
   ```

<div id="required-permissions">
  ### Permisos necesarios
</div>

1. [CREATE PUBLICATION](https://www.postgresql.org/docs/14/sql-createpublication.html) -- privilegio para ejecutar la consulta CREATE.

2. [CREATE&#95;REPLICATION&#95;SLOT](https://www.postgresql.org/docs/10/protocol-replication.html#PROTOCOL-REPLICATION-CREATE-SLOT) -- privilegio de replicación.

3. [pg&#95;drop&#95;replication&#95;slot](https://www.postgresql.org/docs/9.5/functions-admin.html#FUNCTIONS-REPLICATION) -- privilegio de replicación o superuser.

4. [DROP PUBLICATION](https://www.postgresql.org/docs/10/sql-droppublication.html) -- propietario de la publication (`username` en el propio engine MaterializedPostgreSQL).

Es posible evitar ejecutar los comandos `2` y `3`, así como tener esos permisos. Use la configuración `materialized_postgresql_replication_slot` y `materialized_postgresql_snapshot`. Pero con mucho cuidado.

Acceso a las tablas:

1. pg&#95;publication

2. pg&#95;replication&#95;slots

3. pg&#95;publication&#95;tables

<div id="backup-and-restore">
  ### Copia de seguridad y restauración
</div>

Se puede hacer una copia de seguridad de una base de datos `MaterializedPostgreSQL`. Los datos de cada tabla replicada se almacenan en una tabla `ReplacingMergeTree` anidada, por lo que `BACKUP DATABASE` captura esos datos delegando esa operación en la tabla anidada.

```sql
BACKUP DATABASE postgres_db TO Disk('backups', 'postgres_db.zip');
```

Restaurar una base de datos o tabla `MaterializedPostgreSQL` **en el mismo lugar no se admite**. Un objeto `MaterializedPostgreSQL` restaurado empieza inmediatamente a replicarse desde el origen PostgreSQL activo, por lo que restaurar la instantánea de la copia de seguridad sobre él mezclaría la instantánea con el estado remoto actual. Por ello, RESTORE falla por seguridad en este caso. En su lugar, restaure los datos capturados en tablas `ReplacingMergeTree` normales:

* En una copia de seguridad de base de datos, la definición almacenada de cada tabla ya es el `ReplacingMergeTree` anidado sintético (no el motor `MaterializedPostgreSQL`), por lo que cada tabla puede restaurarse directamente en una tabla nueva que aún no exista:

  ```sql
  RESTORE TABLE postgres_db.table1 AS restored_db.table1
  FROM Disk('backups', 'postgres_db.zip')
  SETTINGS allow_different_table_def = 1;
  ```

* En una copia de seguridad de una tabla `MaterializedPostgreSQL` independiente, la definición almacenada es el propio motor `MaterializedPostgreSQL`. Cree de antemano una tabla `ReplacingMergeTree` con la misma estructura que la tabla anidada (incluidas las columnas `_sign` y `_version`) y restaure en ella:

  ```sql
  RESTORE TABLE src AS existing_replacing_mergetree
  FROM Disk('backups', 'table.zip')
  SETTINGS allow_different_table_def = 1;
  ```