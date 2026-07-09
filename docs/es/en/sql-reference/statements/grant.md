---
description: 'Documentación de la sentencia GRANT'
sidebar_label: 'GRANT'
sidebar_position: 38
slug: /sql-reference/statements/grant
title: 'Sentencia GRANT'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="grant-statement">
  # Sentencia GRANT
</div>

* Concede [privilegios](#privileges) a las cuentas de usuario de ClickHouse o a los roles.
* Asigna roles a cuentas de usuario o a otros roles.

Para revocar privilegios, use la sentencia [REVOKE](../../sql-reference/statements/revoke.md). También puede ver los privilegios concedidos con la sentencia [SHOW GRANTS](../../sql-reference/statements/show.md#show-grants).

<div id="granting-privilege-syntax">
  ## Sintaxis para otorgar privilegios
</div>

```sql
GRANT [ON CLUSTER cluster_name] privilege[(column_name [,...])] [,...] ON {db.table[*]|db[*].*|*.*|table[*]|*} TO {user | role | CURRENT_USER} [,...] [WITH GRANT OPTION] [WITH REPLACE OPTION]
```

* `privilege` — Tipo de privilegio.
* `role` — rol de usuario de ClickHouse.
* `user` — cuenta de usuario de ClickHouse.

La cláusula `WITH GRANT OPTION` concede a `user` o `role` el permiso para ejecutar la consulta `GRANT`. Los usuarios pueden conceder privilegios del mismo alcance que tienen o de uno inferior.
La cláusula `WITH REPLACE OPTION` sustituye los privilegios anteriores por otros nuevos para el `user` o `role`; si no se especifica, agrega privilegios.

<div id="assigning-role-syntax">
  ## Sintaxis para asignar roles
</div>

```sql
GRANT [ON CLUSTER cluster_name] role [,...] TO {user | another_role | CURRENT_USER} [,...] [WITH ADMIN OPTION] [WITH REPLACE OPTION]
```

* `role` — rol de usuario de ClickHouse.
* `user` — cuenta de usuario de ClickHouse.

La cláusula `WITH ADMIN OPTION` concede el privilegio [ADMIN OPTION](#admin-option) a `user` o `role`.
La cláusula `WITH REPLACE OPTION` sustituye los roles antiguos por el nuevo rol para `user` o `role`; si no se especifica, añade roles.

<div id="grant-current-grants-syntax">
  ## Sintaxis de GRANT CURRENT GRANTS
</div>

```sql
GRANT CURRENT GRANTS{(privilege[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*}) | ON {db.table|db.*|*.*|table|*}} TO {user | role | CURRENT_USER} [,...] [WITH GRANT OPTION] [WITH REPLACE OPTION]
```

* `privilege` — Tipo de privilegio.
* `role` — rol de usuario de ClickHouse.
* `user` — cuenta de usuario de ClickHouse.

El uso de la sentencia `CURRENT GRANTS` permite otorgar todos los privilegios especificados al usuario o rol indicado.
Si no se especifica ninguno de los privilegios, el usuario o rol indicado recibirá todos los privilegios disponibles para `CURRENT_USER`.

<div id="usage">
  ## Uso
</div>

Para usar `GRANT`, su cuenta debe tener el privilegio `GRANT OPTION`. Solo puede otorgar privilegios dentro del ámbito de los privilegios de su cuenta.

Por ejemplo, el administrador ha otorgado privilegios a la cuenta `john` mediante la consulta:

```sql
GRANT SELECT(x,y) ON db.table TO john WITH GRANT OPTION
```

Significa que `john` tiene permiso para ejecutar:

* `SELECT x,y FROM db.table`.
* `SELECT x FROM db.table`.
* `SELECT y FROM db.table`.

`john` no puede ejecutar `SELECT z FROM db.table`. `SELECT * FROM db.table` tampoco está disponible. Al procesar esta consulta, ClickHouse no devuelve ningún dato, ni siquiera `x` e `y`. La única excepción es cuando una tabla contiene solo las columnas `x` e `y`. En ese caso, ClickHouse devuelve todos los datos.

Además, `john` tiene el privilegio `GRANT OPTION`, por lo que puede conceder a otros usuarios privilegios del mismo o de menor alcance.

El acceso a la base de datos `system` siempre está permitido (ya que esta base de datos se utiliza para procesar consultas).

:::note
Aunque hay muchas tablas del sistema a las que los usuarios nuevos pueden acceder de forma predeterminada, es posible que no puedan acceder a todas las tablas del sistema de forma predeterminada si no se les han concedido privilegios.
Además, el acceso a determinadas tablas del sistema, como `system.zookeeper`, está restringido para los usuarios de Cloud por motivos de seguridad.
:::

Puede conceder varios privilegios a varias cuentas en una sola consulta. La consulta `GRANT SELECT, INSERT ON *.* TO john, robin` permite que las cuentas `john` y `robin` ejecuten las consultas `INSERT` y `SELECT` en todas las tablas de todas las bases de datos del servidor.

<div id="wildcard-grants">
  ## Privilegios con comodines
</div>

Al especificar privilegios, puede usar un asterisco (`*`) en lugar del nombre de una tabla o de una base de datos. Por ejemplo, la consulta `GRANT SELECT ON db.* TO john` permite a `john` ejecutar la consulta `SELECT` sobre todas las tablas de la base de datos `db`.
También puede omitir el nombre de la base de datos. En ese caso, los privilegios se conceden para la base de datos actual.
Por ejemplo, `GRANT SELECT ON * TO john` concede el privilegio sobre todas las tablas de la base de datos actual, mientras que `GRANT SELECT ON mytable TO john` concede el privilegio sobre la tabla `mytable` de la base de datos actual.

:::note
La funcionalidad que se describe a continuación está disponible a partir de la versión 24.10 de ClickHouse.
:::

También puede colocar asteriscos al final del nombre de una tabla o de una base de datos. Esta funcionalidad le permite conceder privilegios sobre un prefijo abstracto de la ruta de la tabla.
Ejemplo: `GRANT SELECT ON db.my_tables* TO john`. Esta consulta permite a `john` ejecutar la consulta `SELECT` sobre todas las tablas de la base de datos `db` cuyo prefijo sea `my_tables*`.

Más ejemplos:

`GRANT SELECT ON db.my_tables* TO john`

```sql
SELECT * FROM db.my_tables -- granted
SELECT * FROM db.my_tables_0 -- granted
SELECT * FROM db.my_tables_1 -- granted

SELECT * FROM db.other_table -- not_granted
SELECT * FROM db2.my_tables -- not_granted
```

`GRANT SELECT ON db*.* TO john`

```sql
SELECT * FROM db.my_tables -- granted
SELECT * FROM db.my_tables_0 -- granted
SELECT * FROM db.my_tables_1 -- granted
SELECT * FROM db.other_table -- granted
SELECT * FROM db2.my_tables -- granted
```

Todas las tablas recién creadas dentro de las rutas con privilegios concedidos heredarán automáticamente todos los privilegios de sus rutas superiores.
Por ejemplo, si ejecuta la consulta `GRANT SELECT ON db.* TO john` y luego crea una nueva tabla `db.new_table`, el usuario `john` podrá ejecutar la consulta `SELECT * FROM db.new_table`.

Puede especificar el asterisco **solo** para los prefijos:

```sql
GRANT SELECT ON db.* TO john -- correct
GRANT SELECT ON db*.* TO john -- correct

GRANT SELECT ON *.my_table TO john -- wrong
GRANT SELECT ON foo*bar TO john -- wrong
GRANT SELECT ON *suffix TO john -- wrong
GRANT SELECT(foo) ON db.table* TO john -- wrong
```

<div id="privileges">
  ## Privilegios
</div>

Un privilegio es un permiso concedido a un usuario para ejecutar tipos específicos de consultas.

Los privilegios tienen una estructura jerárquica, y el conjunto de consultas permitidas depende del alcance del privilegio.

A continuación se muestra la jerarquía de privilegios en ClickHouse:

* [`ALL`](#all)
  * [`GESTIÓN DE ACCESO`](#access-management)
    * `ALLOW SQL SECURITY NONE`
    * `ALTER QUOTA`
    * `ALTER ROLE`
    * `ALTER ROW POLICY`
    * `ALTER SETTINGS PROFILE`
    * `ALTER USER`
    * `CREATE QUOTA`
    * `CREATE ROLE`
    * `CREATE ROW POLICY`
    * `CREATE SETTINGS PROFILE`
    * `CREATE USER`
    * `DROP QUOTA`
    * `DROP ROLE`
    * `DROP ROW POLICY`
    * `DROP SETTINGS PROFILE`
    * `DROP USER`
    * `ROLE ADMIN`
    * `SHOW ACCESS`
      * `SHOW QUOTAS`
      * `SHOW ROLES`
      * `SHOW ROW POLICIES`
      * `SHOW SETTINGS PROFILES`
      * `SHOW USERS`
  * [`ALTER`](#alter)
    * `ALTER DATABASE`
      * `ALTER DATABASE SETTINGS`
    * `ALTER TABLE`
      * `ALTER COLUMN`
        * `ALTER ADD COLUMN`
        * `ALTER CLEAR COLUMN`
        * `ALTER COMMENT COLUMN`
        * `ALTER DROP COLUMN`
        * `ALTER MATERIALIZE COLUMN`
        * `ALTER MODIFY COLUMN`
        * `ALTER RENAME COLUMN`
      * `ALTER CONSTRAINT`
        * `ALTER ADD CONSTRAINT`
        * `ALTER DROP CONSTRAINT`
        * `ALTER MODIFY CONSTRAINT`
      * `ALTER DELETE`
      * `ALTER FETCH PARTITION`
      * `ALTER FREEZE PARTITION`
      * `ALTER INDEX`
        * `ALTER ADD INDEX`
        * `ALTER CLEAR INDEX`
        * `ALTER DROP INDEX`
        * `ALTER MATERIALIZE INDEX`
        * `ALTER ORDER BY`
        * `ALTER SAMPLE BY`
      * `ALTER MATERIALIZE TTL`
      * `ALTER MODIFY COMMENT`
      * `ALTER MOVE PARTITION`
      * `ALTER PROJECTION`
      * `ALTER SETTINGS`
      * `ALTER STATISTICS`
        * `ALTER ADD STATISTICS`
        * `ALTER DROP STATISTICS`
        * `ALTER MATERIALIZE STATISTICS`
        * `ALTER MODIFY STATISTICS`
      * `ALTER TTL`
      * `ALTER UPDATE`
      * `ALTER TABLE EXECUTE`
    * `ALTER VIEW`
      * `ALTER VIEW MODIFY QUERY`
      * `ALTER VIEW REFRESH`
      * `ALTER VIEW MODIFY SQL SECURITY`
  * [`BACKUP`](#backup)
  * [`CLUSTER`](#cluster)
  * [`CREATE`](#create)
    * `CREATE ARBITRARY TEMPORARY TABLE`
      * `CREATE TEMPORARY TABLE`
    * `CREATE DATABASE`
    * `CREATE DICTIONARY`
    * `CREATE FUNCTION`
    * `CREATE RESOURCE`
    * `CREATE TABLE`
    * `CREATE VIEW`
    * `CREATE WORKLOAD`
  * [`dictGet`](#dictget)
  * [`displaySecretsInShowAndSelect`](#displaysecretsinshowandselect)
  * [`DROP`](#drop)
    * `DROP DATABASE`
    * `DROP DICTIONARY`
    * `DROP FUNCTION`
    * `DROP RESOURCE`
    * `DROP TABLE`
    * `DROP VIEW`
    * `DROP WORKLOAD`
  * [`INSERT`](#insert)
  * [`INTROSPECTION`](#introspection)
    * `addressToLine`
    * `addressToLineWithInlines`
    * `addressToSymbol`
    * `demangle`
  * `KILL QUERY`
  * `KILL TRANSACTION`
  * `MOVE PARTITION BETWEEN SHARDS`
  * [`NAMED COLLECTION ADMIN`](#named-collection-admin)
    * `ALTER NAMED COLLECTION`
    * `CREATE NAMED COLLECTION`
    * `DROP NAMED COLLECTION`
    * `NAMED COLLECTION`
    * `SHOW NAMED COLLECTIONS`
    * `SHOW NAMED COLLECTIONS SECRETS`
  * [`OPTIMIZE`](#optimize)
  * [`SELECT`](#select)
  * [`SET DEFINER`](/es/sql-reference/statements/create/view#sql_security)
  * [`SHOW`](#show)
    * `SHOW COLUMNS`
    * `SHOW DATABASES`
    * `SHOW DICTIONARIES`
    * `SHOW TABLES`
  * `SHOW FILESYSTEM CACHES`
  * [`SOURCES`](#sources)
    * `AZURE`
    * `FILE`
    * `HDFS`
    * `HIVE`
    * `JDBC`
    * `KAFKA`
    * `MONGO`
    * `MYSQL`
    * `NATS`
    * `ODBC`
    * `POSTGRES`
    * `RABBITMQ`
    * `REDIS`
    * `REMOTE`
    * `S3`
    * `SQLITE`
    * `URL`
  * [`SYSTEM`](#system)
    * `SYSTEM CLEANUP`
    * `SYSTEM DROP CACHE`
      * `SYSTEM DROP COMPILED EXPRESSION CACHE`
      * `SYSTEM DROP CONNECTIONS CACHE`
      * `SYSTEM DROP DISTRIBUTED CACHE`
      * `SYSTEM DROP DNS CACHE`
      * `SYSTEM DROP FILESYSTEM CACHE`
      * `SYSTEM DROP FORMAT SCHEMA CACHE`
      * `SYSTEM DROP MARK CACHE`
      * `SYSTEM DROP MMAP CACHE`
      * `SYSTEM DROP PAGE CACHE`
      * `SYSTEM DROP PRIMARY INDEX CACHE`
      * `SYSTEM DROP QUERY CACHE`
      * `SYSTEM DROP S3 CLIENT CACHE`
      * `SYSTEM DROP SCHEMA CACHE`
      * `SYSTEM DROP UNCOMPRESSED CACHE`
    * `SYSTEM DROP PRIMARY INDEX CACHE`
    * `SYSTEM DROP REPLICA`
    * `SYSTEM FAILPOINT`
    * `SYSTEM FETCHES`
    * `SYSTEM FLUSH`
      * `SYSTEM FLUSH ASYNC INSERT QUEUE`
      * `SYSTEM FLUSH LOGS`
    * `SYSTEM JEMALLOC`
    * `SYSTEM KILL QUERY`
    * `SYSTEM KILL TRANSACTION`
    * `SYSTEM LISTEN`
    * `SYSTEM LOAD PRIMARY KEY`
    * `SYSTEM MERGES`
    * `SYSTEM MOVES`
    * `SYSTEM PULLING REPLICATION LOG`
    * `SYSTEM REDUCE BLOCKING PARTS`
    * `SYSTEM REPLICATION QUEUES`
    * `SYSTEM REPLICA READINESS`
    * `SYSTEM RESET DDL WORKER`
    * `SYSTEM RESTART DISK`
    * `SYSTEM RESTART REPLICA`
    * `SYSTEM RESTORE REPLICA`
    * `SYSTEM RELOAD`
      * `SYSTEM RELOAD ASYNCHRONOUS METRICS`
      * `SYSTEM RELOAD CONFIG`
        * `SYSTEM RELOAD DICTIONARY`
        * `SYSTEM RELOAD EMBEDDED DICTIONARIES`
        * `SYSTEM RELOAD FUNCTION`
        * `SYSTEM RELOAD MODEL`
        * `SYSTEM RELOAD USERS`
    * `SYSTEM SENDS`
      * `SYSTEM DISTRIBUTED SENDS`
      * `SYSTEM REPLICATED SENDS`
    * `SYSTEM SHUTDOWN`
    * `SYSTEM SYNC DATABASE REPLICA`
    * `SYSTEM SYNC FILE CACHE`
    * `SYSTEM SYNC FILESYSTEM CACHE`
    * `SYSTEM SYNC REPLICA`
    * `SYSTEM SYNC TRANSACTION LOG`
    * `SYSTEM THREAD FUZZER`
    * `SYSTEM TTL MERGES`
    * `SYSTEM UNFREEZE`
    * `SYSTEM UNLOAD PRIMARY KEY`
    * `SYSTEM VIEWS`
    * `SYSTEM VIRTUAL PARTS UPDATE`
    * `SYSTEM WAIT LOADING PARTS`
  * [`Motor de tabla`](#table-engine)
  * [`TRUNCATE`](#truncate)
  * `UNDROP TABLE`
* [`NONE`](#none)

Ejemplos de cómo se trata esta jerarquía:

* El privilegio `ALTER` incluye todos los demás privilegios `ALTER*`.
* `ALTER CONSTRAINT` incluye los privilegios `ALTER ADD CONSTRAINT`, `ALTER DROP CONSTRAINT` y `ALTER MODIFY CONSTRAINT`.

Los privilegios se aplican en distintos niveles. Conocer el nivel indica la sintaxis disponible para el privilegio.

Niveles (de menor a mayor):

* `COLUMN` — El privilegio se puede otorgar para una columna, una tabla, una base de datos o globalmente.
* `TABLE` — El privilegio se puede otorgar para una tabla, una base de datos o globalmente.
* `VIEW` — El privilegio se puede otorgar para una vista, una base de datos o globalmente.
* `DICTIONARY` — El privilegio se puede otorgar para un diccionario, una base de datos o globalmente.
* `DATABASE` — El privilegio se puede otorgar para una base de datos o globalmente.
* `GLOBAL` — El privilegio solo se puede otorgar globalmente.
* `GROUP` — Agrupa privilegios de distintos niveles. Cuando se otorga un privilegio de nivel `GROUP`, solo se conceden los privilegios del grupo que corresponden a la sintaxis utilizada.

Ejemplos de sintaxis permitida:

* `GRANT SELECT(x) ON db.table TO user`
* `GRANT SELECT ON db.* TO user`

Ejemplos de sintaxis no permitida:

* `GRANT CREATE USER(x) ON db.table TO user`
* `GRANT CREATE USER ON db.* TO user`

El privilegio especial [ALL](#all) otorga todos los privilegios a una cuenta de usuario o a un rol.

De forma predeterminada, una cuenta de usuario o un rol no tiene privilegios.

Si un usuario o un rol no tiene privilegios, se muestra como [NONE](#none).

Algunas consultas, por su implementación, requieren un conjunto de privilegios. Por ejemplo, para ejecutar la consulta [RENAME](../../sql-reference/statements/optimize.md), necesita los siguientes privilegios: `SELECT`, `CREATE TABLE`, `INSERT` y `DROP TABLE`.

<div id="select">
  ### SELECT
</div>

Permite ejecutar consultas [SELECT](../../sql-reference/statements/select/index.md).

Nivel de privilegio: `COLUMN`.

**Descripción**

El usuario al que se le haya concedido este privilegio puede ejecutar consultas `SELECT` sobre una lista específica de columnas de la tabla y la base de datos especificadas. Si el usuario incluye columnas distintas de las especificadas, la consulta no devuelve datos.

Considere el siguiente privilegio:

```sql
GRANT SELECT(x,y) ON db.table TO john
```

Este privilegio permite a `john` ejecutar cualquier consulta `SELECT` que use datos de las columnas `x` y/o `y` de `db.table`, por ejemplo, `SELECT x FROM db.table`. `john` no puede ejecutar `SELECT z FROM db.table`. `SELECT * FROM db.table` tampoco está permitido. Al procesar esta consulta, ClickHouse no devuelve ningún dato, ni siquiera `x` e `y`. La única excepción es si una tabla contiene solo las columnas `x` e `y`; en ese caso, ClickHouse devuelve todos los datos.

<div id="insert">
  ### INSERT
</div>

Permite ejecutar consultas [INSERT](../../sql-reference/statements/insert-into.md).

Nivel de privilegio: `COLUMN`.

**Descripción**

El usuario al que se le haya otorgado este privilegio puede ejecutar consultas `INSERT` sobre una lista determinada de columnas en la tabla y la base de datos especificadas. Si el usuario incluye columnas distintas de las especificadas, la consulta no insertará ningún dato.

**Ejemplo**

```sql
GRANT INSERT(x,y) ON db.table TO john
```

El privilegio otorgado permite a `john` insertar datos en las columnas `x` y/o `y` de `db.table`.

<div id="alter">
  ### ALTER
</div>

Permite ejecutar consultas [ALTER](../../sql-reference/statements/alter/index.md) según la siguiente jerarquía de privilegios:

* `ALTER`. Nivel: `COLUMN`.
  * `ALTER TABLE`. Nivel: `GROUP`
  * `ALTER UPDATE`. Nivel: `COLUMN`. Alias: `UPDATE`
  * `ALTER DELETE`. Nivel: `COLUMN`. Alias: `DELETE`
  * `ALTER COLUMN`. Nivel: `GROUP`
  * `ALTER ADD COLUMN`. Nivel: `COLUMN`. Alias: `ADD COLUMN`
  * `ALTER DROP COLUMN`. Nivel: `COLUMN`. Alias: `DROP COLUMN`
  * `ALTER MODIFY COLUMN`. Nivel: `COLUMN`. Alias: `MODIFY COLUMN`
  * `ALTER COMMENT COLUMN`. Nivel: `COLUMN`. Alias: `COMMENT COLUMN`
  * `ALTER CLEAR COLUMN`. Nivel: `COLUMN`. Alias: `CLEAR COLUMN`
  * `ALTER RENAME COLUMN`. Nivel: `COLUMN`. Alias: `RENAME COLUMN`
  * `ALTER INDEX`. Nivel: `GROUP`. Alias: `INDEX`
  * `ALTER ORDER BY`. Nivel: `TABLE`. Alias: `ALTER MODIFY ORDER BY`, `MODIFY ORDER BY`
  * `ALTER SAMPLE BY`. Nivel: `TABLE`. Alias: `ALTER MODIFY SAMPLE BY`, `MODIFY SAMPLE BY`
  * `ALTER ADD INDEX`. Nivel: `TABLE`. Alias: `ADD INDEX`
  * `ALTER DROP INDEX`. Nivel: `TABLE`. Alias: `DROP INDEX`
  * `ALTER MATERIALIZE INDEX`. Nivel: `TABLE`. Alias: `MATERIALIZE INDEX`
  * `ALTER CLEAR INDEX`. Nivel: `TABLE`. Alias: `CLEAR INDEX`
  * `ALTER CONSTRAINT`. Nivel: `GROUP`. Alias: `CONSTRAINT`
  * `ALTER ADD CONSTRAINT`. Nivel: `TABLE`. Alias: `ADD CONSTRAINT`
  * `ALTER DROP CONSTRAINT`. Nivel: `TABLE`. Alias: `DROP CONSTRAINT`
  * `ALTER MODIFY CONSTRAINT`. Nivel: `TABLE`. Alias: `MODIFY CONSTRAINT`
  * `ALTER TTL`. Nivel: `TABLE`. Alias: `ALTER MODIFY TTL`, `MODIFY TTL`
  * `ALTER MATERIALIZE TTL`. Nivel: `TABLE`. Alias: `MATERIALIZE TTL`
  * `ALTER SETTINGS`. Nivel: `TABLE`. Alias: `ALTER SETTING`, `ALTER MODIFY SETTING`, `MODIFY SETTING`
  * `ALTER MOVE PARTITION`. Nivel: `TABLE`. Alias: `ALTER MOVE PART`, `MOVE PARTITION`, `MOVE PART`
  * `ALTER FETCH PARTITION`. Nivel: `TABLE`. Alias: `ALTER FETCH PART`, `FETCH PARTITION`, `FETCH PART`
  * `ALTER FREEZE PARTITION`. Nivel: `TABLE`. Alias: `FREEZE PARTITION`
  * `ALTER EXECUTE`. Nivel: `TABLE`. Alias: `ALTER TABLE EXECUTE`
  * `ALTER VIEW`. Nivel: `GROUP`
  * `ALTER VIEW REFRESH`. Nivel: `VIEW`. Alias: `REFRESH VIEW`
  * `ALTER VIEW MODIFY QUERY`. Nivel: `VIEW`. Alias: `ALTER TABLE MODIFY QUERY`
  * `ALTER VIEW MODIFY SQL SECURITY`. Nivel: `VIEW`. Alias: `ALTER TABLE MODIFY SQL SECURITY`

Ejemplos de cómo se aplica esta jerarquía:

* El privilegio `ALTER` incluye todos los demás privilegios `ALTER*`.
* `ALTER CONSTRAINT` incluye los privilegios `ALTER ADD CONSTRAINT`, `ALTER DROP CONSTRAINT` y `ALTER MODIFY CONSTRAINT`.

**Notas**

* El privilegio `MODIFY SETTING` permite modificar la configuración del motor de tabla. No afecta a la configuración ni a los parámetros de configuración del servidor.
* La operación `ATTACH` requiere el privilegio [CREATE](#create).
* La operación `DETACH` requiere el privilegio [DROP](#drop).
* Para detener una mutación con la consulta [KILL MUTATION](../../sql-reference/statements/kill.md#kill-mutation), debe tener el privilegio necesario para iniciar esa mutación. Por ejemplo, si desea detener la consulta `ALTER UPDATE`, necesita el privilegio `ALTER UPDATE`, `ALTER TABLE` o `ALTER`.

<div id="backup">
  ### BACKUP
</div>

Permite ejecutar [`BACKUP`] en las consultas. Para obtener más información sobre las copias de seguridad, consulte [&quot;Copias de seguridad y restauración&quot;](/es/operations/backup/overview).

<div id="create">
  ### CREATE
</div>

Permite ejecutar las sentencias DDL [CREATE](../../sql-reference/statements/create/index.md) y [ATTACH](../../sql-reference/statements/attach.md) según la siguiente jerarquía de privilegios:

* `CREATE`. Nivel: `GROUP`
  * `CREATE DATABASE`. Nivel: `DATABASE`
  * `CREATE TABLE`. Nivel: `TABLE`
    * `CREATE ARBITRARY TEMPORARY TABLE`. Nivel: `GLOBAL`
      * `CREATE TEMPORARY TABLE`. Nivel: `GLOBAL`
  * `CREATE VIEW`. Nivel: `VIEW`
  * `CREATE DICTIONARY`. Nivel: `DICTIONARY`

**Notas**

* Para eliminar la tabla creada, el usuario necesita [DROP](#drop).

<div id="cluster">
  ### CLUSTER
</div>

Permite ejecutar consultas `ON CLUSTER`.

```sql title="Syntax"
GRANT CLUSTER ON *.* TO <username>
```

Por defecto, las consultas con `ON CLUSTER` requieren que el usuario tenga la concesión `CLUSTER`.
Recibirás el siguiente error si intentas usar `ON CLUSTER` en una consulta sin haber concedido antes el privilegio `CLUSTER`:

```text
Not enough privileges. To execute this query, it's necessary to have the grant CLUSTER ON *.*. 
```

El comportamiento predeterminado puede cambiarse estableciendo la opción `on_cluster_queries_require_cluster_grant`,
ubicada en la sección `access_control_improvements` de `config.xml` (véase más abajo), en `false`.

```yaml title="config.xml"
<access_control_improvements>
    <on_cluster_queries_require_cluster_grant>true</on_cluster_queries_require_cluster_grant>
</access_control_improvements>
```

<div id="drop">
  ### DROP
</div>

Permite ejecutar las consultas [DROP](../../sql-reference/statements/drop.md) y [DETACH](../../sql-reference/statements/detach.md) según la siguiente jerarquía de privilegios:

* `DROP`. Nivel: `GROUP`
  * `DROP DATABASE`. Nivel: `DATABASE`
  * `DROP TABLE`. Nivel: `TABLE`
  * `DROP VIEW`. Nivel: `VIEW`
  * `DROP DICTIONARY`. Nivel: `DICTIONARY`

<div id="truncate">
  ### TRUNCATE
</div>

Permite ejecutar sentencias [TRUNCATE](../../sql-reference/statements/truncate.md).

Nivel de privilegio: `TABLE`.

<div id="optimize">
  ### OPTIMIZE
</div>

Permite ejecutar consultas [OPTIMIZE TABLE](../../sql-reference/statements/optimize.md).

Nivel de privilegio: `TABLE`.

<div id="show">
  ### SHOW
</div>

Permite ejecutar consultas `SHOW`, `DESCRIBE`, `USE` y `EXISTS` según la siguiente jerarquía de privilegios:

* `SHOW`. Nivel: `GROUP`
  * `SHOW DATABASES`. Nivel: `DATABASE`. Permite ejecutar las consultas `SHOW DATABASES`, `SHOW CREATE DATABASE` y `USE <database>`.
  * `SHOW TABLES`. Nivel: `TABLE`. Permite ejecutar las consultas `SHOW TABLES`, `EXISTS <table>` y `CHECK <table>`.
  * `SHOW COLUMNS`. Nivel: `COLUMN`. Permite ejecutar las consultas `SHOW CREATE TABLE` y `DESCRIBE`.
  * `SHOW DICTIONARIES`. Nivel: `DICTIONARY`. Permite ejecutar las consultas `SHOW DICTIONARIES`, `SHOW CREATE DICTIONARY` y `EXISTS <dictionary>`.

**Notas**

Un usuario tiene el privilegio `SHOW` si posee cualquier otro privilegio sobre la tabla, el diccionario o la base de datos especificados.

<div id="kill-query">
  ### KILL QUERY
</div>

Permite ejecutar consultas [KILL](../../sql-reference/statements/kill.md#kill-query) según la siguiente jerarquía de privilegios:

Nivel de privilegio: `GLOBAL`.

**Notas**

El privilegio `KILL QUERY` permite a un usuario cancelar las consultas de otros usuarios.

<div id="access-management">
  ### GESTIÓN DE ACCESO
</div>

Permite a un usuario ejecutar consultas para administrar usuarios, roles y políticas de fila.

* `ACCESS MANAGEMENT`. Nivel: `GROUP`
  * `CREATE USER`. Nivel: `GLOBAL`
  * `ALTER USER`. Nivel: `GLOBAL`
  * `DROP USER`. Nivel: `GLOBAL`
  * `CREATE ROLE`. Nivel: `GLOBAL`
  * `ALTER ROLE`. Nivel: `GLOBAL`
  * `DROP ROLE`. Nivel: `GLOBAL`
  * `ROLE ADMIN`. Nivel: `GLOBAL`
  * `CREATE ROW POLICY`. Nivel: `GLOBAL`. Alias: `CREATE POLICY`
  * `ALTER ROW POLICY`. Nivel: `GLOBAL`. Alias: `ALTER POLICY`
  * `DROP ROW POLICY`. Nivel: `GLOBAL`. Alias: `DROP POLICY`
  * `CREATE QUOTA`. Nivel: `GLOBAL`
  * `ALTER QUOTA`. Nivel: `GLOBAL`
  * `DROP QUOTA`. Nivel: `GLOBAL`
  * `CREATE SETTINGS PROFILE`. Nivel: `GLOBAL`. Alias: `CREATE PROFILE`
  * `ALTER SETTINGS PROFILE`. Nivel: `GLOBAL`. Alias: `ALTER PROFILE`
  * `DROP SETTINGS PROFILE`. Nivel: `GLOBAL`. Alias: `DROP PROFILE`
  * `SHOW ACCESS`. Nivel: `GROUP`
    * `SHOW_USERS`. Nivel: `GLOBAL`. Alias: `SHOW CREATE USER`
    * `SHOW_ROLES`. Nivel: `GLOBAL`. Alias: `SHOW CREATE ROLE`
    * `SHOW_ROW_POLICIES`. Nivel: `GLOBAL`. Alias: `SHOW POLICIES`, `SHOW CREATE ROW POLICY`, `SHOW CREATE POLICY`
    * `SHOW_QUOTAS`. Nivel: `GLOBAL`. Alias: `SHOW CREATE QUOTA`
    * `SHOW_SETTINGS_PROFILES`. Nivel: `GLOBAL`. Alias: `SHOW PROFILES`, `SHOW CREATE SETTINGS PROFILE`, `SHOW CREATE PROFILE`
  * `ALLOW SQL SECURITY NONE`. Nivel: `GLOBAL`. Alias: `CREATE SQL SECURITY NONE`, `SQL SECURITY NONE`, `SECURITY NONE`

El privilegio `ROLE ADMIN` permite a un usuario asignar y revocar cualquier rol, incluidos aquellos que no se le hayan asignado con la opción de administrador.

<div id="system">
  ### SYSTEM
</div>

Permite que un usuario ejecute consultas [SYSTEM](../../sql-reference/statements/system.md) de acuerdo con la siguiente jerarquía de privilegios.

* `SYSTEM`. Nivel: `GROUP`
  * `SYSTEM SHUTDOWN`. Nivel: `GLOBAL`. Alias: `SYSTEM KILL`, `SHUTDOWN`
  * `SYSTEM DROP CACHE`. Alias: `DROP CACHE`
    * `SYSTEM DROP DNS CACHE`. Nivel: `GLOBAL`. Alias: `SYSTEM CLEAR DNS CACHE`, `SYSTEM DROP DNS`, `DROP DNS CACHE`, `DROP DNS`
    * `SYSTEM DROP MARK CACHE`. Nivel: `GLOBAL`. Alias: `SYSTEM CLEAR MARK CACHE`, `SYSTEM DROP MARK`, `DROP MARK CACHE`, `DROP MARKS`
    * `SYSTEM DROP UNCOMPRESSED CACHE`. Nivel: `GLOBAL`. Alias: `SYSTEM CLEAR UNCOMPRESSED CACHE`, `SYSTEM DROP UNCOMPRESSED`, `DROP UNCOMPRESSED CACHE`, `DROP UNCOMPRESSED`
  * `SYSTEM RELOAD`. Nivel: `GROUP`
    * `SYSTEM RELOAD CONFIG`. Nivel: `GLOBAL`. Alias: `RELOAD CONFIG`
    * `SYSTEM RELOAD DICTIONARY`. Nivel: `GLOBAL`. Alias: `SYSTEM RELOAD DICTIONARIES`, `RELOAD DICTIONARY`, `RELOAD DICTIONARIES`
      * `SYSTEM RELOAD EMBEDDED DICTIONARIES`. Nivel: `GLOBAL`. Alias: `RELOAD EMBEDDED DICTIONARIES`
  * `SYSTEM MERGES`. Nivel: `TABLE`. Alias: `SYSTEM STOP MERGES`, `SYSTEM START MERGES`, `STOP MERGES`, `START MERGES`
  * `SYSTEM TTL MERGES`. Nivel: `TABLE`. Alias: `SYSTEM STOP TTL MERGES`, `SYSTEM START TTL MERGES`, `STOP TTL MERGES`, `START TTL MERGES`
  * `SYSTEM FETCHES`. Nivel: `TABLE`. Alias: `SYSTEM STOP FETCHES`, `SYSTEM START FETCHES`, `STOP FETCHES`, `START FETCHES`
  * `SYSTEM MOVES`. Nivel: `TABLE`. Alias: `SYSTEM STOP MOVES`, `SYSTEM START MOVES`, `STOP MOVES`, `START MOVES`
  * `SYSTEM SENDS`. Nivel: `GROUP`. Alias: `SYSTEM STOP SENDS`, `SYSTEM START SENDS`, `STOP SENDS`, `START SENDS`
    * `SYSTEM DISTRIBUTED SENDS`. Nivel: `TABLE`. Alias: `SYSTEM STOP DISTRIBUTED SENDS`, `SYSTEM START DISTRIBUTED SENDS`, `STOP DISTRIBUTED SENDS`, `START DISTRIBUTED SENDS`
    * `SYSTEM REPLICATED SENDS`. Nivel: `TABLE`. Alias: `SYSTEM STOP REPLICATED SENDS`, `SYSTEM START REPLICATED SENDS`, `STOP REPLICATED SENDS`, `START REPLICATED SENDS`
  * `SYSTEM REPLICATION QUEUES`. Nivel: `TABLE`. Alias: `SYSTEM STOP REPLICATION QUEUES`, `SYSTEM START REPLICATION QUEUES`, `STOP REPLICATION QUEUES`, `START REPLICATION QUEUES`
  * `SYSTEM SYNC REPLICA`. Nivel: `TABLE`. Alias: `SYNC REPLICA`
  * `SYSTEM RESTART REPLICA`. Nivel: `TABLE`. Alias: `RESTART REPLICA`
  * `SYSTEM FLUSH`. Nivel: `GROUP`
    * `SYSTEM FLUSH DISTRIBUTED`. Nivel: `TABLE`. Alias: `FLUSH DISTRIBUTED`
    * `SYSTEM FLUSH LOGS`. Nivel: `GLOBAL`. Alias: `FLUSH LOGS`

El privilegio `SYSTEM RELOAD EMBEDDED DICTIONARIES` se concede implícitamente mediante el privilegio `SYSTEM RELOAD DICTIONARY ON *.*`.

<div id="introspection">
  ### INTROSPECTION
</div>

Permite usar funciones de [introspección](../../operations/optimizing-performance/sampling-query-profiler.md).

* `INTROSPECTION`. Nivel: `GROUP`. Alias: `INTROSPECTION FUNCTIONS`
  * `addressToLine`. Nivel: `GLOBAL`
  * `addressToLineWithInlines`. Nivel: `GLOBAL`
  * `addressToSymbol`. Nivel: `GLOBAL`
  * `demangle`. Nivel: `GLOBAL`

<div id="sources">
  ### FUENTES
</div>

Permite usar fuentes de datos externas. Se aplica a los [motores de tabla](../../engines/table-engines/index.md) y a las [funciones de tabla](/es/sql-reference/table-functions).

* `READ`. Nivel: `GLOBAL_WITH_PARAMETER`
* `WRITE`. Nivel: `GLOBAL_WITH_PARAMETER`

Parámetros posibles:

* `AZURE`
* `FILE`
* `HDFS`
* `HIVE`
* `JDBC`
* `KAFKA`
* `MONGO`
* `MYSQL`
* `NATS`
* `ODBC`
* `POSTGRES`
* `RABBITMQ`
* `REDIS`
* `REMOTE`
* `S3`
* `SQLITE`
* `URL`

:::note
La separación de las concesiones READ/WRITE para fuentes está disponible a partir de la versión 25.7 y solo con la configuración del servidor
`access_control_improvements.enable_read_write_grants`

De lo contrario, debe usar la sintaxis `GRANT AZURE ON *.* TO user`, que equivale a la nueva `GRANT READ, WRITE ON AZURE TO user`
:::

Ejemplos:

* Para crear una tabla con el [motor de tabla MySQL](../../engines/table-engines/integrations/mysql.md), necesita los privilegios `CREATE TABLE (ON db.table_name)` y `MYSQL`.
* Para usar la [función de tabla mysql](../../sql-reference/table-functions/mysql.md), necesita los privilegios `CREATE TEMPORARY TABLE` y `MYSQL`.

<div id="source-filter-grants">
  ### Privilegios de filtro de origen
</div>

:::note
Esta función está disponible a partir de la versión 25.8 y solo con la configuración del servidor
`access_control_improvements.enable_read_write_grants`
:::

Puede conceder acceso a URI de origen específicas mediante filtros de expresiones regulares. Esto permite un control preciso sobre las fuentes de datos externas a las que pueden acceder los usuarios.

**Sintaxis:**

```sql
GRANT READ ON S3('regexp_pattern') TO user
```

Esta concesión permitirá al usuario leer solo desde las URI de S3 que coincidan con el patrón de expresión regular especificado.

**Ejemplos:**

Conceda acceso a rutas específicas de buckets de S3:

```sql
-- Allow user to read only from s3://foo/ paths
GRANT READ ON S3('s3://foo/.*') TO john

-- Allow user to read from specific file patterns
GRANT READ ON S3('s3://mybucket/data/2024/.*\.parquet') TO analyst

-- Multiple filters can be granted to the same user
GRANT READ ON S3('s3://foo/.*') TO john
GRANT READ ON S3('s3://bar/.*') TO john
```

:::warning
El filtro source acepta **regexp** como parámetro, por lo que la concesión
`GRANT READ ON URL('http://www.google.com') TO john;`

permitirá consultas

```sql
SELECT * FROM url('https://www.google.com');
SELECT * FROM url('https://www-google.com');
```

porque `.` se interpreta como un `Any Single Character` en las expresiones regulares.
Esto puede dar lugar a una posible vulnerabilidad. La concesión correcta debería ser

```sql
GRANT READ ON URL('https://www\.google\.com') TO john;
```

:::

**Volver a otorgar con GRANT OPTION:**

Si la concesión original incluye `WITH GRANT OPTION`, puede volver a otorgarse mediante `GRANT CURRENT GRANTS`:

```sql
-- Original grant with GRANT OPTION
GRANT READ ON S3('s3://foo/.*') TO john WITH GRANT OPTION

-- John can now regrant this access to others
GRANT CURRENT GRANTS(READ ON S3) TO alice
```

**Limitaciones importantes:**

* **No se permiten revocaciones parciales:** No puede revocar un subconjunto de los patrones de filtro concedidos. Debe revocar la concesión completa y volver a concederla con nuevos patrones si es necesario.
* **No se permiten concesiones con comodines:** No puede usar `GRANT READ ON *('regexp')` ni patrones similares basados únicamente en comodines. Debe especificar un source concreto.

<div id="dictget">
  ### dictGet
</div>

* `dictGet`. Alias: `dictHas`, `dictGetHierarchy`, `dictIsIn`

Permite que un usuario ejecute las funciones [dictGet](/es/sql-reference/functions/ext-dict-functions#dictGet), [dictHas](../../sql-reference/functions/ext-dict-functions.md#dictHas), [dictGetHierarchy](../../sql-reference/functions/ext-dict-functions.md#dictGetHierarchy), [dictIsIn](../../sql-reference/functions/ext-dict-functions.md#dictIsIn).

Nivel de privilegio: `DICTIONARY`.

**Ejemplos**

* `GRANT dictGet ON mydb.mydictionary TO john`
* `GRANT dictGet ON mydictionary TO john`

<div id="displaysecretsinshowandselect">
  ### displaySecretsInShowAndSelect
</div>

Permite que un usuario vea secretos en las consultas `SHOW` y `SELECT` si están habilitadas tanto la
[`display_secrets_in_show_and_select` configuración del servidor](../../operations/server-configuration-parameters/settings#display_secrets_in_show_and_select)
como la
[`format_display_secrets_in_show_and_select` configuración de formato](../../operations/settings/formats#format_display_secrets_in_show_and_select).

<div id="named-collection-admin">
  ### NAMED COLLECTION ADMIN
</div>

Permite realizar una operación determinada sobre una colección nombrada específica. Antes de la versión 23.7 se llamaba NAMED COLLECTION CONTROL, y a partir de la 23.7 se añadió NAMED COLLECTION ADMIN, mientras que NAMED COLLECTION CONTROL se conserva como alias.

* `NAMED COLLECTION ADMIN`. Nivel: `NAMED_COLLECTION`. Alias: `NAMED COLLECTION CONTROL`
  * `CREATE NAMED COLLECTION`. Nivel: `NAMED_COLLECTION`
  * `DROP NAMED COLLECTION`. Nivel: `NAMED_COLLECTION`
  * `ALTER NAMED COLLECTION`. Nivel: `NAMED_COLLECTION`
  * `SHOW NAMED COLLECTIONS`. Nivel: `NAMED_COLLECTION`. Alias: `SHOW NAMED COLLECTIONS`
  * `SHOW NAMED COLLECTIONS SECRETS`. Nivel: `NAMED_COLLECTION`. Alias: `SHOW NAMED COLLECTIONS SECRETS`
  * `NAMED COLLECTION`. Nivel: `NAMED_COLLECTION`. Alias: `NAMED COLLECTION USAGE, USE NAMED COLLECTION`

A diferencia de todas las demás concesiones (CREATE, DROP, ALTER, SHOW), la concesión NAMED COLLECTION solo se añadió en la versión 23.7, mientras que todas las demás se añadieron antes, en la 22.12.

**Ejemplos**

Suponiendo que una colección nombrada se llama abc, otorgamos el privilegio CREATE NAMED COLLECTION al usuario john.

* `GRANT CREATE NAMED COLLECTION ON abc TO john`

<div id="table-engine">
  ### TABLE ENGINE
</div>

Permite usar un motor de tabla específico al crear una tabla. Se aplica a los [motores de tabla](../../engines/table-engines/index.md).

**Ejemplos**

* `GRANT TABLE ENGINE ON * TO john`
* `GRANT TABLE ENGINE ON TinyLog TO john`

:::note
De forma predeterminada, por motivos de compatibilidad con versiones anteriores, al crear una tabla con un motor de tabla específico se omiten las concesiones;
sin embargo, puede cambiar este comportamiento configurando [`table_engines_require_grant` en true](https://github.com/ClickHouse/ClickHouse/blob/df970ed64eaf472de1e7af44c21ec95956607ebb/programs/server/config.xml#L853-L855)
en config.xml.
:::

Algunos motores de tabla con fuentes externas pueden requerir permisos `READ`/`WRITE` sobre la fuente correspondiente. Consulte [Fuentes](#sources).

Por ejemplo, para el motor de tabla AzureBlobStorage, puede ser necesaria la siguiente concesión.

* `GRANT READ, WRITE ON AZURE TO john`

<div id="all">
  ### ALL
</div>

<CloudNotSupportedBadge />

Concede todos los privilegios sobre una entidad regulada a una cuenta de usuario o a un rol.

:::note
El privilegio `ALL` no es compatible con ClickHouse Cloud, donde el usuario `default` tiene permisos limitados. Los usuarios pueden conceder los permisos máximos a un usuario otorgándole `default_role`. Consulta [aquí](/es/cloud/security/manage-cloud-users) para obtener más detalles.
Los usuarios también pueden usar `GRANT CURRENT GRANTS` como el usuario `default` para lograr un efecto similar a `ALL`.
:::

<div id="none">
  ### NONE
</div>

No concede ningún privilegio.

<div id="admin-option">
  ### ADMIN OPTION
</div>

El privilegio `ADMIN OPTION` permite a un usuario conceder su rol a otro usuario.