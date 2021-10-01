---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 39
toc_title: GRANT
---

# GRANT {#grant}

-   Conceder [privilegio](#grant-privileges) a las cuentas o roles de usuario de ClickHouse.
-   Asigna roles a cuentas de usuario u otros roles.

Para revocar privilegios, utilice el [REVOKE](revoke.md) instrucción. También puede enumerar los privilegios concedidos por el [SHOW GRANTS](show.md#show-grants-statement) instrucción.

## Concesión de sintaxis de privilegios {#grant-privigele-syntax}

``` sql
GRANT [ON CLUSTER cluster_name] privilege[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} TO {user | role | CURRENT_USER} [,...] [WITH GRANT OPTION]
```

-   `privilege` — Type of privilege.
-   `role` — ClickHouse user role.
-   `user` — ClickHouse user account.

El `WITH GRANT OPTION` subvenciones de cláusula `user` o `role` con permiso para realizar el `GRANT` consulta. Los usuarios pueden otorgar privilegios del mismo alcance que tienen y menos.

## Concesión de sintaxis de roles {#assign-role-syntax}

``` sql
GRANT [ON CLUSTER cluster_name] role [,...] TO {user | another_role | CURRENT_USER} [,...] [WITH ADMIN OPTION]
```

-   `role` — ClickHouse user role.
-   `user` — ClickHouse user account.

El `WITH ADMIN OPTION` conjuntos de cláusulas [ADMIN OPTION](#admin-option-privilege) privilegio para `user` o `role`.

## Uso {#grant-usage}

Utilizar `GRANT` su cuenta debe tener el `GRANT OPTION` privilegio. Puede otorgar privilegios solo dentro del ámbito de los privilegios de su cuenta.

Por ejemplo, el administrador ha otorgado privilegios a `john` cuenta por la consulta:

``` sql
GRANT SELECT(x,y) ON db.table TO john WITH GRANT OPTION
```

Significa que `john` tiene el permiso para realizar:

-   `SELECT x,y FROM db.table`.
-   `SELECT x FROM db.table`.
-   `SELECT y FROM db.table`.

`john` no puede realizar `SELECT z FROM db.table`. El `SELECT * FROM db.table` también no está disponible. Al procesar esta consulta, ClickHouse no devuelve ningún dato, incluso `x` y `y`. La única excepción es si una tabla `x` y `y` columnas, en este caso ClickHouse devuelve todos los datos.

También `john` tiene el `GRANT OPTION` privilegios, por lo que puede otorgar a otros usuarios con privilegios del mismo o del menor alcance.

Especificación de privilegios que puede utilizar asterisco (`*`) en lugar de una tabla o un nombre de base de datos. Por ejemplo, el `GRANT SELECT ON db.* TO john` consulta permite `john` para realizar el `SELECT` consulta sobre todas las tablas en `db` base. Además, puede omitir el nombre de la base de datos. En este caso, se otorgan privilegios para la base de datos actual, por ejemplo: `GRANT SELECT ON * TO john` otorga el privilegio en todas las tablas de la base de datos actual, `GRANT SELECT ON mytable TO john` otorga el privilegio sobre el `mytable` tabla en la base de datos actual.

Acceso a la `system` base de datos siempre está permitida (ya que esta base de datos se utiliza para procesar consultas).

Puede conceder varios privilegios a varias cuentas en una consulta. Consulta `GRANT SELECT, INSERT ON *.* TO john, robin` permite cuentas `john` y `robin` para realizar el `INSERT` y `SELECT` consultas sobre todas las tablas en todas las bases de datos en el servidor.

## Privilegio {#grant-privileges}

Privilegio es un permiso para realizar un tipo específico de consultas.

Los privilegios tienen una estructura jerárquica. Un conjunto de consultas permitidas depende del ámbito de privilegios.

Jerarquía de privilegios:

-   [SELECT](#grant-select)
-   [INSERT](#grant-insert)
-   [ALTER](#grant-alter)
    -   `ALTER TABLE`
        -   `ALTER UPDATE`
        -   `ALTER DELETE`
        -   `ALTER COLUMN`
            -   `ALTER ADD COLUMN`
            -   `ALTER DROP COLUMN`
            -   `ALTER MODIFY COLUMN`
            -   `ALTER COMMENT COLUMN`
            -   `ALTER CLEAR COLUMN`
            -   `ALTER RENAME COLUMN`
        -   `ALTER INDEX`
            -   `ALTER ORDER BY`
            -   `ALTER ADD INDEX`
            -   `ALTER DROP INDEX`
            -   `ALTER MATERIALIZE INDEX`
            -   `ALTER CLEAR INDEX`
        -   `ALTER CONSTRAINT`
            -   `ALTER ADD CONSTRAINT`
            -   `ALTER DROP CONSTRAINT`
        -   `ALTER TTL`
        -   `ALTER MATERIALIZE TTL`
        -   `ALTER SETTINGS`
        -   `ALTER MOVE PARTITION`
        -   `ALTER FETCH PARTITION`
        -   `ALTER FREEZE PARTITION`
    -   `ALTER VIEW`
        -   `ALTER VIEW REFRESH`
        -   `ALTER VIEW MODIFY QUERY`
-   [CREATE](#grant-create)
    -   `CREATE DATABASE`
    -   `CREATE TABLE`
    -   `CREATE VIEW`
    -   `CREATE DICTIONARY`
    -   `CREATE TEMPORARY TABLE`
-   [DROP](#grant-drop)
    -   `DROP DATABASE`
    -   `DROP TABLE`
    -   `DROP VIEW`
    -   `DROP DICTIONARY`
-   [TRUNCATE](#grant-truncate)
-   [OPTIMIZE](#grant-optimize)
-   [SHOW](#grant-show)
    -   `SHOW DATABASES`
    -   `SHOW TABLES`
    -   `SHOW COLUMNS`
    -   `SHOW DICTIONARIES`
-   [KILL QUERY](#grant-kill-query)
-   [ACCESS MANAGEMENT](#grant-access-management)
    -   `CREATE USER`
    -   `ALTER USER`
    -   `DROP USER`
    -   `CREATE ROLE`
    -   `ALTER ROLE`
    -   `DROP ROLE`
    -   `CREATE ROW POLICY`
    -   `ALTER ROW POLICY`
    -   `DROP ROW POLICY`
    -   `CREATE QUOTA`
    -   `ALTER QUOTA`
    -   `DROP QUOTA`
    -   `CREATE SETTINGS PROFILE`
    -   `ALTER SETTINGS PROFILE`
    -   `DROP SETTINGS PROFILE`
    -   `SHOW ACCESS`
        -   `SHOW_USERS`
        -   `SHOW_ROLES`
        -   `SHOW_ROW_POLICIES`
        -   `SHOW_QUOTAS`
        -   `SHOW_SETTINGS_PROFILES`
    -   `ROLE ADMIN`
-   [SYSTEM](#grant-system)
    -   `SYSTEM SHUTDOWN`
    -   `SYSTEM DROP CACHE`
        -   `SYSTEM DROP DNS CACHE`
        -   `SYSTEM DROP MARK CACHE`
        -   `SYSTEM DROP UNCOMPRESSED CACHE`
    -   `SYSTEM RELOAD`
        -   `SYSTEM RELOAD CONFIG`
        -   `SYSTEM RELOAD DICTIONARY`
        -   `SYSTEM RELOAD EMBEDDED DICTIONARIES`
    -   `SYSTEM MERGES`
    -   `SYSTEM TTL MERGES`
    -   `SYSTEM FETCHES`
    -   `SYSTEM MOVES`
    -   `SYSTEM SENDS`
        -   `SYSTEM DISTRIBUTED SENDS`
        -   `SYSTEM REPLICATED SENDS`
    -   `SYSTEM REPLICATION QUEUES`
    -   `SYSTEM SYNC REPLICA`
    -   `SYSTEM RESTART REPLICA`
    -   `SYSTEM FLUSH`
        -   `SYSTEM FLUSH DISTRIBUTED`
        -   `SYSTEM FLUSH LOGS`
-   [INTROSPECTION](#grant-introspection)
    -   `addressToLine`
    -   `addressToSymbol`
    -   `demangle`
-   [SOURCES](#grant-sources)
    -   `FILE`
    -   `URL`
    -   `REMOTE`
    -   `YSQL`
    -   `ODBC`
    -   `JDBC`
    -   `HDFS`
    -   `S3`
-   [dictGet](#grant-dictget)

Ejemplos de cómo se trata esta jerarquía:

-   El `ALTER` privilegio incluye todos los demás `ALTER*` privilegio.
-   `ALTER CONSTRAINT` incluir `ALTER ADD CONSTRAINT` y `ALTER DROP CONSTRAINT` privilegio.

Los privilegios se aplican a diferentes niveles. Conocer un nivel sugiere sintaxis disponible para privilegios.

Niveles (de menor a mayor):

-   `COLUMN` — Privilege can be granted for column, table, database, or globally.
-   `TABLE` — Privilege can be granted for table, database, or globally.
-   `VIEW` — Privilege can be granted for view, database, or globally.
-   `DICTIONARY` — Privilege can be granted for dictionary, database, or globally.
-   `DATABASE` — Privilege can be granted for database or globally.
-   `GLOBAL` — Privilege can be granted only globally.
-   `GROUP` — Groups privileges of different levels. When `GROUP`-level privilegio se concede, sólo los privilegios del grupo se conceden que corresponden a la sintaxis utilizada.

Ejemplos de sintaxis permitida:

-   `GRANT SELECT(x) ON db.table TO user`
-   `GRANT SELECT ON db.* TO user`

Ejemplos de sintaxis no permitida:

-   `GRANT CREATE USER(x) ON db.table TO user`
-   `GRANT CREATE USER ON db.* TO user`

El privilegio especial [ALL](#grant-all) otorga todos los privilegios a una cuenta de usuario o un rol.

De forma predeterminada, una cuenta de usuario o un rol no tiene privilegios.

Si un usuario o rol no tiene privilegios, se muestra como [NONE](#grant-none) privilegio.

Algunas consultas por su implementación requieren un conjunto de privilegios. Por ejemplo, para realizar el [RENAME](misc.md#misc_operations-rename) consulta necesita los siguientes privilegios: `SELECT`, `CREATE TABLE`, `INSERT` y `DROP TABLE`.

### SELECT {#grant-select}

Permite realizar [SELECT](select/index.md) consulta.

Nivel de privilegio: `COLUMN`.

**Descripci**

El usuario concedido con este privilegio puede realizar `SELECT` consultas sobre una lista especificada de columnas en la tabla y la base de datos especificadas. Si el usuario incluye otras columnas, entonces se especifica una consulta no devuelve datos.

Considere el siguiente privilegio:

``` sql
GRANT SELECT(x,y) ON db.table TO john
```

Este privilegio permite `john` realizar cualquier `SELECT` consulta que involucra datos de la `x` y/o `y` columnas en `db.table`. Por ejemplo, `SELECT x FROM db.table`. `john` no puede realizar `SELECT z FROM db.table`. El `SELECT * FROM db.table` también no está disponible. Al procesar esta consulta, ClickHouse no devuelve ningún dato, incluso `x` y `y`. La única excepción es si una tabla `x` y `y` columnas, en este caso ClickHouse devuelve todos los datos.

### INSERT {#grant-insert}

Permite realizar [INSERT](insert-into.md) consulta.

Nivel de privilegio: `COLUMN`.

**Descripci**

El usuario concedido con este privilegio puede realizar `INSERT` consultas sobre una lista especificada de columnas en la tabla y la base de datos especificadas. Si el usuario incluye otras columnas, entonces se especifica una consulta no inserta ningún dato.

**Ejemplo**

``` sql
GRANT INSERT(x,y) ON db.table TO john
```

El privilegio concedido permite `john` para insertar datos en el `x` y/o `y` columnas en `db.table`.

### ALTER {#grant-alter}

Permite realizar [ALTER](alter.md) consultas correspondientes a la siguiente jerarquía de privilegios:

-   `ALTER`. Nivel: `COLUMN`.
    -   `ALTER TABLE`. Nivel: `GROUP`
        -   `ALTER UPDATE`. Nivel: `COLUMN`. Apodo: `UPDATE`
        -   `ALTER DELETE`. Nivel: `COLUMN`. Apodo: `DELETE`
        -   `ALTER COLUMN`. Nivel: `GROUP`
            -   `ALTER ADD COLUMN`. Nivel: `COLUMN`. Apodo: `ADD COLUMN`
            -   `ALTER DROP COLUMN`. Nivel: `COLUMN`. Apodo: `DROP COLUMN`
            -   `ALTER MODIFY COLUMN`. Nivel: `COLUMN`. Apodo: `MODIFY COLUMN`
            -   `ALTER COMMENT COLUMN`. Nivel: `COLUMN`. Apodo: `COMMENT COLUMN`
            -   `ALTER CLEAR COLUMN`. Nivel: `COLUMN`. Apodo: `CLEAR COLUMN`
            -   `ALTER RENAME COLUMN`. Nivel: `COLUMN`. Apodo: `RENAME COLUMN`
        -   `ALTER INDEX`. Nivel: `GROUP`. Apodo: `INDEX`
            -   `ALTER ORDER BY`. Nivel: `TABLE`. Apodo: `ALTER MODIFY ORDER BY`, `MODIFY ORDER BY`
            -   `ALTER ADD INDEX`. Nivel: `TABLE`. Apodo: `ADD INDEX`
            -   `ALTER DROP INDEX`. Nivel: `TABLE`. Apodo: `DROP INDEX`
            -   `ALTER MATERIALIZE INDEX`. Nivel: `TABLE`. Apodo: `MATERIALIZE INDEX`
            -   `ALTER CLEAR INDEX`. Nivel: `TABLE`. Apodo: `CLEAR INDEX`
        -   `ALTER CONSTRAINT`. Nivel: `GROUP`. Apodo: `CONSTRAINT`
            -   `ALTER ADD CONSTRAINT`. Nivel: `TABLE`. Apodo: `ADD CONSTRAINT`
            -   `ALTER DROP CONSTRAINT`. Nivel: `TABLE`. Apodo: `DROP CONSTRAINT`
        -   `ALTER TTL`. Nivel: `TABLE`. Apodo: `ALTER MODIFY TTL`, `MODIFY TTL`
        -   `ALTER MATERIALIZE TTL`. Nivel: `TABLE`. Apodo: `MATERIALIZE TTL`
        -   `ALTER SETTINGS`. Nivel: `TABLE`. Apodo: `ALTER SETTING`, `ALTER MODIFY SETTING`, `MODIFY SETTING`
        -   `ALTER MOVE PARTITION`. Nivel: `TABLE`. Apodo: `ALTER MOVE PART`, `MOVE PARTITION`, `MOVE PART`
        -   `ALTER FETCH PARTITION`. Nivel: `TABLE`. Apodo: `FETCH PARTITION`
        -   `ALTER FREEZE PARTITION`. Nivel: `TABLE`. Apodo: `FREEZE PARTITION`
    -   `ALTER VIEW` Nivel: `GROUP`
        -   `ALTER VIEW REFRESH`. Nivel: `VIEW`. Apodo: `ALTER LIVE VIEW REFRESH`, `REFRESH VIEW`
        -   `ALTER VIEW MODIFY QUERY`. Nivel: `VIEW`. Apodo: `ALTER TABLE MODIFY QUERY`

Ejemplos de cómo se trata esta jerarquía:

-   El `ALTER` privilegio incluye todos los demás `ALTER*` privilegio.
-   `ALTER CONSTRAINT` incluir `ALTER ADD CONSTRAINT` y `ALTER DROP CONSTRAINT` privilegio.

**Nota**

-   El `MODIFY SETTING` privilege permite modificar la configuración del motor de tablas. In no afecta la configuración o los parámetros de configuración del servidor.
-   El `ATTACH` operación necesita el [CREATE](#grant-create) privilegio.
-   El `DETACH` operación necesita el [DROP](#grant-drop) privilegio.
-   Para detener la mutación por el [KILL MUTATION](misc.md#kill-mutation) consulta, necesita tener un privilegio para iniciar esta mutación. Por ejemplo, si desea detener el `ALTER UPDATE` consulta, necesita el `ALTER UPDATE`, `ALTER TABLE`, o `ALTER` privilegio.

### CREATE {#grant-create}

Permite realizar [CREATE](create.md) y [ATTACH](misc.md#attach) Consultas DDL correspondientes a la siguiente jerarquía de privilegios:

-   `CREATE`. Nivel: `GROUP`
    -   `CREATE DATABASE`. Nivel: `DATABASE`
    -   `CREATE TABLE`. Nivel: `TABLE`
    -   `CREATE VIEW`. Nivel: `VIEW`
    -   `CREATE DICTIONARY`. Nivel: `DICTIONARY`
    -   `CREATE TEMPORARY TABLE`. Nivel: `GLOBAL`

**Nota**

-   Para eliminar la tabla creada, un usuario necesita [DROP](#grant-drop).

### DROP {#grant-drop}

Permite realizar [DROP](misc.md#drop) y [DETACH](misc.md#detach) consultas correspondientes a la siguiente jerarquía de privilegios:

-   `DROP`. Nivel:
    -   `DROP DATABASE`. Nivel: `DATABASE`
    -   `DROP TABLE`. Nivel: `TABLE`
    -   `DROP VIEW`. Nivel: `VIEW`
    -   `DROP DICTIONARY`. Nivel: `DICTIONARY`

### TRUNCATE {#grant-truncate}

Permite realizar [TRUNCATE](misc.md#truncate-statement) consulta.

Nivel de privilegio: `TABLE`.

### OPTIMIZE {#grant-optimize}

Permite realizar el [OPTIMIZE TABLE](misc.md#misc_operations-optimize) consulta.

Nivel de privilegio: `TABLE`.

### SHOW {#grant-show}

Permite realizar `SHOW`, `DESCRIBE`, `USE`, y `EXISTS` consultas, correspondientes a la siguiente jerarquía de privilegios:

-   `SHOW`. Nivel: `GROUP`
    -   `SHOW DATABASES`. Nivel: `DATABASE`. Permite ejecutar `SHOW DATABASES`, `SHOW CREATE DATABASE`, `USE <database>` consulta.
    -   `SHOW TABLES`. Nivel: `TABLE`. Permite ejecutar `SHOW TABLES`, `EXISTS <table>`, `CHECK <table>` consulta.
    -   `SHOW COLUMNS`. Nivel: `COLUMN`. Permite ejecutar `SHOW CREATE TABLE`, `DESCRIBE` consulta.
    -   `SHOW DICTIONARIES`. Nivel: `DICTIONARY`. Permite ejecutar `SHOW DICTIONARIES`, `SHOW CREATE DICTIONARY`, `EXISTS <dictionary>` consulta.

**Nota**

Un usuario tiene el `SHOW` privilegio si tiene algún otro privilegio relativo a la tabla, diccionario o base de datos especificados.

### KILL QUERY {#grant-kill-query}

Permite realizar el [KILL](misc.md#kill-query-statement) consultas correspondientes a la siguiente jerarquía de privilegios:

Nivel de privilegio: `GLOBAL`.

**Nota**

`KILL QUERY` privilege permite a un usuario matar consultas de otros usuarios.

### ACCESS MANAGEMENT {#grant-access-management}

Permite a un usuario realizar consultas que administran usuarios, roles y directivas de fila.

-   `ACCESS MANAGEMENT`. Nivel: `GROUP`
    -   `CREATE USER`. Nivel: `GLOBAL`
    -   `ALTER USER`. Nivel: `GLOBAL`
    -   `DROP USER`. Nivel: `GLOBAL`
    -   `CREATE ROLE`. Nivel: `GLOBAL`
    -   `ALTER ROLE`. Nivel: `GLOBAL`
    -   `DROP ROLE`. Nivel: `GLOBAL`
    -   `ROLE ADMIN`. Nivel: `GLOBAL`
    -   `CREATE ROW POLICY`. Nivel: `GLOBAL`. Apodo: `CREATE POLICY`
    -   `ALTER ROW POLICY`. Nivel: `GLOBAL`. Apodo: `ALTER POLICY`
    -   `DROP ROW POLICY`. Nivel: `GLOBAL`. Apodo: `DROP POLICY`
    -   `CREATE QUOTA`. Nivel: `GLOBAL`
    -   `ALTER QUOTA`. Nivel: `GLOBAL`
    -   `DROP QUOTA`. Nivel: `GLOBAL`
    -   `CREATE SETTINGS PROFILE`. Nivel: `GLOBAL`. Apodo: `CREATE PROFILE`
    -   `ALTER SETTINGS PROFILE`. Nivel: `GLOBAL`. Apodo: `ALTER PROFILE`
    -   `DROP SETTINGS PROFILE`. Nivel: `GLOBAL`. Apodo: `DROP PROFILE`
    -   `SHOW ACCESS`. Nivel: `GROUP`
        -   `SHOW_USERS`. Nivel: `GLOBAL`. Apodo: `SHOW CREATE USER`
        -   `SHOW_ROLES`. Nivel: `GLOBAL`. Apodo: `SHOW CREATE ROLE`
        -   `SHOW_ROW_POLICIES`. Nivel: `GLOBAL`. Apodo: `SHOW POLICIES`, `SHOW CREATE ROW POLICY`, `SHOW CREATE POLICY`
        -   `SHOW_QUOTAS`. Nivel: `GLOBAL`. Apodo: `SHOW CREATE QUOTA`
        -   `SHOW_SETTINGS_PROFILES`. Nivel: `GLOBAL`. Apodo: `SHOW PROFILES`, `SHOW CREATE SETTINGS PROFILE`, `SHOW CREATE PROFILE`

El `ROLE ADMIN` privilege permite a un usuario otorgar y revocar cualquier función, incluidas aquellas que no se otorgan al usuario con la opción de administrador.

### SYSTEM {#grant-system}

Permite a un usuario realizar el [SYSTEM](system.md) consultas correspondientes a la siguiente jerarquía de privilegios.

-   `SYSTEM`. Nivel: `GROUP`
    -   `SYSTEM SHUTDOWN`. Nivel: `GLOBAL`. Apodo: `SYSTEM KILL`, `SHUTDOWN`
    -   `SYSTEM DROP CACHE`. Apodo: `DROP CACHE`
        -   `SYSTEM DROP DNS CACHE`. Nivel: `GLOBAL`. Apodo: `SYSTEM DROP DNS`, `DROP DNS CACHE`, `DROP DNS`
        -   `SYSTEM DROP MARK CACHE`. Nivel: `GLOBAL`. Apodo: `SYSTEM DROP MARK`, `DROP MARK CACHE`, `DROP MARKS`
        -   `SYSTEM DROP UNCOMPRESSED CACHE`. Nivel: `GLOBAL`. Apodo: `SYSTEM DROP UNCOMPRESSED`, `DROP UNCOMPRESSED CACHE`, `DROP UNCOMPRESSED`
    -   `SYSTEM RELOAD`. Nivel: `GROUP`
        -   `SYSTEM RELOAD CONFIG`. Nivel: `GLOBAL`. Apodo: `RELOAD CONFIG`
        -   `SYSTEM RELOAD DICTIONARY`. Nivel: `GLOBAL`. Apodo: `SYSTEM RELOAD DICTIONARIES`, `RELOAD DICTIONARY`, `RELOAD DICTIONARIES`
        -   `SYSTEM RELOAD EMBEDDED DICTIONARIES`. Nivel: `GLOBAL`. Aliases: R`ELOAD EMBEDDED DICTIONARIES`
    -   `SYSTEM MERGES`. Nivel: `TABLE`. Apodo: `SYSTEM STOP MERGES`, `SYSTEM START MERGES`, `STOP MERGES`, `START MERGES`
    -   `SYSTEM TTL MERGES`. Nivel: `TABLE`. Apodo: `SYSTEM STOP TTL MERGES`, `SYSTEM START TTL MERGES`, `STOP TTL MERGES`, `START TTL MERGES`
    -   `SYSTEM FETCHES`. Nivel: `TABLE`. Apodo: `SYSTEM STOP FETCHES`, `SYSTEM START FETCHES`, `STOP FETCHES`, `START FETCHES`
    -   `SYSTEM MOVES`. Nivel: `TABLE`. Apodo: `SYSTEM STOP MOVES`, `SYSTEM START MOVES`, `STOP MOVES`, `START MOVES`
    -   `SYSTEM SENDS`. Nivel: `GROUP`. Apodo: `SYSTEM STOP SENDS`, `SYSTEM START SENDS`, `STOP SENDS`, `START SENDS`
        -   `SYSTEM DISTRIBUTED SENDS`. Nivel: `TABLE`. Apodo: `SYSTEM STOP DISTRIBUTED SENDS`, `SYSTEM START DISTRIBUTED SENDS`, `STOP DISTRIBUTED SENDS`, `START DISTRIBUTED SENDS`
        -   `SYSTEM REPLICATED SENDS`. Nivel: `TABLE`. Apodo: `SYSTEM STOP REPLICATED SENDS`, `SYSTEM START REPLICATED SENDS`, `STOP REPLICATED SENDS`, `START REPLICATED SENDS`
    -   `SYSTEM REPLICATION QUEUES`. Nivel: `TABLE`. Apodo: `SYSTEM STOP REPLICATION QUEUES`, `SYSTEM START REPLICATION QUEUES`, `STOP REPLICATION QUEUES`, `START REPLICATION QUEUES`
    -   `SYSTEM SYNC REPLICA`. Nivel: `TABLE`. Apodo: `SYNC REPLICA`
    -   `SYSTEM RESTART REPLICA`. Nivel: `TABLE`. Apodo: `RESTART REPLICA`
    -   `SYSTEM FLUSH`. Nivel: `GROUP`
        -   `SYSTEM FLUSH DISTRIBUTED`. Nivel: `TABLE`. Apodo: `FLUSH DISTRIBUTED`
        -   `SYSTEM FLUSH LOGS`. Nivel: `GLOBAL`. Apodo: `FLUSH LOGS`

El `SYSTEM RELOAD EMBEDDED DICTIONARIES` privilegios otorgados implícitamente por el `SYSTEM RELOAD DICTIONARY ON *.*` privilegio.

### INTROSPECTION {#grant-introspection}

Permite usar [introspección](../../operations/optimizing-performance/sampling-query-profiler.md) función.

-   `INTROSPECTION`. Nivel: `GROUP`. Apodo: `INTROSPECTION FUNCTIONS`
    -   `addressToLine`. Nivel: `GLOBAL`
    -   `addressToSymbol`. Nivel: `GLOBAL`
    -   `demangle`. Nivel: `GLOBAL`

### SOURCES {#grant-sources}

Permite utilizar fuentes de datos externas. Se aplica a [motores de mesa](../../engines/table-engines/index.md) y [funciones de la tabla](../table-functions/index.md#table-functions).

-   `SOURCES`. Nivel: `GROUP`
    -   `FILE`. Nivel: `GLOBAL`
    -   `URL`. Nivel: `GLOBAL`
    -   `REMOTE`. Nivel: `GLOBAL`
    -   `YSQL`. Nivel: `GLOBAL`
    -   `ODBC`. Nivel: `GLOBAL`
    -   `JDBC`. Nivel: `GLOBAL`
    -   `HDFS`. Nivel: `GLOBAL`
    -   `S3`. Nivel: `GLOBAL`

El `SOURCES` privilege permite el uso de todas las fuentes. También puede otorgar un privilegio para cada fuente individualmente. Para usar fuentes, necesita privilegios adicionales.

Ejemplos:

-   Para crear una tabla con el [Motor de tablas MySQL](../../engines/table-engines/integrations/mysql.md) usted necesita `CREATE TABLE (ON db.table_name)` y `MYSQL` privilegio.
-   Para utilizar el [función de tabla mysql](../table-functions/mysql.md) usted necesita `CREATE TEMPORARY TABLE` y `MYSQL` privilegio.

### dictGet {#grant-dictget}

-   `dictGet`. Apodo: `dictHas`, `dictGetHierarchy`, `dictIsIn`

Permite a un usuario ejecutar [dictGet](../functions/ext-dict-functions.md#dictget), [dictHas](../functions/ext-dict-functions.md#dicthas), [dictGetHierarchy](../functions/ext-dict-functions.md#dictgethierarchy), [DictIsIn](../functions/ext-dict-functions.md#dictisin) función.

Nivel de privilegio: `DICTIONARY`.

**Ejemplos**

-   `GRANT dictGet ON mydb.mydictionary TO john`
-   `GRANT dictGet ON mydictionary TO john`

### ALL {#grant-all}

Otorga todos los privilegios de la entidad regulada a una cuenta de usuario o un rol.

### NONE {#grant-none}

No otorga ningún privilegio.

### ADMIN OPTION {#admin-option-privilege}

El `ADMIN OPTION` privilege permite a un usuario otorgar su rol a otro usuario.

[Artículo Original](https://clickhouse.tech/docs/en/query_language/grant/) <!--hide-->
