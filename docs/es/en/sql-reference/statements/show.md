---
description: 'Documentación de SHOW'
sidebar_label: 'SHOW'
sidebar_position: 37
slug: /sql-reference/statements/show
title: 'Sentencias SHOW'
doc_type: 'reference'
---

:::note

`SHOW CREATE (TABLE|DATABASE|USER)` oculta los secretos, a menos que estén activados los siguientes ajustes:

* [`display_secrets_in_show_and_select`](../../operations/server-configuration-parameters/settings/#display_secrets_in_show_and_select) (ajuste del servidor)
* [`format_display_secrets_in_show_and_select` ](../../operations/settings/formats/#format_display_secrets_in_show_and_select) (ajuste de formato)

Además, el usuario debe tener el privilegio [`displaySecretsInShowAndSelect`](grant.md/#displaysecretsinshowandselect).
:::

<div id="show-create-table--dictionary--view--database">
  ## SHOW CREATE TABLE | DICTIONARY | VIEW | DATABASE
</div>

Estas sentencias devuelven una única columna de tipo String,
que contiene la consulta `CREATE` utilizada para crear el objeto especificado.

<div id="syntax">
  ### Sintaxis
</div>

```sql title="Syntax"
SHOW [CREATE] TABLE | TEMPORARY TABLE | DICTIONARY | VIEW | DATABASE [db.]table|view [INTO OUTFILE filename] [FORMAT format]
```

:::note
Si usas esta sentencia para obtener la consulta `CREATE` de las tablas del sistema,
obtendrás una consulta *falsa* que solo declara la estructura de la tabla,
pero no sirve para crear una tabla.
:::

<div id="show-databases">
  ## SHOW DATABASES
</div>

Esta sentencia muestra una lista de todas las bases de datos.

<div id="syntax">
  ### Sintaxis
</div>

```sql title="Syntax"
SHOW DATABASES [[NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE filename] [FORMAT format]
```

Es igual a la consulta:

```sql
SELECT name FROM system.databases [WHERE name [NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE filename] [FORMAT format]
```

<div id="examples">
  ### Ejemplos
</div>

En este ejemplo usamos `SHOW` para obtener los nombres de las bases de datos que contienen la secuencia de caracteres &#39;de&#39; en sus nombres:

```sql title="Query"
SHOW DATABASES LIKE '%de%'
```

```text title="Response"
┌─name────┐
│ default │
└─────────┘
```

También podemos hacerlo sin distinguir entre mayúsculas y minúsculas:

```sql title="Query"
SHOW DATABASES ILIKE '%DE%'
```

```text title="Response"
┌─name────┐
│ default │
└─────────┘
```

O bien, obtén los nombres de las bases de datos cuyos nombres no contienen &#39;de&#39;:

```sql title="Query"
SHOW DATABASES NOT LIKE '%de%'
```

```text title="Response"
┌─name───────────────────────────┐
│ _temporary_and_external_tables │
│ system                         │
│ test                           │
│ tutorial                       │
└────────────────────────────────┘
```

Por último, podemos obtener los nombres de solo las dos primeras bases de datos:

```sql title="Query"
SHOW DATABASES LIMIT 2
```

```text title="Response"
┌─name───────────────────────────┐
│ _temporary_and_external_tables │
│ default                        │
└────────────────────────────────┘
```

<div id="see-also">
  ### Véase también
</div>

* [`CREATE DATABASE`](/es/sql-reference/statements/create/database)

<div id="show-tables">
  ## SHOW TABLES
</div>

La sentencia `SHOW TABLES` muestra una lista de tablas.

<div id="syntax">
  ### Sintaxis
</div>

```sql title="Syntax"
SHOW [FULL] [TEMPORARY] TABLES [{FROM | IN} <db>] [[NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

Si no se especifica la cláusula `FROM`, la consulta devuelve una lista de tablas de la base de datos actual.

Esta sentencia es idéntica a la consulta:

```sql
SELECT name FROM system.tables [WHERE name [NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

<div id="examples">
  ### Ejemplos
</div>

En este ejemplo, usamos la sentencia `SHOW TABLES` para encontrar todas las tablas que contienen &#39;user&#39; en su nombre:

```sql title="Query"
SHOW TABLES FROM system LIKE '%user%'
```

```text title="Response"
┌─name─────────────┐
│ user_directories │
│ users            │
└──────────────────┘
```

También se puede hacer sin distinguir entre mayúsculas y minúsculas:

```sql title="Query"
SHOW TABLES FROM system ILIKE '%USER%'
```

```text title="Response"
┌─name─────────────┐
│ user_directories │
│ users            │
└──────────────────┘
```

O para encontrar tablas cuyos nombres no contienen la letra &#39;s&#39;:

```sql title="Query"
SHOW TABLES FROM system NOT LIKE '%s%'
```

```text title="Response"
┌─name─────────┐
│ metric_log   │
│ metric_log_0 │
│ metric_log_1 │
└──────────────┘
```

Por último, podemos obtener los nombres de las dos primeras tablas:

```sql title="Query"
SHOW TABLES FROM system LIMIT 2
```

```text title="Response"
┌─name───────────────────────────┐
│ aggregate_function_combinators │
│ asynchronous_metric_log        │
└────────────────────────────────┘
```

<div id="see-also">
  ### Véase también
</div>

* [`Crear tablas`](/es/sql-reference/statements/create/table)
* [`SHOW CREATE TABLE`](#show-create-table--dictionary--view--database)

<div id="show_columns">
  ## SHOW COLUMNS
</div>

La sentencia `SHOW COLUMNS` muestra una lista de columnas.

<div id="syntax">
  ### Sintaxis
</div>

```sql title="Syntax"
SHOW [EXTENDED] [FULL] COLUMNS {FROM | IN} <table> [{FROM | IN} <db>] [{[NOT] {LIKE | ILIKE} '<pattern>' | WHERE <expr>}] [LIMIT <N>] [INTO
OUTFILE <filename>] [FORMAT <format>]
```

El nombre de la base de datos y de la tabla puede especificarse en forma abreviada como `<db>.<table>`,
lo que significa que `FROM tab FROM db` y `FROM db.tab` son equivalentes.
Si no se especifica ninguna base de datos, la consulta devuelve la lista de columnas de la base de datos actual.

También hay dos palabras clave opcionales: `EXTENDED` y `FULL`. La palabra clave `EXTENDED` actualmente no tiene efecto
y existe por compatibilidad con MySQL. La palabra clave `FULL` hace que la salida incluya las columnas de cotejamiento, comentario y privilegio.

La instrucción `SHOW COLUMNS` produce una tabla de resultados con la siguiente estructura:

| Columna     | Descripción                                                                                                                                     | Tipo               |
| ----------- | ----------------------------------------------------------------------------------------------------------------------------------------------- | ------------------ |
| `field`     | El nombre de la columna                                                                                                                         | `String`           |
| `type`      | El tipo de datos de la columna. Si la consulta se realizó a través del MySQL wire protocol, se muestra el nombre de tipo equivalente en MySQL.  | `String`           |
| `null`      | `YES` si el tipo de datos de la columna es Nullable, `NO` en caso contrario                                                                     | `String`           |
| `key`       | `PRI` si la columna forma parte de la clave primaria, `SOR` si la columna forma parte de la clave de ordenación, vacío en caso contrario        | `String`           |
| `default`   | Expresión predeterminada de la columna si es de tipo `ALIAS`, `DEFAULT` o `MATERIALIZED`; en caso contrario, `NULL`.                            | `Nullable(String)` |
| `extra`     | Información adicional, actualmente sin uso                                                                                                      | `String`           |
| `collation` | (solo si se especificó la palabra clave `FULL`) Cotejamiento de la columna, siempre `NULL` porque ClickHouse no tiene cotejamientos por columna | `Nullable(String)` |
| `comment`   | (solo si se especificó la palabra clave `FULL`) Comentario de la columna                                                                        | `String`           |
| `privilege` | (solo si se especificó la palabra clave `FULL`) El privilegio que tiene sobre esta columna, actualmente no disponible                           | `String`           |

<div id="examples">
  ### Ejemplos
</div>

En este ejemplo usaremos la sentencia `SHOW COLUMNS` para obtener información sobre todas las columnas de la tabla &#39;orders&#39;,
a partir de &#39;delivery&#95;&#39;:

```sql title="Query"
SHOW COLUMNS FROM 'orders' LIKE 'delivery_%'
```

```text title="Response"
┌─field───────────┬─type─────┬─null─┬─key─────┬─default─┬─extra─┐
│ delivery_date   │ DateTime │    0 │ PRI SOR │ ᴺᵁᴸᴸ    │       │
│ delivery_status │ Bool     │    0 │         │ ᴺᵁᴸᴸ    │       │
└─────────────────┴──────────┴──────┴─────────┴─────────┴───────┘
```

<div id="see-also">
  ### Véase también
</div>

* [`system.columns`](../../operations/system-tables/columns.md)

<div id="show-dictionaries">
  ## SHOW DICTIONARIES
</div>

La sentencia `SHOW DICTIONARIES` muestra una lista de [Diccionarios](./create/dictionary/overview.md).

<div id="syntax">
  ### Sintaxis
</div>

```sql title="Syntax"
SHOW DICTIONARIES [FROM <db>] [LIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

Si no se especifica la cláusula `FROM`, la consulta devuelve la lista de diccionarios de la base de datos actual.

Puede obtener los mismos resultados que la consulta `SHOW DICTIONARIES` de la siguiente manera:

```sql
SELECT name FROM system.dictionaries WHERE database = <db> [AND name LIKE <pattern>] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

<div id="examples">
  ### Ejemplos
</div>

La siguiente consulta selecciona las dos primeras filas de la lista de tablas de la base de datos `system` cuyos nombres contienen `reg`.

```sql title="Query"
SHOW DICTIONARIES FROM db LIKE '%reg%' LIMIT 2
```

```text title="Response"
┌─name─────────┐
│ regions      │
│ region_names │
└──────────────┘
```

<div id="show-index">
  ## SHOW INDEX
</div>

Muestra una lista de índices primarios y de omisión de datos de una tabla.

Esta sentencia existe principalmente por compatibilidad con MySQL. Las tablas del sistema [`system.tables`](../../operations/system-tables/tables.md) (para
claves primarias) y [`system.data_skipping_indices`](../../operations/system-tables/data_skipping_indices.md) (para índices de omisión de datos)
proporcionan información equivalente, pero de una forma más nativa de ClickHouse.

<div id="syntax">
  ### Sintaxis
</div>

```sql title="Syntax"
SHOW [EXTENDED] {INDEX | INDEXES | INDICES | KEYS } {FROM | IN} <table> [{FROM | IN} <db>] [WHERE <expr>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

El nombre de la base de datos y de la tabla puede especificarse de forma abreviada como `<db>.<table>`, es decir, `FROM tab FROM db` y `FROM db.tab` son
equivalentes. Si no se especifica ninguna base de datos, la consulta toma la base de datos actual.

La palabra clave opcional `EXTENDED` actualmente no tiene ningún efecto y existe por compatibilidad con MySQL.

La sentencia produce una tabla de resultados con la siguiente estructura:

| Columna         | Descripción                                                                                                                                                   | Tipo               |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------ |
| `table`         | El nombre de la tabla.                                                                                                                                        | `String`           |
| `non_unique`    | Siempre `1`, ya que ClickHouse no admite restricciones de unicidad.                                                                                           | `UInt8`            |
| `key_name`      | El nombre del índice; `PRIMARY` si el índice es de clave primaria.                                                                                            | `String`           |
| `seq_in_index`  | Para un índice de clave primaria, la posición de la columna a partir de `1`. Para un índice de omisión de datos: siempre `1`.                                 | `UInt8`            |
| `column_name`   | Para un índice de clave primaria, el nombre de la columna. Para un índice de omisión de datos: `''` (cadena vacía); consulte el campo &quot;expression&quot;. | `String`           |
| `collation`     | El orden de la columna en el índice: `A` si es ascendente, `D` si es descendente, `NULL` si no está ordenada.                                                 | `Nullable(String)` |
| `cardinality`   | Una estimación de la cardinalidad del índice (número de valores únicos en el índice). Actualmente siempre es 0.                                               | `UInt64`           |
| `sub_part`      | Siempre `NULL` porque ClickHouse no admite prefijos de índice como MySQL.                                                                                     | `Nullable(String)` |
| `packed`        | Siempre `NULL` porque ClickHouse no admite índices compactados (como MySQL).                                                                                  | `Nullable(String)` |
| `null`          | Actualmente no se usa                                                                                                                                         |                    |
| `index_type`    | El tipo de índice, por ejemplo, `PRIMARY`, `MINMAX`, `BLOOM_FILTER`, etc.                                                                                     | `String`           |
| `comment`       | Información adicional sobre el índice; actualmente siempre `''` (cadena vacía).                                                                               | `String`           |
| `index_comment` | `''` (cadena vacía) porque los índices en ClickHouse no pueden tener un campo `COMMENT` (como en MySQL).                                                      | `String`           |
| `visible`       | Si el índice es visible para el optimizador, siempre `YES`.                                                                                                   | `String`           |
| `expression`    | Para un índice de omisión de datos, la expresión del índice. Para un índice de clave primaria: `''` (cadena vacía).                                           | `String`           |

<div id="examples">
  ### Ejemplos
</div>

En este ejemplo, usamos la sentencia `SHOW INDEX` para obtener información sobre todos los índices de la tabla &#39;tbl&#39;

```sql title="Query"
SHOW INDEX FROM 'tbl'
```

```text title="Response"
┌─table─┬─non_unique─┬─key_name─┬─seq_in_index─┬─column_name─┬─collation─┬─cardinality─┬─sub_part─┬─packed─┬─null─┬─index_type───┬─comment─┬─index_comment─┬─visible─┬─expression─┐
│ tbl   │          1 │ blf_idx  │ 1            │ 1           │ ᴺᵁᴸᴸ      │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ BLOOM_FILTER │         │               │ YES     │ d, b       │
│ tbl   │          1 │ mm1_idx  │ 1            │ 1           │ ᴺᵁᴸᴸ      │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ MINMAX       │         │               │ YES     │ a, c, d    │
│ tbl   │          1 │ mm2_idx  │ 1            │ 1           │ ᴺᵁᴸᴸ      │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ MINMAX       │         │               │ YES     │ c, d, e    │
│ tbl   │          1 │ PRIMARY  │ 1            │ c           │ A         │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ PRIMARY      │         │               │ YES     │            │
│ tbl   │          1 │ PRIMARY  │ 2            │ a           │ A         │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ PRIMARY      │         │               │ YES     │            │
│ tbl   │          1 │ set_idx  │ 1            │ 1           │ ᴺᵁᴸᴸ      │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ SET          │         │               │ YES     │ e          │
└───────┴────────────┴──────────┴──────────────┴─────────────┴───────────┴─────────────┴──────────┴────────┴──────┴──────────────┴─────────┴───────────────┴─────────┴────────────┘
```

<div id="see-also">
  ### Véase también
</div>

* [`system.tables`](../../operations/system-tables/tables.md)
* [`system.data_skipping_indices`](../../operations/system-tables/data_skipping_indices.md)

<div id="show-processlist">
  ## SHOW PROCESSLIST
</div>

Muestra el contenido de la tabla [`system.processes`](/es/operations/system-tables/processes), que contiene una lista de las consultas que se están procesando en ese momento, excluyendo las consultas `SHOW PROCESSLIST`.

<div id="syntax">
  ### Sintaxis
</div>

```sql title="Syntax"
SHOW PROCESSLIST [INTO OUTFILE filename] [FORMAT format]
```

La consulta `SELECT * FROM system.processes` devuelve información sobre todas las consultas en curso.

:::tip
Ejecute en la consola:

```bash
$ watch -n1 "clickhouse-client --query='SHOW PROCESSLIST'"
```

:::

<div id="show-grants">
  ## SHOW GRANTS
</div>

La sentencia `SHOW GRANTS` muestra los privilegios de un usuario.

<div id="syntax">
  ### Sintaxis
</div>

```sql title="Syntax"
SHOW GRANTS [FOR user1 [, user2 ...]] [WITH IMPLICIT] [FINAL]
```

Si no se especifica el usuario, la consulta devuelve los privilegios del usuario actual.

El modificador `WITH IMPLICIT` permite mostrar los privilegios implícitos (p. ej., `GRANT SELECT ON system.one`)

El modificador `FINAL` combina todos los privilegios del usuario y de sus roles concedidos (con herencia)

<div id="show-create-user">
  ## SHOW CREATE USER
</div>

La sentencia `SHOW CREATE USER` muestra los parámetros utilizados en la [creación del usuario](../../sql-reference/statements/create/user.md).

<div id="syntax">
  ### Sintaxis
</div>

```sql title="Syntax"
SHOW CREATE USER [name1 [, name2 ...] | CURRENT_USER]
```

<div id="show-create-role">
  ## SHOW CREATE ROLE
</div>

La sentencia `SHOW CREATE ROLE` muestra los parámetros utilizados al [crear el rol](../../sql-reference/statements/create/role.md).

<div id="syntax">
  ### Sintaxis
</div>

```sql title="Syntax"
SHOW CREATE ROLE name1 [, name2 ...]
```

<div id="show-create-row-policy">
  ## SHOW CREATE ROW POLICY
</div>

La sentencia `SHOW CREATE ROW POLICY` muestra los parámetros utilizados en la [creación de la ROW POLICY](../../sql-reference/statements/create/row-policy.md).

<div id="syntax">
  ### Sintaxis
</div>

```sql title="Syntax"
SHOW CREATE [ROW] POLICY name ON [database1.]table1 [, [database2.]table2 ...]
```

<div id="show-create-quota">
  ## SHOW CREATE QUOTA
</div>

La sentencia `SHOW CREATE QUOTA` muestra los parámetros utilizados en la [creación de la QUOTA](../../sql-reference/statements/create/quota.md).

<div id="syntax">
  ### Sintaxis
</div>

```sql title="Syntax"
SHOW CREATE QUOTA [name1 [, name2 ...] | CURRENT]
```

<div id="show-create-settings-profile">
  ## SHOW CREATE SETTINGS PROFILE
</div>

La sentencia `SHOW CREATE SETTINGS PROFILE` muestra los parámetros utilizados al [crear el perfil de configuración](../../sql-reference/statements/create/settings-profile.md).

<div id="syntax">
  ### Sintaxis
</div>

```sql title="Syntax"
SHOW CREATE [SETTINGS] PROFILE name1 [, name2 ...]
```

<div id="show-users">
  ## SHOW USERS
</div>

La sentencia `SHOW USERS` devuelve una lista con los nombres de las [cuentas de usuario](../../guides/sre/user-management/index.md#user-account-management).
Para consultar los parámetros de las cuentas de usuario, vea la tabla del sistema [`system.users`](/es/operations/system-tables/users).

<div id="syntax">
  ### Sintaxis
</div>

```sql title="Syntax"
SHOW USERS
```

<div id="show-roles">
  ## SHOW ROLES
</div>

La sentencia `SHOW ROLES` devuelve una lista de [roles](../../guides/sre/user-management/index.md#role-management).
Para ver otros parámetros,
consulte las tablas del sistema [`system.roles`](/es/operations/system-tables/roles) y [`system.role_grants`](/es/operations/system-tables/role_grants).

<div id="syntax">
  ### Sintaxis
</div>

```sql title="Syntax"
SHOW [CURRENT|ENABLED] ROLES
```

<div id="show-profiles">
  ## SHOW PROFILES
</div>

La sentencia `SHOW PROFILES` devuelve una lista de [perfiles de configuración](../../guides/sre/user-management/index.md#settings-profiles-management).
Para ver los parámetros de las cuentas de usuario, consulte la tabla del sistema [`settings_profiles`](/es/operations/system-tables/settings_profiles).

<div id="syntax">
  ### Sintaxis
</div>

```sql title="Syntax"
SHOW [SETTINGS] PROFILES
```

<div id="show-policies">
  ## SHOW POLICIES
</div>

La sentencia `SHOW POLICIES` devuelve una lista de [políticas de filas](../../guides/sre/user-management/index.md#row-policy-management) para la tabla especificada.
Para ver los parámetros de las cuentas de usuario, consulte la tabla del sistema [`system.row_policies`](/es/operations/system-tables/row_policies).

<div id="syntax">
  ### Sintaxis
</div>

```sql title="Syntax"
SHOW [ROW] POLICIES [ON [db.]table]
```

<div id="show-quotas">
  ## SHOW QUOTAS
</div>

La sentencia `SHOW QUOTAS` devuelve una lista de [QUOTAS](../../guides/sre/user-management/index.md#quotas-management).
Para ver los parámetros de las QUOTAS, consulte la tabla del sistema [`system.quotas`](/es/operations/system-tables/quotas).

<div id="syntax">
  ### Sintaxis
</div>

```sql title="Syntax"
SHOW QUOTAS
```

<div id="show-quota">
  ## SHOW QUOTA
</div>

La sentencia `SHOW QUOTA` devuelve el consumo de [QUOTA](../../operations/quotas.md) para todos los usuarios o para el usuario actual.
Para ver otros parámetros, consulte las tablas del sistema [`system.quotas_usage`](/es/operations/system-tables/quotas_usage) y [`system.quota_usage`](/es/operations/system-tables/quota_usage).

<div id="syntax">
  ### Sintaxis
</div>

```sql title="Syntax"
SHOW [CURRENT] QUOTA
```

<div id="show-access">
  ## SHOW ACCESS
</div>

La sentencia `SHOW ACCESS` muestra todos los [usuarios](../../guides/sre/user-management/index.md#user-account-management), [roles](../../guides/sre/user-management/index.md#role-management), [perfiles](../../guides/sre/user-management/index.md#settings-profiles-management), etc., así como todos sus [privilegios](../../sql-reference/statements/grant.md#privileges).

<div id="syntax">
  ### Sintaxis
</div>

```sql title="Syntax"
SHOW ACCESS
```

<div id="show-clusters">
  ## SHOW CLUSTER(S)
</div>

La sentencia `SHOW CLUSTER(S)` devuelve una lista de clústeres.
Todos los clústeres disponibles se muestran en la tabla [`system.clusters`](../../operations/system-tables/clusters.md).

:::note
La consulta `SHOW CLUSTER name` muestra `cluster`, `shard_num`, `replica_num`, `host_name`, `host_address` y `port` de la tabla `system.clusters` para el nombre de clúster especificado.
:::

<div id="syntax">
  ### Sintaxis
</div>

```sql title="Syntax"
SHOW CLUSTER '<name>'
SHOW CLUSTERS [[NOT] LIKE|ILIKE '<pattern>'] [LIMIT <N>]
```

<div id="examples">
  ### Ejemplos
</div>

```sql title="Query"
SHOW CLUSTERS;
```

```text title="Response"
┌─cluster──────────────────────────────────────┐
│ test_cluster_two_shards                      │
│ test_cluster_two_shards_internal_replication │
│ test_cluster_two_shards_localhost            │
│ test_shard_localhost                         │
│ test_shard_localhost_secure                  │
│ test_unavailable_shard                       │
└──────────────────────────────────────────────┘
```

```sql title="Query"
SHOW CLUSTERS LIKE 'test%' LIMIT 1;
```

```text title="Response"
┌─cluster─────────────────┐
│ test_cluster_two_shards │
└─────────────────────────┘
```

```sql title="Query"
SHOW CLUSTER 'test_shard_localhost' FORMAT Vertical;
```

```text title="Response"
Row 1:
──────
cluster:                 test_shard_localhost
shard_num:               1
replica_num:             1
host_name:               localhost
host_address:            127.0.0.1
port:                    9000
```

<div id="show-settings">
  ## SHOW SETTINGS
</div>

La sentencia `SHOW SETTINGS` devuelve una lista de los ajustes del sistema y sus valores.
Selecciona datos de la tabla [`system.settings`](../../operations/system-tables/settings.md).

<div id="syntax">
  ### Sintaxis
</div>

```sql title="Syntax"
SHOW [CHANGED] SETTINGS LIKE|ILIKE <name>
```

<div id="clauses">
  ### Cláusulas
</div>

`LIKE|ILIKE` permiten especificar un patrón de coincidencia para el nombre de la configuración. Puede contener globs como `%` o `_`. La cláusula `LIKE` es sensible a mayúsculas y minúsculas; `ILIKE` no distingue entre mayúsculas y minúsculas.

Cuando se usa la cláusula `CHANGED`, la consulta devuelve solo las configuraciones modificadas con respecto a sus valores predeterminados.

<div id="examples">
  ### Ejemplos
</div>

Consulta con la cláusula `LIKE`:

```sql title="Query"
SHOW SETTINGS LIKE 'send_timeout';
```

```text title="Response"
┌─name─────────┬─type────┬─value─┐
│ send_timeout │ Seconds │ 300   │
└──────────────┴─────────┴───────┘
```

Consulta con la cláusula `ILIKE`:

```sql title="Query"
SHOW SETTINGS ILIKE '%CONNECT_timeout%'
```

```text title="Response"
┌─name────────────────────────────────────┬─type─────────┬─value─┐
│ connect_timeout                         │ Seconds      │ 10    │
│ connect_timeout_with_failover_ms        │ Milliseconds │ 50    │
│ connect_timeout_with_failover_secure_ms │ Milliseconds │ 100   │
└─────────────────────────────────────────┴──────────────┴───────┘
```

Consulta con la cláusula `CHANGED`:

```sql title="Query"
SHOW CHANGED SETTINGS ILIKE '%MEMORY%'
```

```text title="Response"
┌─name─────────────┬─type───┬─value───────┐
│ max_memory_usage │ UInt64 │ 10000000000 │
└──────────────────┴────────┴─────────────┘
```

<div id="show-setting">
  ## SHOW SETTING
</div>

La instrucción `SHOW SETTING` muestra el valor de la configuración correspondiente al nombre de configuración especificado.

<div id="syntax">
  ### Sintaxis
</div>

```sql title="Syntax"
SHOW SETTING <name>
```

<div id="see-also">
  ### Véase también
</div>

* tabla [`system.settings`](../../operations/system-tables/settings.md)

<div id="show-filesystem-caches">
  ## SHOW FILESYSTEM CACHES
</div>

<div id="examples">
  ### Ejemplos
</div>

```sql title="Query"
SHOW FILESYSTEM CACHES
```

```text title="Response"
┌─Caches────┐
│ s3_cache  │
└───────────┘
```

<div id="see-also">
  ### Véase también
</div>

* la tabla [`system.settings`](../../operations/system-tables/settings.md)

<div id="show-engines">
  ## SHOW ENGINES
</div>

La sentencia `SHOW ENGINES` muestra el contenido de la tabla [`system.table_engines`](../../operations/system-tables/table_engines.md),
que contiene la descripción de los motores de tabla compatibles con el servidor y la información sobre las funcionalidades que admiten.

<div id="syntax">
  ### Sintaxis
</div>

```sql title="Syntax"
SHOW ENGINES [INTO OUTFILE filename] [FORMAT format]
```

<div id="see-also">
  ### Véase también
</div>

* tabla [system.table&#95;engines](../../operations/system-tables/table_engines.md)

<div id="show-functions">
  ## SHOW FUNCTIONS
</div>

La sentencia `SHOW FUNCTIONS` muestra el contenido de la tabla [`system.functions`](../../operations/system-tables/functions.md).

<div id="syntax">
  ### Sintaxis
</div>

```sql title="Syntax"
SHOW FUNCTIONS [LIKE | ILIKE '<pattern>']
```

Si se especifica cualquiera de las cláusulas `LIKE` o `ILIKE`, la consulta devuelve una lista de funciones del sistema cuyos nombres coinciden con el `<pattern>` proporcionado.

<div id="see-also">
  ### Véase también
</div>

* tabla [`system.functions`](../../operations/system-tables/functions.md)

<div id="show-merges">
  ## SHOW MERGES
</div>

La instrucción `SHOW MERGES` devuelve una lista de fusiones.
Todas las fusiones se enumeran en la tabla [`system.merges`](../../operations/system-tables/merges.md):

| Columna             | Descripción                                                     |
| ------------------- | --------------------------------------------------------------- |
| `table`             | Nombre de la tabla.                                             |
| `database`          | Nombre de la base de datos en la que se encuentra la tabla.     |
| `estimate_complete` | Tiempo estimado hasta la finalización (en segundos).            |
| `elapsed`           | Tiempo transcurrido (en segundos) desde que comenzó la fusión.  |
| `progress`          | Porcentaje de trabajo completado (del 0 al 100).                |
| `is_mutation`       | 1 si este proceso es una mutación de parte.                     |
| `size_compressed`   | Tamaño total de los datos comprimidos de las partes fusionadas. |
| `memory_usage`      | Consumo de memoria del proceso de fusión.                       |

<div id="syntax">
  ### Sintaxis
</div>

```sql title="Syntax"
SHOW MERGES [[NOT] LIKE|ILIKE '<table_name_pattern>'] [LIMIT <N>]
```

<div id="examples">
  ### Ejemplos
</div>

```sql title="Query"
SHOW MERGES;
```

```text title="Response"
┌─table──────┬─database─┬─estimate_complete─┬─elapsed─┬─progress─┬─is_mutation─┬─size_compressed─┬─memory_usage─┐
│ your_table │ default  │              0.14 │    0.36 │    73.01 │           0 │        5.40 MiB │    10.25 MiB │
└────────────┴──────────┴───────────────────┴─────────┴──────────┴─────────────┴─────────────────┴──────────────┘
```

```sql title="Query"
SHOW MERGES LIKE 'your_t%' LIMIT 1;
```

```text title="Response"
┌─table──────┬─database─┬─estimate_complete─┬─elapsed─┬─progress─┬─is_mutation─┬─size_compressed─┬─memory_usage─┐
│ your_table │ default  │              0.14 │    0.36 │    73.01 │           0 │        5.40 MiB │    10.25 MiB │
└────────────┴──────────┴───────────────────┴─────────┴──────────┴─────────────┴─────────────────┴──────────────┘
```

<div id="show-create-masking-policy">
  ## SHOW CREATE MASKING POLICY
</div>

La sentencia `SHOW CREATE MASKING POLICY` muestra los parámetros que se utilizaron al [crear la política de enmascaramiento](../../sql-reference/statements/create/masking-policy.md).

<div id="syntax">
  ### Sintaxis
</div>

```sql title="Syntax"
SHOW CREATE MASKING POLICY name ON [database.]table
```