---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 38
toc_title: SHOW
---

# MOSTRAR consultas {#show-queries}

## SHOW CREATE TABLE {#show-create-table}

``` sql
SHOW CREATE [TEMPORARY] [TABLE|DICTIONARY] [db.]table [INTO OUTFILE filename] [FORMAT format]
```

Devuelve una sola `String`-tipo ‘statement’ column, which contains a single value – the `CREATE` consulta utilizada para crear el objeto especificado.

## SHOW DATABASES {#show-databases}

``` sql
SHOW DATABASES [INTO OUTFILE filename] [FORMAT format]
```

Imprime una lista de todas las bases de datos.
Esta consulta es idéntica a `SELECT name FROM system.databases [INTO OUTFILE filename] [FORMAT format]`.

## SHOW PROCESSLIST {#show-processlist}

``` sql
SHOW PROCESSLIST [INTO OUTFILE filename] [FORMAT format]
```

Envía el contenido de la [sistema.procesa](../../operations/system-tables.md#system_tables-processes) tabla, que contiene una lista de consultas que se están procesando en este momento, exceptuando `SHOW PROCESSLIST` consulta.

El `SELECT * FROM system.processes` query devuelve datos sobre todas las consultas actuales.

Consejo (ejecutar en la consola):

``` bash
$ watch -n1 "clickhouse-client --query='SHOW PROCESSLIST'"
```

## SHOW TABLES {#show-tables}

Muestra una lista de tablas.

``` sql
SHOW [TEMPORARY] TABLES [{FROM | IN} <db>] [LIKE '<pattern>' | WHERE expr] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

Si el `FROM` no se especifica la cláusula, la consulta devuelve la lista de tablas de la base de datos actual.

Puede obtener los mismos resultados que el `SHOW TABLES` consulta de la siguiente manera:

``` sql
SELECT name FROM system.tables WHERE database = <db> [AND name LIKE <pattern>] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

**Ejemplo**

La siguiente consulta selecciona las dos primeras filas de la lista de tablas `system` base de datos, cuyos nombres contienen `co`.

``` sql
SHOW TABLES FROM system LIKE '%co%' LIMIT 2
```

``` text
┌─name───────────────────────────┐
│ aggregate_function_combinators │
│ collations                     │
└────────────────────────────────┘
```

## SHOW DICTIONARIES {#show-dictionaries}

Muestra una lista de [diccionarios externos](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md).

``` sql
SHOW DICTIONARIES [FROM <db>] [LIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

Si el `FROM` no se especifica la cláusula, la consulta devuelve la lista de diccionarios de la base de datos actual.

Puede obtener los mismos resultados que el `SHOW DICTIONARIES` consulta de la siguiente manera:

``` sql
SELECT name FROM system.dictionaries WHERE database = <db> [AND name LIKE <pattern>] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

**Ejemplo**

La siguiente consulta selecciona las dos primeras filas de la lista de tablas `system` base de datos, cuyos nombres contienen `reg`.

``` sql
SHOW DICTIONARIES FROM db LIKE '%reg%' LIMIT 2
```

``` text
┌─name─────────┐
│ regions      │
│ region_names │
└──────────────┘
```

## SHOW GRANTS {#show-grants-statement}

Muestra privilegios para un usuario.

### Sintaxis {#show-grants-syntax}

``` sql
SHOW GRANTS [FOR user]
```

Si no se especifica user, la consulta devuelve privilegios para el usuario actual.

## SHOW CREATE USER {#show-create-user-statement}

Muestra los parámetros que se usaron en un [creación de usuario](create.md#create-user-statement).

`SHOW CREATE USER` no genera contraseñas de usuario.

### Sintaxis {#show-create-user-syntax}

``` sql
SHOW CREATE USER [name | CURRENT_USER]
```

## SHOW CREATE ROLE {#show-create-role-statement}

Muestra los parámetros que se usaron en un [creación de roles](create.md#create-role-statement)

### Sintaxis {#show-create-role-syntax}

``` sql
SHOW CREATE ROLE name
```

## SHOW CREATE ROW POLICY {#show-create-row-policy-statement}

Muestra los parámetros que se usaron en un [creación de políticas de fila](create.md#create-row-policy-statement)

### Sintaxis {#show-create-row-policy-syntax}

``` sql
SHOW CREATE [ROW] POLICY name ON [database.]table
```

## SHOW CREATE QUOTA {#show-create-quota-statement}

Muestra los parámetros que se usaron en un [creación de cuotas](create.md#create-quota-statement)

### Sintaxis {#show-create-row-policy-syntax}

``` sql
SHOW CREATE QUOTA [name | CURRENT]
```

## SHOW CREATE SETTINGS PROFILE {#show-create-settings-profile-statement}

Muestra los parámetros que se usaron en un [configuración creación de perfil](create.md#create-settings-profile-statement)

### Sintaxis {#show-create-row-policy-syntax}

``` sql
SHOW CREATE [SETTINGS] PROFILE name
```

[Artículo Original](https://clickhouse.tech/docs/en/query_language/show/) <!--hide-->
