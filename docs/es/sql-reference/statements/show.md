---
machine_translated: true
machine_translated_rev: 3e185d24c9fe772c7cf03d5475247fb829a21dfa
toc_priority: 38
toc_title: SHOW
---

# MOSTRAR Consultas {#show-queries}

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

[Artículo Original](https://clickhouse.tech/docs/en/query_language/show/) <!--hide-->
