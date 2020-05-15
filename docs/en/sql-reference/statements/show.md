---
toc_priority: 38
toc_title: SHOW
---

# SHOW Queries {#show-queries}

## SHOW CREATE TABLE {#show-create-table}

``` sql
SHOW CREATE [TEMPORARY] [TABLE|DICTIONARY] [db.]table [INTO OUTFILE filename] [FORMAT format]
```

Returns a single `String`-type ‘statement’ column, which contains a single value – the `CREATE` query used for creating the specified object.

## SHOW DATABASES {#show-databases}

``` sql
SHOW DATABASES [INTO OUTFILE filename] [FORMAT format]
```

Prints a list of all databases.
This query is identical to `SELECT name FROM system.databases [INTO OUTFILE filename] [FORMAT format]`.

## SHOW PROCESSLIST {#show-processlist}

``` sql
SHOW PROCESSLIST [INTO OUTFILE filename] [FORMAT format]
```

Outputs the content of the [system.processes](../../operations/system-tables.md#system_tables-processes) table, that contains a list of queries that is being processed at the moment, excepting `SHOW PROCESSLIST` queries.

The `SELECT * FROM system.processes` query returns data about all the current queries.

Tip (execute in the console):

``` bash
$ watch -n1 "clickhouse-client --query='SHOW PROCESSLIST'"
```

## SHOW TABLES {#show-tables}

Displays a list of tables.

``` sql
SHOW [TEMPORARY] TABLES [{FROM | IN} <db>] [LIKE '<pattern>' | WHERE expr] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

If the `FROM` clause is not specified, the query returns the list of tables from the current database.

You can get the same results as the `SHOW TABLES` query in the following way:

``` sql
SELECT name FROM system.tables WHERE database = <db> [AND name LIKE <pattern>] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

**Example**

The following query selects the first two rows from the list of tables in the `system` database, whose names contain `co`.

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

Displays a list of [external dictionaries](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md).

``` sql
SHOW DICTIONARIES [FROM <db>] [LIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

If the `FROM` clause is not specified, the query returns the list of dictionaries from the current database.

You can get the same results as the `SHOW DICTIONARIES` query in the following way:

``` sql
SELECT name FROM system.dictionaries WHERE database = <db> [AND name LIKE <pattern>] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

**Example**

The following query selects the first two rows from the list of tables in the `system` database, whose names contain `reg`.

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

Shows privileges for a user.

### Syntax {#show-grants-syntax}

``` sql
SHOW GRANTS [FOR user]
```

If user is not specified, the query returns privileges for the current user.



## SHOW CREATE USER {#show-create-user-statement}

Shows parameters that were used at a [user creation](create.md#create-user-statement).

`SHOW CREATE USER` doesn't output user passwords.

### Syntax {#show-create-user-syntax}

``` sql
SHOW CREATE USER [name | CURRENT_USER]
```



## SHOW CREATE ROLE {#show-create-role-statement}

Shows parameters that were used at a [role creation](create.md#create-role-statement).

### Syntax {#show-create-role-syntax}

``` sql
SHOW CREATE ROLE name
```



## SHOW CREATE ROW POLICY {#show-create-row-policy-statement}

Shows parameters that were used at a [row policy creation](create.md#create-row-policy-statement).

### Syntax {#show-create-row-policy-syntax}

```sql
SHOW CREATE [ROW] POLICY name ON [database.]table
```


## SHOW CREATE QUOTA {#show-create-quota-statement}

Shows parameters that were used at a [quota creation](create.md#create-quota-statement).

### Syntax {#show-create-row-policy-syntax}

```sql
SHOW CREATE QUOTA [name | CURRENT]
```


## SHOW CREATE SETTINGS PROFILE {#show-create-settings-profile-statement}

Shows parameters that were used at a [settings profile creation](create.md#create-settings-profile-statement).

### Syntax {#show-create-row-policy-syntax}

```sql
SHOW CREATE [SETTINGS] PROFILE name
```

[Original article](https://clickhouse.tech/docs/en/query_language/show/) <!--hide-->
