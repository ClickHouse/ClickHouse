---
slug: /en/sql-reference/statements/attach
sidebar_position: 40
sidebar_label: ATTACH
title: "ATTACH Statement"
---

Attaches a table or a dictionary, for example, when moving a database to another server.

**Syntax**

``` sql
ATTACH TABLE|DICTIONARY|DATABASE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster] ...
```

The query does not create data on the disk, but assumes that data is already in the appropriate places, and just adds information about the specified table, dictionary or database to the server. After executing the `ATTACH` query, the server will know about the existence of the table, dictionary or database.

If a table was previously detached ([DETACH](../../sql-reference/statements/detach.md) query), meaning that its structure is known, you can use shorthand without defining the structure.

## Attach Existing Table

**Syntax**

``` sql
ATTACH TABLE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
```

This query is used when starting the server. The server stores table metadata as files with `ATTACH` queries, which it simply runs at launch (with the exception of some system tables, which are explicitly created on the server).

If the table was detached permanently, it won't be reattached at the server start, so you need to use `ATTACH` query explicitly.

## Create New Table And Attach Data

### With Specified Path to Table Data

The query creates a new table with provided structure and attaches table data from the provided directory in `user_files`.

**Syntax**

```sql
ATTACH TABLE name FROM 'path/to/data/' (col1 Type1, ...)
```

**Example**

Query:

```sql
DROP TABLE IF EXISTS test;
INSERT INTO TABLE FUNCTION file('01188_attach/test/data.TSV', 'TSV', 's String, n UInt8') VALUES ('test', 42);
ATTACH TABLE test FROM '01188_attach/test' (s String, n UInt8) ENGINE = File(TSV);
SELECT * FROM test;
```
Result:

```sql
┌─s────┬──n─┐
│ test │ 42 │
└──────┴────┘
```

### With Specified Table UUID

This query creates a new table with provided structure and attaches data from the table with the specified UUID.
It is supported by the [Atomic](../../engines/database-engines/atomic.md) database engine.

**Syntax**

```sql
ATTACH TABLE name UUID '<uuid>' (col1 Type1, ...)
```

## Attach Existing Dictionary

Attaches a previously detached dictionary.

**Syntax**

``` sql
ATTACH DICTIONARY [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
```

## Attach Existing Database

Attaches a previously detached database.

**Syntax**

``` sql
ATTACH DATABASE [IF NOT EXISTS] name [ENGINE=<database engine>] [ON CLUSTER cluster]
```
