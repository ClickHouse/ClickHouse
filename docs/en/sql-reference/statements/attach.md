---
toc_priority: 40
toc_title: ATTACH
---

# ATTACH Statement {#attach}

Attaches a table or a dictionary, for example, when moving a database to another server.

**Syntax**

``` sql
ATTACH TABLE|DICTIONARY [IF NOT EXISTS] [db.]name [ON CLUSTER cluster] ...
```

The query does not create data on the disk, but assumes that data is already in the appropriate places, and just adds information about the table or the dictionary to the server. After executing the `ATTACH` query, the server will know about the existence of the table or the dictionary.

If a table was previously detached ([DETACH](../../sql-reference/statements/detach.md) query), meaning that its structure is known, you can use shorthand without defining the structure.

## Attach Existing Table {#attach-existing-table}

**Syntax**

``` sql
ATTACH TABLE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
```

This query is used when starting the server. The server stores table metadata as files with `ATTACH` queries, which it simply runs at launch (with the exception of some system tables, which are explicitly created on the server).

If the table was detached permanently, it won't be reattached at the server start, so you need to use `ATTACH` query explicitly.

## Create New Table And Attach Data {#create-new-table-and-attach-data}

### With Specified Path to Table Data {#attach-with-specified-path}

**Syntax**

```sql
ATTACH TABLE name FROM 'path/to/data/' (col1 Type1, ...)
```

It creates new table with provided structure and attaches table data from provided directory in `user_files`.

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

**With specify table UUID** (Only for `Atomic` database)

```sql
ATTACH TABLE name UUID '<uuid>' (col1 Type1, ...)
```

It creates new table with provided structure and attaches data from table with the specified UUID.

## Attach Existing Dictionary {#attach-existing-dictionary}

**Syntax**

``` sql
ATTACH DICTIONARY [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
```
