---
toc_priority: 32
toc_title: Atomic
---

# Atomic {#atomic}

It supports non-blocking `DROP` and `RENAME TABLE` queries and atomic `EXCHANGE TABLES t1 AND t2` queries. `Atomic` database engine is used by default.

## Creating a Database {#creating-a-database}

``` sql
    CREATE DATABASE test[ ENGINE = Atomic];
```

## Specifics and recommendations {#specifics-and-recommendations}

All tables in database `Atomic` have persistent [UUID](../../sql-reference/data-types/uuid.md) and store data in directory `/clickhouse_path/store/xxx/xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy/`, where `xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy` is UUID of the table. Tables in `Atomic` databases can be accessed by UUID through DatabaseCatalog.

### DROP TABLE

On `DROP TABLE` no data is removed, database `Atomic` just marks table as dropped by moving metadata to `/clickhouse_path/metadata_dropped/` and notifies DatabaseCatalog. Running queries still may use dropped table. Table will be actually removed when it's not in use.

### RENAME TABLE

`RENAME` queries are performed without changing UUID and moving table data. `RENAME` and `DROP` are execute without IStorage-level RWLocks.

### DELETE/DETACH

`DELETE` and `DETACH` queries are executed asynchronously — waits for the running `SELECT` queries to finish but is invisible to the new queries.
You also can specify `NO DELAY` or `SYNC` mode.

## Usage Example {#usage-example}

Input table:

``` text
```

Query:

``` sql
```

Result:

``` text
```

## See Also

-   [system.databases](../../operations/system-tables/databases.md) system table
