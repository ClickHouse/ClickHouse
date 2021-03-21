---
toc_priority: 32
toc_title: Atomic
---

# Atomic {#atomic}

It supports non-blocking `DROP` and `RENAME TABLE` queries and atomic `EXCHANGE TABLES t1 AND t2` queries. `Atomic` database engine is used by default.

## Creating a Database {#creating-a-database}

``` sql
    CREATE DATABASE test ENGINE = Atomic;
```

**Engine Parameters**

## Data Types Support {#data_types-support} 

|  EngineName           | ClickHouse                         |
|-----------------------|------------------------------------|
| NativeDataTypeName    | [ClickHouseDataTypeName](link#)    |


## Specifics and recommendations {#specifics-and-recommendations}

All tables in database `Atomic` have persistent UUID and store data in `/clickhouse_path/store/xxx/xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy/`, where `xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy` is UUID of the table.
`RENAME` queries are performed without changing UUID and moving table data.
Tables in `Atomic` databases can be accessed by UUID through DatabaseCatalog.
On `DROP TABLE` no data is removed, database `Atomic` just marks table as dropped by moving metadata to `/clickhouse_path/metadata_dropped/` and notifies DatabaseCatalog.
Running queries still may use dropped table. Table will be actually removed when it's not in use.
Allows to execute `RENAME` and `DROP` without IStorage-level RWLocks.

## Usage Example {#usage-example}

The example must show usage and use cases. The following text contains the recommended parts of this section.

Input table:

``` text
```

Query:

``` sql
```

Result:

``` text
```

Follow up with any text to clarify the example.
