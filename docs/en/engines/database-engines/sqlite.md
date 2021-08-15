---
toc_priority: 32
toc_title: SQLite
---

# SQLite {#sqlite}

The engine works with [SQLite](https://www.sqlite.org/index.html). 

## Creating a Database {#creating-a-database}

``` sql
    CREATE DATABASE sqlite_database 
    ENGINE = SQLite('db_path')
```

**Engine Parameters**

-   `db_path` — Path to SQLite file with the database.
    
## Data Types Support {#data_types-support}

|  EngineName           | ClickHouse                         |
|-----------------------|------------------------------------|
| NativeDataTypeName    | [ClickHouseDataTypeName](link#)    |


## Specifics and recommendations {#specifics-and-recommendations}

SQLite stores the entire database (definitions, tables, indices, and the data itself) as a single cross-platform file on a host machine. It is locking the entire database file during writing. SQLite read operations can be multitasked, though writes can only be performed sequentially.
SQLite does not require service management (such as startup scripts) or access control based on `GRANT` and passwords. Access control is handled by means of file-system permissions given to the database file itself.

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
