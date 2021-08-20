---
toc_priority: 32
toc_title: SQLite
---

# SQLite {#sqlite}

Allows to connect to [SQLite](https://www.sqlite.org/index.html) database. 

## Creating a Database {#creating-a-database}

``` sql
    CREATE DATABASE sqlite_database 
    ENGINE = SQLite('db_path')
```

**Engine Parameters**

-   `db_path` — Path to a file with SQLite database.
    
## Data Types Support {#data_types-support}

|  SQLite   | ClickHouse                                              |
|---------------|---------------------------------------------------------|
| INTEGER       | [Int32](../../sql-reference/data-types/int-uint.md)     |
| REAL          | [Float32](../../sql-reference/data-types/float.md)      |
| TEXT          | [String](../../sql-reference/data-types/string.md)      |
| BLOB          | [String](../../sql-reference/data-types/string.md)      |

## Specifics and Recommendations {#specifics-and-recommendations}

SQLite stores the entire database (definitions, tables, indices, and the data itself) as a single cross-platform file on a host machine. During writing SQLite locks the entire database file, therefore write operations are performed sequentially. Read operations can be multitasked.
SQLite does not require service management (such as startup scripts) or access control based on `GRANT` and passwords. Access control is handled by means of file-system permissions given to the database file itself.

## Usage Example {#usage-example}

Database in ClickHouse, connected to the SQLite:

``` sql
CREATE DATABASE sqlite_db ENGINE = SQLite('sqlite.db');
SHOW TABLES FROM sqlite_db;
SELECT * FROM sqlite_db.table1;
INSERT INTO sqlite_db.table1 SELECT * FROM clickhouse_table;
```

``` text
┌──name───┐
│ table1  │
│ table2  │  
└─────────┘
```