---
sidebar_position: 69
sidebar_label: "Named connections"
---

# Storing details for connecting to external sources in configuration files

Details for connecting to external sources (dictionaries, tables, table functions) can be saved
in configuration files and thus simplify the creation of objects and hide credentials
from users with only SQL access.

Parameters can be set in XML `<format>CSV</format>` and overridden in SQL `, format = 'TSV'`.
The parameters in SQL can be overridden using format `key` = `value`: `compression_method = 'gzip'`.

Named connections are stored in the `config.xml` file of the ClickHouse server in the `<named_collections>` section and are applied when ClickHouse starts.

Example of configuration:
```xml
$ cat /etc/clickhouse-server/config.d/named_collections.xml
<clickhouse>
     <named_collections>
     ...
     </named_collections>
</clickhouse>
```

## Named connections for accessing S3.

The description of parameters see [s3 Table Function](../sql-reference/table-functions/s3.md).

Example of configuration:
```xml
<clickhouse>
    <named_collections>
        <s3_mydata>
            <access_key_id>AKIAIOSFODNN7EXAMPLE</access_key_id>
            <secret_access_key>wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY</secret_access_key>
            <format>CSV</format>
            <url>https://s3.us-east-1.amazonaws.com/yourbucket/mydata/</url>
        </s3_mydata>
    </named_collections>
</clickhouse>
```

### Example of using named connections with the s3 function

```sql
INSERT INTO FUNCTION s3(s3_mydata, filename = 'test_file.tsv.gz',
   format = 'TSV', structure = 'number UInt64', compression_method = 'gzip')
SELECT * FROM numbers(10000);

SELECT count()
FROM s3(s3_mydata, filename = 'test_file.tsv.gz')

┌─count()─┐
│   10000 │
└─────────┘
1 rows in set. Elapsed: 0.279 sec. Processed 10.00 thousand rows, 90.00 KB (35.78 thousand rows/s., 322.02 KB/s.)
```

### Example of using named connections with an S3 table

```sql
CREATE TABLE s3_engine_table (number Int64)
ENGINE=S3(s3_mydata, url='https://s3.us-east-1.amazonaws.com/yourbucket/mydata/test_file.tsv.gz', format = 'TSV')
SETTINGS input_format_with_names_use_header = 0;

SELECT * FROM s3_engine_table LIMIT 3;
┌─number─┐
│      0 │
│      1 │
│      2 │
└────────┘
```

## Named connections for accessing MySQL database

The description of parameters see [mysql](../sql-reference/table-functions/mysql.md).

Example of configuration:
```xml
<clickhouse>
    <named_collections>
        <mymysql>
            <user>myuser</user>
            <password>mypass</password>
            <host>127.0.0.1</host>
            <port>3306</port>
            <database>test</database>
            <connection_pool_size>8</connection_pool_size>
            <on_duplicate_clause>1</on_duplicate_clause>
            <replace_query>1</replace_query>
        </mymysql>
    </named_collections>
</clickhouse>
```

### Example of using named connections with the mysql function

```sql
SELECT count() FROM mysql(mymysql, table = 'test');

┌─count()─┐
│       3 │
└─────────┘
```

### Example of using named connections with an MySQL table

```sql
CREATE TABLE mytable(A Int64) ENGINE = MySQL(mymysql, table = 'test', connection_pool_size=3, replace_query=0);
SELECT count() FROM mytable;

┌─count()─┐
│       3 │
└─────────┘
```

### Example of using named connections with database with engine MySQL

```sql
CREATE DATABASE mydatabase ENGINE = MySQL(mymysql);

SHOW TABLES FROM mydatabase;

┌─name───┐
│ source │
│ test   │
└────────┘
```

### Example of using named connections with an external dictionary with source MySQL

```sql
CREATE DICTIONARY dict (A Int64, B String)
PRIMARY KEY A
SOURCE(MYSQL(NAME mymysql TABLE 'source'))
LIFETIME(MIN 1 MAX 2)
LAYOUT(HASHED());

SELECT dictGet('dict', 'B', 2);

┌─dictGet('dict', 'B', 2)─┐
│ two                     │
└─────────────────────────┘
```

## Named connections for accessing PostgreSQL database

The description of parameters see [postgresql](../sql-reference/table-functions/postgresql.md).

Example of configuration:
```xml
<clickhouse>
    <named_collections>
        <mypg>
            <user>pguser</user>
            <password>jw8s0F4</password>
            <host>127.0.0.1</host>
            <port>5432</port>
            <database>test</database>
            <schema>test_schema</schema>
            <connection_pool_size>8</connection_pool_size>
        </mypg>
    </named_collections>
</clickhouse>
```

### Example of using named connections with the postgresql function

```sql
SELECT * FROM postgresql(mypg, table = 'test');

┌─a─┬─b───┐
│ 2 │ two │
│ 1 │ one │
└───┴─────┘


SELECT * FROM postgresql(mypg, table = 'test', schema = 'public');

┌─a─┐
│ 1 │
│ 2 │
│ 3 │
└───┘
```


### Example of using named connections with database with engine PostgreSQL

```sql
CREATE TABLE mypgtable (a Int64) ENGINE = PostgreSQL(mypg, table = 'test', schema = 'public');

SELECT * FROM mypgtable;

┌─a─┐
│ 1 │
│ 2 │
│ 3 │
└───┘
```

### Example of using named connections with database with engine PostgreSQL

```sql
CREATE DATABASE mydatabase ENGINE = PostgreSQL(mypg);

SHOW TABLES FROM mydatabase

┌─name─┐
│ test │
└──────┘
```

### Example of using named connections with an external dictionary with source POSTGRESQL

```sql
CREATE DICTIONARY dict (a Int64, b String)
PRIMARY KEY a
SOURCE(POSTGRESQL(NAME mypg TABLE test))
LIFETIME(MIN 1 MAX 2)
LAYOUT(HASHED());

SELECT dictGet('dict', 'b', 2);

┌─dictGet('dict', 'b', 2)─┐
│ two                     │
└─────────────────────────┘
```
