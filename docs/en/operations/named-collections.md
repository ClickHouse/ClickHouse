---
slug: /en/operations/named-collections
sidebar_position: 69
sidebar_label: "Named collections"
title: "Named collections"
---

Named collections provide a way to store collections of key-value pairs to be 
used to configure integrations with external sources. You can use named collections with
dictionaries, tables, table functions, and object storage. 

Named collections can be configured with DDL or in configuration files and are applied
when ClickHouse starts. They simplify the creation of objects and the hiding of credentials
from users without administrative access.

The keys in a named collection must match the parameter names of the corresponding
function, table engine, database, etc. In the examples below the parameter list is
linked to for each type.

Parameters set in a named collection can be overridden in SQL, this is shown in the examples
below.

## Storing named collections in the system database

### DDL example

```sql
CREATE NAMED COLLECTION name AS
key_1 = 'value',
key_2 = 'value2',
url = 'https://connection.url/'
```

### Permissions to create named collections with DDL

To manage named collections with DDL a user must have the `named_control_collection` privilege.  This can be assigned by adding a file to `/etc/clickhouse-server/users.d/`.  The example gives the user `default` both the `access_management` and `named_collection_control` privileges:

```xml title='/etc/clickhouse-server/users.d/user_default.xml'
<clickhouse>
  <users>
    <default>
      <password_sha256_hex>65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5</password_sha256_hex replace=true>
      <access_management>1</access_management>
      <!-- highlight-start -->
      <named_collection_control>1</named_collection_control>
      <!-- highlight-end -->
    </default>
  </users>
</clickhouse>
```

:::tip
In the above example the `password_sha256_hex` value is the hexadecimal representation of the SHA256 hash of the password.  This configuration for the user `default` has the attribute `replace=true` as in the default configuration has a plain text `password` set, and it is not possible to have both plain text and sha256 hex passwords set for a user. 
:::

## Storing named collections in configuration files

### XML example

```xml title='/etc/clickhouse-server/config.d/named_collections.xml'
<clickhouse>
     <named_collections>
        <name>
            <key_1>value</key_1>
            <key_2>value_2</key_2>
            <url>https://connection.url/</url>
        </name>
     </named_collections>
</clickhouse>
```

## Modifying named collections

Named collections that are created with DDL queries can be altered or dropped with DDL. Named collections created with XML files can be managed by editing or deleting the corresponding XML.

### Alter a DDL named collection

Change or add the keys `key1` and `key3` of the collection `collection2`:
```sql
ALTER NAMED COLLECTION collection2 SET key1=4, key3='value3'
```

Remove the key `key2` from `collection2`:
```sql
ALTER NAMED COLLECTION collection2 DELETE key2
```

Change or add the key `key1` and delete the key `key3` of the collection `collection2`:
```sql
ALTER NAMED COLLECTION collection2 SET key1=4, DELETE key3
```

### Drop the DDL named collection `collection2`:
```sql
DROP NAMED COLLECTION collection2
```

## Named collections for accessing S3

The description of parameters see [s3 Table Function](../sql-reference/table-functions/s3.md).

### DDL example

```sql
CREATE NAMED COLLECTION s3_mydata AS
access_key_id = 'AKIAIOSFODNN7EXAMPLE',
secret_access_key = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
format = 'CSV',
url = 'https://s3.us-east-1.amazonaws.com/yourbucket/mydata/'
```

### XML example

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

### s3() function and S3 Table named collection examples

Both of the following examples use the same named collection `s3_mydata`:

#### s3() function

```sql
INSERT INTO FUNCTION s3(s3_mydata, filename = 'test_file.tsv.gz',
   format = 'TSV', structure = 'number UInt64', compression_method = 'gzip')
SELECT * FROM numbers(10000);
```

:::tip
The first argument to the `s3()` function above is the name of the collection, `s3_mydata`.  Without named collections, the access key ID, secret, format, and URL would all be passed in every call to the `s3()` function.
:::

#### S3 table

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

## Named collections for accessing MySQL database

The description of parameters see [mysql](../sql-reference/table-functions/mysql.md).

### DDL example

```sql
CREATE NAMED COLLECTION mymysql AS
user = 'myuser',
password = 'mypass',
host = '127.0.0.1',
port = 3306,
database = 'test',
connection_pool_size = 8,
replace_query = 1
```

### XML example

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
            <replace_query>1</replace_query>
        </mymysql>
    </named_collections>
</clickhouse>
```

### mysql() function, MySQL table, MySQL database, and Dictionary named collection examples

The four following examples use the same named collection `mymysql`:

#### mysql() function

```sql
SELECT count() FROM mysql(mymysql, table = 'test');

┌─count()─┐
│       3 │
└─────────┘
```
:::note
The named collection does not specify the `table` parameter, so it is specified in the function call as `table = 'test'`.
:::

#### MySQL table

```sql
CREATE TABLE mytable(A Int64) ENGINE = MySQL(mymysql, table = 'test', connection_pool_size=3, replace_query=0);
SELECT count() FROM mytable;

┌─count()─┐
│       3 │
└─────────┘
```

:::note
The DDL overrides the named collection setting for connection_pool_size.
:::

#### MySQL database

```sql
CREATE DATABASE mydatabase ENGINE = MySQL(mymysql);

SHOW TABLES FROM mydatabase;

┌─name───┐
│ source │
│ test   │
└────────┘
```

#### MySQL Dictionary

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

## Named collections for accessing PostgreSQL database

The description of parameters see [postgresql](../sql-reference/table-functions/postgresql.md).

```sql
CREATE NAMED COLLECTION mypg AS
user = 'pguser',
password = 'jw8s0F4',
host = '127.0.0.1',
port = 5432,
database = 'test',
schema = 'test_schema',
connection_pool_size = 8
```

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

### Example of using named collections with the postgresql function

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

### Example of using named collections with database with engine PostgreSQL

```sql
CREATE TABLE mypgtable (a Int64) ENGINE = PostgreSQL(mypg, table = 'test', schema = 'public');

SELECT * FROM mypgtable;

┌─a─┐
│ 1 │
│ 2 │
│ 3 │
└───┘
```

### Example of using named collections with database with engine PostgreSQL

```sql
CREATE DATABASE mydatabase ENGINE = PostgreSQL(mypg);

SHOW TABLES FROM mydatabase

┌─name─┐
│ test │
└──────┘
```

### Example of using named collections with a dictionary with source POSTGRESQL

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

## Named collections for accessing a remote ClickHouse database

The description of parameters see [remote](../sql-reference/table-functions/remote.md/#parameters).

Example of configuration:

```sql
CREATE NAMED COLLECTION remote1 AS
host = 'remote_host',
port = 9000,
database = 'system',
user = 'foo',
password = 'secret',
secure = 1
```

```xml
<clickhouse>
    <named_collections>
        <remote1>
            <host>remote_host</host>
            <port>9000</port>
            <database>system</database>
            <user>foo</user>
            <password>secret</password>
            <secure>1</secure>
        </remote1>
    </named_collections>
</clickhouse>
```
`secure` is not needed for connection because of `remoteSecure`, but it can be used for dictionaries.

### Example of using named collections with the `remote`/`remoteSecure` functions

```sql
SELECT * FROM remote(remote1, table = one);
┌─dummy─┐
│     0 │
└───────┘

SELECT * FROM remote(remote1, database = merge(system, '^one'));
┌─dummy─┐
│     0 │
└───────┘

INSERT INTO FUNCTION remote(remote1, database = default, table = test) VALUES (1,'a');

SELECT * FROM remote(remote1, database = default, table = test);
┌─a─┬─b─┐
│ 1 │ a │
└───┴───┘
```

### Example of using named collections with a dictionary with source ClickHouse

```sql
CREATE DICTIONARY dict(a Int64, b String)
PRIMARY KEY a
SOURCE(CLICKHOUSE(NAME remote1 TABLE test DB default))
LIFETIME(MIN 1 MAX 2)
LAYOUT(HASHED());

SELECT dictGet('dict', 'b', 1);
┌─dictGet('dict', 'b', 1)─┐
│ a                       │
└─────────────────────────┘
```

