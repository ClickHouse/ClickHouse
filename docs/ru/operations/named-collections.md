---
toc_priority: 69
toc_title: "Именованные соединения"
---

# Хранение реквизитов для подключения к внешним источникам в конфигурационных файлах {#named-collections}

Реквизиты для подключения к внешним источникам (словарям, таблицам, табличным функциям) можно сохранить
в конфигурационных файлах и таким образом упростить создание объектов и скрыть реквизиты (пароли)
от пользователей, имеющих только SQL доступ.

Параметры можно задать в XML `<format>CSV</format>` и переопределить в SQL `, format = 'TSV'`.
При использовании именованных соединений, параметры в SQL задаются в формате `ключ` = `значение`: `compression_method = 'gzip'`.

Именованные соединения хранятся в файле `config.xml` сервера ClickHouse в секции `<named_collections>` и применяются при старте ClickHouse.

Пример конфигурации:
```xml
$ cat /etc/clickhouse-server/config.d/named_collections.xml
<clickhouse>
     <named_collections>
     ...
     </named_collections>
</clickhouse>
```

## Именованные соединения для доступа к S3

Описание параметров смотри [Табличная Функция S3](../sql-reference/table-functions/s3.md).

Пример конфигурации:
```xml
<clickhouse>
    <named_collections>
        <s3_mydata>
            <access_key_id>AKIAIOSFODNN7EXAMPLE</access_key_id>
            <secret_access_key> wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY</secret_access_key>
            <format>CSV</format>
        </s3_mydata>
    </named_collections>
</clickhouse>
```

### Пример использования именованных соединений с функцией s3

```sql
INSERT INTO FUNCTION s3(s3_mydata, url = 'https://s3.us-east-1.amazonaws.com/yourbucket/mydata/test_file.tsv.gz',
   format = 'TSV', structure = 'number UInt64', compression_method = 'gzip')
SELECT * FROM numbers(10000);

SELECT count()
FROM s3(s3_mydata, url = 'https://s3.us-east-1.amazonaws.com/yourbucket/mydata/test_file.tsv.gz')

┌─count()─┐
│   10000 │
└─────────┘
1 rows in set. Elapsed: 0.279 sec. Processed 10.00 thousand rows, 90.00 KB (35.78 thousand rows/s., 322.02 KB/s.)
```

### Пример использования именованных соединений с таблицей S3

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

## Пример использования именованных соединений с базой данных MySQL

Описание параметров смотри [mysql](../sql-reference/table-functions/mysql.md).

Пример конфигурации:
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

### Пример использования именованных соединений с табличной функцией mysql

```sql
SELECT count() FROM mysql(mymysql, table = 'test');

┌─count()─┐
│       3 │
└─────────┘
```

### Пример использования именованных соединений таблицей с движком mysql

```sql
CREATE TABLE mytable(A Int64) ENGINE = MySQL(mymysql, table = 'test', connection_pool_size=3, replace_query=0);
SELECT count() FROM mytable;

┌─count()─┐
│       3 │
└─────────┘
```

### Пример использования именованных соединений базой данных с движком MySQL

```sql
CREATE DATABASE mydatabase ENGINE = MySQL(mymysql);

SHOW TABLES FROM mydatabase;

┌─name───┐
│ source │
│ test   │
└────────┘
```

### Пример использования именованных соединений с внешним словарем с источником mysql

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

## Пример использования именованных соединений с базой данных PostgreSQL

Описание параметров смотри [postgresql](../sql-reference/table-functions/postgresql.md).

Пример конфигурации:
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

### Пример использования именованных соединений с табличной функцией postgresql

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

### Пример использования именованных соединений таблицей с движком PostgreSQL

```sql
CREATE TABLE mypgtable (a Int64) ENGINE = PostgreSQL(mypg, table = 'test', schema = 'public');

SELECT * FROM mypgtable;

┌─a─┐
│ 1 │
│ 2 │
│ 3 │
└───┘
```

### Пример использования именованных соединений базой данных с движком PostgreSQL

```sql
CREATE DATABASE mydatabase ENGINE = PostgreSQL(mypg);

SHOW TABLES FROM mydatabase

┌─name─┐
│ test │
└──────┘
```

### Пример использования именованных соединений с внешним словарем с источником POSTGRESQL

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
