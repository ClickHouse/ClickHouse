---
machine_translated: true
machine_translated_rev: e8cd92bba3269f47787db090899f7c242adf7818
toc_priority: 30
toc_title: MySQL
---

# MySQL {#mysql}

Uzak bir MySQL sunucusunda veritabanlarına bağlanmak ve gerçekleştirmek için izin verir `INSERT` ve `SELECT` ClickHouse ve MySQL arasında veri alışverişi için sorgular.

Bu `MySQL` veritabanı motoru sorguları MySQL sunucusuna çevirir, böylece aşağıdaki gibi işlemleri gerçekleştirebilirsiniz `SHOW TABLES` veya `SHOW CREATE TABLE`.

Aşağıdaki sorguları gerçekleştiremiyor:

-   `RENAME`
-   `CREATE TABLE`
-   `ALTER`

## Veritabanı oluşturma {#creating-a-database}

``` sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster]
ENGINE = MySQL('host:port', ['database' | database], 'user', 'password')
```

**Motor Parametreleri**

-   `host:port` — MySQL server address.
-   `database` — Remote database name.
-   `user` — MySQL user.
-   `password` — User password.

## Veri Türleri Desteği {#data_types-support}

| MySQL                            | ClickHouse                                                   |
|----------------------------------|--------------------------------------------------------------|
| UNSIGNED TINYINT                 | [Uİnt8](../../sql_reference/data_types/int_uint.md)          |
| TINYINT                          | [Int8](../../sql_reference/data_types/int_uint.md)           |
| UNSIGNED SMALLINT                | [Uınt16](../../sql_reference/data_types/int_uint.md)         |
| SMALLINT                         | [Int16](../../sql_reference/data_types/int_uint.md)          |
| UNSIGNED INT, UNSIGNED MEDIUMINT | [Uİnt32](../../sql_reference/data_types/int_uint.md)         |
| INT, MEDIUMINT                   | [Int32](../../sql_reference/data_types/int_uint.md)          |
| UNSIGNED BIGINT                  | [Uİnt64](../../sql_reference/data_types/int_uint.md)         |
| BIGINT                           | [Int64](../../sql_reference/data_types/int_uint.md)          |
| FLOAT                            | [Float32](../../sql_reference/data_types/float.md)           |
| DOUBLE                           | [Float64](../../sql_reference/data_types/float.md)           |
| DATE                             | [Tarihli](../../sql_reference/data_types/date.md)            |
| DATETIME, TIMESTAMP              | [DateTime](../../sql_reference/data_types/datetime.md)       |
| BINARY                           | [FixedString](../../sql_reference/data_types/fixedstring.md) |

Diğer tüm MySQL veri türleri dönüştürülür [Dize](../../sql_reference/data_types/string.md).

[Nullable](../../sql_reference/data_types/nullable.md) desteklenir.

## Kullanım Örnekleri {#examples-of-use}

MySQL tablo:

``` text
mysql> USE test;
Database changed

mysql> CREATE TABLE `mysql_table` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `float` FLOAT NOT NULL,
    ->   PRIMARY KEY (`int_id`));
Query OK, 0 rows affected (0,09 sec)

mysql> insert into mysql_table (`int_id`, `float`) VALUES (1,2);
Query OK, 1 row affected (0,00 sec)

mysql> select * from mysql_table;
+------+-----+
| int_id | value |
+------+-----+
|      1 |     2 |
+------+-----+
1 row in set (0,00 sec)
```

Clickhouse'daki veritabanı, MySQL sunucusu ile veri alışverişi:

``` sql
CREATE DATABASE mysql_db ENGINE = MySQL('localhost:3306', 'test', 'my_user', 'user_password')
```

``` sql
SHOW DATABASES
```

``` text
┌─name─────┐
│ default  │
│ mysql_db │
│ system   │
└──────────┘
```

``` sql
SHOW TABLES FROM mysql_db
```

``` text
┌─name─────────┐
│  mysql_table │
└──────────────┘
```

``` sql
SELECT * FROM mysql_db.mysql_table
```

``` text
┌─int_id─┬─value─┐
│      1 │     2 │
└────────┴───────┘
```

``` sql
INSERT INTO mysql_db.mysql_table VALUES (3,4)
```

``` sql
SELECT * FROM mysql_db.mysql_table
```

``` text
┌─int_id─┬─value─┐
│      1 │     2 │
│      3 │     4 │
└────────┴───────┘
```

[Orijinal makale](https://clickhouse.tech/docs/en/database_engines/mysql/) <!--hide-->
