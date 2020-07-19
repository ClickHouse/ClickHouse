---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 34
toc_title: JDBC
---

# JDBC {#table-engine-jdbc}

ClickHouse üzerinden harici veritabanlarına bağlanmak için izin verir [JDBC](https://en.wikipedia.org/wiki/Java_Database_Connectivity).

JDBC bağlantısını uygulamak için ClickHouse ayrı programı kullanır [clickhouse-JDBC-köprü](https://github.com/alex-krash/clickhouse-jdbc-bridge) bu bir daemon olarak çalışmalıdır.

Bu motor destekler [Nullable](../../../sql-reference/data-types/nullable.md) veri türü.

## Tablo oluşturma {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    columns list...
)
ENGINE = JDBC(dbms_uri, external_database, external_table)
```

**Motor Parametreleri**

-   `dbms_uri` — URI of an external DBMS.

    Biçimli: `jdbc:<driver_name>://<host_name>:<port>/?user=<username>&password=<password>`.
    MySQL örneği: `jdbc:mysql://localhost:3306/?user=root&password=root`.

-   `external_database` — Database in an external DBMS.

-   `external_table` — Name of the table in `external_database`.

## Kullanım Örneği {#usage-example}

Doğrudan konsol istemcisine bağlanarak MySQL sunucusunda bir tablo oluşturma:

``` text
mysql> CREATE TABLE `test`.`test` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `int_nullable` INT NULL DEFAULT NULL,
    ->   `float` FLOAT NOT NULL,
    ->   `float_nullable` FLOAT NULL DEFAULT NULL,
    ->   PRIMARY KEY (`int_id`));
Query OK, 0 rows affected (0,09 sec)

mysql> insert into test (`int_id`, `float`) VALUES (1,2);
Query OK, 1 row affected (0,00 sec)

mysql> select * from test;
+------+----------+-----+----------+
| int_id | int_nullable | float | float_nullable |
+------+----------+-----+----------+
|      1 |         NULL |     2 |           NULL |
+------+----------+-----+----------+
1 row in set (0,00 sec)
```

ClickHouse Server'da bir tablo oluşturma ve ondan veri seçme:

``` sql
CREATE TABLE jdbc_table
(
    `int_id` Int32,
    `int_nullable` Nullable(Int32),
    `float` Float32,
    `float_nullable` Nullable(Float32)
)
ENGINE JDBC('jdbc:mysql://localhost:3306/?user=root&password=root', 'test', 'test')
```

``` sql
SELECT *
FROM jdbc_table
```

``` text
┌─int_id─┬─int_nullable─┬─float─┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴────────────────┘
```

## Ayrıca Bakınız {#see-also}

-   [JDBC tablo işlevi](../../../sql-reference/table-functions/jdbc.md).

[Orijinal makale](https://clickhouse.tech/docs/en/operations/table_engines/jdbc/) <!--hide-->
