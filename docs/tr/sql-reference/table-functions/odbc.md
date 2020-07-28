---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 44
toc_title: odbc
---

# odbc {#table-functions-odbc}

Üzerinden bağlanan tabloyu döndürür [ODBC](https://en.wikipedia.org/wiki/Open_Database_Connectivity).

``` sql
odbc(connection_settings, external_database, external_table)
```

Parametre:

-   `connection_settings` — Name of the section with connection settings in the `odbc.ini` Dosya.
-   `external_database` — Name of a database in an external DBMS.
-   `external_table` — Name of a table in the `external_database`.

ODBC bağlantılarını güvenli bir şekilde uygulamak için ClickHouse ayrı bir program kullanır `clickhouse-odbc-bridge`. ODBC sürücüsü doğrudan yüklenmişse `clickhouse-server`, sürücü sorunları ClickHouse sunucu çökmesine neden olabilir. ClickHouse otomatik olarak başlar `clickhouse-odbc-bridge` gerekli olduğunda. ODBC Köprüsü programı aynı paketten yüklenir `clickhouse-server`.

Alanları ile `NULL` dış tablodaki değerler, temel veri türü için varsayılan değerlere dönüştürülür. Örneğin, uzak bir MySQL tablo alanı `INT NULL` yazın 0'a dönüştürülür (ClickHouse için varsayılan değer `Int32` veri türü).

## Kullanım Örneği {#usage-example}

**ODBC üzerinden yerel MySQL kurulumundan veri alma**

Bu örnek Ubuntu Linux 18.04 ve MySQL server 5.7 için kontrol edilir.

UnixODBC ve MySQL Connector yüklü olduğundan emin olun.

Varsayılan olarak (paketlerden yüklüyse), ClickHouse kullanıcı olarak başlar `clickhouse`. Bu nedenle, bu kullanıcıyı MySQL sunucusunda oluşturmanız ve yapılandırmanız gerekir.

``` bash
$ sudo mysql
```

``` sql
mysql> CREATE USER 'clickhouse'@'localhost' IDENTIFIED BY 'clickhouse';
mysql> GRANT ALL PRIVILEGES ON *.* TO 'clickhouse'@'clickhouse' WITH GRANT OPTION;
```

Sonra bağlantıyı yapılandırın `/etc/odbc.ini`.

``` bash
$ cat /etc/odbc.ini
[mysqlconn]
DRIVER = /usr/local/lib/libmyodbc5w.so
SERVER = 127.0.0.1
PORT = 3306
DATABASE = test
USERNAME = clickhouse
PASSWORD = clickhouse
```

Kullanarak bağlantıyı kontrol edebilirsiniz `isql` unixodbc yüklemesinden yardımcı program.

``` bash
$ isql -v mysqlconn
+-------------------------+
| Connected!                            |
|                                       |
...
```

MySQL tablo:

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

Clickhouse'daki MySQL tablosundan veri alma:

``` sql
SELECT * FROM odbc('DSN=mysqlconn', 'test', 'test')
```

``` text
┌─int_id─┬─int_nullable─┬─float─┬─float_nullable─┐
│      1 │            0 │     2 │              0 │
└────────┴──────────────┴───────┴────────────────┘
```

## Ayrıca Bakınız {#see-also}

-   [ODBC harici sözlükler](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-odbc)
-   [ODBC tablo motoru](../../engines/table-engines/integrations/odbc.md).

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/table_functions/jdbc/) <!--hide-->
