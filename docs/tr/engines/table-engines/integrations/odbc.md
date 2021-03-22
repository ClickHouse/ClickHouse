---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 35
toc_title: ODBC
---

# ODBC {#table-engine-odbc}

ClickHouse üzerinden harici veritabanlarına bağlanmak için izin verir [ODBC](https://en.wikipedia.org/wiki/Open_Database_Connectivity).

ODBC bağlantılarını güvenli bir şekilde uygulamak için ClickHouse ayrı bir program kullanır `clickhouse-odbc-bridge`. ODBC sürücüsü doğrudan yüklenmişse `clickhouse-server`, sürücü sorunları ClickHouse sunucu çökmesine neden olabilir. ClickHouse otomatik olarak başlar `clickhouse-odbc-bridge` gerekli olduğunda. ODBC Köprüsü programı aynı paketten yüklenir `clickhouse-server`.

Bu motor destekler [Nullable](../../../sql-reference/data-types/nullable.md) veri türü.

## Tablo oluşturma {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1],
    name2 [type2],
    ...
)
ENGINE = ODBC(connection_settings, external_database, external_table)
```

Ayrıntılı bir açıklamasını görmek [CREATE TABLE](../../../sql-reference/statements/create.md#create-table-query) sorgu.

Tablo yapısı kaynak tablo yapısından farklı olabilir:

-   Sütun adları kaynak tablodaki ile aynı olmalıdır, ancak yalnızca bu sütunlardan bazılarını ve herhangi bir sırada kullanabilirsiniz.
-   Sütun türleri kaynak tablodakilerden farklı olabilir. ClickHouse çalışır [döküm](../../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) ClickHouse veri türleri için değerler.

**Motor Parametreleri**

-   `connection_settings` — Name of the section with connection settings in the `odbc.ini` Dosya.
-   `external_database` — Name of a database in an external DBMS.
-   `external_table` — Name of a table in the `external_database`.

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

Clickhouse'daki tablo, MySQL tablosundan veri alma:

``` sql
CREATE TABLE odbc_t
(
    `int_id` Int32,
    `float_nullable` Nullable(Float32)
)
ENGINE = ODBC('DSN=mysqlconn', 'test', 'test')
```

``` sql
SELECT * FROM odbc_t
```

``` text
┌─int_id─┬─float_nullable─┐
│      1 │           ᴺᵁᴸᴸ │
└────────┴────────────────┘
```

## Ayrıca Bakınız {#see-also}

-   [ODBC harici sözlükler](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-odbc)
-   [ODBC tablo işlevi](../../../sql-reference/table-functions/odbc.md)

[Orijinal makale](https://clickhouse.tech/docs/en/operations/table_engines/odbc/) <!--hide-->
