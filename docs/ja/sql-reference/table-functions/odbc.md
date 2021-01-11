---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 44
toc_title: odbc
---

# odbc {#table-functions-odbc}

接続されているテーブルを返します [ODBC](https://en.wikipedia.org/wiki/Open_Database_Connectivity).

``` sql
odbc(connection_settings, external_database, external_table)
```

パラメータ:

-   `connection_settings` — Name of the section with connection settings in the `odbc.ini` ファイル
-   `external_database` — Name of a database in an external DBMS.
-   `external_table` — Name of a table in the `external_database`.

ODBC接続を安全に実装するために、ClickHouseは別のプログラムを使用します `clickhouse-odbc-bridge`. ODBCドライバーが直接ロードされる場合 `clickhouse-server` ドライバの問題でクラッシュのClickHouseサーバーです。 ClickHouseは自動的に起動します `clickhouse-odbc-bridge` それが必要なとき。 ODBC bridgeプログラムは、 `clickhouse-server`.

を持つフィールド `NULL` 外部テーブルの値は、基本データ型の既定値に変換されます。 たとえば、リモートMySQLテーブルフィールドに `INT NULL` 0に変換される型(ClickHouseのデフォルト値 `Int32` データ型）。

## 使用例 {#usage-example}

**PpsはインタラクティブのMySQLのインストール目盛**

この例では、Ubuntu Linux18.04およびMySQL server5.7がチェックされています。

UnixODBCとMySQL Connectorがインストールされていることを確認します。

デフォルトでインストールされた場合、パッケージから),ClickHouse開始してユーザー `clickhouse`. したがって、MySQLサーバでこのユーザを作成して構成する必要があります。

``` bash
$ sudo mysql
```

``` sql
mysql> CREATE USER 'clickhouse'@'localhost' IDENTIFIED BY 'clickhouse';
mysql> GRANT ALL PRIVILEGES ON *.* TO 'clickhouse'@'clickhouse' WITH GRANT OPTION;
```

次に、接続を設定します `/etc/odbc.ini`.

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

接続を確認するには `isql` unixODBCの取付けからの実用性。

``` bash
$ isql -v mysqlconn
+-------------------------+
| Connected!                            |
|                                       |
...
```

MySQLのテーブル:

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

ClickHouseのMySQLテーブルからのデータの取得:

``` sql
SELECT * FROM odbc('DSN=mysqlconn', 'test', 'test')
```

``` text
┌─int_id─┬─int_nullable─┬─float─┬─float_nullable─┐
│      1 │            0 │     2 │              0 │
└────────┴──────────────┴───────┴────────────────┘
```

## も参照。 {#see-also}

-   [ODBC外部辞書](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-odbc)
-   [ODBCテーブルエンジン](../../engines/table-engines/integrations/odbc.md).

[元の記事](https://clickhouse.tech/docs/en/query_language/table_functions/jdbc/) <!--hide-->
