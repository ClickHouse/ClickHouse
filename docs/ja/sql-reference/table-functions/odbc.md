---
slug: /ja/sql-reference/table-functions/odbc
sidebar_position: 150
sidebar_label: odbc
---

# odbc

[ODBC](https://en.wikipedia.org/wiki/Open_Database_Connectivity)を介して接続されたテーブルを返します。

``` sql
odbc(connection_settings, external_database, external_table)
```

パラメータ:

- `connection_settings` — `odbc.ini`ファイル内の接続設定が書かれたセクションの名前。
- `external_database` — 外部DBMSのデータベース名。
- `external_table` — `external_database`内のテーブル名。

ODBC接続を安全に実装するために、ClickHouseは別のプログラム `clickhouse-odbc-bridge` を使用します。ODBCドライバーを直接`clickhouse-server`からロードすると、ドライバーの問題がClickHouseサーバーをクラッシュさせる可能性があります。必要に応じて、ClickHouseは自動的に`clickhouse-odbc-bridge`を起動します。このODBCブリッジプログラムは`clickhouse-server`と同じパッケージからインストールされます。

外部テーブルから`NULL`値を持つフィールドは、基本データ型のデフォルト値に変換されます。例えば、リモートのMySQLテーブルフィールドが`INT NULL`型である場合、0（ClickHouseの`Int32`データ型のデフォルト値）に変換されます。

## 使用例

**ODBCを介してローカルのMySQLインストールからデータを取得**

この例は Ubuntu Linux 18.04 と MySQL サーバー 5.7 で確認されています。

unixODBCとMySQL Connectorがインストールされていることを確認してください。

デフォルトでは（パッケージからインストールされた場合）、ClickHouseはユーザー`clickhouse`として起動します。したがって、このユーザーをMySQLサーバーに作成して設定する必要があります。

``` bash
$ sudo mysql
```

``` sql
mysql> CREATE USER 'clickhouse'@'localhost' IDENTIFIED BY 'clickhouse';
mysql> GRANT ALL PRIVILEGES ON *.* TO 'clickhouse'@'localhost' WITH GRANT OPTION;
```

次に、`/etc/odbc.ini`で接続を設定します。

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

unixODBCインストールからの`isql`ユーティリティを使用して接続を確認できます。

``` bash
$ isql -v mysqlconn
+-------------------------+
| Connected!              |
|                         |
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
+--------+--------------+-------+----------------+
| int_id | int_nullable | float | float_nullable |
+--------+--------------+-------+----------------+
|      1 |         NULL |     2 |           NULL |
+--------+--------------+-------+----------------+
1 row in set (0,00 sec)
```

ClickHouseでMySQLテーブルからデータを取得:

``` sql
SELECT * FROM odbc('DSN=mysqlconn', 'test', 'test')
```

``` text
┌─int_id─┬─int_nullable─┬─float─┬─float_nullable─┐
│      1 │            0 │     2 │              0 │
└────────┴──────────────┴───────┴────────────────┘
```

## 関連項目

- [ODBC dictionaries](../../sql-reference/dictionaries/index.md#dictionary-sources#dicts-external_dicts_dict_sources-odbc)
- [ODBC table engine](../../engines/table-engines/integrations/odbc.md).
