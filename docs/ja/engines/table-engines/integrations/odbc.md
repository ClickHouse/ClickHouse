---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 35
toc_title: ODBC
---

# ODBC {#table-engine-odbc}

ClickHouseが外部データベースに接続できるようにする [ODBC](https://en.wikipedia.org/wiki/Open_Database_Connectivity).

ODBC接続を安全に実装するために、ClickHouseは別のプログラムを使用します `clickhouse-odbc-bridge`. ODBCドライバーが直接ロードされる場合 `clickhouse-server` ドライバの問題でクラッシュのClickHouseサーバーです。 ClickHouseは自動的に起動します `clickhouse-odbc-bridge` それが必要なとき。 ODBC bridgeプログラムは、 `clickhouse-server`.

このエンジンは [Null可能](../../../sql-reference/data-types/nullable.md) データ型。

## テーブルの作成 {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1],
    name2 [type2],
    ...
)
ENGINE = ODBC(connection_settings, external_database, external_table)
```

の詳細な説明を参照してください [CREATE TABLE](../../../sql-reference/statements/create.md#create-table-query) クエリ。

表構造は、ソース表構造とは異なる場合があります:

-   列名はソーステーブルと同じにする必要がありますが、これらの列の一部だけを任意の順序で使用できます。
-   列の型は、ソーステーブルの型と異なる場合があります。 ClickHouseは [キャスト](../../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) ClickHouseデータ型の値。

**エンジン変数**

-   `connection_settings` — Name of the section with connection settings in the `odbc.ini` ファイル
-   `external_database` — Name of a database in an external DBMS.
-   `external_table` — Name of a table in the `external_database`.

## 使用例 {#usage-example}

**取得データから地元のMySQLのインストール目盛**

この例では、Ubuntu Linux18.04およびMySQL server5.7がチェックされています。

UnixODBCとMySQL Connectorがインストールされていることを確認します。

デフォルトでインストールされた場合、パッケージから),ClickHouse開始してユーザー `clickhouse`. したがって、MySQLサーバーでこのユーザーを作成して構成する必要があります。

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

ClickHouseのテーブル、MySQLテーブルからデータを取得する:

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

## も参照。 {#see-also}

-   [ODBC外部辞書](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-odbc)
-   [ODBCテーブル関数](../../../sql-reference/table-functions/odbc.md)

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/odbc/) <!--hide-->
