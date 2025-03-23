---

slug: /ja/engines/table-engines/integrations/odbc
sidebar_position: 150
sidebar_label: ODBC

---

# ODBC

ClickHouseが[ODBC](https://en.wikipedia.org/wiki/Open_Database_Connectivity)を介して外部データベースに接続できるようにします。

ODBC接続を安全に実装するために、ClickHouseは別のプログラム`clickhouse-odbc-bridge`を使用します。もしODBCドライバが`clickhouse-server`から直接ロードされた場合、ドライバの問題がClickHouseサーバをクラッシュさせる可能性があります。ClickHouseは必要に応じて`clickhouse-odbc-bridge`を自動的に開始します。ODBCブリッジプログラムは、`clickhouse-server`と同じパッケージからインストールされます。

このエンジンは[Nullable](../../../sql-reference/data-types/nullable.md)データ型をサポートします。

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

[CREATE TABLE](../../../sql-reference/statements/create/table.md#create-table-query)クエリの詳細な説明を参照してください。

テーブル構造はソーステーブル構造と異なる可能性があります：

- カラム名はソーステーブルと同じである必要がありますが、これらのカラムの一部だけを任意の順序で使用することができます。
- カラムタイプはソーステーブルと異なる場合があります。ClickHouseは値をClickHouseデータ型に[キャスト](../../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast)しようとします。
- [external_table_functions_use_nulls](../../../operations/settings/settings.md#external-table-functions-use-nulls)設定はNullableカラムの処理方法を定義します。デフォルト値: 1。0の場合、テーブル関数はNullableカラムを作成せず、nullの代わりにデフォルト値を挿入します。これは配列内のNULL値にも適用されます。

**エンジンパラメータ**

- `connection_settings` — `odbc.ini`ファイルの接続設定を含むセクションの名前。
- `external_database` — 外部DBMS内のデータベース名。
- `external_table` — `external_database`内のテーブル名。

## 使用例 {#usage-example}

**ODBCを介してローカルMySQLインストールからデータを取得する**

この例はUbuntu Linux 18.04およびMySQLサーバ5.7で確認されています。

unixODBCとMySQL Connectorがインストールされていることを確認します。

デフォルトでは（パッケージからインストールされた場合）、ClickHouseはユーザ`clickhouse`として開始されます。そのため、MySQLサーバでこのユーザを作成して設定する必要があります。

``` bash
$ sudo mysql
```

``` sql
mysql> CREATE USER 'clickhouse'@'localhost' IDENTIFIED BY 'clickhouse';
mysql> GRANT ALL PRIVILEGES ON *.* TO 'clickhouse'@'localhost' WITH GRANT OPTION;
```

その後、`/etc/odbc.ini`で接続を設定します。

``` bash
$ cat /etc/odbc.ini
[mysqlconn]
DRIVER = /usr/local/lib/libmyodbc5w.so
SERVER = 127.0.0.1
PORT = 3306
DATABASE = test
USER = clickhouse
PASSWORD = clickhouse
```

unixODBCインストールの`isql`ユーティリティを使って接続を確認できます。

``` bash
$ isql -v mysqlconn
+-------------------------+
| Connected!              |
|                         |
...
```

MySQLのテーブル:

``` text
mysql> CREATE DATABASE test;
Query OK, 1 row affected (0,01 sec)

mysql> CREATE TABLE `test`.`test` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `int_nullable` INT NULL DEFAULT NULL,
    ->   `float` FLOAT NOT NULL,
    ->   `float_nullable` FLOAT NULL DEFAULT NULL,
    ->   PRIMARY KEY (`int_id`));
Query OK, 0 rows affected (0,09 sec)

mysql> insert into test.test (`int_id`, `float`) VALUES (1,2);
Query OK, 1 row affected (0,00 sec)

mysql> select * from test.test;
+------+-------------+-------+----------------+
| int_id | int_nullable | float | float_nullable |
+------+-------------+-------+----------------+
|      1 |        NULL |     2 |            NULL |
+------+-------------+-------+----------------+
1 row in set (0,00 sec)
```

MySQLテーブルからデータを取得するClickHouseのテーブル:

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

## 参照 {#see-also}

- [ODBC dictionaries](../../../sql-reference/dictionaries/index.md#dictionary-sources#dicts-external_dicts_dict_sources-odbc)
- [ODBC table function](../../../sql-reference/table-functions/odbc.md)
