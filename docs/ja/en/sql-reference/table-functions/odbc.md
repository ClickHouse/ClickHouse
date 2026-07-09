---
description: 'ODBC 経由で接続されたテーブルを返します。'
sidebar_label: 'odbc'
sidebar_position: 150
slug: /sql-reference/table-functions/odbc
title: 'odbc'
doc_type: 'reference'
---

[ODBC](https://en.wikipedia.org/wiki/Open_Database_Connectivity) 経由で接続されたテーブルを返します。

<div id="syntax">
  ## 構文
</div>

```sql
odbc(datasource, external_database, external_table)
odbc(datasource, external_table)
odbc(named_collection)
```

<div id="arguments">
  ## 引数
</div>

| Argument            | Description                          |
| ------------------- | ------------------------------------ |
| `datasource`        | `odbc.ini` ファイル内で接続設定が記述されているセクション名。 |
| `external_database` | 外部 DBMS 内のデータベース名。                   |
| `external_table`    | `external_database` 内のテーブル名。         |

これらのパラメータは、[名前付きコレクション](/ja/operations/named-collections.md)を使って渡すこともできます。

ODBC 接続を安全に実装するため、ClickHouse は `clickhouse-odbc-bridge` という別プロセスを使用します。ODBC ドライバを `clickhouse-server` から直接読み込むと、ドライバの問題によって ClickHouse サーバーがクラッシュする可能性があります。ClickHouse は、必要に応じて `clickhouse-odbc-bridge` を自動的に起動します。ODBC ブリッジプログラムは、`clickhouse-server` と同じパッケージからインストールされます。

外部テーブルで `NULL` 値を持つフィールドは、基底データ型のデフォルト値に変換されます。たとえば、リモートの MySQL テーブルのフィールドが `INT NULL` 型の場合、0 (ClickHouse の `Int32` データ型のデフォルト値) に変換されます。

<div id="usage-example">
  ## 使用例
</div>

**ODBC 経由でローカルにインストールされた MySQL からデータを取得する**

この例は、Ubuntu Linux 18.04 および MySQL server 5.7 で動作確認されています。

unixODBC と MySQL Connector がインストールされていることを確認してください。

デフォルトでは (パッケージからインストールした場合) 、ClickHouse はユーザー `clickhouse` として起動します。そのため、MySQL server 側でこのユーザーを作成し、設定する必要があります。

```bash
$ sudo mysql
```

```sql
mysql> CREATE USER 'clickhouse'@'localhost' IDENTIFIED BY 'clickhouse';
mysql> GRANT ALL PRIVILEGES ON *.* TO 'clickhouse'@'clickhouse' WITH GRANT OPTION;
```

次に、`/etc/odbc.ini` で接続設定を行います。

```bash
$ cat /etc/odbc.ini
[mysqlconn]
DRIVER = /usr/local/lib/libmyodbc5w.so
SERVER = 127.0.0.1
PORT = 3306
DATABASE = test
USERNAME = clickhouse
PASSWORD = clickhouse
```

`unixODBC` のインストールに含まれる `isql` ユーティリティを使って、接続を確認できます。

```bash
$ isql -v mysqlconn
+-------------------------+
| Connected!                            |
|                                       |
...
```

MySQLのテーブル：

```text
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

ClickHouse 上の MySQL テーブルからデータを取得する:

```sql
SELECT * FROM odbc('DSN=mysqlconn', 'test', 'test')
```

```text
┌─int_id─┬─int_nullable─┬─float─┬─float_nullable─┐
│      1 │            0 │     2 │              0 │
└────────┴──────────────┴───────┴────────────────┘
```

<div id="see-also">
  ## 関連
</div>

* [ODBC Dictionary](/ja/sql-reference/statements/create/dictionary/sources/odbc)
* [ODBC テーブルエンジン](/ja/engines/table-engines/integrations/odbc).