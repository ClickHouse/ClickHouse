---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 33
toc_title: MySQL
---

# Mysql {#mysql}

MySQLエンジンでは、次の操作を実行できます `SELECT` リモートMySQLサーバーに格納されているデータを照会します。

## テーブルの作成 {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
) ENGINE = MySQL('host:port', 'database', 'table', 'user', 'password'[, replace_query, 'on_duplicate_clause']);
```

の詳細な説明を参照してください [CREATE TABLE](../../../sql-reference/statements/create.md#create-table-query) クエリ。

テーブル構造は元のmysqlテーブル構造とは異なる場合があります:

-   列名は元のmysqlテーブルと同じでなければなりませんが、これらの列の一部だけを任意の順序で使用できます。
-   カラムの型は、元のmysqlテーブルの型と異なる場合があります。 クリックハウスは [キャスト](../../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) クリックハウスのデータ型への値。

**エンジン変数**

-   `host:port` — MySQL server address.

-   `database` — Remote database name.

-   `table` — Remote table name.

-   `user` — MySQL user.

-   `password` — User password.

-   `replace_query` — Flag that converts `INSERT INTO` へのクエリ `REPLACE INTO`. もし `replace_query=1` クエリは置換されます。

-   `on_duplicate_clause` — The `ON DUPLICATE KEY on_duplicate_clause` に追加される式 `INSERT` クエリ。

    例えば: `INSERT INTO t (c1,c2) VALUES ('a', 2) ON DUPLICATE KEY UPDATE c2 = c2 + 1`、どこ `on_duplicate_clause` は `UPDATE c2 = c2 + 1`. を見る [MySQLの文書](https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html) これを見つけるには `on_duplicate_clause` あなたはで使用することができ `ON DUPLICATE KEY` 句。

    指定するには `on_duplicate_clause` 合格する必要があります `0` に `replace_query` パラメータ。 あなたが同時に渡す場合 `replace_query = 1` と `on_duplicate_clause`、ClickHouseは例外を生成します。

シンプル `WHERE` 次のような句 `=, !=, >, >=, <, <=` MySQLサーバで実行されます。

残りの条件と `LIMIT` サンプリング制約は、MySQLへのクエリが終了した後にのみClickHouseで実行されます。

## 使用例 {#usage-example}

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

ClickHouseのテーブル、上記で作成したMySQLテーブルからデータを取得する:

``` sql
CREATE TABLE mysql_table
(
    `float_nullable` Nullable(Float32),
    `int_id` Int32
)
ENGINE = MySQL('localhost:3306', 'test', 'test', 'bayonet', '123')
```

``` sql
SELECT * FROM mysql_table
```

``` text
┌─float_nullable─┬─int_id─┐
│           ᴺᵁᴸᴸ │      1 │
└────────────────┴────────┘
```

## また見なさい {#see-also}

-   [その ‘mysql’ テーブル機能](../../../sql-reference/table-functions/mysql.md)
-   [MySQLを外部辞書のソースとして使用する](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-mysql)

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/mysql/) <!--hide-->
