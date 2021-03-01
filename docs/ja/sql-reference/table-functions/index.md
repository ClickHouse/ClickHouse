---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_folder_title: "\u30C6\u30FC\u30D6\u30EB\u95A2\u6570"
toc_priority: 34
toc_title: "\u306F\u3058\u3081\u306B"
---

# テーブル関数 {#table-functions}

表関数は、表を構築するためのメソッドです。

次の表関数を使用できます:

-   [FROM](../statements/select/from.md) の節 `SELECT` クエリ。

        The method for creating a temporary table that is available only in the current query. The table is deleted when the query finishes.

-   [テーブルを\<table\_function()\>として作成](../statements/create.md#create-table-query) クエリ。

        It's one of the methods of creating a table.

!!! warning "警告"
    テーブル関数を使用することはできません。 [allow\_ddl](../../operations/settings/permissions-for-queries.md#settings_allow_ddl) 設定は無効です。

| 関数                  | 説明                                                                                                                                   |
|-----------------------|----------------------------------------------------------------------------------------------------------------------------------------|
| [ファイル](file.md)   | を作成します。 [ファイル](../../engines/table-engines/special/file.md)-エンジンテーブル。                                              |
| [マージ](merge.md)    | を作成します。 [マージ](../../engines/table-engines/special/merge.md)-エンジンテーブル。                                               |
| [数字](numbers.md)    | 整数で満たされた単一の列を持つテーブルを作成します。                                                                                   |
| [リモート](remote.md) | へ自由にアクセスできるリモートサーバーを作成することなく [分散](../../engines/table-engines/special/distributed.md)-エンジンテーブル。 |
| [url](url.md)         | を作成します。 [Url](../../engines/table-engines/special/url.md)-エンジンテーブル。                                                    |
| [mysql](mysql.md)     | を作成します。 [MySQL](../../engines/table-engines/integrations/mysql.md)-エンジンテーブル。                                           |
| [jdbc](jdbc.md)       | を作成します。 [JDBC](../../engines/table-engines/integrations/jdbc.md)-エンジンテーブル。                                             |
| [odbc](odbc.md)       | を作成します。 [ODBC](../../engines/table-engines/integrations/odbc.md)-エンジンテーブル。                                             |
| [hdfs](hdfs.md)       | を作成します。 [HDFS](../../engines/table-engines/integrations/hdfs.md)-エンジンテーブル。                                             |

[元の記事](https://clickhouse.tech/docs/en/query_language/table_functions/) <!--hide-->
