---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_folder_title: Table Functions
toc_priority: 34
toc_title: "\u5C0E\u5165"
---

# テーブル関数 {#table-functions}

テーブル機能の方法を構築します。

テーブル関数は次の場所で使用できます:

-   [FROM](../statements/select.md#select-from) の句 `SELECT` クエリ。

        The method for creating a temporary table that is available only in the current query. The table is deleted when the query finishes.

-   [テーブルを\<table\_function()\>として作成](../statements/create.md#create-table-query) クエリ。

        It's one of the methods of creating a table.

!!! warning "警告"
    テーブル関数を使用することはできません。 [allow\_ddl](../../operations/settings/permissions-for-queries.md#settings_allow_ddl) 設定は無効です。

| 機能                  | 説明                                                                                                                                     |
|-----------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| [ファイル](file.md)   | を作成します。 [ファイル](../../engines/table-engines/special/file.md)-エンジンのテーブル。                                              |
| [マージ](merge.md)    | を作成します。 [マージ](../../engines/table-engines/special/merge.md)-エンジンのテーブル。                                               |
| [数字](numbers.md)    | 単一の列が整数で埋められたテーブルを作成します。                                                                                         |
| [リモート](remote.md) | へ自由にアクセスできるリモートサーバーを作成することなく [分散](../../engines/table-engines/special/distributed.md)-エンジンのテーブル。 |
| [url](url.md)         | を作成します。 [Url](../../engines/table-engines/special/url.md)-エンジンのテーブル。                                                    |
| [mysql](mysql.md)     | を作成します。 [MySQL](../../engines/table-engines/integrations/mysql.md)-エンジンのテーブル。                                           |
| [jdbc](jdbc.md)       | を作成します。 [JDBC](../../engines/table-engines/integrations/jdbc.md)-エンジンのテーブル。                                             |
| [odbc](odbc.md)       | を作成します。 [ODBC](../../engines/table-engines/integrations/odbc.md)-エンジンのテーブル。                                             |
| [hdfs](hdfs.md)       | を作成します。 [HDFS](../../engines/table-engines/integrations/hdfs.md)-エンジンのテーブル。                                             |

[元の記事](https://clickhouse.tech/docs/en/query_language/table_functions/) <!--hide-->
