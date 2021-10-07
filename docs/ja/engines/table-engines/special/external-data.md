---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 34
toc_title: "\u5916\u90E8\u30C7\u30FC\u30BF"
---

# クエリ処理の外部データ {#external-data-for-query-processing}

ClickHouseでは、クエリの処理に必要なデータをSELECTクエリと共にサーバーに送信できます。 このデータは一時テーブルに格納されます(セクションを参照 “Temporary tables”）とクエリで使用できます（たとえば、in演算子）。

たとえば、重要なユーザー識別子を持つテキストファイルがある場合は、このリストによるフィルタリングを使用するクエリと共にサーバーにアップロードで

大量の外部データを含む複数のクエリを実行する必要がある場合は、この機能を使用しないでください。 事前にDBにデータをアップロードする方が良いです。

外部データは、コマンドラインクライアント(非対話モード)またはHTTPインターフェイスを使用してアップロードできます。

コマンドラインクライアントでは、parametersセクションを次の形式で指定できます

``` bash
--external --file=... [--name=...] [--format=...] [--types=...|--structure=...]
```

して複数の部分のように、このテーブルが送信されます。

**–external** – Marks the beginning of a clause.
**–file** – Path to the file with the table dump, or -, which refers to stdin.
Stdinから取得できるのは単一のテーブルのみです。

次のパラメータは省略可能です: **–name**– Name of the table. If omitted, _data is used.
**–format** – Data format in the file. If omitted, TabSeparated is used.

次のいずれかのパラメータが必要です:**–types** – A list of comma-separated column types. For example: `UInt64,String`. The columns will be named _1, _2, …
**–structure**– The table structure in the format`UserID UInt64`, `URL String`. 列名と型を定義します。

で指定されたファイル ‘file’ に指定された形式で解析されます。 ‘format’ で指定されたデータ型を使用します ‘types’ または ‘structure’. のテーブルがアップロードサーバへのアクセスが一時テーブルの名前 ‘name’.

例:

``` bash
$ echo -ne "1\n2\n3\n" | clickhouse-client --query="SELECT count() FROM test.visits WHERE TraficSourceID IN _data" --external --file=- --types=Int8
849897
$ cat /etc/passwd | sed 's/:/\t/g' | clickhouse-client --query="SELECT shell, count() AS c FROM passwd GROUP BY shell ORDER BY c DESC" --external --file=- --name=passwd --structure='login String, unused String, uid UInt16, gid UInt16, comment String, home String, shell String'
/bin/sh 20
/bin/false      5
/bin/bash       4
/usr/sbin/nologin       1
/bin/sync       1
```

HTTPインターフェイスを使用する場合、外部データはmultipart/form-data形式で渡されます。 各テーブルは別々のファイルとして送信されます。 テーブル名は、ファイル名から取得されます。 その ‘query_string’ パラメータが渡されます ‘name_format’, ‘name_types’,and ‘name_structure’,ここで ‘name’ これらのパラメーターが対応するテーブルの名前を指定します。 パラメーターの意味は、コマンドラインクライアントを使用する場合と同じです。

例:

``` bash
$ cat /etc/passwd | sed 's/:/\t/g' > passwd.tsv

$ curl -F 'passwd=@passwd.tsv;' 'http://localhost:8123/?query=SELECT+shell,+count()+AS+c+FROM+passwd+GROUP+BY+shell+ORDER+BY+c+DESC&passwd_structure=login+String,+unused+String,+uid+UInt16,+gid+UInt16,+comment+String,+home+String,+shell+String'
/bin/sh 20
/bin/false      5
/bin/bash       4
/usr/sbin/nologin       1
/bin/sync       1
```

分散クエリ処理では、一時テーブルがすべてのリモートサーバーに送信されます。

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/external_data/) <!--hide-->
