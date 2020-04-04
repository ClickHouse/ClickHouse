---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 34
toc_title: "\u5916\u90E8\u30C7\u30FC\u30BF"
---

# クエリ処理のための外部データ {#external-data-for-query-processing}

ClickHouseでは、クエリの処理に必要なデータとSELECTクエリをサーバーに送信できます。 このデータは一時テーブルに格納されます(セクションを参照 “Temporary tables” また、クエリで使用することもできます(たとえば、IN演算子など)。

たとえば、重要なユーザー識別子を持つテキストファイルがある場合は、このリストのフィルタリングを使用するクエリと一緒にサーバーにアップロードで

大量の外部データを含む複数のクエリを実行する必要がある場合は、この機能を使用しないでください。 事前にデータをdbにアップロードする方が良いです。

外部データは、コマンドラインクライアント(非対話モード)またはhttpインターフェイスを使用してアップロードできます。

コマンドラインクライアントでは、次の形式でparametersセクションを指定できます

``` bash
--external --file=... [--name=...] [--format=...] [--types=...|--structure=...]
```

送信されるテーブルの数については、このような複数のセクションがあります。

**–external** – Marks the beginning of a clause.
**–file** – Path to the file with the table dump, or -, which refers to stdin.
Stdinから取得できるのは、単一のテーブルだけです。

次のパラメーターは省略可能です: **–name**– Name of the table. If omitted, \_data is used.
**–format** – Data format in the file. If omitted, TabSeparated is used.

次のパラメーターのいずれかが必要です:**–types** – A list of comma-separated column types. For example: `UInt64,String`. The columns will be named \_1, \_2, …
**–structure**– The table structure in the format`UserID UInt64`, `URL String`. 列の名前と型を定義します。

で指定されたファイル ‘file’ で指定された形式によって解析されます。 ‘format’ で指定されたデータ型を使用する。 ‘types’ または ‘structure’. テーブルはサーバーにアップロードされ、そこにアクセスできるようになります。 ‘name’.

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

HTTPインターフェイスを使用する場合、外部データはmultipart/form-data形式で渡されます。 各テーブルは別のファイルとして送信されます。 テーブル名はファイル名から取得されます。 その ‘query\_string’ パラメータが渡されます ‘name\_format’, ‘name\_types’、と ‘name\_structure’、どこ ‘name’ これらのパラメーターが対応するテーブルの名前です。 パラメータの意味は、コマンドラインクライアントを使用する場合と同じです。

例えば:

``` bash
$ cat /etc/passwd | sed 's/:/\t/g' > passwd.tsv

$ curl -F 'passwd=@passwd.tsv;' 'http://localhost:8123/?query=SELECT+shell,+count()+AS+c+FROM+passwd+GROUP+BY+shell+ORDER+BY+c+DESC&passwd_structure=login+String,+unused+String,+uid+UInt16,+gid+UInt16,+comment+String,+home+String,+shell+String'
/bin/sh 20
/bin/false      5
/bin/bash       4
/usr/sbin/nologin       1
/bin/sync       1
```

分散クエリ処理の場合、一時テーブルはすべてのリモートサーバーに送信されます。

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/external_data/) <!--hide-->
