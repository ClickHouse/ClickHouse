---
description: 'ClickHouse では、`SELECT` クエリとともに、クエリの処理に必要なデータをサーバーに送信できます。このデータは一時テーブルに格納され、クエリ内で使用できます（たとえば、`IN` 演算子で使用できます）。'
sidebar_label: 'クエリ処理用の外部データ'
sidebar_position: 130
slug: /engines/table-engines/special/external-data
title: 'クエリ処理用の外部データ'
doc_type: 'reference'
---

ClickHouse では、`SELECT` クエリとともに、クエリの処理に必要なデータをサーバーに送信できます。このデータは一時テーブル (「一時テーブル」セクションを参照) に格納され、クエリ内で使用できます (たとえば、`IN` 演算子で使用できます) 。

たとえば、重要なユーザー識別子を含むテキストファイルがある場合、そのリストで絞り込むクエリと一緒にサーバーへアップロードできます。

大量の外部データを使って複数のクエリを実行する必要がある場合は、この機能は使用しないでください。事前にデータを DB にアップロードしておくほうが適切です。

外部データは、コマンドラインクライアント (非対話型モード) または HTTPインターフェイス を使用してアップロードできます。

コマンドラインクライアントでは、次のフォーマットでパラメーターセクションを指定できます

```bash
--external --file=... [--name=...] [--format=...] [--types=...|--structure=...]
```

このようなセクションが複数存在する場合があり、その数は転送されるテーブル数に対応します。

**–external** – 句の開始を示します。
**–file** – テーブルのダンプが格納されたファイルへのパス、または stdin を指す - です。
stdin から取得できるテーブルは 1 つだけです。

次のパラメータは任意です: **–name**– テーブル名。省略した場合は &#95;data が使用されます。
**–format** – ファイル内のデータのフォーマット。省略した場合は TabSeparated が使用されます。

次のパラメータのいずれか 1 つが必要です:**–types** – カンマ区切りのカラム型の一覧。例: `UInt64,String`。カラム名は &#95;1, &#95;2, ... になります。
**–structure**– `UserID UInt64`, `URL String` の形式で指定するテーブル構造です。カラム名と型を定義します。

&#39;file&#39; で指定されたファイルは、&#39;format&#39; で指定されたフォーマットで、&#39;types&#39; または &#39;structure&#39; で指定されたデータ型を使用してパースされます。このテーブルはサーバーにアップロードされ、そこで &#39;name&#39; で指定した名前の一時テーブルとしてアクセスできるようになります。

Examples:

```bash
$ echo -ne "1\n2\n3\n" | clickhouse-client --query="SELECT count() FROM test.visits WHERE TraficSourceID IN _data" --external --file=- --types=Int8
849897
$ cat /etc/passwd | sed 's/:/\t/g' | clickhouse-client --query="SELECT shell, count() AS c FROM passwd GROUP BY shell ORDER BY c DESC" --external --file=- --name=passwd --structure='login String, unused String, uid UInt16, gid UInt16, comment String, home String, shell String'
/bin/sh 20
/bin/false      5
/bin/bash       4
/usr/sbin/nologin       1
/bin/sync       1
```

HTTPインターフェイスを使用する場合、外部データは `multipart/form-data` フォーマットで渡されます。各テーブルはそれぞれ個別のファイルとして送信されます。テーブル名はファイル名から取得されます。`query_string` には `name_format`、`name_types`、`name_structure` というパラメータが渡されます。ここで、`name` はこれらのパラメータに対応するテーブル名です。これらのパラメータの意味は、コマンドラインクライアントを使用する場合と同じです。

例:

```bash
$ cat /etc/passwd | sed 's/:/\t/g' > passwd.tsv

$ curl -F 'passwd=@passwd.tsv;' 'http://localhost:8123/?query=SELECT+shell,+count()+AS+c+FROM+passwd+GROUP+BY+shell+ORDER+BY+c+DESC&passwd_structure=login+String,+unused+String,+uid+UInt16,+gid+UInt16,+comment+String,+home+String,+shell+String'
/bin/sh 20
/bin/false      5
/bin/bash       4
/usr/sbin/nologin       1
/bin/sync       1
```

分散クエリ処理では、一時テーブルがすべてのリモートサーバーに送信されます。