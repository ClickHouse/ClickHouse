---
slug: /ja/engines/table-engines/special/external-data
sidebar_position: 130
sidebar_label: 外部データ
---

# クエリ処理用の外部データ

ClickHouseでは、`SELECT`クエリと一緒に、クエリの処理に必要なデータをサーバに送信することができます。このデータは一時テーブル（「一時テーブル」のセクションを参照）に置かれ、クエリで使用することができます（例えば、`IN`演算子内で）。

例えば、重要なユーザー識別子を持つテキストファイルがある場合、このリストによるフィルタリングを行うクエリと一緒に、サーバにアップロードすることができます。

大量の外部データで複数のクエリを実行する必要がある場合、この機能は使用しないでください。このデータを事前にデータベースにアップロードする方が良いです。

外部データは、コマンドラインクライアント（非対話モード）やHTTPインターフェースを使用してアップロードできます。

コマンドラインクライアントでは、次の形式でパラメータセクションを指定できます。

``` bash
--external --file=... [--name=...] [--format=...] [--types=...|--structure=...]
```

このようなセクションを複数持つことができ、送信されるテーブルの数に応じます。

**–external** – 節の開始を示します。
**–file** – テーブルダンプのファイルへのパス、またはstdinを指す-です。stdinからは1つのテーブルしか取得できません。

以下のパラメータは任意です:**–name**– テーブルの名前。省略した場合、_dataが使用されます。
**–format** – ファイル内のデータ形式。省略した場合、TabSeparatedが使用されます。

次のいずれかのパラメータは必須です:**–types** – カンマで区切られたカラムタイプのリスト。例: `UInt64,String`。カラムは_1, _2, ...と命名されます。
**–structure**– テーブル構造で`UserID UInt64`, `URL String`の形式で定義。カラム名とタイプを定義します。

‘file’で指定されたファイルは、‘format’で指定された形式で解析され、‘types’や‘structure’で指定されたデータ型を使用します。このテーブルはサーバにアップロードされ、‘name’で指定された名前で一時テーブルとしてアクセス可能になります。

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

HTTPインターフェースを使用する場合、外部データはmultipart/form-data形式で渡されます。各テーブルは個別のファイルとして送信されます。テーブル名はファイル名から取得されます。`query_string`には`name_format`、`name_types`、`name_structure`のパラメータが渡されます。`name`はこれらのパラメータに対応するテーブル名です。パラメータの意味はコマンドラインクライアントを使用するときと同じです。

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

分散クエリ処理の場合、一時テーブルはすべてのリモートサーバに送信されます。
