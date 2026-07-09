---
description: 'クラスター内の複数のノードで、指定したパスに一致するファイルを同時に処理できるようにします。イニシエーターは
  ワーカーノードへの接続を確立し、ファイルパス内のグロブを展開して、ファイル読み取りタスクをワーカーノードに
  割り当てます。各ワーカーノードは処理する次のファイルを取得するためにイニシエーターに問い合わせを行い、
  すべてのタスクが完了する（すべてのファイルが読み取られる）までこれを繰り返します。'
sidebar_label: 'fileCluster'
sidebar_position: 61
slug: /sql-reference/table-functions/fileCluster
title: 'fileCluster'
doc_type: 'reference'
---

クラスター内の複数のノードで、指定したパスに一致するファイルを同時に処理できるようにします。イニシエーターはワーカーノードへの接続を確立し、ファイルパス内のグロブを展開して、ファイル読み取りタスクをワーカーノードに割り当てます。各ワーカーノードは処理する次のファイルを取得するためにイニシエーターに問い合わせを行い、すべてのタスクが完了する (すべてのファイルが読み取られる) までこれを繰り返します。

:::note
この関数が *正しく* 動作するのは、最初に指定したパスに一致するファイルの集合がすべてのノードで同一であり、かつその内容がノード間で整合している場合に限られます。
これらのファイルがノードごとに異なる場合、戻り値は事前に決定できず、ワーカーノードがイニシエーターにタスクをリクエストする順序に依存します。
:::

<div id="syntax">
  ## 構文
</div>

```sql
fileCluster(cluster_name, path[, format, structure, compression_method])
```

<div id="arguments">
  ## 引数
</div>

| 引数                   | 説明                                                                                                                                                           |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `cluster_name`       | リモートおよびローカルのserverへのアドレスのセットと接続パラメーターの構築に使用されるクラスター名。                                                                                                        |
| `path`               | [user&#95;files&#95;path](/ja/operations/server-configuration-parameters/settings.md#user_files_path) からのファイルへの相対パス。ファイルパスでは [グロブ](#globs-in-path) もサポートされます。 |
| `format`             | ファイルの [フォーマット](/ja/sql-reference/formats)。型: [String](../../sql-reference/data-types/string.md)。                                                                |
| `structure`          | `'UserID UInt64, Name String'` フォーマットのtable structure。カラム名と型を指定します。型: [String](../../sql-reference/data-types/string.md)。                                    |
| `compression_method` | 圧縮方式。サポートされる圧縮タイプは `gz`、`br`、`xz`、`zst`、`lz4`、`bz2` です。                                                                                                      |

<div id="returned_value">
  ## 戻り値
</div>

指定したフォーマットと構造を持ち、指定したパスに一致するファイルのデータを含むテーブル。

**例**

`my_cluster` という名前のクラスターがあり、設定 `user_files_path` の値が次のとおりであるとします。

```bash
$ grep user_files_path /etc/clickhouse-server/config.xml
    <user_files_path>/var/lib/clickhouse/user_files/</user_files_path>
```

また、各クラスターのノードの`user_files_path`内にファイル`test1.csv`と`test2.csv`があり、それらの内容がノード間で同一である場合:

```bash
$ cat /var/lib/clickhouse/user_files/test1.csv
    1,"file1"
    11,"file11"

$ cat /var/lib/clickhouse/user_files/test2.csv
    2,"file2"
    22,"file22"
```

たとえば、各クラスターノードで次の2つのクエリを実行すると、これらのファイルを作成できます。

```sql
INSERT INTO TABLE FUNCTION file('file1.csv', 'CSV', 'i UInt32, s String') VALUES (1,'file1'), (11,'file11');
INSERT INTO TABLE FUNCTION file('file2.csv', 'CSV', 'i UInt32, s String') VALUES (2,'file2'), (22,'file22');
```

次に、`fileCluster` テーブル関数を使って `test1.csv` と `test2.csv` のデータ内容を読み込みます:

```sql
SELECT * FROM fileCluster('my_cluster', 'file{1,2}.csv', 'CSV', 'i UInt32, s String') ORDER BY i, s
```

```response
┌──i─┬─s──────┐
│  1 │ file1  │
│ 11 │ file11 │
└────┴────────┘
┌──i─┬─s──────┐
│  2 │ file2  │
│ 22 │ file22 │
└────┴────────┘
```

<div id="globs-in-path">
  ## パス内のグロブ
</div>

[File](../../sql-reference/table-functions/file.md#globs-in-path) テーブル関数でサポートされているすべてのパターンは、FileCluster でもサポートされています。

<div id="related">
  ## 関連
</div>

* [Fileテーブル関数](../../sql-reference/table-functions/file.md)