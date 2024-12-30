---
slug: /ja/sql-reference/table-functions/fileCluster
sidebar_position: 61
sidebar_label: fileCluster
---

# fileCluster テーブル関数

クラスター内の複数のノードで、指定されたパスに一致するファイルを同時に処理できるようにします。イニシエーターはワーカーノードへの接続を確立し、ファイルパスのグロブを展開し、ファイル読み取りタスクをワーカーノードに委任します。各ワーカーノードは、次に処理するファイルをイニシエーターにクエリして要求し、すべてのタスクが完了するまで（すべてのファイルが読み取られるまで）このプロセスを繰り返します。

:::note    
この関数が_正しく_動作するのは、最初に指定されたパスに一致するファイルのセットがすべてのノードで同一であり、異なるノード間でも内容が一致している場合に限ります。  
これらのファイルがノード間で異なる場合、ワーカーノードがイニシエーターからタスクを要求する順序に依存するため、返される値は予測できません。
:::

**構文**

``` sql
fileCluster(cluster_name, path[, format, structure, compression_method])
```

**引数**

- `cluster_name` — リモートおよびローカルサーバーへのアドレスと接続パラメータのセットを構築するために使用されるクラスターの名前。
- `path` — [user_files_path](/docs/ja/operations/server-configuration-parameters/settings.md#user_files_path) からファイルへの相対パス。ファイルへのパスは[グロブ](#globs-in-path)をサポートしています。
- `format` — ファイルの[フォーマット](../../interfaces/formats.md#formats)。タイプ: [String](../../sql-reference/data-types/string.md)。
- `structure` — `'UserID UInt64, Name String'`形式のテーブル構造。カラム名とタイプを決定します。タイプ: [String](../../sql-reference/data-types/string.md)。
- `compression_method` — 圧縮方式。サポートされる圧縮タイプは `gz`、`br`、`xz`、`zst`、`lz4`、`bz2` です。

**返される値**

指定されたフォーマットと構造を持ち、指定されたパスに一致するファイルからデータを持つテーブル。

**例**

`my_cluster` という名前のクラスターと、以下の設定値 `user_files_path` が与えられている場合:

``` bash
$ grep user_files_path /etc/clickhouse-server/config.xml
    <user_files_path>/var/lib/clickhouse/user_files/</user_files_path>
```
また、各クラスターノードの `user_files_path` 内に `test1.csv` と `test2.csv` があり、その内容が異なるノード間で同一である場合:
```bash
$ cat /var/lib/clickhouse/user_files/test1.csv
    1,"file1"
    11,"file11"

$ cat /var/lib/clickhouse/user_files/test2.csv
    2,"file2"
    22,"file22"
```

例えば、これらのファイルを各クラスターノードで次の2つのクエリを実行することで作成できます:
```sql
INSERT INTO TABLE FUNCTION file('file1.csv', 'CSV', 'i UInt32, s String') VALUES (1,'file1'), (11,'file11');
INSERT INTO TABLE FUNCTION file('file2.csv', 'CSV', 'i UInt32, s String') VALUES (2,'file2'), (22,'file22');
```

次に、`fileCluster` テーブル関数を通じて `test1.csv` と `test2.csv` のデータ内容を読み取ります:

```sql
SELECT * FROM fileCluster('my_cluster', 'file{1,2}.csv', 'CSV', 'i UInt32, s String') ORDER BY i, s
```

```
┌──i─┬─s──────┐
│  1 │ file1  │
│ 11 │ file11 │
└────┴────────┘
┌──i─┬─s──────┐
│  2 │ file2  │
│ 22 │ file22 │
└────┴────────┘
```

## パスにおけるグロブ

[File](../../sql-reference/table-functions/file.md#globs-in-path) テーブル関数でサポートされているすべてのパターンが、FileClusterでもサポートされています。

**関連項目**

- [Fileテーブル関数](../../sql-reference/table-functions/file.md)
