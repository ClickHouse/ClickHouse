---
slug: /ja/sql-reference/table-functions/hdfsCluster
sidebar_position: 81
sidebar_label: hdfsCluster
---

# hdfsCluster テーブル関数

指定されたクラスタ内の複数のノードからHDFSのファイルを並行して処理できるようにします。イニシエータではクラスタ内のすべてのノードに接続を作成し、HDFSファイルパスのアスタリスクを展開し、各ファイルを動的に配分します。ワーカーノードでは、次に処理すべきタスクをイニシエータに問い合わせ、それを処理します。すべてのタスクが完了するまでこのプロセスを繰り返します。

**構文**

``` sql
hdfsCluster(cluster_name, URI, format, structure)
```

**引数**

- `cluster_name` — リモートおよびローカルサーバーへのアドレスと接続パラメータのセットを構築するために使用されるクラスタの名前。
- `URI` — ファイルまたは複数のファイルへのURI。読み取り専用モードで次のワイルドカードをサポートします: `*`, `**`, `?`, `{'abc','def'}` および `{N..M}` ここで `N`, `M` — 数値, `abc`, `def` — 文字列。詳細は[パス内のワイルドカード](../../engines/table-engines/integrations/s3.md#wildcards-in-path)を参照してください。
- `format` — ファイルの[フォーマット](../../interfaces/formats.md#formats)。
- `structure` — テーブルの構造。形式 `'column1_name column1_type, column2_name column2_type, ...'`。

**返される値**

指定されたファイル内のデータを読み取るための指定された構造を持つテーブル。

**例**

1. `cluster_simple` という名前のClickHouseクラスタがあり、HDFSに以下のURIを持つ複数のファイルがあるとします:

- ‘hdfs://hdfs1:9000/some_dir/some_file_1’
- ‘hdfs://hdfs1:9000/some_dir/some_file_2’
- ‘hdfs://hdfs1:9000/some_dir/some_file_3’
- ‘hdfs://hdfs1:9000/another_dir/some_file_1’
- ‘hdfs://hdfs1:9000/another_dir/some_file_2’
- ‘hdfs://hdfs1:9000/another_dir/some_file_3’

2.  これらのファイルに含まれる行の数をクエリします:

``` sql
SELECT count(*)
FROM hdfsCluster('cluster_simple', 'hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32')
```

3.  これら2つのディレクトリ内のすべてのファイルの行数をクエリします:

``` sql
SELECT count(*)
FROM hdfsCluster('cluster_simple', 'hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV', 'name String, value UInt32')
```

:::note
ファイルのリストに先頭ゼロが付いた数値範囲が含まれている場合、各桁を個別に中括弧で囲むか、`?` を使用します。
:::

**参照**

- [HDFS エンジン](../../engines/table-engines/integrations/hdfs.md)
- [HDFS テーブル関数](../../sql-reference/table-functions/hdfs.md)
