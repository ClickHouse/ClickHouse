---
description: '指定したクラスター内の多数のノードから、HDFS のファイルを並列に処理できます。'
sidebar_label: 'hdfsCluster'
sidebar_position: 81
slug: /sql-reference/table-functions/hdfsCluster
title: 'hdfsCluster'
doc_type: 'reference'
---

指定したクラスター内の多数のノードから、HDFS のファイルを並列に処理できます。イニシエーターでは、クラスター内のすべてのノードへの接続を確立し、HDFS のファイルパス内のアスタリスクを展開して、各ファイルを動的に割り当てます。ワーカーノードでは、次に処理するタスクをイニシエーターに問い合わせて処理します。これを、すべてのタスクが完了するまで繰り返します。

<div id="syntax">
  ## 構文
</div>

```sql
hdfsCluster(cluster_name, URI, format, structure)
```

<div id="arguments">
  ## 引数
</div>

| 引数             | 説明                                                                                                                                                                                                                                           |
| -------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name` | リモートおよびローカルのサーバーのアドレス集合と接続パラメーターの構築に使用されるクラスター名。                                                                                                                                                                                             |
| `URI`          | ファイルまたは複数のファイルを指す URI。readonly モードでは、次のワイルドカードをサポートします: `*`, `**`, `?`, `{'abc','def'}` および `{N..M}`。ここで、`N`、`M` は数値、`abc`、`def` は文字列です。詳細は [Wildcards In Path](../../engines/table-engines/integrations/s3.md#wildcards-in-path) を参照してください。 |
| `format`       | ファイルの [フォーマット](/ja/sql-reference/formats)。                                                                                                                                                                                                      |
| `structure`    | テーブルの構造。形式は `'column1_name column1_type, column2_name column2_type, ...'` です。                                                                                                                                                                |

<div id="returned_value">
  ## 戻り値
</div>

指定したファイル内のデータを読み取るための、指定した構造を持つテーブル。

<div id="examples">
  ## 例
</div>

1. `cluster_simple` という名前の ClickHouse クラスターがあり、HDFS 上に次の URI を持つ複数のファイルがあるとします。

* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;1&#39;
* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;2&#39;
* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;3&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;1&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;2&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;3&#39;

2. これらのファイルの行数をクエリします。

```sql
SELECT count(*)
FROM hdfsCluster('cluster_simple', 'hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32')
```

3. これら2つのディレクトリ内にあるすべてのファイルの行数をクエリします:

```sql
SELECT count(*)
FROM hdfsCluster('cluster_simple', 'hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV', 'name String, value UInt32')
```

:::note
ファイル一覧に先頭がゼロの数値範囲が含まれる場合は、各桁ごとに波かっこを使う構文を使用するか、`?` を使用してください。
:::

<div id="related">
  ## 関連項目
</div>

* [HDFS エンジン](../../engines/table-engines/integrations/hdfs.md)
* [HDFS テーブル関数](../../sql-reference/table-functions/hdfs.md)