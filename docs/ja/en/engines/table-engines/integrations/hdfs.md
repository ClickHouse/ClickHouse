---
description: 'このエンジンは、ClickHouse 経由で HDFS 上のデータを管理できるようにすることで、Apache Hadoop エコシステムとのインテグレーションを提供します。File エンジンおよび URL エンジンに似ていますが、Hadoop 固有の機能を備えています。'
sidebar_label: 'HDFS'
sidebar_position: 80
slug: /engines/table-engines/integrations/hdfs
title: 'HDFS テーブルエンジン'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="hdfs-table-engine">
  # HDFS テーブルエンジン
</div>

<CloudNotSupportedBadge />

このエンジンは、ClickHouse 経由で [HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html) 上のデータを管理できるようにすることで、[Apache Hadoop](https://en.wikipedia.org/wiki/Apache_Hadoop) エコシステムとのインテグレーションを実現します。このエンジンは [File](/ja/engines/table-engines/special/file) および [URL](/ja/engines/table-engines/special/url) エンジンと似ていますが、Hadoop 固有の機能を備えています。

この機能は ClickHouse エンジニアによるサポート対象ではなく、品質にも難があることが知られています。問題が発生した場合は、自分で修正し、プルリクエストを送信してください。

<div id="usage">
  ## 使い方
</div>

```sql
ENGINE = HDFS(URI, format)
```

**エンジンパラメータ**

* `URI` - HDFS 内のファイル全体の URI。`URI` の パス 部分には グロブ を含めることができます。この場合、table は readonly になります。
* `format` - 利用可能なファイルフォーマットのいずれか 1 つを指定します。`SELECT` クエリを実行するには、そのフォーマットが入力に対応している必要があり、`INSERT` クエリを実行するには出力に対応している必要があります。利用可能なフォーマットは [Formats](/ja/sql-reference/formats#formats-overview) セクションに一覧されています。
* [PARTITION BY expr]

<div id="partition-by">
  ### PARTITION BY
</div>

`PARTITION BY` — 任意です。ほとんどの場合、パーティションキーは必要ありません。必要な場合でも、通常は月単位より細かいパーティションキーにする必要はありません。パーティション化してもクエリは高速化されません (`ORDER BY` 式とは異なります) 。細かすぎるパーティション化は決して行わないでください。クライアント識別子や名前でデータをパーティション化しないでください (代わりに、クライアント識別子または名前を `ORDER BY` 式の最初のカラムにします) 。

月単位でパーティション化するには、`toYYYYMM(date_column)` 式を使用します。ここで `date_column` は [Date](/ja/sql-reference/data-types/date.md) 型の日付カラムです。ここでのパーティション名のフォーマットは `"YYYYMM"` です。

**例:**

**1.** `hdfs_engine_table` テーブルを設定します:

```sql
CREATE TABLE hdfs_engine_table (name String, value UInt32) ENGINE=HDFS('hdfs://hdfs1:9000/other_storage', 'TSV')
```

**2.** ファイルにデータを書き込む:

```sql
INSERT INTO hdfs_engine_table VALUES ('one', 1), ('two', 2), ('three', 3)
```

**3.** データをクエリします:

```sql
SELECT * FROM hdfs_engine_table LIMIT 2
```

```text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

<div id="implementation-details">
  ## 実装の詳細
</div>

* 読み取りと書き込みは並列に実行できます。
* サポートされていません:

  * `ALTER` 操作および `SELECT...SAMPLE` 操作。
  * 索引。
  * [ゼロコピー](../../../operations/storing-data.md#zero-copy) レプリケーションは可能ですが、推奨されません。

  :::note ゼロコピー レプリケーションは本番環境向けではありません
  ゼロコピー レプリケーションは ClickHouse バージョン 22.8 以降ではデフォルトで無効になっています。この機能を本番環境で使用することは推奨されません。
  :::

**パス内のグロブ**

複数のパスコンポーネントにグロブを含めることができます。処理対象のファイルは存在し、パスパターン全体に一致している必要があります。ファイル一覧は `SELECT` の実行時に決定されます (`CREATE` 時ではありません) 。

* `*` — 空文字列を含む、`/` を除く任意の文字列に一致します。
* `?` — 任意の 1 文字に一致します。
* `{some_string,another_string,yet_another_one}` — `'some_string'`、`'another_string'`、`'yet_another_one'` のいずれかの文字列に一致します。
* `{N..M}` — N から M までの範囲内の任意の数値 (両端を含む) に一致します。

`{}` を使った構文は、[remote](../../../sql-reference/table-functions/remote.md) テーブル関数に似ています。

**例**

1. HDFS 上に、以下の URI を持つ TSV フォーマットのファイルが複数あるとします:

   * &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;1&#39;
   * &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;2&#39;
   * &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;3&#39;
   * &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;1&#39;
   * &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;2&#39;
   * &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;3&#39;

2. これら 6 つすべてのファイルから成るテーブルを作成する方法はいくつかあります:

{/* */ }

```sql
CREATE TABLE table_with_range (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV')
```

別の方法：

```sql
CREATE TABLE table_with_question_mark (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_?', 'TSV')
```

テーブルは、両方のディレクトリ内にあるすべてのファイルで構成されます (すべてのファイルが、クエリで指定されたフォーマットとスキーマに適合している必要があります) :

```sql
CREATE TABLE table_with_asterisk (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV')
```

:::note
ファイル一覧に先頭がゼロの数値範囲が含まれている場合は、各桁ごとに中括弧を使った構文を使用するか、`?` を使用してください。
:::

**例**

`file000`、`file001`、...、`file999` という名前のファイルを持つテーブルを作成します:

```sql
CREATE TABLE big_table (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/big_dir/file{0..9}{0..9}{0..9}', 'CSV')
```

<div id="configuration">
  ## 設定
</div>

GraphiteMergeTree と同様に、HDFS engine は ClickHouse の設定ファイルを使った拡張設定をサポートしています。使用できる設定キーは 2 種類あり、グローバル (`hdfs`) とユーザーレベル (`hdfs_*`) です。まずグローバル設定が適用され、その後、ユーザーレベルの設定が存在する場合はそれが適用されます。

```xml
<!-- Global configuration options for HDFS engine type -->
<hdfs>
  <hadoop_kerberos_keytab>/tmp/keytab/clickhouse.keytab</hadoop_kerberos_keytab>
  <hadoop_kerberos_principal>clickuser@TEST.CLICKHOUSE.TECH</hadoop_kerberos_principal>
  <hadoop_security_authentication>kerberos</hadoop_security_authentication>
</hdfs>

<!-- Configuration specific for user "root" -->
<hdfs_root>
  <hadoop_kerberos_principal>root@TEST.CLICKHOUSE.TECH</hadoop_kerberos_principal>
</hdfs_root>
```

<div id="configuration-options">
  ### 設定オプション
</div>

<div id="supported-by-libhdfs3">
  #### libhdfs3 でサポートされる設定
</div>

| **パラメータ**                                                               | **デフォルト値**                        |
| ----------------------------------------------------------------------- | --------------------------------- |
| rpc&#95;client&#95;connect&#95;tcpnodelay                               | true                              |
| dfs&#95;client&#95;read&#95;shortcircuit                                | true                              |
| output&#95;replace-datanode-on-failure                                  | true                              |
| input&#95;notretry-another-node                                         | false                             |
| input&#95;localread&#95;mappedfile                                      | true                              |
| dfs&#95;client&#95;use&#95;legacy&#95;blockreader&#95;local             | false                             |
| rpc&#95;client&#95;ping&#95;interval                                    | 10  * 1000                        |
| rpc&#95;client&#95;connect&#95;timeout                                  | 600 * 1000                        |
| rpc&#95;client&#95;read&#95;timeout                                     | 3600 * 1000                       |
| rpc&#95;client&#95;write&#95;timeout                                    | 3600 * 1000                       |
| rpc&#95;client&#95;socket&#95;linger&#95;timeout                        | -1                                |
| rpc&#95;client&#95;connect&#95;retry                                    | 10                                |
| rpc&#95;client&#95;timeout                                              | 3600 * 1000                       |
| dfs&#95;default&#95;replica                                             | 3                                 |
| input&#95;connect&#95;timeout                                           | 600 * 1000                        |
| input&#95;read&#95;timeout                                              | 3600 * 1000                       |
| input&#95;write&#95;timeout                                             | 3600 * 1000                       |
| input&#95;localread&#95;default&#95;buffersize                          | 1 * 1024 * 1024                   |
| dfs&#95;prefetchsize                                                    | 10                                |
| input&#95;read&#95;getblockinfo&#95;retry                               | 3                                 |
| input&#95;localread&#95;blockinfo&#95;cachesize                         | 1000                              |
| input&#95;read&#95;max&#95;retry                                        | 60                                |
| output&#95;default&#95;chunksize                                        | 512                               |
| output&#95;default&#95;packetsize                                       | 64 * 1024                         |
| output&#95;default&#95;write&#95;retry                                  | 10                                |
| output&#95;connect&#95;timeout                                          | 600 * 1000                        |
| output&#95;read&#95;timeout                                             | 3600 * 1000                       |
| output&#95;write&#95;timeout                                            | 3600 * 1000                       |
| output&#95;close&#95;timeout                                            | 3600 * 1000                       |
| output&#95;packetpool&#95;size                                          | 1024                              |
| output&#95;heartbeat&#95;interval                                       | 10 * 1000                         |
| dfs&#95;client&#95;failover&#95;max&#95;attempts                        | 15                                |
| dfs&#95;client&#95;read&#95;shortcircuit&#95;streams&#95;cache&#95;size | 256                               |
| dfs&#95;client&#95;socketcache&#95;expiryMsec                           | 3000                              |
| dfs&#95;client&#95;socketcache&#95;capacity                             | 16                                |
| dfs&#95;default&#95;blocksize                                           | 64 * 1024 * 1024                  |
| dfs&#95;default&#95;uri                                                 | &quot;hdfs://localhost:9000&quot; |
| hadoop&#95;security&#95;authentication                                  | &quot;simple&quot;                |
| hadoop&#95;security&#95;kerberos&#95;ticket&#95;cache&#95;path          | &quot;&quot;                      |
| dfs&#95;client&#95;log&#95;severity                                     | &quot;INFO&quot;                  |
| dfs&#95;domain&#95;socket&#95;path                                      | &quot;&quot;                      |

一部のパラメータについては、[HDFS Configuration Reference](https://hawq.apache.org/docs/userguide/2.3.0.0-incubating/reference/HDFSConfigurationParameterReference.html) を参照してください。

<div id="clickhouse-extras">
  #### ClickHouse の追加設定
</div>

| **パラメータ**                         | **デフォルト値**   |
| --------------------------------- | ------------ |
| hadoop&#95;kerberos&#95;keytab    | &quot;&quot; |
| hadoop&#95;kerberos&#95;principal | &quot;&quot; |
| libhdfs3&#95;conf                 | &quot;&quot; |

<div id="limitations">
  ### 制限事項
</div>

* `hadoop_security_kerberos_ticket_cache_path` と `libhdfs3_conf` はグローバル設定のみ可能で、ユーザーごとの設定はできません

<div id="kerberos-support">
  ## Kerberos サポート
</div>

`hadoop_security_authentication` パラメータの値が `kerberos` の場合、ClickHouse は Kerberos で認証を行います。
パラメータは[こちら](#clickhouse-extras)にあり、`hadoop_security_kerberos_ticket_cache_path` が役立つことがあります。
libhdfs3 の制限により、サポートされるのは従来方式のみである点に注意してください。
datanode 間の通信は SASL では保護されません (`HADOOP_SECURE_DN_USER` はこのような
セキュリティ方式を示す信頼できる指標です) 。参考として `tests/integration/test_storage_kerberized_hdfs/hdfs_configs/bootstrap.sh` を参照してください。

`hadoop_kerberos_keytab`、`hadoop_kerberos_principal`、または `hadoop_security_kerberos_ticket_cache_path` のいずれかが指定されている場合は、Kerberos 認証が使用されます。この場合、`hadoop_kerberos_keytab` と `hadoop_kerberos_principal` は必須です。

<div id="namenode-ha">
  ## HDFS Namenode HA のサポート
</div>

libhdfs3 は HDFS Namenode HA をサポートしています。

* HDFS ノードから `hdfs-site.xml` を `/etc/clickhouse-server/` にコピーします。
* ClickHouse の設定ファイルに以下の設定を追加します：

```xml
  <hdfs>
    <libhdfs3_conf>/etc/clickhouse-server/hdfs-site.xml</libhdfs3_conf>
  </hdfs>
```

* 次に、HDFS URI の namenode アドレスには、`hdfs-site.xml` の `dfs.nameservices` タグの値を使用します。たとえば、`hdfs://appadmin@192.168.101.11:8020/abc/` を `hdfs://appadmin@my_nameservice/abc/` に置き換えます。

<div id="virtual-columns">
  ## 仮想カラム
</div>

* `_path` — ファイルのパス。型: `LowCardinality(String)`.
* `_file` — ファイル名。型: `LowCardinality(String)`.
* `_size` — ファイルサイズ (バイト単位) 。型: `Nullable(UInt64)`. サイズが不明な場合、値は `NULL` です。
* `_time` — ファイルの最終更新時刻。型: `Nullable(DateTime)`. 時刻が不明な場合、値は `NULL` です。

<div id="storage-settings">
  ## ストレージ設定
</div>

* [hdfs&#95;truncate&#95;on&#95;insert](/ja/operations/settings/settings.md#hdfs_truncate_on_insert) - ファイルに insert する前に、そのファイルを切り詰めることを許可します。デフォルトでは無効です。
* [hdfs&#95;create&#95;new&#95;file&#95;on&#95;insert](/ja/operations/settings/settings.md#hdfs_create_new_file_on_insert) - フォーマットに接尾辞がある場合、insert のたびに新しいファイルを作成することを許可します。デフォルトでは無効です。
* [hdfs&#95;skip&#95;empty&#95;files](/ja/operations/settings/settings.md#hdfs_skip_empty_files) - 読み取り時に空のファイルをスキップすることを許可します。デフォルトでは無効です。

**関連項目**

* [仮想カラム](../../../engines/table-engines/index.md#table_engines-virtual_columns)