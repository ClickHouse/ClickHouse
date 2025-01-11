---
slug: /ja/engines/table-engines/integrations/hdfs
sidebar_position: 80
sidebar_label: HDFS
---

# HDFS

このエンジンは、[Apache Hadoop](https://en.wikipedia.org/wiki/Apache_Hadoop) エコシステムとの統合を提供し、ClickHouseを通じて [HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html) 上のデータを管理することを可能にします。このエンジンは [File](../../../engines/table-engines/special/file.md#table_engines-file) や [URL](../../../engines/table-engines/special/url.md#table_engines-url) エンジンに似ていますが、Hadoop特有の機能を提供します。

この機能はClickHouseのエンジニアによるサポート対象ではなく、品質が不安定であることが知られています。問題が発生した場合は、自分で修正してプルリクエストを送ってください。

## 使用法 {#usage}

``` sql
ENGINE = HDFS(URI, format)
```

**エンジンパラメータ**

- `URI` - HDFS内の全体のファイルURI。`URI`のパス部分にはグロブを含めることができます。この場合、テーブルは読み取り専用になります。
- `format` - 利用可能なファイルフォーマットのいずれかを指定します。
`SELECT` クエリを実行する場合、フォーマットは入力用にサポートされている必要があり、`INSERT` クエリを実行する場合は出力用にサポートされている必要があります。利用可能なフォーマットは [Formats](../../../interfaces/formats.md#formats) セクションにリストされています。
- [PARTITION BY expr]

### PARTITION BY

`PARTITION BY` — オプションです。ほとんどの場合、パーティションキーは必要ありませんが、必要な場合でも通常は月ごとに分けるほど詳細なパーティションキーは必要ありません。パーティショニングはクエリを高速化しません（ORDER BY 式とは対照的に）。非常に詳細なパーティショニングは避けてください。クライアントIDや名前でデータをパーティション分割しないでください（代わりに、クライアントIDや名前をORDER BY 式の最初のカラムにします）。

月ごとにパーティションを分ける場合は、`toYYYYMM(date_column)` 式を使用します。ここで、`date_column` は [Date](/docs/ja/sql-reference/data-types/date.md) 型の日付を持つカラムです。ここでのパーティション名は `"YYYYMM"` フォーマットになります。

**例:**

**1.** `hdfs_engine_table` テーブルをセットアップ：

``` sql
CREATE TABLE hdfs_engine_table (name String, value UInt32) ENGINE=HDFS('hdfs://hdfs1:9000/other_storage', 'TSV')
```

**2.** ファイルを埋める：

``` sql
INSERT INTO hdfs_engine_table VALUES ('one', 1), ('two', 2), ('three', 3)
```

**3.** データをクエリ：

``` sql
SELECT * FROM hdfs_engine_table LIMIT 2
```

``` text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

## 実装の詳細 {#implementation-details}

- 読み取りと書き込みは並列で行われる可能性があります。
- サポートされていないもの：
    - `ALTER` および `SELECT...SAMPLE` 操作。
    - インデックス。
    - [ゼロコピー](../../../operations/storing-data.md#zero-copy) レプリケーションは可能ですが、推奨されません。

  :::note
  ゼロコピーのレプリケーションは本番環境には対応していません
  ClickHouseバージョン22.8以降ではデフォルトで無効になっています。この機能は本番環境での使用を推奨しません。
  :::

**パス内のグロブ**

複数のパスコンポーネントがグロブを含むことができます。処理されるためには、ファイルが存在し、全体のパスパターンに一致する必要があります。ファイルのリストは `SELECT` 中に決定されます（`CREATE` 時点ではなく）。

- `*` — `/` を除く任意の文字を空文字列として含めた任意の数に置き換えます。
- `?` — 任意の一文字に置き換えます。
- `{some_string,another_string,yet_another_one}` — 文字列 `'some_string'`, `'another_string'`, `'yet_another_one'` のいずれかに置き換えます。
- `{N..M}` — N から M までの範囲の任意の数字に置き換えます（両端を含む）。

`{}`を用いた構文は [remote](../../../sql-reference/table-functions/remote.md) テーブル関数に似ています。

**例**

1.  TSV形式の数ファイルがHDFSに以下のURIで保存されているとします：

    - 'hdfs://hdfs1:9000/some_dir/some_file_1'
    - 'hdfs://hdfs1:9000/some_dir/some_file_2'
    - 'hdfs://hdfs1:9000/some_dir/some_file_3'
    - 'hdfs://hdfs1:9000/another_dir/some_file_1'
    - 'hdfs://hdfs1:9000/another_dir/some_file_2'
    - 'hdfs://hdfs1:9000/another_dir/some_file_3'

1.  すべての6つのファイルから成るテーブルを作成する方法はいくつかあります：

``` sql
CREATE TABLE table_with_range (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV')
```

別の方法：

``` sql
CREATE TABLE table_with_question_mark (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_?', 'TSV')
```

両方のディレクトリのすべてのファイルから構成されるテーブル（クエリで記述されたフォーマットとスキーマに適合するすべてのファイル）：

``` sql
CREATE TABLE table_with_asterisk (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV')
```

:::note
ファイルのリストが先頭ゼロ付きの数値範囲を含む場合は、個々の数字に対して中括弧 `{}` を使用するか、`?` を使用してください。
:::

**例**

ファイル名が `file000`, `file001`, ... , `file999` のファイルを持つテーブルを作成：

``` sql
CREATE TABLE big_table (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/big_dir/file{0..9}{0..9}{0..9}', 'CSV')
```

## 設定 {#configuration}

GraphiteMergeTreeと同様に、HDFSエンジンはClickHouseの設定ファイルを使用した拡張設定をサポートしています。使用できる設定キーはグローバル（`hdfs`）とユーザーレベル（`hdfs_*`）の二つです。グローバル設定が最初に適用され、次にユーザーレベルの設定が適用されます（存在する場合）。

``` xml
  <!-- HDFSエンジンタイプのグローバル設定オプション -->
  <hdfs>
	<hadoop_kerberos_keytab>/tmp/keytab/clickhouse.keytab</hadoop_kerberos_keytab>
	<hadoop_kerberos_principal>clickuser@TEST.CLICKHOUSE.TECH</hadoop_kerberos_principal>
	<hadoop_security_authentication>kerberos</hadoop_security_authentication>
  </hdfs>

  <!-- ユーザー "root" の特定の設定 -->
  <hdfs_root>
	<hadoop_kerberos_principal>root@TEST.CLICKHOUSE.TECH</hadoop_kerberos_principal>
  </hdfs_root>
```

### 設定オプション {#configuration-options}

#### libhdfs3によってサポートされている設定 {#supported-by-libhdfs3}

| **パラメータ**                                         | **デフォルト値**       |
| -                                                  | -                    |
| rpc\_client\_connect\_tcpnodelay                      | true                    |
| dfs\_client\_read\_shortcircuit                       | true                    |
| output\_replace-datanode-on-failure                   | true                    |
| input\_notretry-another-node                          | false                   |
| input\_localread\_mappedfile                          | true                    |
| dfs\_client\_use\_legacy\_blockreader\_local          | false                   |
| rpc\_client\_ping\_interval                           | 10  * 1000              |
| rpc\_client\_connect\_timeout                         | 600 * 1000              |
| rpc\_client\_read\_timeout                            | 3600 * 1000             |
| rpc\_client\_write\_timeout                           | 3600 * 1000             |
| rpc\_client\_socket\_linger\_timeout                  | -1                      |
| rpc\_client\_connect\_retry                           | 10                      |
| rpc\_client\_timeout                                  | 3600 * 1000             |
| dfs\_default\_replica                                 | 3                       |
| input\_connect\_timeout                               | 600 * 1000              |
| input\_read\_timeout                                  | 3600 * 1000             |
| input\_write\_timeout                                 | 3600 * 1000             |
| input\_localread\_default\_buffersize                 | 1 * 1024 * 1024         |
| dfs\_prefetchsize                                     | 10                      |
| input\_read\_getblockinfo\_retry                      | 3                       |
| input\_localread\_blockinfo\_cachesize                | 1000                    |
| input\_read\_max\_retry                               | 60                      |
| output\_default\_chunksize                            | 512                     |
| output\_default\_packetsize                           | 64 * 1024               |
| output\_default\_write\_retry                         | 10                      |
| output\_connect\_timeout                              | 600 * 1000              |
| output\_read\_timeout                                 | 3600 * 1000             |
| output\_write\_timeout                                | 3600 * 1000             |
| output\_close\_timeout                                | 3600 * 1000             |
| output\_packetpool\_size                              | 1024                    |
| output\_heartbeat\_interval                          | 10 * 1000               |
| dfs\_client\_failover\_max\_attempts                  | 15                      |
| dfs\_client\_read\_shortcircuit\_streams\_cache\_size | 256                     |
| dfs\_client\_socketcache\_expiryMsec                  | 3000                    |
| dfs\_client\_socketcache\_capacity                    | 16                      |
| dfs\_default\_blocksize                               | 64 * 1024 * 1024        |
| dfs\_default\_uri                                     | "hdfs://localhost:9000" |
| hadoop\_security\_authentication                      | "simple"                |
| hadoop\_security\_kerberos\_ticket\_cache\_path       | ""                      |
| dfs\_client\_log\_severity                            | "INFO"                  |
| dfs\_domain\_socket\_path                             | ""                      |


[HDFS Configuration Reference](https://hawq.apache.org/docs/userguide/2.3.0.0-incubating/reference/HDFSConfigurationParameterReference.html) はいくつかのパラメータを説明するかもしれません。

#### ClickHouseエクストラ {#clickhouse-extras}

| **パラメータ**                                         | **デフォルト値**       |
| -                                                  | -                    |
|hadoop\_kerberos\_keytab                               | ""                      |
|hadoop\_kerberos\_principal                            | ""                      |
|libhdfs3\_conf                                         | ""                      |

### 制限事項 {#limitations}
* `hadoop_security_kerberos_ticket_cache_path` および `libhdfs3_conf` はグローバルのみで、ユーザー固有にはできません

## Kerberosサポート {#kerberos-support}

もし `hadoop_security_authentication` パラメータが `kerberos` の値を持つとき、ClickHouseはKerberosを通じて認証します。
パラメータは [こちら](#clickhouse-extras) で、`hadoop_security_kerberos_ticket_cache_path` が役立つかもしれません。
libhdfs3の制限により古い方法のみがサポートされており、
データノードの通信はSASLで保護されていません（`HADOOP_SECURE_DN_USER` はそのようなセキュリティアプローチの信頼できる指標です）。`tests/integration/test_storage_kerberized_hdfs/hdfs_configs/bootstrap.sh` を参照として使用してください。

`hadoop_kerberos_keytab`、`hadoop_kerberos_principal` または `hadoop_security_kerberos_ticket_cache_path` が指定されている場合、Kerberos認証が使用されます。この場合、`hadoop_kerberos_keytab` と `hadoop_kerberos_principal` は必須です。

## HDFS Namenode HAサポート {#namenode-ha}

libhdfs3はHDFS namenode HAをサポートします。

- `hdfs-site.xml` をHDFSノードから `/etc/clickhouse-server/` にコピーします。
- ClickHouseの設定ファイルに以下の部分を追加します：

``` xml
  <hdfs>
    <libhdfs3_conf>/etc/clickhouse-server/hdfs-site.xml</libhdfs3_conf>
  </hdfs>
```

- その後、HDFS URIでnamenodeアドレスとして `hdfs-site.xml` の `dfs.nameservices` タグ値を使用します。たとえば、`hdfs://appadmin@192.168.101.11:8020/abc/` を `hdfs://appadmin@my_nameservice/abc/` に置き換えます。

## 仮想カラム {#virtual-columns}

- `_path` — ファイルへのパス。型：`LowCardinalty(String)`。
- `_file` — ファイルの名称。型：`LowCardinalty(String)`。
- `_size` — ファイルのバイト単位のサイズ。型：`Nullable(UInt64)`。サイズが不明な場合、値は `NULL` です。
- `_time` — ファイルの最終修正時刻。型：`Nullable(DateTime)`。時刻が不明な場合、値は `NULL` です。

## ストレージ設定 {#storage-settings}

- [hdfs_truncate_on_insert](/docs/ja/operations/settings/settings.md#hdfs_truncate_on_insert) - 挿入時にファイルを切り詰めることを許可します。デフォルトでは無効になっています。
- [hdfs_create_new_file_on_insert](/docs/ja/operations/settings/settings.md#hdfs_create_new_file_on_insert) - サフィックスを持つフォーマットでは、挿入ごとに新しいファイルを作成することを許可します。デフォルトでは無効になっています。
- [hdfs_skip_empty_files](/docs/ja/operations/settings/settings.md#hdfs_skip_empty_files) - 読み取り時に空のファイルをスキップすることを許可します。デフォルトでは無効になっています。

**関連項目**

- [仮想カラム](../../../engines/table-engines/index.md#table_engines-virtual_columns)
