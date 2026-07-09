---
description: '`URL` から、指定した `format` と `structure` でテーブルを作成します'
sidebar_label: 'url'
sidebar_position: 200
slug: /sql-reference/table-functions/url
title: 'url'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="url-table-function">
  # `url` テーブル関数
</div>

`url` 関数は、指定された `format` と `structure` を使用して `URL` からテーブルを作成します。

`url` 関数は、[URL](../../engines/table-engines/special/url.md) テーブル内のデータに対する `SELECT` クエリおよび `INSERT` クエリで使用できます。

<div id="syntax">
  ## 構文
</div>

```sql
url(URL [,format] [,structure] [,headers])
```

<div id="parameters">
  ## パラメータ
</div>

| Parameter   | Description                                                                                                                                                                                                                                                                                                                              |
| ----------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `URL`       | スキームによってバックエンドが選択される、単一引用符で囲まれた URL。`http`/`https` (または未認識の) URL は、`GET` または `POST` リクエストを受け付けるサーバーアドレスです (それぞれ `SELECT` または `INSERT` クエリに対応) 。一方、認識される非 HTTP スキーム (`file://`、`s3://`、`az://`、`hdfs://`、…) は、対応するテーブル関数に委譲されます。詳しくは [ディスパッチ](#scheme-dispatch) を参照してください。型: [String](../../sql-reference/data-types/string.md)。 |
| `format`    | データの [フォーマット](/ja/sql-reference/formats)。型: [String](../../sql-reference/data-types/string.md)。                                                                                                                                                                                                                                             |
| `structure` | `'UserID UInt64, Name String'` フォーマットのテーブル構造です。カラム名と型を決定します。型: [String](../../sql-reference/data-types/string.md)。                                                                                                                                                                                                                       |
| `headers`   | `'headers('key1'='value1', 'key2'='value2')'` フォーマットのヘッダーです。HTTP 呼び出し用のヘッダーを設定できます。                                                                                                                                                                                                                                                      |

<div id="returned_value">
  ## 戻り値
</div>

指定されたフォーマットと構造を持ち、指定した `URL` のデータを含むテーブル。

<div id="examples">
  ## 例
</div>

[CSV](/ja/interfaces/formats/CSV)フォーマットで応答するHTTPサーバーから、`String` 型および [UInt32](../../sql-reference/data-types/int-uint.md) 型のカラムを含むテーブルの先頭3行を取得する例です。

```sql
SELECT * FROM url('http://127.0.0.1:12345/', CSV, 'column1 String, column2 UInt32', headers('Accept'='text/csv; charset=utf-8')) LIMIT 3;
```

`URL` からテーブルへデータを挿入する:

```sql
CREATE TABLE test_table (column1 String, column2 UInt32) ENGINE=Memory;
INSERT INTO FUNCTION url('http://127.0.0.1:8123/?query=INSERT+INTO+test_table+FORMAT+CSV', 'CSV', 'column1 String, column2 UInt32') VALUES ('http interface', 42);
SELECT * FROM test_table;
```

<div id="scheme-dispatch">
  ## URL スキームによるディスパッチ
</div>

`url` 関数は、他のファイルおよびオブジェクトストレージのテーブル関数をまとめて扱う統一的なラッパーとして機能し、URL スキームに基づいて適切なバックエンドにディスパッチします。これにより、サポートされている任意の場所から、単一の統一された構文で読み取ることができます。

| Scheme                                        | Dispatches to                                |
| --------------------------------------------- | -------------------------------------------- |
| `http`, `https` (and any unrecognized scheme) | `URL` engine 自体 (HTTP `GET`/`POST`)          |
| `file`                                        | [`file`](file.md) 関数                         |
| `s3`, `gs`, `gcs`, `oss`                      | [`s3`](s3.md) 関数                             |
| `az`, `azure`, `abfss`, `abfs`                | [`azureBlobStorage`](azureBlobStorage.md) 関数 |
| `hdfs`                                        | [`hdfs`](hdfs.md) 関数                         |

ディスパッチされるのは、S3 URI mapper が追加設定なしで具体的なエンドポイントに解決できる S3 スキーム (`s3`、および `gs`/`gcs`/`oss`) のみです。その他の S3-compatible ベンダーのスキーム (`cos`, `obs`, `eos`, …) は地域固有で、デフォルトのエンドポイントマッピングがありません。そのため、`cos://…` URL は認識されないスキームとして扱われ、エラーとして報告されます。これらのバックエンドでは、[`s3`](s3.md) 関数を直接使用してください (`url_scheme_mappers` を設定したうえで) 。

`file://` では、相対パス (`file://data.csv`) は [user&#95;files](/ja/operations/server-configuration-parameters/settings#user_files_path) ディレクトリ内で解決され、絶対パス (`file:///home/user/data.csv`) は通常どおりその内部を指している必要があります。

`format`、`structure`、`compression_method` 引数と [url&#95;base](#resolving-relative-urls) 設定は、ディスパッチ先にかかわらず同じように機能します。

```sql
SELECT * FROM url('file://data.csv', CSV, 'a UInt32, b String');
SELECT * FROM url('s3://clickhouse-public-datasets/hits_compatible/hits.csv');
```

スキームの振り分けは、まだ [`urlCluster`](urlCluster.md) では機能しません。`urlCluster` に `http(s)` 以外のスキームを渡すと、エラーとして拒否されます。代わりに、それらのバックエンドには対応するクラスター関数 (`s3Cluster`、`azureBlobStorageCluster`、`hdfsCluster`、…) を使用してください。

<div id="globs-in-url">
  ## URL 内の glob
</div>

`{ }` 内のパターンは、一連の分片を生成したり、フェイルオーバーアドレスを指定したりするために使用されます。サポートされているパターンの種類と例については、[remote](remote.md#globs-in-addresses) 関数の説明を参照してください。
パターン内の文字 `|` は、フェイルオーバーアドレスを指定するために使用されます。これらは、パターン内に記載された順序で順に試行されます。生成されるアドレス数は、[glob&#95;expansion&#95;max&#95;elements](../../operations/settings/settings.md#glob_expansion_max_elements) 設定によって制限されます。
URL パス内の path glob 構文 (`*`、`{a,b}`、`{N..M}`、`**` など) については、[Globs in path](file.md#globs-in-path) を参照してください。`?` は URL ではクエリ文字列の開始を示すため、path 部分ではワイルドカードとして使用できない点に注意してください。

<div id="wildcards-with-http-index-pages">
  ## HTTPインデックスページでのワイルドカード
</div>

`url` および `URL` テーブルエンジンでは、ClickHouse は HTTP インデックスページ (HTML または平文) を取得し、レスポンスボディから URL を抽出することで、ワイルドカードを展開できます。これにより、サーバーがディレクトリ一覧を公開している場合に、`/**/` のようなパターンを使用できます。

注意:

* 相対 URL は、インデックスページの URL を基準に解決されます。
* `URL` テンプレートは、インデックスページを取得する前に展開されます。これには、カンマ区切りおよび数値範囲の分片展開と、パス部分の外側にある `|` フェイルオーバーオプションが含まれます。
* パス部分の内側にある `|` フェイルオーバーパターンは、HTTP インデックスページ展開ではサポートされていません。
* ワイルドカードマッチングは、URL のパス部分に適用されます。
* 一覧に含まれる URL にクエリ文字列またはフラグメントがすでに含まれている場合は、ソース URL のものよりそちらが優先されます。含まれていない場合は、ソース URL のクエリ文字列とフラグメントが使用されます。
* 空の一覧も許可されます。インデックスページに対する HTTP エラー (例: 404) は例外を発生させます。
* インデックスページの最大サイズは、[max&#95;http&#95;index&#95;page&#95;size](/ja/operations/server-configuration-parameters/settings.md#max_http_index_page_size) によって制限されます。
* 再帰的な展開時に読み取るディレクトリの最大数は、[url&#95;wildcard&#95;max&#95;directories&#95;to&#95;read](/ja/operations/settings/settings.md#url_wildcard_max_directories_to_read) によって制限されます。

例:

```sql
SELECT count()
FROM url('https://ftp.gnu.org/gnu/wget/wget-1.21*.tar.gz', 'RawBLOB')
SETTINGS max_threads = 1, allow_experimental_url_wildcard_from_index_pages = 1;
```

<div id="virtual-columns">
  ## 仮想カラム
</div>

* `_path` — `URL` のパス。型: `LowCardinality(String)`。
* `_file` — `URL` のリソース名。型: `LowCardinality(String)`。
* `_size` — リソースのサイズ (バイト単位) 。型: `Nullable(UInt64)`。サイズが不明な場合、値は `NULL` です。
* `_time` — ファイルの最終更新時刻。型: `Nullable(DateTime)`。時刻が不明な場合、値は `NULL` です。
* `_headers` - HTTP レスポンスヘッダー。型: `Map(LowCardinality(String), LowCardinality(String))`。

<div id="hive-style-partitioning">
  ## `use_hive_partitioning` 設定
</div>

`use_hive_partitioning` を 1 に設定すると、ClickHouse はパス (`/name=value/`) 内の Hive-style partitioning を検出し、クエリでパーティションカラムを仮想カラムとして使用できるようになります。これらの仮想カラムには、パーティション化されたパス内と同じ名前が付けられます。

**例**

Hive-style partitioning で作成された仮想カラムを使用します

```sql
SELECT * FROM url('http://data/path/date=*/country=*/code=*/*.parquet') WHERE date > '2020-01-01' AND country = 'Netherlands' AND code = 42;
```

<div id="resolving-relative-urls">
  ## 相対URLの解決
</div>

[url&#95;base](/ja/operations/settings/settings.md#url_base) 設定では、`url` 関数に相対URLを渡せます。`url_base` が設定されていて、関数の引数が相対参照である場合は、[RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986) に従ってベースURLに対して解決されます。

解決ルールは次のとおりです。

* **パス相対** (例: `data.csv`) : ベースURLのパスとマージされ、ベースパスの最後の `/` より後ろはすべて置き換えられます。末尾のスラッシュの有無は重要です。`https://example.com/dir/` + `data.csv` は `https://example.com/dir/data.csv` になりますが、`https://example.com/dir` + `data.csv` は `https://example.com/data.csv` になります。ドットセグメント (`./` および `../`) は正規化されます。
* **ホスト相対** (例: `/test/data.csv`) : ベースURLのスキームとホストを使って解決されます。
* **スキーム相対** (例: `//other.com/test/data.csv`) : ベースURLのスキームを使って解決されます。
* **クエリのみ** (例: `?x=1`) : ベースURLの完全なパスに追加され、既存のクエリまたはフラグメントは置き換えられます。
* **フラグメントのみ** (例: `#frag`) : クエリを保持したままベースURLに追加され、既存のフラグメントは置き換えられます。
* **空**: フラグメントを除いたベースURLを返します。
* **絶対URL**: 変更せずそのまま渡されます。`url_base` は無視されます。

**例**

```sql
SET url_base = 'https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/';
SELECT * FROM url('tests/queries/0_stateless/data_csv/data.csv', CSV) LIMIT 3;
```

<div id="storage-settings">
  ## ストレージ設定
</div>

* [engine&#95;url&#95;skip&#95;empty&#95;files](/ja/operations/settings/settings.md#engine_url_skip_empty_files) - 読み取り時に空のファイルをスキップできるようにします。デフォルトでは無効です。
* [enable&#95;url&#95;encoding](/ja/operations/settings/settings.md#enable_url_encoding) - URI 内のパスのデコード/エンコードを有効または無効にできるようにします。デフォルトで有効です。
* [url&#95;base](/ja/operations/settings/settings.md#url_base) - `url` 関数に渡される相対URLを解決するためのベースURLです。

<div id="permissions">
  ## 権限
</div>

`url` function には `CREATE TEMPORARY TABLE` 権限が必要です。そのため、[readonly](/ja/operations/settings/permissions-for-queries#readonly) = 1 に設定されたユーザーでは動作しません。少なくとも readonly = 2 が必要です。

<div id="related">
  ## 関連
</div>

* [仮想カラム](/ja/engines/table-engines/index.md#table_engines-virtual_columns)