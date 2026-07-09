---
description: 'リモートの HTTP/HTTPS サーバーとの間でデータをクエリします。このエンジンは
  File エンジンに似ています。'
sidebar_label: 'URL'
sidebar_position: 80
slug: /engines/table-engines/special/url
title: 'URL table engine'
doc_type: 'reference'
---

リモートの HTTP/HTTPS サーバーとの間でデータをクエリします。このエンジンは [File](../../../engines/table-engines/special/file.md) エンジンに似ています。

構文: `URL(URL [,Format] [,CompressionMethod])`

* `URL` パラメータは Uniform Resource Locator の構造に従っている必要があります。`http`/`https` URL (デフォルトのバックエンド) の場合は、HTTP または HTTPS を使用するサーバーを指している必要があり、サーバーから応答を取得するのに追加のヘッダーを必要としません。認識される非 HTTP スキーム (`file://`、`s3://`、`az://`、`hdfs://`、…) を持つ URL は、代わりに対応するエンジンに振り分けられます。詳細は後述の [URL スキームによる振り分け](#scheme-dispatch) を参照してください。

* `Format` は、ClickHouse が `SELECT` クエリで使用でき、必要に応じて `INSERT` でも使用できるものでなければなりません。対応フォーマットの完全な一覧については、[Formats](/ja/interfaces/formats#formats-overview) を参照してください。

  この引数が指定されていない場合、ClickHouse は `URL` パラメータの接尾辞からフォーマットを自動的に検出します。`URL` パラメータの接尾辞が対応フォーマットのいずれにも一致しない場合、テーブルの作成は失敗します。たとえば、エンジン式 `URL('http://localhost/test.json')` では、`JSON` フォーマットが適用されます。

* `CompressionMethod` は、HTTP ボディを圧縮するかどうかを示します。圧縮が有効になっている場合、URL エンジンによって送信される HTTP パケットには、使用されている圧縮方式を示す `Content-Encoding` ヘッダーが含まれます。

圧縮を有効にするには、まず `URL` パラメータで指定されるリモート HTTP エンドポイントが対応する圧縮アルゴリズムをサポートしていることを確認してください。

サポートされる `CompressionMethod` は、次のいずれかである必要があります。

* gzip または gz
* deflate
* brotli または br
* lzma または xz
* zstd または zst
* lz4
* bz2
* snappy
* none
* auto

`CompressionMethod` が指定されていない場合、デフォルトは `auto` です。これは、ClickHouse が `URL` パラメータの接尾辞から圧縮方式を自動的に検出することを意味します。接尾辞が上記の圧縮方式のいずれかに一致する場合は対応する圧縮が適用され、一致しない場合は圧縮は有効になりません。

たとえば、エンジン式 `URL('http://localhost/test.gzip')` では `gzip` 圧縮方式が適用されますが、`URL('http://localhost/test.fr')` では接尾辞 `fr` が上記のどの圧縮方式にも一致しないため、圧縮は有効になりません。

<div id="scheme-dispatch">
  ## URLスキームによるディスパッチ
</div>

`URL` エンジンは、他のファイルストレージおよびオブジェクトストレージのエンジンを統一的に扱うラッパーです。URL スキームに基づいて適切なバックエンドにディスパッチします。`http`/`https` (および認識されないスキーム) は `URL` エンジン自身で処理され、`file://` は [File](../../../engines/table-engines/special/file.md) エンジン、`s3://`、`gs://`、`gcs://`、`oss://` は [S3](/ja/engines/table-engines/integrations/s3) エンジン、`az://`、`azure://`、`abfss://`、`abfs://` は [AzureBlobStorage](/ja/engines/table-engines/integrations/azureBlobStorage) エンジン、`hdfs://` は [HDFS](/ja/engines/table-engines/integrations/hdfs) エンジンで処理されます。

ディスパッチされるのは、S3 URI mapper が追加設定なしで具体的なエンドポイントに解決できる S3 スキーム (`s3`、および `gs`/`gcs`/`oss`) だけです。その他の S3-compatible ベンダーのスキーム (`cos`、`obs`、`eos`、…) は Region 固有で、デフォルトのエンドポイントマッピングがないため、そのような URL を `URL` エンジンに渡すと認識されないスキームとして扱われ、エラーとして報告されます。これらのバックエンドについては、[S3](/ja/engines/table-engines/integrations/s3) エンジンを直接使用してください (`url_scheme_mappers` を設定) 。

[url&#95;base](/ja/operations/settings/settings.md#url_base) 設定はスキームのディスパッチ前に適用されるため、相対参照はまず base に対して解決され、その後、対応するエンジンにルーティングされます。

```sql
CREATE TABLE file_via_url (a UInt32, b String) ENGINE = URL('file://data.csv', CSV);
CREATE TABLE s3_via_url (a UInt32, b String) ENGINE = URL('s3://bucket/key.csv', CSV);
```

<div id="using-the-engine-in-the-clickhouse-server">
  ## 使用法
</div>

`INSERT` クエリと `SELECT` クエリは、それぞれ `POST` リクエストと `GET` リクエストに変換されます。
`POST` リクエストを処理するには、接続先サーバーが
[Chunked transfer encoding](https://en.wikipedia.org/wiki/Chunked_transfer_encoding) に対応している必要があります。

[max&#95;http&#95;get&#95;redirects](/ja/operations/settings/settings#max_http_get_redirects) 設定を使うと、HTTP GET リダイレクトの最大ホップ数を制限できます。

<div id="wildcards-with-http-index-pages">
  ## HTTPインデックスページでのワイルドカード
</div>

[allow&#95;experimental&#95;url&#95;wildcard&#95;from&#95;index&#95;pages](/ja/operations/settings/settings.md#allow_experimental_url_wildcard_from_index_pages) が有効になっている場合、`URL` テーブルエンジンは HTTPインデックスページを取得してそこからリンクを抽出することで、ワイルドカードを展開できます。
これは [`url`](../../../sql-reference/table-functions/url.md#wildcards-with-http-index-pages) テーブル関数と同じ仕組みです。

展開は、取得する各インデックスページについては [max&#95;http&#95;index&#95;page&#95;size](/ja/operations/server-configuration-parameters/settings.md#max_http_index_page_size) によって、再帰的なディレクトリ走査については [url&#95;wildcard&#95;max&#95;directories&#95;to&#95;read](/ja/operations/settings/settings.md#url_wildcard_max_directories_to_read) によって制限されます。

<div id="example">
  ## 例
</div>

**1.** サーバー上で `url_engine_table` テーブルを作成します：

```sql
CREATE TABLE url_engine_table (word String, value UInt64)
ENGINE=URL('http://127.0.0.1:12345/', CSV)
```

**2.** 標準の Python 3 ツールを使って簡易 HTTP サーバーを作成し、
起動します:

```python3
from http.server import BaseHTTPRequestHandler, HTTPServer

class CSVHTTPServer(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/csv')
        self.end_headers()

        self.wfile.write(bytes('Hello,1\nWorld,2\n', "utf-8"))

if __name__ == "__main__":
    server_address = ('127.0.0.1', 12345)
    HTTPServer(server_address, CSVHTTPServer).serve_forever()
```

```bash
$ python3 server.py
```

**3.** データをリクエストします:

```sql
SELECT * FROM url_engine_table
```

```text
┌─word──┬─value─┐
│ Hello │     1 │
│ World │     2 │
└───────┴───────┘
```

<div id="details-of-implementation">
  ## 実装の詳細
</div>

* 読み取りと書き込みは並列実行できます
* サポートされていません:
  * `ALTER` および `SELECT...SAMPLE` 操作。
  * 索引。
  * レプリケーション。

<div id="virtual-columns">
  ## 仮想カラム
</div>

* `_path` — `URL` のパス。型: `LowCardinality(String)`。
* `_file` — `URL` のリソース名。型: `LowCardinality(String)`。
* `_size` — リソースのサイズ (バイト単位) 。型: `Nullable(UInt64)`。サイズが不明な場合、値は `NULL` です。
* `_time` — ファイルの最終更新時刻。型: `Nullable(DateTime)`。時刻が不明な場合、値は `NULL` です。
* `_headers` - HTTP レスポンスヘッダー。型: `Map(LowCardinality(String), LowCardinality(String))`。

<div id="resolving-relative-urls">
  ## 相対URLの解決
</div>

[url&#95;base](/ja/operations/settings/settings.md#url_base) 設定を使用すると、`URL` エンジンで相対URLを使用できます。`url_base` が設定されている場合、エンジンに渡された URL は [RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986) に従ってそれを基準に解決されます。解決ルールの詳細については、[url テーブル関数 docs](../../../sql-reference/table-functions/url.md#resolving-relative-urls) を参照してください。

**例**

```sql
SET url_base = 'http://127.0.0.1:12345/';
CREATE TABLE url_engine_table (word String, value UInt64) ENGINE = URL('hello.csv', CSV);
SELECT * FROM url_engine_table;
```

<div id="storage-settings">
  ## ストレージ設定
</div>

* [engine&#95;url&#95;skip&#95;empty&#95;files](/ja/operations/settings/settings.md#engine_url_skip_empty_files) - 読み取り時に空のファイルをスキップできるようにします。デフォルトでは無効です。
* [enable&#95;url&#95;encoding](/ja/operations/settings/settings.md#enable_url_encoding) - URI 内の path のデコード/エンコードを有効/無効にします。デフォルトで有効です。
* [url&#95;base](/ja/operations/settings/settings.md#url_base) - engine に渡される相対URLを解決するためのベースURLです。