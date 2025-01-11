---
slug: /ja/engines/table-engines/special/url
sidebar_position: 80
sidebar_label: URL
---

# URLテーブルエンジン

リモートHTTP/HTTPSサーバーとの間でデータをクエリします。このエンジンは、[File](../../../engines/table-engines/special/file.md)エンジンと似ています。

構文: `URL(URL [,Format] [,CompressionMethod])`

- `URL`パラメータは、Uniform Resource Locatorの構造に従う必要があります。指定されたURLはHTTPまたはHTTPSを使用するサーバーを指している必要があります。サーバーから応答を得るために追加のヘッダーは必要ありません。

- `Format`は、ClickHouseが`SELECT`クエリで使用でき、必要に応じて`INSERT`でも使用できる形式である必要があります。サポートされている形式の全リストについては、[Formats](../../../interfaces/formats.md#formats)を参照してください。

    この引数が指定されていない場合、ClickHouseは`URL`パラメータのサフィックスから自動的に形式を検出します。`URL`パラメータのサフィックスがサポートされている形式と一致しない場合、テーブルの作成に失敗します。例えば、エンジンの式`URL('http://localhost/test.json')`では、`JSON`形式が適用されます。

- `CompressionMethod`は、HTTPボディを圧縮するかどうかを示します。圧縮が有効になっている場合、URLエンジンによって送信されるHTTPパケットには、使用される圧縮方式を示す'Content-Encoding'ヘッダーが含まれます。

圧縮を有効にするには、まず`URL`パラメータで示されるリモートHTTPエンドポイントが対応する圧縮アルゴリズムをサポートしていることを確認してください。

サポートされている`CompressionMethod`は以下のいずれかです:
- gzip または gz
- deflate
- brotli または br
- lzma または xz
- zstd または zst
- lz4
- bz2
- snappy
- none
- auto

`CompressionMethod`が指定されていない場合、デフォルトでは`auto`になります。これは、ClickHouseが`URL`パラメータのサフィックスから自動的に圧縮方式を検出することを意味します。サフィックスが上記の圧縮方式のいずれかと一致する場合、対応する圧縮が適用され、そうでない場合は圧縮は適用されません。

例えば、エンジンの式`URL('http://localhost/test.gzip')`では、`gzip`圧縮方式が適用されますが、`URL('http://localhost/test.fr')`では、サフィックス`fr`が上記の圧縮方式に一致しないため、圧縮は有効化されません。

## 使用例 {#using-the-engine-in-the-clickhouse-server}

`INSERT`および`SELECT`クエリはそれぞれ`POST`および`GET`リクエストに変換されます。`POST`リクエストを処理するには、リモートサーバーが[チャンク転送エンコーディング](https://en.wikipedia.org/wiki/Chunked_transfer_encoding)をサポートしている必要があります。

HTTP GETリダイレクトホップの最大数を制限するには、[max_http_get_redirects](../../../operations/settings/settings.md#setting-max_http_get_redirects)設定を使用できます。

## 例 {#example}

**1.** サーバー上で`url_engine_table`テーブルを作成します:

``` sql
CREATE TABLE url_engine_table (word String, value UInt64)
ENGINE=URL('http://127.0.0.1:12345/', CSV)
```

**2.** 標準のPython 3ツールを使用して基本的なHTTPサーバーを作成し、起動します:

``` python3
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

``` bash
$ python3 server.py
```

**3.** データをリクエストします:

``` sql
SELECT * FROM url_engine_table
```

``` text
┌─word──┬─value─┐
│ Hello │     1 │
│ World │     2 │
└───────┴───────┘
```

## 実装の詳細 {#details-of-implementation}

- 読み取りと書き込みは並行して行うことができます
- サポートされていない機能:
    - `ALTER`および`SELECT...SAMPLE`操作。
    - インデックス。
    - レプリケーション。

## PARTITION BY

`PARTITION BY` — 任意です。パーティションキーに基づいてデータを分割して、別々のファイルを作成することができます。ほとんどの場合、パーティションキーは不要であり、必要な場合でも一般的に月ごと以上に細かいパーティションキーは不要です。パーティション分割はクエリを高速化しません（ORDER BY式とは対照的です）。過度に細かいパーティション分割は避けてください。クライアントの識別子や名前でデータをパーティション分割しないでください（代わりに、クライアント識別子や名前をORDER BY式の最初のカラムにしてください）。

月ごとにパーティション分割するには、`toYYYYMM(date_column)`式を使用します。ここで`date_column`は[Date](/docs/ja/sql-reference/data-types/date.md)型の日付を持つカラムです。パーティション名は`"YYYYMM"`形式になります。

## 仮想カラム {#virtual-columns}

- `_path` — `URL`へのパスです。型: `LowCardinalty(String)`。
- `_file` — `URL`のリソース名です。型: `LowCardinalty(String)`。
- `_size` — リソースのバイト単位のサイズです。型: `Nullable(UInt64)`。サイズが不明な場合、値は`NULL`です。
- `_time` — ファイルの最終更新時間です。型: `Nullable(DateTime)`。時間が不明な場合、値は`NULL`です。
- `_headers` - HTTP応答ヘッダーです。型: `Map(LowCardinality(String), LowCardinality(String))`。

## ストレージ設定 {#storage-settings}

- [engine_url_skip_empty_files](/docs/ja/operations/settings/settings.md#engine_url_skip_empty_files) - 読み込み時に空のファイルをスキップすることを許可します。デフォルトでは無効です。
- [enable_url_encoding](/docs/ja/operations/settings/settings.md#enable_url_encoding) - URIのパスのデコード/エンコードを有効/無効にすることを許可します。デフォルトで有効です。
