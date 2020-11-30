---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 41
toc_title: URL
---

# URL(URL,形式) {#table_engines-url}

リモートHTTP/HTTPSサーバー上のデータを管理します。 このエンジンは同様です
に [ファイル](file.md) エンジン

## ClickHouseサーバーでのエンジンの使用 {#using-the-engine-in-the-clickhouse-server}

その `format` ClickHouseが使用できるものでなければなりません
`SELECT` クエリと、必要に応じて、 `INSERTs`. サポートされている形式の完全な一覧については、
[形式](../../../interfaces/formats.md#formats).

その `URL` Uniform Resource Locatorの構造に準拠している必要があります。 指定したURLはサーバーを指す必要があります
HTTPまたはHTTPSを使用します。 これは必要ありません
サーバーからの応答を取得するための追加ヘッダー。

`INSERT` と `SELECT` 質問への `POST` と `GET` 要求,
それぞれ。 処理のため `POST` 要求は、リモートサーバーが
[チャンク転送エンコード](https://en.wikipedia.org/wiki/Chunked_transfer_encoding).

HTTP GETリダイレクトホップの最大数を制限するには [max\_http\_get\_redirects](../../../operations/settings/settings.md#setting-max_http_get_redirects) 設定。

**例:**

**1.** 作成 `url_engine_table` サーバー上のテーブル :

``` sql
CREATE TABLE url_engine_table (word String, value UInt64)
ENGINE=URL('http://127.0.0.1:12345/', CSV)
```

**2.** 標準のPython3ツールを使用して基本的なHTTPサーバーを作成し、
それを開始:

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

**3.** 要求データ:

``` sql
SELECT * FROM url_engine_table
```

``` text
┌─word──┬─value─┐
│ Hello │     1 │
│ World │     2 │
└───────┴───────┘
```

## 実施内容 {#details-of-implementation}

-   読み書きできる並列
-   対応していません:
    -   `ALTER` と `SELECT...SAMPLE` 作戦だ
    -   インデックス。
    -   複製。

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/url/) <!--hide-->
