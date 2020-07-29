---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 41
toc_title: URL
---

# URL(URL,フォーマット) {#table_engines-url}

リモートhttp/httpsサーバー上のデータを管理します。 このエンジンは同様です
に [ファイル](file.md) エンジン。

## Clickhouseサーバーでのエンジンの使用 {#using-the-engine-in-the-clickhouse-server}

その `format` ClickHouseが使用できるものでなければなりません
`SELECT` クエリと、必要に応じて、 `INSERTs`. サポートさ
[形式](../../../interfaces/formats.md#formats).

その `URL` 統一リソースロケータの構造に準拠する必要があります。 指定されたURLはサーバーを指す必要があります
HTTPまたはHTTPSを使用します。 これには何も必要ありません
サーバーからの応答を取得するための追加のヘッダー。

`INSERT` と `SELECT` 質問への `POST` と `GET` リクエスト,
それぞれ。 処理のため `POST` ご要望遠隔のサーバをサポートする必要があ
[チャンク転送エンコード](https://en.wikipedia.org/wiki/Chunked_transfer_encoding).

HTTP GETリダイレクトホップの最大数を制限するには、次のコマンドを使用します [max\_http\_get\_redirects](../../../operations/settings/settings.md#setting-max_http_get_redirects) 設定。

**例えば:**

**1.** 作成する `url_engine_table` サーバー上の表 :

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

## 実装の詳細 {#details-of-implementation}

-   読み書きできる並列
-   サポートなし:
    -   `ALTER` と `SELECT...SAMPLE` オペレーション
    -   インデックス。
    -   複製だ

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/url/) <!--hide-->
