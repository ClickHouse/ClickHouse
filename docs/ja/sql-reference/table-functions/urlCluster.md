---
slug: /ja/sql-reference/table-functions/urlCluster
sidebar_position: 201
sidebar_label: urlCluster
---

# urlCluster テーブル関数

URL からのファイルを指定したクラスタの多くのノードから並行して処理することを可能にします。イニシエータではすべてのクラスタ内のノードに接続を作成し、URLファイルパス内のアスタリスクを公開し、各ファイルを動的に分配します。ワーカーノードではイニシエータに次の処理タスクを問い合わせ、それを処理します。この処理はすべてのタスクが完了するまで繰り返されます。

**構文**

``` sql
urlCluster(cluster_name, URL, format, structure)
```

**引数**

-   `cluster_name` — リモートおよびローカルサーバーへのアドレスセットと接続パラメータを構築するために使用されるクラスタの名前。
- `URL` — `GET`リクエストを受け入れることができる HTTP または HTTPS サーバーアドレス。型: [String](../../sql-reference/data-types/string.md)。
- `format` — データの[フォーマット](../../interfaces/formats.md#formats)。型: [String](../../sql-reference/data-types/string.md)。
- `structure` — `'UserID UInt64, Name String'`形式のテーブル構造。カラム名とタイプを決定します。型: [String](../../sql-reference/data-types/string.md)。

**返される値**

定義された`URL`からのデータを指定されたフォーマットと構造で持つテーブル。

**例**

HTTP サーバーが [CSV](../../interfaces/formats.md#csv)形式で応答する`String`と[UInt32](../../sql-reference/data-types/int-uint.md) 型のカラムを含むテーブルの最初の3行を取得します。

1. 標準の Python 3 ツールを使用して基本的な HTTP サーバーを作成し、起動します:

```python
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

``` sql
SELECT * FROM urlCluster('cluster_simple','http://127.0.0.1:12345', CSV, 'column1 String, column2 UInt32')
```

## URL のグロブ

中括弧 `{ }` 内のパターンはシャードのセットを生成するか、フェイルオーバーアドレスを指定するために使用されます。サポートされているパターンタイプと例については、[remote](remote.md#globs-in-addresses) 関数の説明を参照してください。
パターン内の文字 `|` はフェイルオーバーアドレスを指定するために使用されます。それらはパターンにリストされた順で反復されます。生成されるアドレスの数は [glob_expansion_max_elements](../../operations/settings/settings.md#glob_expansion_max_elements) 設定によって制限されます。

**関連項目**

-   [HDFS エンジン](../../engines/table-engines/special/url.md)
-   [URL テーブル関数](../../sql-reference/table-functions/url.md)
