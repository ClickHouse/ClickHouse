---
description: '指定した
  クラスター内の多数のノードから、URL のファイルを並列に処理できます。'
sidebar_label: 'urlCluster'
sidebar_position: 201
slug: /sql-reference/table-functions/urlCluster
title: 'urlCluster'
doc_type: 'reference'
---

指定したクラスター内の多数のノードから、URL のファイルを並列に処理できます。イニシエーターでは、クラスター内のすべてのノードへの connection を作成し、URL の file path 内の asterisk を展開して、各ファイルを動的に割り当てます。worker node では、次に処理する task をイニシエーターに問い合わせて処理します。これを、すべての tasks が完了するまで繰り返します。

<div id="syntax">
  ## 構文
</div>

```sql
urlCluster(cluster_name, URL, format, structure)
```

<div id="arguments">
  ## 引数
</div>

| 引数             | 説明                                                                                                             |
| -------------- | -------------------------------------------------------------------------------------------------------------- |
| `cluster_name` | リモートおよびローカルサーバー用のアドレス群と接続パラメーター一式を構築するために使用されるクラスター名です。                                                        |
| `URL`          | `GET` リクエストを受け付ける HTTP または HTTPS のサーバーアドレスです。型: [String](../../sql-reference/data-types/string.md)。            |
| `format`       | データの [フォーマット](/ja/sql-reference/formats) です。型: [String](../../sql-reference/data-types/string.md)。                |
| `structure`    | `'UserID UInt64, Name String'` 形式のテーブル構造です。カラム名と型を決定します。型: [String](../../sql-reference/data-types/string.md)。 |

<div id="returned_value">
  ## 戻り値
</div>

指定されたフォーマットとテーブル構造を持ち、指定した `URL` のデータを含むテーブル。

<div id="examples">
  ## 例
</div>

[CSV](/ja/interfaces/formats/CSV)フォーマットで応答するHTTPサーバーから、`String` 型および [UInt32](../../sql-reference/data-types/int-uint.md) 型のカラムを含むテーブルの先頭3行を取得します。

1. 標準の Python 3 ツールを使用して簡単な HTTP サーバーを作成し、起動します:

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

```sql
SELECT * FROM urlCluster('cluster_simple','http://127.0.0.1:12345', CSV, 'column1 String, column2 UInt32')
```

<div id="globs-in-url">
  ## URL 内のグロブ
</div>

`{ }` 内のパターンは、分片のセットを生成したり、フェイルオーバー先のアドレスを指定したりするために使用されます。サポートされているパターンの種類と例については、[remote](remote.md#globs-in-addresses) 関数の説明を参照してください。
パターン内の文字 `|` は、フェイルオーバー先のアドレスを指定するために使用されます。これらのアドレスは、パターンに記載された順序どおりに順番に試行されます。生成されるアドレス数は、[glob&#95;expansion&#95;max&#95;elements](../../operations/settings/settings.md#glob_expansion_max_elements) 設定によって制限されます。

<div id="related">
  ## 関連
</div>

* [HDFS engine](/ja/engines/table-engines/integrations/hdfs)
* [URLテーブル関数](/ja/engines/table-engines/special/url)