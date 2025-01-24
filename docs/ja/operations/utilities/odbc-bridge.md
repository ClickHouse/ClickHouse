---
slug: /ja/operations/utilities/odbc-bridge
title: clickhouse-odbc-bridge
---

ODBCドライバーのプロキシとして機能するシンプルなHTTPサーバーです。主な動機は、ODBC実装で可能性のあるセグメンテーションフォルトや他のフォルトが原因で、clickhouse-serverプロセス全体をクラッシュさせることがあるためです。

このツールはHTTP経由で動作し、パイプ、共有メモリ、TCPでは動作しません。理由は以下の通りです：
- 実装が簡単であること
- デバッグがしやすいこと
- jdbc-bridgeも同様に実装することができること

## 使用方法

`clickhouse-server`は、このツールをODBCテーブル関数およびStorageODBCの内部で使用します。しかしながら、以下のパラメータをPOSTリクエストのURLで指定することにより、コマンドラインからスタンドアロンツールとして使用することもできます：
- `connection_string` -- ODBC接続文字列。
- `sample_block` -- ClickHouse NamesAndTypesList形式でのカラムの説明、名前はバッククォート、型は文字列として記述します。名前と型はスペースで区切り、行は改行で区切ります。
- `max_block_size` -- オプションのパラメータで、単一ブロックの最大サイズを設定します。クエリはポストボディで送信されます。レスポンスはRowBinary形式で返されます。

## 例:

```bash
$ clickhouse-odbc-bridge --http-port 9018 --daemon

$ curl -d "query=SELECT PageID, ImpID, AdType FROM Keys ORDER BY PageID, ImpID" --data-urlencode "connection_string=DSN=ClickHouse;DATABASE=stat" --data-urlencode "sample_block=columns format version: 1
3 columns:
\`PageID\` String
\`ImpID\` String
\`AdType\` String
"  "http://localhost:9018/" > result.txt

$ cat result.txt
12246623837185725195925621517
```

