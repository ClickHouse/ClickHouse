---
description: 'Odbc Bridge のドキュメント'
slug: /operations/utilities/odbc-bridge
title: 'clickhouse-odbc-bridge'
doc_type: 'reference'
---

ODBC ドライバのプロキシのように動作するシンプルな HTTP サーバーです。主な理由は、
ODBC 実装で segfault やその他の不具合が発生し、
clickhouse-server プロセス全体がクラッシュする可能性があるためです。

このツールがパイプ、共有メモリ、TCP ではなく HTTP 経由で動作するのは、次の理由によります。

* 実装が簡単だからです
* デバッグが簡単だからです
* jdbc-bridge も同じ方法で実装できるからです

<div id="usage">
  ## 使用方法
</div>

`clickhouse-server` は、このツールを odbc table function および StorageODBC 内部で使用します。
ただし、コマンドラインからスタンドアロンツールとして使用することもでき、その場合は
POSTリクエストのURLに以下のパラメータを指定します。

* `connection_string` -- ODBC 接続文字列。
* `sample_block` -- ClickHouse の NamesAndTypesList フォーマットによるカラムの説明。名前はバッククォートで囲み、
  型は文字列として指定します。名前と型はスペース区切り、行は
  改行区切りです。
* `max_block_size` -- 任意のパラメータ。単一ブロックの最大サイズを設定します。
  クエリはPOSTボディで送信されます。レスポンスは RowBinary フォーマットで返されます。

<div id="example">
  ## 例:
</div>

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