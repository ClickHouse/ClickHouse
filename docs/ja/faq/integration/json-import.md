---
slug: /ja/faq/integration/json-import
title: ClickHouseにJSONをインポートする方法
toc_hidden: true
toc_priority: 11
---

# ClickHouseにJSONをインポートする方法 {#how-to-import-json-into-clickhouse}

ClickHouseは、[入力および出力のためのデータ形式](../../interfaces/formats.md)を幅広くサポートしています。データ取り込みに最も一般的に使用されるJSONのバリエーションの1つは、[JSONEachRow](../../interfaces/formats.md#jsoneachrow)です。この形式では、1つのJSONオブジェクトが1行に対応し、各オブジェクトが改行で区切られます。

## 例 {#examples}

[HTTPインターフェース](../../interfaces/http.md)を使用する場合:

``` bash
$ echo '{"foo":"bar"}' | curl 'http://localhost:8123/?query=INSERT%20INTO%20test%20FORMAT%20JSONEachRow' --data-binary @-
```

[CLIインターフェース](../../interfaces/cli.md)を使用する場合:

``` bash
$ echo '{"foo":"bar"}'  | clickhouse-client --query="INSERT INTO test FORMAT JSONEachRow"
```

データを手動で挿入する代わりに、[インテグレーションツール](../../integrations/index.mdx)を使用することも検討してください。

## 便利な設定 {#useful-settings}

- `input_format_skip_unknown_fields` は、テーブルスキーマに存在しない追加のフィールドがあっても、それらを無視してJSONを挿入できるようにします。
- `input_format_import_nested_json` は、[Nested](../../sql-reference/data-types/nested-data-structures/index.md)型のカラムにネストされたJSONオブジェクトを挿入できるようにします。

:::note
設定は、HTTPインターフェースでは `GET` パラメータとして、`CLI` インターフェースでは `--` で始まる追加のコマンドライン引数として指定されます。
:::
