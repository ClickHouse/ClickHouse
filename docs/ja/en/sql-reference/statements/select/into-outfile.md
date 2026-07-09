---
description: 'INTO OUTFILE 句のドキュメント'
sidebar_label: 'INTO OUTFILE'
slug: /sql-reference/statements/select/into-outfile
title: 'INTO OUTFILE 句'
doc_type: 'reference'
---

`INTO OUTFILE` 句は、`SELECT` クエリの結果を**クライアント**側のファイルにリダイレクトします。

圧縮ファイルもサポートされます。圧縮形式はファイル名の拡張子から判定されます (デフォルトではモード `'auto'` が使用されます) 。または、`COMPRESSION` 句で明示的に指定することもできます。特定の圧縮形式の圧縮レベルは、`LEVEL` 句で指定できます。

**構文**

```sql
SELECT <expr_list> INTO OUTFILE file_name [AND STDOUT] [APPEND | TRUNCATE] [COMPRESSION type [LEVEL level]]
```

`file_name` と `type` は文字列リテラルです。サポートされている圧縮タイプは次のとおりです: `'none'`, `'gzip'`, `'deflate'`, `'br'`, `'xz'`, `'zstd'`, `'lz4'`, `'bz2'`。

`level` は数値リテラルです。次の範囲の正の整数がサポートされています: `lz4` タイプでは `1-12`、`zstd` タイプでは `1-22`、その他の圧縮タイプでは `1-9` です。

<div id="implementation-details">
  ## 実装の詳細
</div>

* この機能は、[コマンドラインクライアント](../../../interfaces/client.md) と [clickhouse-local](../../../operations/utilities/clickhouse-local.md) で利用できます。そのため、[HTTPインターフェイス](/ja/interfaces/http) 経由で送信したクエリは失敗します。
* 同じファイル名のファイルがすでに存在する場合、クエリは失敗します。
* デフォルトの [出力フォーマット](../../../interfaces/formats.md) は `TabSeparated` です (コマンドラインクライアントのバッチモードと同様) 。変更するには [FORMAT](format.md) 句を使用します。
* クエリ内に `AND STDOUT` が指定されている場合、ファイルに書き込まれる出力は標準出力にも表示されます。圧縮と併用した場合は、平文が標準出力に表示されます。
* クエリ内に `APPEND` が指定されている場合、出力は既存のファイルに追記されます。圧縮を使用する場合、`APPEND` は使用できません。
* すでに存在するファイルに書き込む場合は、`APPEND` または `TRUNCATE` を使用する必要があります。

**例**

次のクエリを [コマンドラインクライアント](../../../interfaces/client.md) で実行します。

```bash title="Query"
clickhouse-client --query="SELECT 1,'ABC' INTO OUTFILE 'select.gz' FORMAT CSV;"
zcat select.gz 
```

```text title="Response"
1,"ABC"
```