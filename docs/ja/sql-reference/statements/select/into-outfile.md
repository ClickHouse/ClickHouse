---
slug: /ja/sql-reference/statements/select/into-outfile
sidebar_label: INTO OUTFILE
---

# INTO OUTFILE 節

`INTO OUTFILE` 節は、`SELECT` クエリの結果を**クライアント**側のファイルにリダイレクトします。

圧縮ファイルがサポートされています。圧縮タイプはファイル名の拡張子によって検出されます（デフォルトで `'auto'` モードが使用されます）。また、`COMPRESSION` 節で明示的に指定することもできます。特定の圧縮タイプに対する圧縮レベルは、`LEVEL` 節で指定できます。

**構文**

```sql
SELECT <expr_list> INTO OUTFILE file_name [AND STDOUT] [APPEND | TRUNCATE] [COMPRESSION type [LEVEL level]]
```

`file_name` および `type` は文字列リテラルです。サポートされている圧縮タイプは、`'none'`, `'gzip'`, `'deflate'`, `'br'`, `'xz'`, `'zstd'`, `'lz4'`, `'bz2'` です。

`level` は数値リテラルです。次の範囲の正の整数がサポートされています：`lz4` タイプの場合 `1-12`、`zstd` タイプの場合 `1-22`、その他の圧縮タイプの場合 `1-9`。

## 実装の詳細

- この機能は、[コマンドラインクライアント](../../../interfaces/cli.md)および[clickhouse-local](../../../operations/utilities/clickhouse-local.md)で使用可能です。そのため、[HTTP インターフェース](../../../interfaces/http.md)経由で送信されたクエリは失敗します。
- 同じファイル名のファイルが既に存在する場合、クエリは失敗します。
- デフォルトの[出力フォーマット](../../../interfaces/formats.md)は `TabSeparated` です（コマンドラインクライアントのバッチモードと同様）。変更するには [FORMAT](format.md) 節を使用します。
- クエリに `AND STDOUT` が記載されている場合、ファイルに書き込まれた出力は標準出力にも表示されます。圧縮が使用されている場合は、平文が標準出力に表示されます。
- クエリに `APPEND` が記載されている場合、出力は既存のファイルに追加されます。圧縮が使用されている場合、追加は使用できません。
- 既に存在するファイルに書き込む場合、`APPEND` または `TRUNCATE` を使用する必要があります。

**例**

次のクエリを [コマンドラインクライアント](../../../interfaces/cli.md)で実行します：

```bash
clickhouse-client --query="SELECT 1,'ABC' INTO OUTFILE 'select.gz' FORMAT CSV;"
zcat select.gz 
```

結果：

```text
1,"ABC"
```
