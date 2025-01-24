---
slug: /ja/sql-reference/functions/files
sidebar_position: 75
sidebar_label: Files
---

## file

ファイルを文字列として読み込み、指定されたカラムにデータをロードします。ファイルの内容は解釈されません。

テーブル関数 [file](../table-functions/file.md) も参照してください。

**構文**

``` sql
file(path[, default])
```

**引数**

- `path` — [user_files_path](../../operations/server-configuration-parameters/settings.md#user_files_path) に対するファイルの相対パス。ワイルドカード `*`, `**`, `?`, `{abc,def}` および `{N..M}` がサポートされており、`N` と `M` は数値、`'abc', 'def'` は文字列です。
- `default` — ファイルが存在しないかアクセスできない場合に返される値。サポートされるデータタイプは [String](../data-types/string.md) および [NULL](../../sql-reference/syntax.md#null-literal) です。

**例**

ファイル a.txt および b.txt のデータを文字列としてテーブルに挿入する例:

``` sql
INSERT INTO table SELECT file('a.txt'), file('b.txt');
```

