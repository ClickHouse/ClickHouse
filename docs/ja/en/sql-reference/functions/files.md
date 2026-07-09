---
description: 'Files に関するドキュメント'
sidebar_label: 'Files'
slug: /sql-reference/functions/files
title: 'Files'
doc_type: 'reference'
---

<div id="file">
  ## file
</div>

ファイルを文字列として読み取り、そのデータを指定したカラムに読み込みます。ファイルの内容は解釈されません。

あわせて参照 テーブル関数 [file](../table-functions/file.md)。

**構文**

```sql
file(path[, default])
```

**引数**

* `path` — [user&#95;files&#95;path](../../operations/server-configuration-parameters/settings.md#user_files_path) からの相対パスで指定するファイルパスです。ワイルドカード `*`、`**`、`?`、`{abc,def}`、`{N..M}` をサポートします。ここで、`N` と `M` は数値、`'abc'` と `'def'` は文字列です。
* `default` — ファイルが存在しない、またはアクセスできない場合に返される値です。サポートされているデータ型: [String](../data-types/string.md) および [NULL](/ja/operations/settings/formats#input_format_null_as_default)。

**例**

a.txt と b.txt のファイルからデータを文字列としてテーブルに挿入する例:

```sql
INSERT INTO table SELECT file('a.txt'), file('b.txt');
```