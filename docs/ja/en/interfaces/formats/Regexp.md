---
alias: []
description: 'Regexp フォーマットのドキュメント'
input_format: true
keywords: ['Regexp']
output_format: false
slug: /interfaces/formats/Regexp
title: 'Regexp'
doc_type: 'reference'
---

| 入力 | 出力 | エイリアス |
| -- | -- | ----- |
| ✔  | ✗  |       |

<div id="description">
  ## 説明
</div>

`Regex` フォーマットは、指定された正規表現に基づいて、インポートされたデータの各行を解析します。

**使用法**

[format&#95;regexp](/ja/operations/settings/settings-formats.md/#format_regexp) 設定で指定した正規表現が、インポートされたデータの各行に適用されます。正規表現内の部分パターンの数は、インポートするデータセットのカラム数と一致している必要があります。

インポートされたデータの各行は、改行文字 `'\n'` または DOS 形式の改行 `"\r\n"` で区切る必要があります。

一致した各部分パターンの内容は、[format&#95;regexp&#95;escaping&#95;rule](/ja/operations/settings/settings-formats.md/#format_regexp_escaping_rule) 設定に従って、対応するデータ型の解析方法で解析されます。

正規表現がその行に一致せず、かつ [format&#95;regexp&#95;skip&#95;unmatched](/ja/operations/settings/settings-formats.md/#format_regexp_escaping_rule) が 1 に設定されている場合、その行は何も出力せずにスキップされます。そうでない場合は、例外がスローされます。

<div id="example-usage">
  ## 使用例
</div>

次の `data.tsv` ファイルを考えます:

```text title="data.tsv"
id: 1 array: [1,2,3] string: str1 date: 2020-01-01
id: 2 array: [1,2,3] string: str2 date: 2020-01-02
id: 3 array: [1,2,3] string: str3 date: 2020-01-03
```

および、テーブル `imp_regex_table`:

```sql title="Query"
CREATE TABLE imp_regex_table (id UInt32, array Array(UInt32), string String, date Date) ENGINE = Memory;
```

前述のファイルのデータを、次のクエリを使用して上記のテーブルに挿入します：

```bash title="Query"
$ cat data.tsv | clickhouse-client  --query "INSERT INTO imp_regex_table SETTINGS format_regexp='id: (.+?) array: (.+?) string: (.+?) date: (.+?)', format_regexp_escaping_rule='Escaped', format_regexp_skip_unmatched=0 FORMAT Regexp;"
```

これで、テーブルからデータを `SELECT` して、`Regex` フォーマットがファイル内のデータをどのようにパースしたかを確認できます:

```sql title="Query"
SELECT * FROM imp_regex_table;
```

```text title="Response"
┌─id─┬─array───┬─string─┬───────date─┐
│  1 │ [1,2,3] │ str1   │ 2020-01-01 │
│  2 │ [1,2,3] │ str2   │ 2020-01-02 │
│  3 │ [1,2,3] │ str3   │ 2020-01-03 │
└────┴─────────┴────────┴────────────┘
```

<div id="format-settings">
  ## フォーマット設定
</div>

`Regexp` フォーマットを使用する場合は、次の設定を使用できます。

* `format_regexp` — [String](/ja/sql-reference/data-types/string.md)。[re2](https://github.com/google/re2/wiki/Syntax) フォーマットの正規表現を指定します。

* `format_regexp_escaping_rule` — [String](/ja/sql-reference/data-types/string.md)。次のエスケープ規則をサポートしています。

  * CSV ([CSV](/ja/interfaces/formats/CSV) と同様)
  * JSON ([JSONEachRow](/ja/interfaces/formats/JSONEachRow) と同様)
  * Escaped ([TSV](/ja/interfaces/formats/TabSeparated) と同様)
  * Quoted ([Values](/ja/interfaces/formats/Values) と同様)
  * Raw (サブパターン全体をそのまま抽出し、エスケープ規則は適用されません。[TSVRaw](/ja/interfaces/formats/TabSeparated) と同様)

* `format_regexp_skip_unmatched` — [UInt8](/ja/sql-reference/data-types/int-uint.md)。`format_regexp` の式がインポートするデータに一致しない場合に例外をスローするかどうかを指定します。`0` または `1` に設定できます。