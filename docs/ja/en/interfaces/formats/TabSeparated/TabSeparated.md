---
alias: ['TSV']
description: 'TSVフォーマットのドキュメント'
input_format: true
keywords: ['TabSeparated', 'TSV']
output_format: true
slug: /interfaces/formats/TabSeparated
title: 'TabSeparated'
doc_type: 'reference'
---

| 入力 | 出力 | 別名    |
| -- | -- | ----- |
| ✔  | ✔  | `TSV` |

<div id="description">
  ## 説明
</div>

TabSeparatedフォーマットでは、データは行ごとに書き込まれます。各行には、タブで区切られた値が含まれます。各値の後にはタブが続きますが、行内の最後の値の後には改行が続きます。すべての箇所で、厳密に Unix の改行が使われるものとみなされます。最後の行の末尾にも改行が必要です。値は引用符で囲まずにテキストフォーマットで書き込まれ、特殊文字はエスケープされます。

このフォーマットは、`TSV` という名前でも利用できます。

`TabSeparated` フォーマットは、独自のプログラムやスクリプトを使ってデータを処理するのに便利です。これは、HTTP インターフェイスおよびコマンドラインクライアントのバッチモードでデフォルトで使用されます。また、このフォーマットを使うと、異なるDBMS間でデータを転送できます。たとえば、MySQL からダンプを取得して ClickHouse にアップロードすることも、その逆を行うこともできます。

`TabSeparated` フォーマットは、合計値 (WITH TOTALS を使用する場合) と極値 (`extremes` が 1 に設定されている場合) の出力をサポートしています。これらの場合、合計値と極値はメインデータの後に出力されます。メインの結果、合計値、極値は、それぞれ空行で区切られます。例:

```sql
SELECT EventDate, count() AS c FROM test.hits GROUP BY EventDate WITH TOTALS ORDER BY EventDate FORMAT TabSeparated

2014-03-17      1406958
2014-03-18      1383658
2014-03-19      1405797
2014-03-20      1353623
2014-03-21      1245779
2014-03-22      1031592
2014-03-23      1046491

1970-01-01      8873898

2014-03-17      1031592
2014-03-23      1406958
```

<div id="tabseparated-data-formatting">
  ## データのフォーマット
</div>

整数は十進表記で記述されます。数値の先頭には追加の `+` 文字を付けることができます (パース時には無視され、フォーマット時には記録されません) 。非負の数値に負号を含めることはできません。読み取り時には、空文字列を 0 としてパースすること、または (符号付き型では) マイナス記号だけから成る文字列を 0 としてパースすることが許可されています。対応するデータ型に収まらない数値は、エラーメッセージなしで別の数値としてパースされる場合があります。

浮動小数点数は十進表記で記述されます。小数点の区切りにはドットが使用されます。指数表記に対応しており、`inf`、`+inf`、`-inf`、`nan` もサポートされます。浮動小数点数の値は、小数点で始まることも終わることもできます。
フォーマット時には、浮動小数点数で精度が失われる場合があります。
パース時には、機械上で表現可能な最も近い数値を厳密に読み取る必要はありません。

Date は YYYY-MM-DD フォーマットで記述され、同じフォーマットでパースされますが、区切り文字には任意の文字を使用できます。
時刻を含む Date は `YYYY-MM-DD hh:mm:ss` フォーマットで記述され、同じフォーマットでパースされますが、区切り文字には任意の文字を使用できます。
これらはすべて、client または server の起動時点のシステムタイムゾーンで行われます (どちらがデータをフォーマットするかによって異なります) 。時刻を含む Date については、夏時間は規定されていません。そのため、dump に夏時間中の時刻が含まれている場合、その dump はデータと一義的に対応せず、パース時には 2 つの時刻のいずれかが選択されます。
読み取り時には、不正な Date や時刻を含む Date が、自然なオーバーフローとして、または null の Date や時刻として、エラーメッセージなしでパースされることがあります。

例外として、時刻を含む Date のパースでは、ちょうど 10 桁の十進数から成る場合に限り、Unix timestamp フォーマットもサポートされます。結果はタイムゾーンに依存しません。フォーマット `YYYY-MM-DD hh:mm:ss` と `NNNNNNNNNN` は自動的に区別されます。

String は、特殊文字をバックスラッシュでエスケープして出力されます。出力には次のエスケープシーケンスが使用されます: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\'`, `\\`。パースでは、`\a`、`\v`、`\xHH` (16 進エスケープシーケンス) 、および任意の `\c` シーケンス (ここで `c` は任意の文字で、これらのシーケンスは `c` に変換されます) もサポートされます。したがって、データの読み取りでは、改行を `\n`、`\`、または改行 自体として記述できるフォーマットをサポートします。たとえば、文字列 `Hello world` の単語間のスペースを 改行 に置き換えたものは、次のいずれのバリエーションでもパースできます。

```text
Hello\nworld

Hello\
world
```

2 番目のバリアントがサポートされているのは、MySQL がタブ区切りの dump を書き出す際にこれを使用するためです。

TabSeparated フォーマットでデータを渡す際にエスケープが必要な最小限の文字セットは、タブ、改行 (LF)、および backslash です。

エスケープされる記号はごく一部だけです。そのため、出力時に端末によって文字列値が簡単に壊されることがあります。

Arrays は `[]` 内にカンマ区切りの値のリストとして書き込まれます。配列内の Number 項目は通常どおりにフォーマットされます。`Date` および `DateTime` 型はシングルクォートで書き込まれます。String は、上記と同じエスケープ規則でシングルクォート付きで書き込まれます。

[NULL](/ja/sql-reference/syntax.md) は、設定 [format&#95;tsv&#95;null&#95;representation](/ja/operations/settings/settings-formats.md/#format_tsv_null_representation) に従ってフォーマットされます (デフォルト値は `\N` です) 。

入力データでは、ENUM の値は名前または id として表現できます。まず、入力値を ENUM 名に一致させようとします。失敗し、かつ入力値が数値である場合は、この数値を ENUM id に一致させようとします。
入力データに ENUM id しか含まれない場合は、ENUM のパースを最適化するために、設定 [input&#95;format&#95;tsv&#95;enum&#95;as&#95;number](/ja/operations/settings/settings-formats.md/#input_format_tsv_enum_as_number) を有効にすることを推奨します。

[Nested](/ja/sql-reference/data-types/nested-data-structures/index.md) 構造の各要素は配列として表現されます。

例:

```sql
CREATE TABLE nestedt
(
    `id` UInt8,
    `aux` Nested(
        a UInt8,
        b String
    )
)
ENGINE = TinyLog
```

```sql
INSERT INTO nestedt VALUES ( 1, [1], ['a'])
```

```sql
SELECT * FROM nestedt FORMAT TSV
```

```response
1  [1]    ['a']
```

<div id="example-usage">
  ## 使用例
</div>

<div id="inserting-data">
  ### データの挿入
</div>

`football.tsv` という名前の次のTSVファイルを使用します。

```tsv
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

データを挿入します:

```sql
INSERT INTO football FROM INFILE 'football.tsv' FORMAT TabSeparated;
```

<div id="reading-data">
  ### データの読み取り
</div>

`TabSeparated` フォーマットを使用してデータを読み取ります：

```sql
SELECT *
FROM football
FORMAT TabSeparated
```

出力はタブ区切り形式です:

```tsv
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

<div id="format-settings">
  ## フォーマット設定
</div>

| 設定                                                                                                                                                       | 説明                                                                                                                                                                                                          | デフォルト   |
| -------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| [`format_tsv_null_representation`](/ja/operations/settings/settings-formats.md/#format_tsv_null_representation)                                             | TSVフォーマットでのカスタムNULL表現。                                                                                                                                                                                      | `\N`    |
| [`input_format_tsv_empty_as_default`](/ja/operations/settings/settings-formats.md/#input_format_tsv_empty_as_default)                                       | TSV入力の空のフィールドをデフォルト値として扱います。複雑なデフォルト式の場合は、[input&#95;format&#95;defaults&#95;for&#95;omitted&#95;fields](/ja/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields) も有効にする必要があります。 | `false` |
| [`input_format_tsv_enum_as_number`](/ja/operations/settings/settings-formats.md/#input_format_tsv_enum_as_number)                                           | TSVフォーマットで挿入される enum 値を enum のインデックスとして扱います。                                                                                                                                                                | `false` |
| [`input_format_tsv_use_best_effort_in_schema_inference`](/ja/operations/settings/settings-formats.md/#input_format_tsv_use_best_effort_in_schema_inference) | TSVフォーマットでスキーマを推論する際に、各種調整やヒューリスティクスを使用します。無効な場合、すべてのフィールドは String として推論されます。                                                                                                                               | `true`  |
| [`output_format_tsv_crlf_end_of_line`](/ja/operations/settings/settings-formats.md/#output_format_tsv_crlf_end_of_line)                                     | `true` に設定すると、TSV出力フォーマットの行末は `\n` ではなく `\r\n` になります。                                                                                                                                                       | `false` |
| [`input_format_tsv_crlf_end_of_line`](/ja/operations/settings/settings-formats.md/#input_format_tsv_crlf_end_of_line)                                       | `true` に設定すると、TSV入力フォーマットの行末は `\n` ではなく `\r\n` になります。                                                                                                                                                       | `false` |
| [`input_format_tsv_skip_first_lines`](/ja/operations/settings/settings-formats.md/#input_format_tsv_skip_first_lines)                                       | データの先頭で指定した行数をスキップします。                                                                                                                                                                                      | `0`     |
| [`input_format_tsv_detect_header`](/ja/operations/settings/settings-formats.md/#input_format_tsv_detect_header)                                             | TSVフォーマット内の名前と型を含むヘッダーを自動検出します。                                                                                                                                                                             | `true`  |
| [`input_format_tsv_skip_trailing_empty_lines`](/ja/operations/settings/settings-formats.md/#input_format_tsv_skip_trailing_empty_lines)                     | データ末尾の空行をスキップします。                                                                                                                                                                                           | `false` |
| [`input_format_tsv_allow_variable_number_of_columns`](/ja/operations/settings/settings-formats.md/#input_format_tsv_allow_variable_number_of_columns)       | TSVフォーマットで可変数のカラムを許可し、余分なカラムは無視し、不足しているカラムにはデフォルト値を使用します。                                                                                                                                                   | `false` |