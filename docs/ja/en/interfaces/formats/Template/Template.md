---
alias: []
description: 'Template フォーマットのドキュメント'
input_format: true
keywords: ['Template']
output_format: true
slug: /interfaces/formats/Template
title: 'Template'
doc_type: 'guide'
---

| 入力 | 出力 | エイリアス |
| -- | -- | ----- |
| ✔  | ✔  |       |

<div id="description">
  ## 説明
</div>

他の標準フォーマットでは足りない、より細かなカスタマイズが必要な場合、
`Template` フォーマットでは、値用のプレースホルダーを含む独自のフォーマット文字列や、
データのエスケープ規則を指定できます。

このフォーマットでは、次の設定を使用します。

| Setting                                                                                                  | Description                                                                   |
| -------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| [`format_template_row`](#format_template_row)                                                            | 行のフォーマット文字列を含むファイルのパスを指定します。                                                  |
| [`format_template_resultset`](#format_template_resultset)                                                | 行のフォーマット文字列を含むファイルのパスを指定します                                                   |
| [`format_template_rows_between_delimiter`](#format_template_rows_between_delimiter)                      | 行間の区切り文字を指定します。これは最後の行を除くすべての行の後に出力される (または入力として期待される) ものです (デフォルトは `\n`)     |
| `format_template_row_format`                                                                             | 行のフォーマット文字列を[インライン](#inline_specification)で指定します。                             |
| `format_template_resultset_format`                                                                       | 結果セット のフォーマット文字列を[インライン](#inline_specification)で指定します。                   |
| Some settings of other formats (e.g.`output_format_json_quote_64bit_integers` when using `JSON` escaping | 他のフォーマットの一部の設定 (例: `JSON` エスケープ使用時の `output_format_json_quote_64bit_integers` |

<div id="settings-and-escaping-rules">
  ## 設定とエスケープルール
</div>

<div id="format_template_row">
  ### format_template_row
</div>

設定 `format_template_row` は、次の構文に従った行用のフォーマット文字列を含むファイルのパスを指定します。

```text
delimiter_1${column_1:serializeAs_1}delimiter_2${column_2:serializeAs_2} ... delimiter_N
```

各項目の意味は次のとおりです。

| 構文の要素           | 説明                                                     |
| --------------- | ------------------------------------------------------ |
| `delimiter_i`   | 値の間の区切り文字 (`$` 記号は `$$` としてエスケープできます)                  |
| `column_i`      | 選択または挿入する値に対応するカラムの名前またはインデックス (空の場合は、そのカラムはスキップされます)  |
| `serializeAs_i` | カラムの値に対するエスケープ規則                                       |

サポートされているエスケープ規則は次のとおりです。

| エスケープ規則              | 説明                    |
| -------------------- | --------------------- |
| `CSV`, `JSON`, `XML` | 同名のフォーマットと同様          |
| `Escaped`            | `TSV` と同様             |
| `Quoted`             | `Values` と同様          |
| `Raw`                | エスケープなしで、`TSVRaw` と同様 |
| `None`               | エスケープ規則なし。以下の注記を参照    |

:::note
エスケープ規則を省略した場合は、`None` が使用されます。`XML` は出力にのみ適しています。
:::

例を見てみましょう。次のフォーマット文字列があるとします。

```text
Search phrase: ${s:Quoted}, count: ${c:Escaped}, ad price: $$${p:JSON};
```

次の値は、それぞれカラム `Search phrase:`, `, count:`, `, ad price: $` と `;` の区切り文字に挟まれて、`SELECT` を使用する場合は出力され、`INPUT` を使用する場合は期待されます。

* `s` (エスケープ規則 `Quoted`) 
* `c` (エスケープ規則 `Escaped`) 
* `p` (エスケープ規則 `JSON`) 

例:

* `INSERT` する場合、以下の行は想定されるテンプレートに一致しており、値 `bathroom interior design`, `2166`, `$3` がカラム `Search phrase`, `count`, `ad price` に読み込まれます。
* `SELECT` する場合、値 `bathroom interior design`, `2166`, `$3` がすでにテーブルのカラム `Search phrase`, `count`, `ad price` に格納されているとすると、以下の行が出力されます。

```yaml
Search phrase: 'bathroom interior design', count: 2166, ad price: $3;
```

<div id="format_template_rows_between_delimiter">
  ### format_template_rows_between_delimiter
</div>

設定 `format_template_rows_between_delimiter` は、行間の区切り文字を指定します。これは、最後の行を除く各行の後に出力される (または入力として想定される) もので、デフォルトは `\n` です

<div id="format_template_resultset">
  ### format_template_resultset
</div>

設定 `format_template_resultset` は、結果セット のフォーマット文字列を含むファイルへのパスを指定します。

結果セット のフォーマット文字列は、行のフォーマット文字列と同じ構文です。
これにより、プレフィックス、接尾辞、追加情報の出力方法を指定でき、カラム名の代わりに次のプレースホルダーを使用できます。

* `data` は、`format_template_row` フォーマットのデータ行を `format_template_rows_between_delimiter` で区切ったものです。このプレースホルダーは、フォーマット文字列内の最初のプレースホルダーでなければなりません。
* `totals` は、`format_template_row` フォーマットの合計値の行です (WITH TOTALS 使用時) 。
* `min` は、`format_template_row` フォーマットの最小値の行です (extremes が 1 に設定されている場合) 。
* `max` は、`format_template_row` フォーマットの最大値の行です (extremes が 1 に設定されている場合) 。
* `rows` は、出力行の総数です。
* `rows_before_limit` は、LIMIT がなかった場合に存在していたはずの最小行数です。クエリに LIMIT が含まれる場合にのみ出力されます。クエリに GROUP BY が含まれる場合、rows&#95;before&#95;limit&#95;at&#95;least は LIMIT がなかった場合の正確な行数になります。
* `time` は、リクエストの実行時間 (秒) です。
* `rows_read` は、読み込まれた行数です。
* `bytes_read` は、読み込まれたバイト数 (非圧縮) です。

プレースホルダー `data`、`totals`、`min`、`max` にはエスケープルールを指定してはいけません (または `None` を明示的に指定する必要があります) 。それ以外のプレースホルダーには任意のエスケープルールを指定できます。

:::note
`format_template_resultset` 設定が空文字列の場合、デフォルト値として `${data}` が使用されます。
:::

INSERTクエリ用のフォーマットでは、プレフィックスまたは接尾辞があれば、一部のカラムまたはフィールドを省略できます (例を参照) 。

<div id="inline_specification">
  ### インライン指定
</div>

テンプレートフォーマットのフォーマット設定
 (`format_template_row`、`format_template_resultset` で指定) を、クラスター内のすべてのノード上のディレクトリにデプロイするのは、難しい場合や不可能な場合があります。
また、フォーマットが非常に単純で、ファイルに置く必要がないこともあります。

このような場合は、`format_template_row_format` (`format_template_row` 用) および `format_template_resultset_format` (`format_template_resultset` 用) を使うことで、テンプレート文字列を、その文字列を含むファイルへのパスとしてではなく、クエリ内に直接指定できます。

:::note
フォーマット文字列とエスケープシーケンスのルールは、以下の場合と同じです。

* `format_template_row_format` を使用する場合の [`format_template_row`](#format_template_row)
* `format_template_resultset_format` を使用する場合の [`format_template_resultset`](#format_template_resultset)
  :::

<div id="example-usage">
  ## 使用例
</div>

`Template`フォーマットの使用例を 2 つ見ていきましょう。まずはデータの選択、次にデータの挿入です。

<div id="selecting-data">
  ### データの取得
</div>

```sql title="Query"
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase ORDER BY c DESC LIMIT 5 FORMAT Template SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format', format_template_rows_between_delimiter = '\n    '
```

```text title="/some/path/resultset.format"
<!DOCTYPE HTML>
<html> <head> <title>Search phrases</title> </head>
 <body>
  <table border="1"> <caption>Search phrases</caption>
    <tr> <th>Search phrase</th> <th>Count</th> </tr>
    ${data}
  </table>
  <table border="1"> <caption>Max</caption>
    ${max}
  </table>
  <b>Processed ${rows_read:XML} rows in ${time:XML} sec</b>
 </body>
</html>
```

```text title="/some/path/row.format"
<tr> <td>${0:XML}</td> <td>${1:XML}</td> </tr>
```

```html title="Response"
<!DOCTYPE HTML>
<html> <head> <title>Search phrases</title> </head>
 <body>
  <table border="1"> <caption>Search phrases</caption>
    <tr> <th>Search phrase</th> <th>Count</th> </tr>
    <tr> <td></td> <td>8267016</td> </tr>
    <tr> <td>bathroom interior design</td> <td>2166</td> </tr>
    <tr> <td>clickhouse</td> <td>1655</td> </tr>
    <tr> <td>spring 2014 fashion</td> <td>1549</td> </tr>
    <tr> <td>freeform photos</td> <td>1480</td> </tr>
  </table>
  <table border="1"> <caption>Max</caption>
    <tr> <td></td> <td>8873898</td> </tr>
  </table>
  <b>Processed 3095973 rows in 0.1569913 sec</b>
 </body>
</html>
```

<div id="inserting-data">
  ### データの挿入
</div>

```text
Some header
Page views: 5, User id: 4324182021466249494, Useless field: hello, Duration: 146, Sign: -1
Page views: 6, User id: 4324182021466249494, Useless field: world, Duration: 185, Sign: 1
Total rows: 2
```

```sql
INSERT INTO UserActivity SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format'
FORMAT Template
```

```text title="/some/path/resultset.format"
Some header\n${data}\nTotal rows: ${:CSV}\n
```

```text title="/some/path/row.format"
Page views: ${PageViews:CSV}, User id: ${UserID:CSV}, Useless field: ${:CSV}, Duration: ${Duration:CSV}, Sign: ${Sign:CSV}
```

プレースホルダー内の`PageViews`、`UserID`、`Duration`、`Sign`は、テーブルのカラム名です。行では`Useless field`の後ろの値、接尾辞では`\nTotal rows:`の後ろの値は無視されます。
入力データ内のすべての区切り文字は、指定されたフォーマット文字列中の区切り文字と厳密に一致している必要があります。

<div id="inline_specification">
  ### インライン指定
</div>

Markdown テーブルを手作業でフォーマットするのにうんざりしていませんか？この例では、`Template` フォーマットとインライン指定の設定を使って、単純なタスク、つまり `system.formats` テーブルからいくつかの ClickHouse フォーマット名を `SELECT` し、それらを Markdown テーブルとしてフォーマットする方法を見ていきます。これは、`Template` フォーマットと `format_template_row_format` および `format_template_resultset_format` の各設定を使えば簡単に実現できます。

前の例では、結果セットと行のフォーマット文字列を別々のファイルに指定し、それらのファイルへのパスをそれぞれ `format_template_resultset` と `format_template_row` の設定で指定しました。ここでは、テンプレートが Markdown テーブルを作るための数個の `|` と `-` だけからなる単純なものなので、インラインで記述します。結果セットのテンプレート文字列は、`format_template_resultset_format` 設定を使って指定します。テーブルヘッダーを作るため、`${data}` の前に `|ClickHouse Formats|\n|---|\n` を追加しています。行のテンプレート文字列 ``|`{0:XML}`|`` は、`format_template_row_format` 設定を使って指定します。`Template` フォーマットは、指定したフォーマットで行をプレースホルダー `${data}` に挿入します。この例ではカラムは 1 つだけですが、さらに追加したい場合は、適切なエスケープ規則を選んだうえで、行のテンプレート文字列に `{1:XML}`、`{2:XML}`... などを追加できます。この例では、エスケープ規則として `XML` を使用しています。

```sql title="Query"
WITH formats AS
(
 SELECT * FROM system.formats
 ORDER BY rand()
 LIMIT 5
)
SELECT * FROM formats
FORMAT Template
SETTINGS
 format_template_row_format='|`${0:XML}`|',
 format_template_resultset_format='|ClickHouse Formats|\n|---|\n${data}\n'
```

ご覧のとおり、Markdownの表を作るために、あれだけの`|`や`-`を手作業で追加する手間が省けました。

```response title="Response"
|ClickHouse Formats|
|---|
|`BSONEachRow`|
|`CustomSeparatedWithNames`|
|`Prometheus`|
|`DWARF`|
|`Avro`|
```