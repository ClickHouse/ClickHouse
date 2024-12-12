---
slug: /ja/interfaces/formats
sidebar_position: 21
sidebar_label: すべてのフォーマットを表示…
title: 入出力データのフォーマット
---

ClickHouseは様々なフォーマットでデータを受け入れることができ、返すことができます。入力をサポートするフォーマットは、`INSERT`に提供するデータの解析、FileやURL、HDFSにバックアップされたテーブルからの`SELECT`の実行、もしくはDictionaryの読み込みに使用できます。出力をサポートするフォーマットは、`SELECT`の結果を整形するのに使用できます。また、ファイルにバックアップされたテーブルへの`INSERT`を実行するためにも使用できます。すべてのフォーマット名は大文字小文字を区別しません。

サポートされているフォーマットは以下の通りです：

| フォーマット                                                                                    | 入力 | 出力 |
|-----------------------------------------------------------------------------------------------|------|-------|
| [TabSeparated](#tabseparated)                                                                 | ✔    | ✔     |
| [TabSeparatedRaw](#tabseparatedraw)                                                           | ✔    | ✔     |
| [TabSeparatedWithNames](#tabseparatedwithnames)                                               | ✔    | ✔     |
| [TabSeparatedWithNamesAndTypes](#tabseparatedwithnamesandtypes)                               | ✔    | ✔     |
| [TabSeparatedRawWithNames](#tabseparatedrawwithnames)                                         | ✔    | ✔     |
| [TabSeparatedRawWithNamesAndTypes](#tabseparatedrawwithnamesandtypes)                         | ✔    | ✔     |
| [Template](#format-template)                                                                  | ✔    | ✔     |
| [TemplateIgnoreSpaces](#templateignorespaces)                                                 | ✔    | ✗     |
| [CSV](#csv)                                                                                   | ✔    | ✔     |
| [CSVWithNames](#csvwithnames)                                                                 | ✔    | ✔     |
| [CSVWithNamesAndTypes](#csvwithnamesandtypes)                                                 | ✔    | ✔     |
| [CustomSeparated](#format-customseparated)                                                    | ✔    | ✔     |
| [CustomSeparatedWithNames](#customseparatedwithnames)                                         | ✔    | ✔     |
| [CustomSeparatedWithNamesAndTypes](#customseparatedwithnamesandtypes)                         | ✔    | ✔     |
| [SQLInsert](#sqlinsert)                                                                       | ✗    | ✔     |
| [Values](#data-format-values)                                                                 | ✔    | ✔     |
| [Vertical](#vertical)                                                                         | ✗    | ✔     |
| [JSON](#json)                                                                                 | ✔    | ✔     |
| [JSONAsString](#jsonasstring)                                                                 | ✔    | ✗     |
| [JSONAsObject](#jsonasobject)                                                                 | ✔    | ✗     |
| [JSONStrings](#jsonstrings)                                                                   | ✔    | ✔     |
| [JSONColumns](#jsoncolumns)                                                                   | ✔    | ✔     |
| [JSONColumnsWithMetadata](#jsoncolumnsmonoblock)                                              | ✔    | ✔     |
| [JSONCompact](#jsoncompact)                                                                   | ✔    | ✔     |
| [JSONCompactStrings](#jsoncompactstrings)                                                     | ✗    | ✔     |
| [JSONCompactColumns](#jsoncompactcolumns)                                                     | ✔    | ✔     |
| [JSONEachRow](#jsoneachrow)                                                                   | ✔    | ✔     |
| [PrettyJSONEachRow](#prettyjsoneachrow)                                                       | ✗    | ✔     |
| [JSONEachRowWithProgress](#jsoneachrowwithprogress)                                           | ✗    | ✔     |
| [JSONStringsEachRow](#jsonstringseachrow)                                                     | ✔    | ✔     |
| [JSONStringsEachRowWithProgress](#jsonstringseachrowwithprogress)                             | ✗    | ✔     |
| [JSONCompactEachRow](#jsoncompacteachrow)                                                     | ✔    | ✔     |
| [JSONCompactEachRowWithNames](#jsoncompacteachrowwithnames)                                   | ✔    | ✔     |
| [JSONCompactEachRowWithNamesAndTypes](#jsoncompacteachrowwithnamesandtypes)                   | ✔    | ✔     |
| [JSONCompactStringsEachRow](#jsoncompactstringseachrow)                                       | ✔    | ✔     |
| [JSONCompactStringsEachRowWithNames](#jsoncompactstringseachrowwithnames)                     | ✔    | ✔     |
| [JSONCompactStringsEachRowWithNamesAndTypes](#jsoncompactstringseachrowwithnamesandtypes)     | ✔    | ✔     |
| [JSONObjectEachRow](#jsonobjecteachrow)                                                       | ✔    | ✔     |
| [BSONEachRow](#bsoneachrow)                                                                   | ✔    | ✔     |
| [TSKV](#tskv)                                                                                 | ✔    | ✔     |
| [Pretty](#pretty)                                                                             | ✗    | ✔     |
| [PrettyNoEscapes](#prettynoescapes)                                                           | ✗    | ✔     |
| [PrettyMonoBlock](#prettymonoblock)                                                           | ✗    | ✔     |
| [PrettyNoEscapesMonoBlock](#prettynoescapesmonoblock)                                         | ✗    | ✔     |
| [PrettyCompact](#prettycompact)                                                               | ✗    | ✔     |
| [PrettyCompactNoEscapes](#prettycompactnoescapes)                                             | ✗    | ✔     |
| [PrettyCompactMonoBlock](#prettycompactmonoblock)                                             | ✗    | ✔     |
| [PrettyCompactNoEscapesMonoBlock](#prettycompactnoescapesmonoblock)                           | ✗    | ✔     |
| [PrettySpace](#prettyspace)                                                                   | ✗    | ✔     |
| [PrettySpaceNoEscapes](#prettyspacenoescapes)                                                 | ✗    | ✔     |
| [PrettySpaceMonoBlock](#prettyspacemonoblock)                                                 | ✗    | ✔     |
| [PrettySpaceNoEscapesMonoBlock](#prettyspacenoescapesmonoblock)                               | ✗    | ✔     |
| [Prometheus](#prometheus)                                                                     | ✗    | ✔     |
| [Protobuf](#protobuf)                                                                         | ✔    | ✔     |
| [ProtobufSingle](#protobufsingle)                                                             | ✔    | ✔     |
| [ProtobufList](-format-avro-confluent)                                                        | ✔    | ✔     |
| [Avro](#data-format-avro)                                                                     | ✔    | ✔     |
| [AvroConfluent](#data-format-avro-confluent)                                                  | ✔    | ✗     |
| [Parquet](#data-format-parquet)                                                               | ✔    | ✔     |
| [ParquetMetadata](#data-format-parquet-metadata)                                              | ✔    | ✗     |
| [Arrow](#data-format-arrow)                                                                   | ✔    | ✔     |
| [ArrowStream](#data-format-arrow-stream)                                                      | ✔    | ✔     |
| [ORC](#data-format-orc)                                                                       | ✔    | ✔     |
| [One](#data-format-one)                                                                       | ✔    | ✗     |
| [Npy](#data-format-npy)                                                                       | ✔    | ✔     |
| [RowBinary](#rowbinary)                                                                       | ✔    | ✔     |
| [RowBinaryWithNames](#rowbinarywithnamesandtypes)                                             | ✔    | ✔     |
| [RowBinaryWithNamesAndTypes](#rowbinarywithnamesandtypes)                                     | ✔    | ✔     |
| [RowBinaryWithDefaults](#rowbinarywithdefaults)                                               | ✔    | ✗     |
| [Native](#native)                                                                             | ✔    | ✔     |
| [Null](#null)                                                                                 | ✗    | ✔     |
| [XML](#xml)                                                                                   | ✗    | ✔     |
| [CapnProto](#capnproto)                                                                       | ✔    | ✔     |
| [LineAsString](#lineasstring)                                                                 | ✔    | ✔     |
| [Regexp](#data-format-regexp)                                                                 | ✔    | ✗     |
| [RawBLOB](#rawblob)                                                                           | ✔    | ✔     |
| [MsgPack](#msgpack)                                                                           | ✔    | ✔     |
| [MySQLDump](#mysqldump)                                                                       | ✔    | ✗     |
| [DWARF](#dwarf)                                                                               | ✔    | ✗     |
| [Markdown](#markdown)                                                                         | ✗    | ✔     |
| [Form](#form)                                                                                 | ✔    | ✗     |

ClickHouse設定により、いくつかのフォーマット処理パラメータを制御することができます。詳細については、[設定](/docs/ja/operations/settings/settings-formats.md) セクションを参照してください。

## TabSeparated {#tabseparated}

TabSeparatedフォーマットでは、データは行ごとに書き込まれます。各行の値はタブで区切られ、最後の値には改行が必須です。ユニックス形式の改行が必要です。最後の行も改行を含める必要があります。値はテキスト形式で書かれ、囲み文字はありません。特別な文字はエスケープされます。

このフォーマットは`TSV`という名前でも利用できます。

`TabSeparated`フォーマットは、カスタムプログラムやスクリプトを使用してデータを処理するのに便利です。HTTPインターフェイスや、コマンドラインクライアントのバッチモードでデフォルトで使用されます。このフォーマットを使用して、異なるDBMS間でデータを転送することもできます。例えば、MySQLからダンプを取得し、それをClickHouseにアップロードすることができます。

`TabSeparated`フォーマットは、総計値（WITH TOTALSを使用時）や極値（`extremes`を1に設定時）の出力をサポートしています。この場合、総計値と極値はメインデータの後に出力されます。メイン結果、総計値、極値は、空行で分けられます。例：

``` sql
SELECT EventDate, count() AS c FROM test.hits GROUP BY EventDate WITH TOTALS ORDER BY EventDate FORMAT TabSeparated
```

``` response
2014-03-17    1406958
2014-03-18    1383658
2014-03-19    1405797
2014-03-20    1353623
2014-03-21    1245779
2014-03-22    1031592
2014-03-23    1046491

1970-01-01    8873898

2014-03-17    1031592
2014-03-23    1406958
```

### データフォーマティング {#tabseparated-data-formatting}

整数は10進法で書かれます。数値の先頭には「+」記号を含めることができます（解析時に無視され、フォーマット時には記録されません）。非負の数値には負符号をつけることはできません。解析時には空の文字列を0と解釈することが許され、また（符号付きタイプの場合）マイナス記号だけの文字列を0と解釈します。該当するデータタイプに収まらない数字は、エラーメッセージなしに別の数として解析されることがあります。

浮動小数点数は10進法で書かれます。小数点は小数の区切り記号として使われます。指数表記や `inf`, `+inf`, `-inf`, `nan` がサポートされています。浮動小数点数のエントリは、小数点で始まるか終わることがあります。
フォーマット時に、浮動小数点数の精度が失われる場合があります。
解析時には、機械で表現可能な最も近い数値を必ずしも読まなければならないわけではありません。

日付はYYYY-MM-DD形式で書かれ、任意の文字を区切り文字として解析されます。
日時付きの日付は、`YYYY-MM-DD hh:mm:ss`形式で書かれ、任意の文字を区切り文字として解析されます。
これは、クライアントまたはサーバーが起動したときのシステムタイムゾーンで行われます（データをフォーマットする側に依存します）。夏時間の時は指定されません。したがって、ダンプに夏時間中の時間が含まれていると、ダンプがデータと一致しないことがあり、解析時には2つの時間のうち1つが選ばれます。
読み取り操作中に、不正な日付や日時付きの日付は、自然なオーバーフローまたはnullの日付や時間として解析され、エラーメッセージはされません。

例外として、日時の解析が10進数桁が10個だけから成るUnixタイムスタンプ形式でもサポートされています。結果はタイムゾーンに依存しません。`YYYY-MM-DD hh:mm:ss`と `NNNNNNNNNN` の形式は自動的に区別されます。

文字列は、バックスラッシュでエスケープされた特殊文字と共に出力されます。出力には以下のエスケープシーケンスを使用します: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\'`, `\\`. 解析もまた、シーケンス `\a`, `\v`, `\xHH` (16進エスケープシーケンス）および任意の`\c`シーケンスをサポートし、ここで`c`は任意の文字です（これらのシーケンスは`c`に変換されます）。したがって、データの読み取りは、改行が`\n`または `\`、または改行として書かれる形式をサポートします。例えば、単語間のスペースの代わりに改行を使用した`Hello world`文字列は、以下のどのバリエーションでも解析できます：

```text
Hello\nworld

Hello\
world
```

2番目のバリアントは、MySQLがタブ分割されたダンプを書き込むときに使用するためサポートされています。

TabSeparatedフォーマットでデータを渡すときにエスケープする必要がある文字の最小セット：タブ、改行（LF）、およびバックスラッシュ。

ごく少数の記号のみがエスケープされます。出力時にあなたのターミナルを損ねる文字列値に簡単に行き当たることがあります。

配列は四角い括弧で囲まれたコンマ区切りの値リストとして書かれます。配列内の番号アイテムは通常通り書式が設定されます。`Date` と `DateTime` 型はシングルクォートで囲まれます。文字列は上記と同じエスケープルールでシングルクォートで囲まれます。

[NULL](/docs/ja/sql-reference/syntax.md)は[format_tsv_null_representation](/docs/ja/operations/settings/settings-formats.md/#format_tsv_null_representation)設定（デフォルト値は`\N`）に従ってフォーマットされます。

入力データにおいて、ENUMの値は名前またはidとして表現できます。まず、入力値をENUM名にマッチしようとします。失敗した場合でかつ入力値が数字である場合、その数字をENUM idにマッチさせようとします。
入力データがENUM idのみを含む場合、ENUM解析を最適化するために、[input_format_tsv_enum_as_number](/docs/ja/operations/settings/settings-formats.md/#input_format_tsv_enum_as_number)設定を有効化することをお勧めします。

[Nested](/docs/ja/sql-reference/data-types/nested-data-structures/index.md)構造の各要素は配列として表現されます。

例えば：

``` sql
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

``` sql
INSERT INTO nestedt Values ( 1, [1], ['a'])
```

``` sql
SELECT * FROM nestedt FORMAT TSV
```

``` response
1  [1]    ['a']
```

### TabSeparatedフォーマット設定 {#tabseparated-format-settings}

- [format_tsv_null_representation](/docs/ja/operations/settings/settings-formats.md/#format_tsv_null_representation) - TSVフォーマットでのNULLのカスタム表現。デフォルト値 - `\N`。
- [input_format_tsv_empty_as_default](/docs/ja/operations/settings/settings-formats.md/#input_format_tsv_empty_as_default) - TSV入力の空フィールドをデフォルト値として扱います。デフォルト値 - `false`。複雑なデフォルト式の場合、[input_format_defaults_for_omitted_fields](/docs/ja/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields)も有効にする必要があります。
- [input_format_tsv_enum_as_number](/docs/ja/operations/settings/settings-formats.md/#input_format_tsv_enum_as_number) - TSV形式で挿入されたenum値をenumインデックスとして扱います。デフォルト値 - `false`。
- [input_format_tsv_use_best_effort_in_schema_inference](/docs/ja/operations/settings/settings-formats.md/#input_format_tsv_use_best_effort_in_schema_inference) - TSV形式でスキーマを推測するためにいくつかの調整とヒューリスティックを使用します。無効化されると、すべてのフィールドは文字列として推測されます。デフォルト値 - `true`。
- [output_format_tsv_crlf_end_of_line](/docs/ja/operations/settings/settings-formats.md/#output_format_tsv_crlf_end_of_line) - TSV出力形式で行末を`\r\n`にする場合はtrueに設定。デフォルト値 - `false`。
- [input_format_tsv_crlf_end_of_line](/docs/ja/operations/settings/settings-formats.md/#input_format_tsv_crlf_end_of_line) - TSV入力形式で行末を`\r\n`にする場合はtrueに設定。デフォルト値 - `false`。
- [input_format_tsv_skip_first_lines](/docs/ja/operations/settings/settings-formats.md/#input_format_tsv_skip_first_lines) - データの最初の行を指定された数だけスキップします。デフォルト値 - `0`。
- [input_format_tsv_detect_header](/docs/ja/operations/settings/settings-formats.md/#input_format_tsv_detect_header) - TSV形式で名前とタイプを含むヘッダーを自動的に検出します。デフォルト値 - `true`。
- [input_format_tsv_skip_trailing_empty_lines](/docs/ja/operations/settings/settings-formats.md/#input_format_tsv_skip_trailing_empty_lines) - データの末尾の空行をスキップします。デフォルト値 - `false`。
- [input_format_tsv_allow_variable_number_of_columns](/docs/ja/operations/settings/settings-formats.md/#input_format_tsv_allow_variable_number_of_columns) - TSV形式でカラム数を変動可能にします。余分なカラムを無視し、欠落したカラムはデフォルト値を使用。デフォルト値 - `false`。

## TabSeparatedRaw {#tabseparatedraw}

`TabSeparated`フォーマットと異なり、行はエスケープなしで書き込まれる。
このフォーマットでの解析では、各フィールドにタブや改行を含むことは許可されていません。

このフォーマットは`TSVRaw`、`Raw`という名前でも利用できます。

## TabSeparatedWithNames {#tabseparatedwithnames}

`TabSeparated`フォーマットと異なり、カラム名が最初の行に書かれます。

解析中、最初の行はカラム名を含むことを期待されています。カラム名を使用して位置を決定したり、正確性を確認することができます。

:::note
設定 [input_format_with_names_use_header](/docs/ja/operations/settings/settings-formats.md/#input_format_with_names_use_header) が1に設定されている場合、
入力データのカラムは名前でテーブルのカラムにマッピングされ、不明な名前のカラムは、設定 [input_format_skip_unknown_fields](/docs/ja/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) が1に設定されている場合にスキップされます。
それ以外の場合、最初の行はスキップされます。
:::

このフォーマットは`TSVWithNames`という名前でも利用できます。

## TabSeparatedWithNamesAndTypes {#tabseparatedwithnamesandtypes}

`TabSeparated`フォーマットと異なり、カラム名は最初の行に書かれ、カラムタイプは2行目に書かれます。

:::note
設定 [input_format_with_names_use_header](/docs/ja/operations/settings/settings-formats.md/#input_format_with_names_use_header) が1に設定されている場合、
入力データのカラムは名前でテーブルのカラムにマッピングされ、不明な名前のカラムは、設定 [input_format_skip_unknown_fields](/docs/ja/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) が1に設定されている場合にスキップされます。
それ以外の場合、最初の行はスキップされます。
設定 [input_format_with_types_use_header](/docs/ja/operations/settings/settings-formats.md/#input_format_with_types_use_header) が1に設定されている場合、
入力データのタイプは対応するテーブルのカラムタイプと比較されます。それ以外の場合、2行目はスキップされます。
:::

このフォーマットは`TSVWithNamesAndTypes`という名前でも利用できます。

## TabSeparatedRawWithNames {#tabseparatedrawwithnames}

`TabSeparatedWithNames`フォーマットと異なり、行はエスケープなしで書かれます。
このフォーマットでの解析では、各フィールドにタブや改行を含むことは許可されていません。

このフォーマットは`TSVRawWithNames`、`RawWithNames`という名前でも利用できます。

## TabSeparatedRawWithNamesAndTypes {#tabseparatedrawwithnamesandtypes}

`TabSeparatedWithNamesAndTypes`フォーマットと異なり、行はエスケープなしで書かれます。
このフォーマットでの解析では、各フィールドにタブや改行を含むことは許可されていません。

このフォーマットは`TSVRawWithNamesAndNames`、`RawWithNamesAndNames`という名前でも利用できます。

## Template {#format-template}

このフォーマットは、指定されたエスケープルールを持つ値のプレースホルダーを使用して、カスタムフォーマット文字列を指定できることを可能にします。

設定は`format_template_resultset`、`format_template_row`（`format_template_row_format`）、`format_template_rows_between_delimiter`と、その他のいくつかの形式の設定（例：`JSON`エスケープ使用時の`output_format_json_quote_64bit_integers`）。

設定`format_template_row`は、次のシンタックスを持つ行に対するフォーマット文字列を含むファイルのパスを指定します：

`delimiter_1${column_1:serializeAs_1}delimiter_2${column_2:serializeAs_2} ... delimiter_N`、

ここで`delimiter_i`は値間のデリミタ（`$`シンボルは `$$`でエスケープ可能）、
`column_i`は選択または挿入される値のカラム名またはインデックス（空の場合、そのカラムはスキップされる）、
`serializeAs_i`はカラム値のエスケープルールを意味します。サポートされるエスケープルールは以下の通りです：

- `CSV`, `JSON`, `XML`（同じ名前のフォーマットに類似）
- `Escaped`（`TSV`に類似）
- `Quoted`（`Values`に類似）
- `Raw`（エスケープなし、`TSVRaw`に類似）
- `None`（エスケープルールなし、詳細は後述）

エスケープルールが省略された場合、`None`が利用されます。`XML`は出力にのみ適しています。

したがって、次のフォーマット文字列：

      `Search phrase: ${SearchPhrase:Quoted}, count: ${c:Escaped}, ad price: $$${price:JSON};`

では、`SearchPhrase`、`c`、`price`カラムの値が `Quoted`、`Escaped`、`JSON` としてエスケープされ、`Search phrase:`、`, count:`、`, ad price: $`、`;`のデリミタ間に（選択/挿入フォーマット時に）出力されます。例えば：

`Search phrase: 'bathroom interior design', count: 2166, ad price: $3;`

クラスタ内のすべてのノードにテンプレートフォーマットの出力設定を配置することが挑戦的または可能でない場合や、フォーマットが簡単な場合は、`format_template_row_format`を使用して、ファイルのパスではなく、クエリ内で直接テンプレート文字列を設定できます。

設定`format_template_rows_between_delimiter`は、行間のデリミタを指定し、最後の行以外のすべての行の後に印刷される（もしくは期待される）（デフォルトは`\n`）設定です。

設定`format_template_resultset`は、結果セットのフォーマット文字列を含むファイルのパスを指定します。設定`format_template_resultset_format`は、結果セットのテンプレート文字列をクエリ内で直接設定することも可能にします。結果セットのフォーマット文字列には、行用のフォーマット文字列と同じシンタックスがあります。プレフィックス、サフィックス、追加情報の方法を指定することができます。カラム名の代わりに次のプレースホルダーを含む：

- `data`は、`format_template_row`フォーマットでデータを持つ行で、`format_template_rows_between_delimiter`で分割されます。このプレースホルダーはフォーマット文字列で最初のプレースホルダーでなければなりません。
- `totals`は、`format_template_row`フォーマットの合計値の行です（WITH TOTALS使用時）
- `min`は、`format_template_row`フォーマットの最小値の行です（極限が1に設定されているとき）
- `max`は、`format_template_row`フォーマットの最大値の行です（極限が1に設定されているとき）
- `rows`は、出力行の総数
- `rows_before_limit`は、LIMITなしの場合でも少なくともある行数。クエリにLIMITが含まれる場合のみ出力します。GROUP BYが含まれる場合、LIMITがなければ、`rows_before_limit_at_least`は正確な行数です。
- `time`は、リクエストの実行時間（秒）
- `rows_read`は、読み取られた行の数
- `bytes_read`は、読み取られたバイト数（非圧縮）

プレースホルダー`data`、`totals`、`min`、および`max`には、エス케ープルールが指定されてはなりません（または`None`が明示的に指定される必要があります）。残りのプレースホルダーには、任意のエスケープルールが指定できます。
`format_template_resultset`設定が空の文字列の場合、`${data}`がデフォルト値として使用されます。
挿入クエリに対するフォーマットでは、プレフィックスやサフィックスがあれば、一部のカラムやフィールドをスキップできるようにしています（例を参照）。

選択例：

``` sql
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase ORDER BY c DESC LIMIT 5 FORMAT Template SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format', format_template_rows_between_delimiter = '\n    '
```

`/some/path/resultset.format`：

``` text
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

`/some/path/row.format`：

``` text
<tr> <td>${0:XML}</td> <td>${1:XML}</td> </tr>
```

結果：

``` html
<!DOCTYPE HTML>
<html> <head> <title>Search phrases</title> </head>
 <body>
  <table border="1"> <caption>Search phrases</caption>
    <tr> <th>Search phrase</th> <th>Count</th> </tr>
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

挿入例：

``` text
Some header
Page views: 5, User id: 4324182021466249494, Useless field: hello, Duration: 146, Sign: -1
Page views: 6, User id: 4324182021466249494, Useless field: world, Duration: 185, Sign: 1
Total rows: 2
```

``` sql
INSERT INTO UserActivity SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format'
FORMAT Template
```

`/some/path/resultset.format`：

``` text
Some header\n${data}\nTotal rows: ${:CSV}\n
```

`/some/path/row.format`：

``` text
Page views: ${PageViews:CSV}, User id: ${UserID:CSV}, Useless field: ${:CSV}, Duration: ${Duration:CSV}, Sign: ${Sign:CSV}
```

`PageViews`、`UserID`、`Duration`、および`Sign`はテーブル内のカラム名です。行内の`Useless field`の後やサフィックスの`\nTotal rows:`の後の値は無視されます。
入力データのすべてのデリミタは、指定されたフォーマット文字列のデリミタと厳密に等しい必要があります。

## TemplateIgnoreSpaces {#templateignorespaces}

このフォーマットは入力専用です。
`Template`に似ていますが、入力ストリームのデリミタと値の間の空白文字をスキップします。ただし、フォーマット文字列に空白文字が含まれている場合、これらの文字は、入力ストリームで期待されます。また、いくつかのデリミタを別々に分けるために、空のプレースホルダー（`${}`または`${:None}`）を指定することも許可します。このようなプレースホルダーは、空白文字をスキップするだけに使用されます。そのため、すべての行でのカラム値の順序が同じ場合、`JSON`を読み取ることができます。例えば、JSONフォーマットの例から出力されたデータを挿入するための形式として、以下のリクエストが使用されることがあります：

``` sql
INSERT INTO table_name SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format', format_template_rows_between_delimiter = ','
FORMAT TemplateIgnoreSpaces
```

`/some/path/resultset.format`：

``` text
{${}"meta"${}:${:JSON},${}"data"${}:${}[${data}]${},${}"totals"${}:${:JSON},${}"extremes"${}:${:JSON},${}"rows"${}:${:JSON},${}"rows_before_limit_at_least"${}:${:JSON}${}}
```

`/some/path/row.format`：

``` text
{${}"SearchPhrase"${}:${}${phrase:JSON}${},${}"c"${}:${}${cnt:JSON}${}}
```

## TSKV {#tskv}

TabSeparatedに似ていますが、値を名前=value形式で出力します。名前はTabSeparatedフォーマットと同様にエスケープされ、=`記号もエスケープされます。

``` text
SearchPhrase=   count()=8267016
SearchPhrase=bathroom interior design    count()=2166
SearchPhrase=clickhouse     count()=1655
SearchPhrase=2014 spring fashion    count()=1549
SearchPhrase=freeform photos       count()=1480
SearchPhrase=angelina jolie    count()=1245
SearchPhrase=omsk       count()=1112
SearchPhrase=photos of dog breeds    count()=1091
SearchPhrase=curtain designs        count()=1064
SearchPhrase=baku       count()=1000
```

[NULL](/docs/ja/sql-reference/syntax.md)は`\N`としてフォーマットされます。

``` sql
SELECT * FROM t_null FORMAT TSKV
```

``` text
x=1    y=\N
```

小さなカラムが多数ある場合、このフォーマットは非効率的ですし、通常使用する理由はありません。それにもかかわらず、効率においてJSONEachRowに劣ることはありません。

データの出力と解析の両方がこのフォーマットでサポートされています。解析中は、異なるカラムの値の順序は任意です。一部の値が省略されていることが許可されており、それらはデフォルト値と同等と見なされます。この場合、零や空白行がデフォルト値として使用されます。テーブルに指定されうる複雑な値はデフォルトとしてサポートされていません。

解析が`tskv`という追加フィールドの存在を許可します。このフィールドは実際には無視されます。

インポート中に、不明な名前のカラムは、設定[input_format_skip_unknown_fields](/docs/ja/operations/settings/settings-formats.md/#input_format_skip_unknown_fields)が1に設定されている場合、スキップされます。

## CSV {#csv}

コンマ区切り値形式（[RFC](https://tools.ietf.org/html/rfc4180)）。

書式設定時、行は二重引用符で囲まれます。文字列内の二重引用符は、二重引用符列として出力されます。他のエスケープ文字に関するルールはありません。日付と日時は二重引用符で囲まれます。数値は引用符なしで出力されます。値はデリミタ文字で分割され、デフォルトは`,`です。デリミタ文字は設定[format_csv_delimiter](/docs/ja/operations/settings/settings-formats.md/#format_csv_delimiter)で定義されます。行の区切りにはUnixの改行（LF）が使用されます。配列は、最初にTabSeparated形式で文字列としてシリアライズした後、結果の文字列を二重引用符で囲んで出力します。CSV形式では、タプルは別のカラムとしてシリアライズされます（故に、タプル内部のネストは失われます）。

```bash
$ clickhouse-client --format_csv_delimiter="|" --query="INSERT INTO test.csv FORMAT CSV" < data.csv
```

\*デフォルトでは、デリミタは`,`です。詳細については[format_csv_delimiter](/docs/ja/operations/settings/settings-formats.md/#format_csv_delimiter)設定を参照してください。

解析時、すべての値は引用符ありまたは引用符なしで解析できます。ダブル引用符およびシングル引用符の両方がサポートされています。行も引用符なしで配置することができ、それを区切り文字または改行（CRまたはLF）まで解析します。RFCに違反して、引用符なしの行解析中に、先頭および末尾のスペースおよびタブは無視されます。改行の種類に関しては、Unix（LF）、Windows（CR LF）、およびMac OS Classic（CR LF）がすべてサポートされています。

`NULL`は設定[format_csv_null_representation](/docs/ja/operations/settings/settings-formats.md/#format_tsv_null_representation)（デフォルト値は`\N`）に従ってフォーマットされます。

入力データにおいて、ENUMの値は名前またはidとして表現できます。最初に入力値をENUM名に合わせようとし、失敗し、値が数値である場合、ENUM idに合わせようとします。
入力データがENUM idのみを含む場合、ENUM解析を最適化するために、[input_format_csv_enum_as_number](/docs/ja/operations/settings/settings-formats.md/#input_format_csv_enum_as_number)設定を有効化することをお勧めします。

CSV形式は、`TabSeparated`と同様に、合計や極値の出力をサポートしています。

### CSVフォーマット設定 {#csv-format-settings}

- [format_csv_delimiter](/docs/ja/operations/settings/settings-formats.md/#format_csv_delimiter) - CSVデータでデリミタと見なされる文字。デフォルト値 - `,`。
- [format_csv_allow_single_quotes](/docs/ja/operations/settings/settings-formats.md/#format_csv_allow_single_quotes) - シングルクォート内の文字列を許可。デフォルト値 - `true`。
- [format_csv_allow_double_quotes](/docs/ja/operations/settings/settings-formats.md/#format_csv_allow_double_quotes) - ダブルクォート内の文字列を許可。デフォルト値 - `true`。
- [format_csv_null_representation](/docs/ja/operations/settings/settings-formats.md/#format_tsv_null_representation) - CSVフォーマット内のNULLのカスタム表現。デフォルト値 - `\N`。
- [input_format_csv_empty_as_default](/docs/ja/operations/settings/settings-formats.md/#input_format_csv_empty_as_default) - CSV入力の空フィールドをデフォルト値として扱う。デフォルト値 - `true`。複雑なデフォルト式の場合、[input_format_defaults_for_omitted_fields](/docs/ja/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields)も有効にする必要があります。 
- [input_format_csv_enum_as_number](/docs/ja/operations/settings/settings-formats.md/#input_format_csv_enum_as_number) - CSV形式で挿入されたenum値をenumインデックスとして扱います。デフォルト値 - `false`。
- [input_format_csv_use_best_effort_in_schema_inference](/docs/ja/operations/settings/settings-formats.md/#input_format_csv_use_best_effort_in_schema_inference) - CSV形式でスキーマを推測するためのいくつかの調整やヒューリスティックを使用します。無効化されると、すべてのフィールドは文字列として推測されます。デフォルト値 - `true`。
- [input_format_csv_arrays_as_nested_csv](/docs/ja/operations/settings/settings-formats.md/#input_format_csv_arrays_as_nested_csv) - CSVからArrayを読み取る際に、それの要素がネストされたCSVとしてシリアライズされ、それから文字列に入れられると仮定します。デフォルト値 - `false`。
- [output_format_csv_crlf_end_of_line](/docs/ja/operations/settings/settings-formats.md/#output_format_csv_crlf_end_of_line) - trueに設定されている場合、CSV出力形式の行の終了は`\r\n`となり、`\n`ではありません。デフォルト値 - `false`。
- [input_format_csv_skip_first_lines](/docs/ja/operations/settings/settings-formats.md/#input_format_csv_skip_first_lines) - データの最初に指定された数の行をスキップします。デフォルト値 - `0`。
- [input_format_csv_detect_header](/docs/ja/operations/settings/settings-formats.md/#input_format_csv_detect_header) - CSV形式で名前とタイプを含むヘッダーを自動的に検出します。デフォルト値 - `true`。
- [input_format_csv_skip_trailing_empty_lines](/docs/ja/operations/settings/settings-formats.md/#input_format_csv_skip_trailing_empty_lines) - データの末尾の空行をスキップします。デフォルト値 - `false`。
- [input_format_csv_trim_whitespaces](/docs/ja/operations/settings/settings-formats.md/#input_format_csv_trim_whitespaces) - 非引用されたCSV文字列内のスペースやタブをトリムします。デフォルト値 - `true`。
- [input_format_csv_allow_whitespace_or_tab_as_delimiter](/docs/ja/operations/settings/settings-formats.md/#input_format_csv_allow_whitespace_or_tab_as_delimiter) - CSV文字列内でのフィールドデリミタとしてホワイトスペースやタブを使用できるようにします。デフォルト値 - `false`。
- [input_format_csv_allow_variable_number_of_columns](/docs/ja/operations/settings/settings-formats.md/#input_format_csv_allow_variable_number_of_columns) - CSV形式でカラム数を変動可能にし、余分なカラムは無視し、欠落したカラムにはデフォルト値を使用。デフォルト値 - `false`。
- [input_format_csv_use_default_on_bad_values](/docs/ja/operations/settings/settings-formats.md/#input_format_csv_use_default_on_bad_values) - CSVフィールドの不正な値のデシリアライズが失敗したとき、カラムにデフォルト値を設定できるようにします。デフォルト値 - `false`。
- [input_format_csv_try_infer_numbers_from_strings](/docs/ja/operations/settings/settings-formats.md/#input_format_csv_try_infer_numbers_from_strings) - スキーマ推測中に文字列フィールドから数値を推測しようとします。デフォルト値 - `false`。

## CSVWithNames {#csvwithnames}

カラム名でヘッダー行も出力します。[TabSeparatedWithNames](#tabseparatedwithnames)に似ています。

:::note
設定 [input_format_with_names_use_header](/docs/ja/operations/settings/settings-formats.md/#input_format_with_names_use_header) が1に設定されている場合、
入力データのカラムは名前でテーブルのカラムにマッピングされ、不明な名前のカラムは、設定 [input_format_skip_unknown_fields](/docs/ja/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) が1に設定されている場合にスキップされます。
それ以外の場合、最初の行はスキップされます。
:::

## CSVWithNamesAndTypes {#csvwithnamesandtypes}

カラム名とタイプと共に2つのヘッダー行を書き込みます。これは[TabSeparatedWithNamesAndTypes](#tabseparatedwithnamesandtypes)に似ています。

:::note
設定 [input_format_with_names_use_header](/docs/ja/operations/settings/settings-formats.md/#input_format_with_names_use_header) が1に設定されている場合、
入力データのカラムは名前でテーブルのカラムにマッピングされ、不明な名前のカラムは、設定 [input_format_skip_unknown_fields](/docs/ja/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) が1に設定されている場合にスキップされます。
それ以外の場合、最初の行はスキップされます。
設定 [input_format_with_types_use_header](/docs/ja/operations/settings/settings-formats.md/#input_format_with_types_use_header) が1に設定されている場合、
入力データのタイプは対応するテーブルのカラムタイプと比較されます。それ以外の場合、2行目はスキップされます。
:::

## CustomSeparated {#format-customseparated}

[Template](#format-template)に似ていますが、すべてのカラムの名前とタイプを印刷もしくは読み取り、エスケープルールは[format_custom_escaping_rule](/docs/ja/operations/settings/settings-formats.md/#format_custom_escaping_rule)設定、デリミタは[format_custom_field_delimiter](/docs/ja/operations/settings/settings-formats.md/#format_custom_field_delimiter)、[format_custom_row_before_delimiter](/docs/ja/operations/settings/settings-formats.md/#format_custom_row_before_delimiter)、[format_custom_row_after_delimiter](/docs/ja/operations/settings/settings-formats.md/#format_custom_row_after_delimiter)、[format_custom_row_between_delimiter](/docs/ja/operations/settings/settings-formats.md/#format_custom_row_between_delimiter)、[format_custom_result_before_delimiter](/docs/ja/operations/settings/settings-formats.md/#format_custom_result_before_delimiter)および[format_custom_result_after_delimiter](/docs/ja/operations/settings/settings-formats.md/#format_custom_result_after_delimiter)から取得しますが、フォーマット文字列からは取得しません。

追加設定：
- [input_format_custom_detect_header](/docs/ja/operations/settings/settings-formats.md/#input_format_custom_detect_header) - ヘッダを自動的に検出することを有効にします。デフォルト値 - `true`。
- [input_format_custom_skip_trailing_empty_lines](/docs/ja/operations/settings/settings-formats.md/#input_format_custom_skip_trailing_empty_lines) - ファイル末尾の空行をスキップします。デフォルト値 - `false`。
- [input_format_custom_allow_variable_number_of_columns](/docs/ja/operations/settings/settings-formats.md/#input_format_custom_allow_variable_number_of_columns) - カスタムセパレーテッド形式でカラム数を変動可能にし、余分なカラムは無視し、欠落したカラムにはデフォルト値を使用。デフォルト値 - `false`。

また、[TemplateIgnoreSpaces](#templateignorespaces)に類似する`CustomSeparatedIgnoreSpaces`フォーマットもあります。

## CustomSeparatedWithNames {#customseparatedwithnames}

カラム名でヘッダー行も出力します。[TabSeparatedWithNames](#tabseparatedwithnames)に似ています。

:::note
設定 [input_format_with_names_use_header](/docs/ja/operations/settings/settings-formats.md/#input_format_with_names_use_header) が1に設定されている場合、
入力データのカラムは名前でテーブルのカラムにマッピングされ、不明な名前のカラムは、設定 [input_format_skip_unknown_fields](/docs/ja/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) が1に設定されている場合にスキップされます。
それ以外の場合、最初の行はスキップされます。
:::

## CustomSeparatedWithNamesAndTypes {#customseparatedwithnamesandtypes}

カラム名とタイプと共に2つのヘッダー行を書き込みます。これは[TabSeparatedWithNamesAndTypes](#tabseparatedwithnamesandtypes)に似ています。

:::note
設定 [input_format_with_names_use_header](/docs/ja/operations/settings/settings-formats.md/#input_format_with_names_use_header) が1に設定されている場合、
入力データのカラムは名前でテーブルのカラムにマッピングされ、不明な名前のカラムは、設定 [input_format_skip_unknown_fields](/docs/ja/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) が1に設定されている場合にスキップされます。
それ以外の場合、最初の行はスキップされます。
設定 [input_format_with_types_use_header](/docs/ja/operations/settings/settings-formats.md/#input_format_with_types_use_header) が1に設定されている場合、入力データのタイプは対応するテーブルのカラムタイプと比較されます。それ以外の場合、2行目はスキップされます。
:::

## SQLInsert {#sqlinsert}

データを`INSERT INTO table (columns...) VALUES (...), (...) ...;`ステートメントのシーケンスとして出力します。

例：

```sql
SELECT number AS x, number + 1 AS y, 'Hello' AS z FROM numbers(10) FORMAT SQLInsert SETTINGS output_format_sql_insert_max_batch_size = 2
```

```sql
INSERT INTO table (x, y, z) VALUES (0, 1, 'Hello'), (1, 2, 'Hello');
INSERT INTO table (x, y, z) VALUES (2, 3, 'Hello'), (3, 4, 'Hello');
INSERT INTO table (x, y, z) VALUES (4, 5, 'Hello'), (5, 6, 'Hello');
INSERT INTO table (x, y, z) VALUES (6, 7, 'Hello'), (7, 8, 'Hello');
INSERT INTO table (x, y, z) VALUES (8, 9, 'Hello'), (9, 10, 'Hello');
```

この形式で出力されたデータを読み取るには、[MySQLDump](#mysqldump)入力形式を使用できます。

### SQLInsertフォーマット設定 {#sqlinsert-format-settings}

- [output_format_sql_insert_max_batch_size](/docs/ja/operations/settings/settings-formats.md/#output_format_sql_insert_max_batch_size) - 1つのINSERTステートメント内の最大行数。デフォルト値 - `65505`。
- [output_format_sql_insert_table_name](/docs/ja/operations/settings/settings-formats.md/#output_format_sql_insert_table_name) - 出力INSERTクエリのテーブル名。デフォルト値 - `'table'`。
- [output_format_sql_insert_include_column_names](/docs/ja/operations/settings/settings-formats.md/#output_format_sql_insert_include_column_names) - INSERTクエリにカラム名を含めます。デフォルト値 - `true`。
- [output_format_sql_insert_use_replace](/docs/ja/operations/settings/settings-formats.md/#output_format_sql_insert_use_replace) - INSERTの代わりにREPLACEステートメントを使用します。デフォルト値 - `false`。
- [output_format_sql_insert_quote_names](/docs/ja/operations/settings/settings-formats.md/#output_format_sql_insert_quote_names) - カラム名を"\`"文字で引用します。デフォルト値 - `true`。

## JSON {#json}

データをJSON形式で出力します。データテーブル以外にも、カラム名とタイプのほか、出力された行の総数、LIMITがなければ出力可能だった行数などの追加情報も出力されます。例：

``` sql
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase WITH TOTALS ORDER BY c DESC LIMIT 5 FORMAT JSON
```

``` json
{
        "meta":
        [
                {
                        "name": "num",
                        "type": "Int32"
                },
                {
                        "name": "str",
                        "type": "String"
                },
                {
                        "name": "arr",
                        "type": "Array(UInt8)"
                }
        ],

        "data":
        [
                {
                        "num": 42,
                        "str": "hello",
                        "arr": [0,1]
                },
                {
                        "num": 43,
                        "str": "hello",
                        "arr": [0,1,2]
                },
                {
                        "num": 44,
                        "str": "hello",
                        "arr": [0,1,2,3]
                }
        ],

        "rows": 3,

        "rows_before_limit_at_least": 3,

        "statistics":
        {
                "elapsed": 0.001137687,
                "rows_read": 3,
                "bytes_read": 24
        }
}
```

このJSONはJavaScriptと互換性があります。これを保証するために、いくつかの文字はさらにエスケープされています：スラッシュ `/` は `\/` としてエスケープされます。ブラウザの問題を引き起こす`U+2028`と`U+2029`の特殊な改行は、`\uXXXX`としてエスケープされます。ASCII制御文字はエスケープされます：バックスペース、フォームフィード、ラインフィード、キャリッジリターン、および水平タブは `\b`, `\f`, `\n`, `\r`, `\t` として、00-1F範囲にある残りのバイトは `\uXXXX` シーケンスとして置き換えます。無効なUTF-8シーケンスは置き換え文字に変更されるので、出力テキストは有効なUTF-8シーケンスで構成されます。JavaScriptとの互換性のため、Int64 および UInt64 の整数はデフォルトで引用符で囲まれています。引用符を除去するには、設定パラメータ[output_format_json_quote_64bit_integers](/docs/ja/operations/settings/settings-formats.md/#output_format_json_quote_64bit_integers)を0に設定します。

`rows` – 出力行の総数。

`rows_before_limit_at_least` – LIMITがなければ最低限残っていた行数。クエリにLIMITが含まれる場合のみ出力されます。
クエリにGROUP BYが含まれる場合、`rows_before_limit_at_least`はLIMITがない場合の正確な行数です。

`totals` – 合計値（WITH TOTALSを使用時）。

`extremes` – 極値（`extremes`が1に設定時）。

ClickHouseは[NULL](/docs/ja/sql-reference/syntax.md)をサポートしており、JSON出力で`null`として表示されます。出力で`+nan`、`-nan`、`+inf`、および`-inf`を有効にするには、出力形式の引数を変更してください[output_format_json_quote_denormals](/docs/ja/operations/settings/settings-formats.md/#output_format_json_quote_denormals)を1に設定します。

**関連項目**

- [JSONEachRow](#jsoneachrow)フォーマット
- [output_format_json_array_of_rows](/docs/ja/operations/settings/settings-formats.md/#output_format_json_array_of_rows)設定

JSON入力形式では、設定[input_format_json_validate_types_from_metadata](/docs/ja/operations/settings/settings-formats.md/#input_format_json_validate_types_from_metadata)が1に設定されている場合、入力データのメタデータから検証されたタイプが、対応するテーブルのカラムのタイプと比較されます。

## JSONStrings {#jsonstrings}

JSONと異なり、データフィールドが文字列として出力され、型つきJSON値ではありません。

例：

```json
{
        "meta":
        [
                {
                        "name": "num",
                        "type": "Int32"
                },
                {
                        "name": "str",
                        "type": "String"
                },
                {
                        "name": "arr",
                        "type": "Array(UInt8)"
                }
        ],

        "data":
        [
                {
                        "num": "42",
                        "str": "hello",
                        "arr": "[0,1]"
                },
                {
                        "num": "43",
                        "str": "hello",
                        "arr": "[0,1,2]"
                },
                {
                        "num": "44",
                        "str": "hello",
                        "arr": "[0,1,2,3]"
                }
        ],

        "rows": 3,

        "rows_before_limit_at_least": 3,

        "statistics":
        {
                "elapsed": 0.001403233,
                "rows_read": 3,
                "bytes_read": 24
        }
}
```

## JSONColumns {#jsoncolumns}

:::tip
JSONColumns*形式の出力は、ClickHouseフィールド名とそのフィールドのテーブルの各行の内容を提供します。視覚的には、データが左90度回転したように見えます。
:::

このフォーマットでは、すべてのデータが1つのJSONオブジェクトとして表現されます。
JSONColumns出力形式はすべてのデータをメモリにバッファリングして、単一ブロックとして出力するため、高メモリ消費につながる可能性があります。

例：
```json
{
	"num": [42, 43, 44],
	"str": ["hello", "hello", "hello"],
	"arr": [[0,1], [0,1,2], [0,1,2,3]]
}
```

インポート中、設定[input_format_skip_unknown_fields](/docs/ja/operations/settings/settings-formats.md/#input_format_skip_unknown_fields)が1に設定されていると、不明な名前のカラムはスキップされます。
ブロックに存在しないカラムは、デフォルト値で満たされます（ここで[input_format_defaults_for_omitted_fields](/docs/ja/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields)設定を使用できます）

## JSONColumnsWithMetadata {#jsoncolumnsmonoblock}

JSONColumns形式と異なり、いくつかのメタデータや統計情報（JSON形式と類似）も含んでいます。
出力形式はすべてのデータをメモリにバッファリングして、単一ブロックとして出力するため、高メモリ消費につながる可能性があります。

例：
```json
{
        "meta":
        [
                {
                        "name": "num",
                        "type": "Int32"
                },
                {
                        "name": "str",
                        "type": "String"
                },

                {
                        "name": "arr",
                        "type": "Array(UInt8)"
                }
        ],

        "data":
        {
                "num": [42, 43, 44],
                "str": ["hello", "hello", "hello"],
                "arr": [[0,1], [0,1,2], [0,1,2,3]]
        },

        "rows": 3,

        "rows_before_limit_at_least": 3,

        "statistics":
        {
                "elapsed": 0.000272376,
                "rows_read": 3,
                "bytes_read": 24
        }
}
```

JSONColumnsWithMetadata入力形式において、[input_format_json_validate_types_from_metadata](/docs/ja/operations/settings/settings-formats.md/#input_format_json_validate_types_from_metadata)の設定が1に設定されている場合は、入力データのメタデータからの型をテーブルの対応するカラムの型と比較します。

## JSONAsString {#jsonasstring}

この形式では、単一のJSONオブジェクトを単一の値として解釈します。入力に複数のJSONオブジェクト（カンマで区切られる）がある場合、それらは別々の行として解釈されます。入力データが角括弧で囲まれている場合、それはJSONの配列として解釈されます。

この形式は、[String](/docs/ja/sql-reference/data-types/string.md)型の単一フィールドのテーブルに対してのみ解析できます。残りのカラムは[DEFAULT](/docs/ja/sql-reference/statements/create/table.md/#default)または[MATERIALIZED](/docs/ja/sql-reference/statements/create/table.md/#materialized)として設定するか、省略する必要があります。全体のJSONオブジェクトを文字列として収集した後に、[JSON関数](/docs/ja/sql-reference/functions/json-functions.md)を使用して処理できます。

**例**

クエリ:

``` sql
DROP TABLE IF EXISTS json_as_string;
CREATE TABLE json_as_string (json String) ENGINE = Memory;
INSERT INTO json_as_string (json) FORMAT JSONAsString {"foo":{"bar":{"x":"y"},"baz":1}},{},{"any json stucture":1}
SELECT * FROM json_as_string;
```

結果:

``` response
┌─json──────────────────────────────┐
│ {"foo":{"bar":{"x":"y"},"baz":1}} │
│ {}                                │
│ {"any json stucture":1}           │
└───────────────────────────────────┘
```

**複数のJSONオブジェクトからなる配列**

クエリ:

``` sql
CREATE TABLE json_square_brackets (field String) ENGINE = Memory;
INSERT INTO json_square_brackets FORMAT JSONAsString [{"id": 1, "name": "name1"}, {"id": 2, "name": "name2"}];

SELECT * FROM json_square_brackets;
```

結果:

```response
┌─field──────────────────────┐
│ {"id": 1, "name": "name1"} │
│ {"id": 2, "name": "name2"} │
└────────────────────────────┘
```

## JSONAsObject {#jsonasobject}

この形式では、単一のJSONオブジェクトを単一の[JSON](/docs/ja/sql-reference/data-types/newjson.md)値として解釈します。入力に複数のJSONオブジェクト（カンマで区切られる）がある場合、それらは別々の行として解釈されます。入力データが角括弧で囲まれている場合、それはJSONの配列として解釈されます。

この形式は、[JSON](/docs/ja/sql-reference/data-types/newjson.md)型の単一フィールドのテーブルに対してのみ解析できます。残りのカラムは[DEFAULT](/docs/ja/sql-reference/statements/create/table.md/#default)または[MATERIALIZED](/docs/ja/sql-reference/statements/create/table.md/#materialized)として設定する必要があります。

**例**

クエリ:

``` sql
SET allow_experimental_json_type = 1;
CREATE TABLE json_as_object (json JSON) ENGINE = Memory;
INSERT INTO json_as_object (json) FORMAT JSONAsObject {"foo":{"bar":{"x":"y"},"baz":1}},{},{"any json stucture":1}
SELECT * FROM json_as_object FORMAT JSONEachRow;
```

結果:

``` response
{"json":{"foo":{"bar":{"x":"y"},"baz":"1"}}}
{"json":{}}
{"json":{"any json stucture":"1"}}
```

**複数のJSONオブジェクトからなる配列**

クエリ:

``` sql
SET allow_experimental_json_type = 1;
CREATE TABLE json_square_brackets (field JSON) ENGINE = Memory;
INSERT INTO json_square_brackets FORMAT JSONAsObject [{"id": 1, "name": "name1"}, {"id": 2, "name": "name2"}];
SELECT * FROM json_square_brackets FORMAT JSONEachRow;
```

結果:

```response
{"field":{"id":"1","name":"name1"}}
{"field":{"id":"2","name":"name2"}}
```

**デフォルト値を持つカラム**

```sql
SET allow_experimental_json_type = 1;
CREATE TABLE json_as_object (json JSON, time DateTime MATERIALIZED now()) ENGINE = Memory;
INSERT INTO json_as_object (json) FORMAT JSONAsObject {"foo":{"bar":{"x":"y"},"baz":1}};
INSERT INTO json_as_object (json) FORMAT JSONAsObject {};
INSERT INTO json_as_object (json) FORMAT JSONAsObject {"any json stucture":1}
SELECT time, json FROM json_as_object FORMAT JSONEachRow
```

```response
{"time":"2024-09-16 12:18:10","json":{}}
{"time":"2024-09-16 12:18:13","json":{"any json stucture":"1"}}
{"time":"2024-09-16 12:18:08","json":{"foo":{"bar":{"x":"y"},"baz":"1"}}}
```

## JSONCompact {#jsoncompact}

データ行をオブジェクトではなく配列で出力する点でJSONとは異なります。

例:

```json
{
    "meta":
    [
        {
            "name": "num",
            "type": "Int32"
        },
        {
            "name": "str",
            "type": "String"
        },
        {
            "name": "arr",
            "type": "Array(UInt8)"
        }
    ],

    "data":
    [
        [42, "hello", [0,1]],
        [43, "hello", [0,1,2]],
        [44, "hello", [0,1,2,3]]
    ],

    "rows": 3,

    "rows_before_limit_at_least": 3,

    "statistics":
    {
        "elapsed": 0.001222069,
        "rows_read": 3,
        "bytes_read": 24
    }
}
```

## JSONCompactStrings {#jsoncompactstrings}

データ行をオブジェクトではなく配列で出力する点でJSONStringsとは異なります。

例:

```json
{
    "meta":
    [
        {
            "name": "num",
            "type": "Int32"
        },
        {
            "name": "str",
            "type": "String"
        },
        {
            "name": "arr",
            "type": "Array(UInt8)"
        }
    ],

    "data":
    [
        ["42", "hello", "[0,1]"],
        ["43", "hello", "[0,1,2]"],
        ["44", "hello", "[0,1,2,3]"]
    ],

    "rows": 3,

    "rows_before_limit_at_least": 3,

    "statistics":
    {
        "elapsed": 0.001572097,
        "rows_read": 3,
        "bytes_read": 24
    }
}
```

## JSONCompactColumns {#jsoncompactcolumns}

この形式では、すべてのデータが単一のJSON配列として表現されます。
JSONCompactColumns出力形式は、すべてのデータをメモリにバッファーして単一のブロックとして出力するため、高メモリ消費につながる可能性があります。

例:

```json
[
    [42, 43, 44],
    ["hello", "hello", "hello"],
    [[0,1], [0,1,2], [0,1,2,3]]
]
```

ブロックに存在しないカラムはデフォルト値で埋められます（ここで[input_format_defaults_for_omitted_fields](/docs/ja/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields)設定を使用できます）。

## JSONEachRow {#jsoneachrow}

この形式では、ClickHouseは各行を区切り、新しい行ごとに区切られたJSONオブジェクトとして出力します。

例:

```json
{"num":42,"str":"hello","arr":[0,1]}
{"num":43,"str":"hello","arr":[0,1,2]}
{"num":44,"str":"hello","arr":[0,1,2,3]}
```

データをインポートすると、[input_format_skip_unknown_fields](/docs/ja/operations/settings/settings-formats.md/#input_format_skip_unknown_fields)が1に設定されている場合に、名前の知られていないカラムはスキップされます。

## PrettyJSONEachRow {#prettyjsoneachrow}

JSONEachRowと異なる点は、JSONが改行デリミタと4つのスペースのインデントで整形されて出力される点です。出力専用に適しています。

例:

```json
{
    "num": "42",
    "str": "hello",
    "arr": [
        "0",
        "1"
    ],
    "tuple": {
        "num": 42,
        "str": "world"
    }
}
{
    "num": "43",
    "str": "hello",
    "arr": [
        "0",
        "1",
        "2"
    ],
    "tuple": {
        "num": 43,
        "str": "world"
    }
}
```

## JSONStringsEachRow {#jsonstringseachrow}

JSONEachRowと異なる点は、データフィールドが文字列として出力され、型付きJSON値としてではないことです。

例:

```json
{"num":"42","str":"hello","arr":"[0,1]"}
{"num":"43","str":"hello","arr":"[0,1,2]"}
{"num":"44","str":"hello","arr":"[0,1,2,3]"}
```

## JSONCompactEachRow {#jsoncompacteachrow}

JSONEachRowとは異なる点は、データ行がオブジェクトではなく配列で出力されることです。

例:

```json
[42, "hello", [0,1]]
[43, "hello", [0,1,2]]
[44, "hello", [0,1,2,3]]
```

## JSONCompactStringsEachRow {#jsoncompactstringseachrow}

JSONCompactEachRowと異なる点は、データフィールドが文字列として出力され、型付きJSON値としてではないことです。

例:

```json
["42", "hello", "[0,1]"]
["43", "hello", "[0,1,2]"]
["44", "hello", "[0,1,2,3]"]
```

## JSONEachRowWithProgress {#jsoneachrowwithprogress}
## JSONStringsEachRowWithProgress {#jsonstringseachrowwithprogress}

`JSONEachRow`/`JSONStringsEachRow`と異なる点は、ClickHouseが進捗情報もJSON値として提供することです。

```json
{"row":{"num":42,"str":"hello","arr":[0,1]}}
{"row":{"num":43,"str":"hello","arr":[0,1,2]}}
{"row":{"num":44,"str":"hello","arr":[0,1,2,3]}}
{"progress":{"read_rows":"3","read_bytes":"24","written_rows":"0","written_bytes":"0","total_rows_to_read":"3"}}
```

## JSONCompactEachRowWithNames {#jsoncompacteachrowwithnames}

`JSONCompactEachRow`形式と異なる点は、カラム名を含むヘッダー行も出力されることです。[TabSeparatedWithNames](#tabseparatedwithnames)形式に似ています。

:::note
[settings-formats-input_format_with_names_use_header](/docs/ja/operations/settings/settings-formats.md/#input_format_with_names_use_header)の設定が1に設定されている場合、
入力データのカラムは名前でテーブルのカラムにマッピングされます。未知の名前のカラムは、[input_format_skip_unknown_fields](/docs/ja/operations/settings/settings-formats.md/#input_format_skip_unknown_fields)の設定が1に設定されている場合、スキップされます。
それ以外の場合は、最初の行がスキップされます。
:::

## JSONCompactEachRowWithNamesAndTypes {#jsoncompacteachrowwithnamesandtypes}

`JSONCompactEachRow`形式と異なる点は、カラム名と型を含む2つのヘッダー行も出力されることです。[TabSeparatedWithNamesAndTypes](#tabseparatedwithnamesandtypes)形式に似ています。

:::note
[settings-formats-input_format_with_names_use_header](/docs/ja/operations/settings/settings-formats.md/#input_format_with_names_use_header)の設定が1に設定されている場合、
入力データのカラムは名前でテーブルのカラムにマッピングされます。未知の名前のカラムは、[input_format_skip_unknown_fields](/docs/ja/operations/settings/settings-formats.md/#input_format_skip_unknown_fields)の設定が1に設定されている場合、スキップされます。
それ以外の場合は、最初の行がスキップされます。
[settings-formats-input_format_with_types_use_header](/docs/ja/operations/settings/settings-formats.md/#input_format_with_types_use_header)の設定が1に設定されている場合、
入力データの型はテーブルの対応するカラムの型と比較されます。そうでない場合、2番目の行はスキップされます。
:::

## JSONCompactStringsEachRowWithNames {#jsoncompactstringseachrowwithnames}

`JSONCompactStringsEachRow`とは異なる点は、カラム名を含むヘッダー行も出力されることです。[TabSeparatedWithNames](#tabseparatedwithnames)形式に似ています。

:::note
[settings-formats-input_format_with_names_use_header](/docs/ja/operations/settings/settings-formats.md/#input_format_with_names_use_header)の設定が1に設定されている場合、
入力データのカラムは名前でテーブルのカラムにマッピングされます。未知の名前のカラムは、[input_format_skip_unknown_fields](/docs/ja/operations/settings/settings-formats.md/#input_format_skip_unknown_fields)の設定が1に設定されている場合、スキップされます。
それ以外の場合は、最初の行がスキップされます。
:::

## JSONCompactStringsEachRowWithNamesAndTypes {#jsoncompactstringseachrowwithnamesandtypes}

`JSONCompactStringsEachRow`とは異なる点は、カラム名と型を含む2つのヘッダー行も出力されることです。[TabSeparatedWithNamesAndTypes](#tabseparatedwithnamesandtypes)形式に似ています。

:::note
[settings-formats-input_format_with_names_use_header](/docs/ja/operations/settings/settings-formats.md/#input_format_with_names_use_header)の設定が1に設定されている場合、
入力データのカラムは名前でテーブルのカラムにマッピングされます。未知の名前のカラムは、[input_format_skip_unknown_fields](/docs/ja/operations/settings/settings-formats.md/#input_format_skip_unknown_fields)の設定が1に設定されている場合、スキップされます。
それ以外の場合は、最初の行がスキップされます。
[settings-formats-input_format_with_types_use_header](/docs/ja/operations/settings/settings-formats.md/#input_format_with_types_use_header)の設定が1に設定されている場合、
入力データの型はテーブルの対応するカラムの型と比較されます。そうでない場合、2番目の行はスキップされます。
:::

```json
["num", "str", "arr"]
["Int32", "String", "Array(UInt8)"]
[42, "hello", [0,1]]
[43, "hello", [0,1,2]]
[44, "hello", [0,1,2,3]]
```

## JSONObjectEachRow {#jsonobjecteachrow}

この形式では、すべてのデータが単一のJSONオブジェクトとして表現され、各行がこのオブジェクトの個別のフィールドとして表され、JSONEachRow形式に似ています。

例:

```json
{
    "row_1": {"num": 42, "str": "hello", "arr":  [0,1]},
    "row_2": {"num": 43, "str": "hello", "arr":  [0,1,2]},
    "row_3": {"num": 44, "str": "hello", "arr":  [0,1,2,3]}
}
```

オブジェクト名をカラム値として使用するには、特別な設定[format_json_object_each_row_column_for_object_name](/docs/ja/operations/settings/settings-formats.md/#format_json_object_each_row_column_for_object_name)を使用できます。この設定の値は、結果のオブジェクトで行のJSONキーとして使用されるカラムの名前に設定されます。

例:

出力の場合:

テーブル`test`を次のように仮定します。2つのカラムがあります:
```
┌─object_name─┬─number─┐
│ first_obj   │      1 │
│ second_obj  │      2 │
│ third_obj   │      3 │
└─────────────┴────────┘
```
`JSONObjectEachRow`形式で出力し、`format_json_object_each_row_column_for_object_name`設定を使用しましょう:

```sql
select * from test settings format_json_object_each_row_column_for_object_name='object_name'
```

出力:
```json
{
    "first_obj": {"number": 1},
    "second_obj": {"number": 2},
    "third_obj": {"number": 3}
}
```

入力の場合:

前の例からの出力を`data.json`というファイルに保存したと仮定します:
```sql
select * from file('data.json', JSONObjectEachRow, 'object_name String, number UInt64') settings format_json_object_each_row_column_for_object_name='object_name'
```

```
┌─object_name─┬─number─┐
│ first_obj   │      1 │
│ second_obj  │      2 │
│ third_obj   │      3 │
└─────────────┴────────┘
```

スキーマ推論でも動作します:

```sql
desc file('data.json', JSONObjectEachRow) settings format_json_object_each_row_column_for_object_name='object_name'
```

```
┌─name────────┬─type────────────┐
│ object_name │ String          │
│ number      │ Nullable(Int64) │
└─────────────┴─────────────────┘
```

### データの挿入 {#json-inserting-data}

``` sql
INSERT INTO UserActivity FORMAT JSONEachRow {"PageViews":5, "UserID":"4324182021466249494", "Duration":146,"Sign":-1} {"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

ClickHouseは以下を許可します:

- オブジェクト内のキーと値のペアの順序は任意です。
- 一部の値の省略。

ClickHouseは要素間の空白やオブジェクト後のカンマを無視します。すべてのオブジェクトを1行で渡すことができます。行区切りは必要ありません。

**省略された値の処理**

ClickHouseは、省略された値を対応する[データ型](/docs/ja/sql-reference/data-types/index.md)のデフォルト値で補います。

`DEFAULT expr`が指定されている場合、ClickHouseは、[input_format_defaults_for_omitted_fields](/docs/ja/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields)設定に応じて異なる置換ルールを使用します。

次のテーブルを考慮してください:

``` sql
CREATE TABLE IF NOT EXISTS example_table
(
    x UInt32,
    a DEFAULT x * 2
) ENGINE = Memory;
```

- `input_format_defaults_for_omitted_fields = 0`の場合、`x`と`a`のデフォルト値はともに`0`（`UInt32`データ型のデフォルト値）です。
- `input_format_defaults_for_omitted_fields = 1`の場合、`x`のデフォルト値は`0`ですが、`a`のデフォルト値は`x * 2`です。

:::note
`input_format_defaults_for_omitted_fields = 1`でデータを挿入すると、`input_format_defaults_for_omitted_fields = 0`での挿入に比べて、ClickHouseはより多くの計算資源を消費します。
:::

### データの選択 {#json-selecting-data}

`UserActivity`テーブルを例として考慮しましょう:

``` response
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

クエリ`SELECT * FROM UserActivity FORMAT JSONEachRow`は以下を返します:

``` response
{"UserID":"4324182021466249494","PageViews":5,"Duration":146,"Sign":-1}
{"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

[JSON](#json)形式と異なり、無効なUTF-8シーケンスの置換はありません。値は`JSON`と同じようにエスケープされます。

:::info
任意のバイトセットは文字列に出力できます。データが情報を失うことなくJSONとしてフォーマット可能であると確信している場合は、`JSONEachRow`形式を使用してください。
:::

### ネスト構造の使用 {#jsoneachrow-nested}

[Nested](/docs/ja/sql-reference/data-types/nested-data-structures/index.md)データ型カラムを持つテーブルがある場合、同じ構造のJSONデータを挿入できます。[input_format_import_nested_json](/docs/ja/operations/settings/settings-formats.md/#input_format_import_nested_json)設定を有効にしてください。

例として、次のテーブルを考慮してください:

``` sql
CREATE TABLE json_each_row_nested (n Nested (s String, i Int32) ) ENGINE = Memory
```

`Nested`データ型の説明で示すように、ClickHouseはネストされた構造の各要素を別々のカラムとして扱います（我々のテーブルでは`n.s`と`n.i`）。次のようにデータを挿入できます:

``` sql
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n.s": ["abc", "def"], "n.i": [1, 23]}
```

データを階層型JSONオブジェクトとして挿入するには、[input_format_import_nested_json=1](/docs/ja/operations/settings/settings-formats.md/#input_format_import_nested_json)に設定します。

``` json
{
    "n": {
        "s": ["abc", "def"],
        "i": [1, 23]
    }
}
```

この設定なしでは、ClickHouseは例外を投げます。

``` sql
SELECT name, value FROM system.settings WHERE name = 'input_format_import_nested_json'
```

``` response
┌─name────────────────────────────┬─value─┐
│ input_format_import_nested_json │ 0     │
└─────────────────────────────────┴───────┘
```

``` sql
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
```

``` response
Code: 117. DB::Exception: Unknown field found while parsing JSONEachRow format: n: (at row 1)
```

``` sql
SET input_format_import_nested_json=1
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
SELECT * FROM json_each_row_nested
```

``` response
┌─n.s───────────┬─n.i────┐
│ ['abc','def'] │ [1,23] │
└───────────────┴────────┘
```

### JSON形式の設定 {#json-formats-settings}

- [input_format_import_nested_json](/docs/ja/operations/settings/settings-formats.md/#input_format_import_nested_json) - ネストされたJSONデータをネストされたテーブルにマップします（これはJSONEachRow形式で機能します）。デフォルト値 - `false`。
- [input_format_json_read_bools_as_numbers](/docs/ja/operations/settings/settings-formats.md/#input_format_json_read_bools_as_numbers) - JSON入力形式でブール値を数値として解析します。デフォルト値 - `true`。
- [input_format_json_read_bools_as_strings](/docs/ja/operations/settings/settings-formats.md/#input_format_json_read_bools_as_strings) - JSON入力形式でブール値を文字列として解析します。デフォルト値 - `true`。
- [input_format_json_read_numbers_as_strings](/docs/ja/operations/settings/settings-formats.md/#input_format_json_read_numbers_as_strings) - JSON入力形式で数値を文字列として解析します。デフォルト値 - `true`。
- [input_format_json_read_arrays_as_strings](/docs/ja/operations/settings/settings-formats.md/#input_format_json_read_arrays_as_strings) - JSON入力形式で配列を文字列として解析します。デフォルト値 - `true`。
- [input_format_json_read_objects_as_strings](/docs/ja/operations/settings/settings-formats.md/#input_format_json_read_objects_as_strings) - JSON入力形式でオブジェクトを文字列として解析します。デフォルト値 - `true`。
- [input_format_json_named_tuples_as_objects](/docs/ja/operations/settings/settings-formats.md/#input_format_json_named_tuples_as_objects) - 名前付きタプルカラムをJSONオブジェクトとして解析します。デフォルト値 - `true`。
- [input_format_json_try_infer_numbers_from_strings](/docs/ja/operations/settings/settings-formats.md/#input_format_json_try_infer_numbers_from_strings) - スキーマ推論中に文字列フィールドから数値を推測しようとします。デフォルト値 - `false`。
- [input_format_json_try_infer_named_tuples_from_objects](/docs/ja/operations/settings/settings-formats.md/#input_format_json_try_infer_named_tuples_from_objects) - スキーマ推論中にJSONオブジェクトから名前付きタプルを推測しようとします。デフォルト値 - `true`。
- [input_format_json_infer_incomplete_types_as_strings](/docs/ja/operations/settings/settings-formats.md/#input_format_json_infer_incomplete_types_as_strings) - JSON入力形式でスキーマ推論中にNullまたは空のオブジェクト/配列のみを含むKeysにString型を使用します。デフォルト値 - `true`。
- [input_format_json_defaults_for_missing_elements_in_named_tuple](/docs/ja/operations/settings/settings-formats.md/#input_format_json_defaults_for_missing_elements_in_named_tuple) - 名前付きタプル解析中に、JSONオブジェクト内で欠けている要素に対してデフォルト値を挿入します。デフォルト値 - `true`。
- [input_format_json_ignore_unknown_keys_in_named_tuple](/docs/ja/operations/settings/settings-formats.md/#input_format_json_ignore_unknown_keys_in_named_tuple) - 名前付きタプルのjsonオブジェクト内で未知のキーを無視します。デフォルト値 - `false`。
- [input_format_json_compact_allow_variable_number_of_columns](/docs/ja/operations/settings/settings-formats.md/#input_format_json_compact_allow_variable_number_of_columns) - JSONCompact/JSONCompactEachRow形式で変動するカラム数を許可し、余分なカラムを無視し、欠けているカラムにデフォルト値を使用します。デフォルト値 - `false`。
- [input_format_json_throw_on_bad_escape_sequence](/docs/ja/operations/settings/settings-formats.md/#input_format_json_throw_on_bad_escape_sequence) - JSON文字列に誤ったエスケープシーケンスが含まれている場合に例外をスローします。無効化された場合、誤ったエスケープシーケンスはデータ内でそのまま残ります。デフォルト値 - `true`。
- [input_format_json_empty_as_default](/docs/ja/operations/settings/settings-formats.md/#input_format_json_empty_as_default) - JSON入力で空のフィールドをデフォルト値として処理します。デフォルト値 - `false`。複雑なデフォルト式の場合は、[input_format_defaults_for_omitted_fields](/docs/ja/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields)も有効にする必要があります。
- [output_format_json_quote_64bit_integers](/docs/ja/operations/settings/settings-formats.md/#output_format_json_quote_64bit_integers) - JSON出力形式で64ビット整数の引用を制御します。デフォルト値 - `true`。
- [output_format_json_quote_64bit_floats](/docs/ja/operations/settings/settings-formats.md/#output_format_json_quote_64bit_floats) - JSON出力形式で64ビット浮動小数点数の引用を制御します。デフォルト値 - `false`。
- [output_format_json_quote_denormals](/docs/ja/operations/settings/settings-formats.md/#output_format_json_quote_denormals) - JSON出力形式での`+nan`、`-nan`、`+inf`、`-inf` の出力を有効にします。デフォルト値 - `false`。
- [output_format_json_quote_decimals](/docs/ja/operations/settings/settings-formats.md/#output_format_json_quote_decimals) - JSON出力形式での小数の引用を制御します。デフォルト値 - `false`。
- [output_format_json_escape_forward_slashes](/docs/ja/operations/settings/settings-formats.md/#output_format_json_escape_forward_slashes) - JSON出力形式での文字列出力のスラッシュのエスケープを制御します。デフォルト値 - `true`。
- [output_format_json_named_tuples_as_objects](/docs/ja/operations/settings/settings-formats.md/#output_format_json_named_tuples_as_objects) - 名前付きタプルカラムをJSONオブジェクトとしてシリアライズします。デフォルト値 - `true`。
- [output_format_json_array_of_rows](/docs/ja/operations/settings/settings-formats.md/#output_format_json_array_of_rows) - JSONEachRow(Compact)形式で全行のJSON配列を出力します。デフォルト値 - `false`。
- [output_format_json_validate_utf8](/docs/ja/operations/settings/settings-formats.md/#output_format_json_validate_utf8) - JSON出力形式でのUTF-8シーケンスの検証を有効にします（それはJSON/JSONCompact/JSONColumnsWithMetadata形式に影響しません、それらは常にutf8を検証します）。デフォルト値 - `false`。

## BSONEachRow {#bsoneachrow}

この形式では、ClickHouseはデータを区分なしでBSONドキュメントのシーケンスとしてフォーマット/解析します。
各行は単一のドキュメントとしてフォーマットされ、各カラムはカラム名をキーとして単一のBSONドキュメントフィールドとしてフォーマットされます。

出力のために、ClickHouseのタイプとBSONタイプの対応関係は次の通りです：

| ClickHouseのタイプ                                                                                              | BSONタイプ                                                                                                |
|-----------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------|
| [Bool](/docs/ja/sql-reference/data-types/boolean.md)                                  | `\x08` boolean                                                                                       |
| [Int8/UInt8](/docs/ja/sql-reference/data-types/int-uint.md)/[Enum8](/docs/ja/sql-reference/data-types/enum.md)          | `\x10` int32                                                                                         |
| [Int16/UInt16](/docs/ja/sql-reference/data-types/int-uint.md)/[Enum16](/docs/ja/sql-reference/data-types/enum.md)       | `\x10` int32                                                                                         |
| [Int32](/docs/ja/sql-reference/data-types/int-uint.md)                                | `\x10` int32                                                                                         |
| [UInt32](/docs/ja/sql-reference/data-types/int-uint.md)                               | `\x12` int64                                                                                         |
| [Int64/UInt64](/docs/ja/sql-reference/data-types/int-uint.md)                         | `\x12` int64                                                                                         |
| [Float32/Float64](/docs/ja/sql-reference/data-types/float.md)                         | `\x01` double                                                                                        |
| [Date](/docs/ja/sql-reference/data-types/date.md)/[Date32](/docs/ja/sql-reference/data-types/date32.md)               | `\x10` int32                                                                                         |
| [DateTime](/docs/ja/sql-reference/data-types/datetime.md)                             | `\x12` int64                                                                                         |
| [DateTime64](/docs/ja/sql-reference/data-types/datetime64.md)                         | `\x09` datetime                                                                                      |
| [Decimal32](/docs/ja/sql-reference/data-types/decimal.md)                             | `\x10` int32                                                                                         |
| [Decimal64](/docs/ja/sql-reference/data-types/decimal.md)                             | `\x12` int64                                                                                         |
| [Decimal128](/docs/ja/sql-reference/data-types/decimal.md)                            | `\x05` binary, `\x00` binary subtype, size = 16                                                      |
| [Decimal256](/docs/ja/sql-reference/data-types/decimal.md)                            | `\x05` binary, `\x00` binary subtype, size = 32                                                      |
| [Int128/UInt128](/docs/ja/sql-reference/data-types/int-uint.md)                       | `\x05` binary, `\x00` binary subtype, size = 16                                                      |
| [Int256/UInt256](/docs/ja/sql-reference/data-types/int-uint.md)                       | `\x05` binary, `\x00` binary subtype, size = 32                                                      |
| [String](/docs/ja/sql-reference/data-types/string.md)/[FixedString](/docs/ja/sql-reference/data-types/fixedstring.md)     | `\x05` binary, `\x00` binary subtype or \x02 string if setting output_format_bson_string_as_string is enabled |
| [UUID](/docs/ja/sql-reference/data-types/uuid.md)                                     | `\x05` binary, `\x04` uuid subtype, size = 16                                                        |
| [Array](/docs/ja/sql-reference/data-types/array.md)                                   | `\x04` array                                                                                         |
| [Tuple](/docs/ja/sql-reference/data-types/tuple.md)                                   | `\x04` array                                                                                         |
| [Named Tuple](/docs/ja/sql-reference/data-types/tuple.md)                             | `\x03` document                                                                                      |
| [Map](/docs/ja/sql-reference/data-types/map.md)                                       | `\x03` document                                                                                      |
| [IPv4](/docs/ja/sql-reference/data-types/ipv4.md)                                     | `\x10` int32                                                                                         |
| [IPv6](/docs/ja/sql-reference/data-types/ipv6.md)                                     | `\x05` binary, `\x00` binary subtype                                                                 |

入力のために、BSONタイプとClickHouseのタイプの対応関係は次の通りです:

| BSONタイプ                    | ClickHouseタイプ                                                                                                    |
|------------------------------|------------------------------------------------------------------------------------------------------|
| `\x01` double                | [Float32/Float64](/docs/ja/sql-reference/data-types/float.md)                                    |
| `\x02` string                | [String](/docs/ja/sql-reference/data-types/string.md)/[FixedString](/docs/ja/sql-reference/data-types/fixedstring.md)                |
| `\x03` document              | [Map](/docs/ja/sql-reference/data-types/map.md)/[Named Tuple](/docs/ja/sql-reference/data-types/tuple.md)          |
| `\x04` array                 | [Array](/docs/ja/sql-reference/data-types/array.md)/[Tuple](/docs/ja/sql-reference/data-types/tuple.md)            |
| `\x05` binary, `\x00`      | [String](/docs/ja/sql-reference/data-types/string.md)/[FixedString](/docs/ja/sql-reference/data-types/fixedstring.md)/[IPv6](/docs/ja/sql-reference/data-types/ipv6.md) |
| `\x05` binary, `\x02` old | [String](/docs/ja/sql-reference/data-types/string.md)/[FixedString](/docs/ja/sql-reference/data-types/fixedstring.md)                |
| `\x05` binary, `\x03` old | [UUID](/docs/ja/sql-reference/data-types/uuid.md)                                                    |
| `\x05` binary, `\x04`     | [UUID](/docs/ja/sql-reference/data-types/uuid.md)                                                    |
| `\x07` ObjectId              | [String](/docs/ja/sql-reference/data-types/string.md)/[FixedString](/docs/ja/sql-reference/data-types/fixedstring.md)                |
| `\x08` boolean               | [Bool](/docs/ja/sql-reference/data-types/boolean.md)                                               |
| `\x09` datetime              | [DateTime64](/docs/ja/sql-reference/data-types/datetime64.md)                                      |
| `\x0A` null value            | [NULL](/docs/ja/sql-reference/data-types/nullable.md)                                              |
| `\x0D` JavaScript code       | [String](/docs/ja/sql-reference/data-types/string.md)/[FixedString](/docs/ja/sql-reference/data-types/fixedstring.md)                |
| `\x0E` symbol                | [String](/docs/ja/sql-reference/data-types/string.md)/[FixedString](/docs/ja/sql-reference/data-types/fixedstring.md)                |
| `\x10` int32                 | [Int32/UInt32](/docs/ja/sql-reference/data-types/int-uint.md)/[Decimal32](/docs/ja/sql-reference/data-types/decimal.md)/[IPv4](/docs/ja/sql-reference/data-types/ipv4.md)/[Enum8/Enum16](/docs/ja/sql-reference/data-types/enum.md) |
| `\x12` int64                 | [Int64/UInt64](/docs/ja/sql-reference/data-types/int-uint.md)/[Decimal64](/docs/ja/sql-reference/data-types/decimal.md)/[DateTime64](/docs/ja/sql-reference/data-types/datetime64.md) |

他のBSONタイプはサポートされていません。また、異なる整数型間の変換を行います（たとえば、BSON int32値をClickHouse UInt8に挿入できます）。
大きな整数および小数（Int128/UInt128/Int256/UInt256/Decimal128/Decimal256）は、`\x00`バイナリサブタイプを持つBSONバイナリ値から解析できます。この場合、この形式はバイナリデータのサイズが予想値のサイズと等しいことを検証します。

注: この形式は、ビッグエンディアンプラットフォームでは適切に動作しません。

### BSON形式の設定 {#bson-format-settings}

- [output_format_bson_string_as_string](/docs/ja/operations/settings/settings-formats.md/#output_format_bson_string_as_string) - Stringカラムに対してBSON Stringタイプを使用します。デフォルト値 - `false`。
- [input_format_bson_skip_fields_with_unsupported_types_in_schema_inference](/docs/ja/operations/settings/settings-formats.md/#input_format_bson_skip_fields_with_unsupported_types_in_schema_inference) - フォーマットBSONEachRowのスキーマ推論中にサポートされていない型を持つカラムをスキップすることを許可します。デフォルト値 - `false`。

## Native {#native}

最も効率的な形式です。データはバイナリ形式でブロックごとに書き込まれ読み取られます。各ブロックについて、行数、カラム数、カラム名と型、そしてこのブロックのカラム部分が次々に記録されます。言い換えれば、この形式は「カラム型」であり、カラムを行に変換しません。これは、サーバー間のインターフェイスで使用される形式であり、コマンドラインクライアントやC++クライアントで使用するためのものです。

この形式を使用して、ClickHouse DBMSでのみ読み取ることができるダンプを迅速に生成することができます。この形式を自分で操作する意味はありません。

## Null {#null}

何も出力しません。ただし、クエリは処理され、コマンドラインクライアントを使用している場合には、データがクライアントに伝送されます。これは、テスト、特にパフォーマンステストに使用されます。
当然ながら、この形式は出力専用であり、解析には適していません。

## Pretty {#pretty}

Unicodeアートテーブルとしてデータを出力し、ANSIエスケープシーケンスを使用して端末内の色を設定します。
テーブルの完全なグリッドが描画され、各行は端末内で2行を占有します。
各結果ブロックは個別のテーブルとして出力されます。これは、結果をバッファリングせずにブロックを出力するために必要です（値の可視幅をすべて事前に計算するためにはバッファリングが必要になります）。

[NULL](/docs/ja/sql-reference/syntax.md)は`ᴺᵁᴸᴸ`として出力されます。

例（[PrettyCompact](#prettycompact)形式で表示）:

``` sql
SELECT * FROM t_null
```

``` response
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

行はPretty*形式ではエスケープされません。例は[PrettyCompact](#prettycompact)形式で表示されます:

``` sql
SELECT 'String with \'quotes\' and \t character' AS Escaping_test
```

``` response
┌─Escaping_test────────────────────────┐
│ String with 'quotes' and      character │
└──────────────────────────────────────┘
```

端末にあまりにも多くのデータをダンプしないように、最初の10,000行のみが印刷されます。行数が10,000以上の場合、「Showed first 10 000」というメッセージが印刷されます。この形式は、クエリ結果を出力するためにのみ適しており、解析には適していません（テーブルに挿入するためにデータを取得する）。

Pretty形式は、総合値（WITH TOTALSを使用した場合）と極端値（'extremes'が1に設定されている場合）の出力をサポートします。この場合、総合値と極端値はメインデータの後に個別のテーブルで出力されます。例（[PrettyCompact](#prettycompact)形式で表示）:

``` sql
SELECT EventDate, count() AS c FROM test.hits GROUP BY EventDate WITH TOTALS ORDER BY EventDate FORMAT PrettyCompact
```

``` response
┌──EventDate─┬───────c─┐
│ 2014-03-17 │ 1406958 │
│ 2014-03-18 │ 1383658 │
│ 2014-03-19 │ 1405797 │
│ 2014-03-20 │ 1353623 │
│ 2014-03-21 │ 1245779 │
│ 2014-03-22 │ 1031592 │
│ 2014-03-23 │ 1046491 │
└────────────┴─────────┘

Totals:
┌──EventDate─┬───────c─┐
│ 1970-01-01 │ 8873898 │
└────────────┴─────────┘

Extremes:
┌──EventDate─┬───────c─┐
│ 2014-03-17 │ 1031592 │
│ 2014-03-23 │ 1406958 │
└────────────┴─────────┘
```

## PrettyNoEscapes {#prettynoescapes}

[Pretty](#pretty)とは異なりANSIエスケープシーケンスは使用されません。これはブラウザでこの形式を表示するのに必要であり、また、「watch」コマンドラインユーティリティを使用する際にも役立ちます。

例:

``` bash
$ watch -n1 "clickhouse-client --query='SELECT event, value FROM system.events FORMAT PrettyCompactNoEscapes'"
```

ブラウザで表示するためには、HTTPインターフェイスを使用することができます。

## PrettyMonoBlock {#prettymonoblock}

[Pretty](#pretty)とは異なり、最大10,000行がバッファリングされ、ブロックではなく単一のテーブルとして出力されます。

## PrettyNoEscapesMonoBlock {#prettynoescapesmonoblock}

[PrettyNoEscapes](#prettynoescapes)とは異なり、最大10,000行がバッファリングされ、ブロックではなく単一のテーブルとして出力されます。


## PrettyCompact {#prettycompact}

[Pretty](#pretty)とは異なり、行間にグリッドが描画され、結果がよりコンパクトになります。
この形式は、対話モードのコマンドラインクライアントでデフォルトで使用されます。

## PrettyCompactNoEscapes {#prettycompactnoescapes}

[PrettyCompact](#prettycompact)とは異なり、ANSIエスケープシーケンスは使用されません。これはブラウザでこの形式を表示するのに必要であり、「watch」コマンドラインユーティリティを使用する際にも役立ちます。

## PrettyCompactMonoBlock {#prettycompactmonoblock}

[PrettyCompact](#prettycompact)とは異なり、最大10,000行がバッファリングされ、ブロックではなく単一のテーブルとして出力されます。

## PrettyCompactNoEscapesMonoBlock {#prettycompactnoescapesmonoblock}

[PrettyCompactNoEscapes](#prettycompactnoescapes)とは異なり、最大10,000行がバッファリングされ、ブロックではなく単一のテーブルとして出力されます。

## PrettySpace {#prettyspace}

[PrettyCompact](#prettycompact)とは異なり、グリッドの代わりにホワイトスペース（スペース文字）が使用されます。

## PrettySpaceNoEscapes {#prettyspacenoescapes}

[PrettySpace](#prettyspace)とは異なり、ANSIエスケープシーケンスは使用されません。これはブラウザでこの形式を表示するのに必要であり、「watch」コマンドラインユーティリティを使用する際にも役立ちます。

## PrettySpaceMonoBlock {#prettyspacemonoblock}

[PrettySpace](#prettyspace)とは異なり、最大10,000行がバッファリングされ、ブロックではなく単一のテーブルとして出力されます。

## PrettySpaceNoEscapesMonoBlock {#prettyspacenoescapesmonoblock}

[PrettySpaceNoEscapes](#prettyspacenoescapes)とは異なり、最大10,000行がバッファリングされ、ブロックではなく単一のテーブルとして出力されます。

## Pretty形式の設定 {#pretty-formats-settings}

- [output_format_pretty_max_rows](/docs/ja/operations/settings/settings-formats.md/#output_format_pretty_max_rows) - Prettyフォーマットの行のリミット。デフォルト値 - `10000`。
- [output_format_pretty_max_column_pad_width](/docs/ja/operations/settings/settings-formats.md/#output_format_pretty_max_column_pad_width) - Prettyフォーマットでカラム内のすべての値をパッドする最大幅。デフォルト値 - `250`。
- [output_format_pretty_max_value_width](/docs/ja/operations/settings/settings-formats.md/#output_format_pretty_max_value_width) - Prettyフォーマットで表示する最大の値の幅。これを超える場合は切り捨てられます。デフォルト値 - `10000`。
- [output_format_pretty_color](/docs/ja/operations/settings/settings-formats.md/#output_format_pretty_color) - PrettyフォーマットでANSIエスケープシーケンスを使用して色を塗る。デフォルト値 - `true`。
- [output_format_pretty_grid_charset](/docs/ja/operations/settings/settings-formats.md/#output_format_pretty_grid_charset) - グリッドの罫線を印刷するための文字セット。使用可能な文字セット: ASCII, UTF-8。デフォルト値 - `UTF-8`。
- [output_format_pretty_row_numbers](/docs/ja/operations/settings/settings-formats.md/#output_format_pretty_row_numbers) - Pretty出力形式で各行の前に行番号を追加する。デフォルト値 - `true`。
- [output_format_pretty_display_footer_column_names](/docs/ja/operations/settings/settings-formats.md/#output_format_pretty_display_footer_column_names) - テーブル内に多くの行がある場合、フッターでカラム名を表示する。デフォルト値 - `true`。
- [output_format_pretty_display_footer_column_names_min_rows](/docs/ja/operations/settings/settings-formats.md/#output_format_pretty_display_footer_column_names_min_rows) - フッターが表示される最小行数を設定します。[output_format_pretty_display_footer_column_names](/docs/ja/operations/settings/settings-formats.md/#output_format_pretty_display_footer_column_names)が有効な場合に適用されます。デフォルト値 - 50。

## RowBinary {#rowbinary}

データを行ごとにバイナリ形式でフォーマットおよび解析します。行と値は区切りなしで列挙されます。データがバイナリ形式であるため、`FORMAT RowBinary`の後の区切り文字は次のように厳密に指定されます：任意の数の空白（スペース`' '` - スペース、コード`0x20`; タブ`'\t'` - タブ、コード`0x09`; 改ページ`'\f'` - 改ページ、コード`0x0C`）に続いて正確に1つの新しい行シーケンス（Windowsスタイル`"\r\n"`またはUnixスタイル`'\n'`）であり、そのすぐ後にバイナリデータがあります。
この形式は、ネイティブ形式よりも効率が悪く、行ベースであるためです。

整数は固定長リトルエンディアン表現を使用します。たとえば、UInt64は8バイトを使用します。
DateTimeはUnixタイムスタンプを値として格納するUInt32として表現されます。
Dateは1970-01-01からの日数を値として格納するUInt16オブジェクトとして表現されます。
Stringは可変長（符号なし[LEB128](https://en.wikipedia.org/wiki/LEB128)）として表現され、文字列のバイトが続きます。
FixedStringは単にバイトのシーケンスとして表されます。

Arrayは可変長（符号なし[LEB128](https://en.wikipedia.org/wiki/LEB128)）として表現され、配列の連続する要素が続きます。

[NULL](/docs/ja/sql-reference/syntax.md/#null-literal)サポートのため、各[Nullable](/docs/ja/sql-reference/data-types/nullable.md)値の前に1または0を含む追加のバイトが追加されます。1の場合、値は`NULL`とされ、このバイトは別の値として解釈されます。0の場合、このバイトの後の値は`NULL`ではありません。

## RowBinaryWithNames {#rowbinarywithnames}

[RowBinary](#rowbinary)と類似しますが、ヘッダーが追加されています:

- [LEB128](https://en.wikipedia.org/wiki/LEB128)でエンコードされたカラム数(N)
- N個の`String`がカラム名を指定

:::note
[settings-formats-input_format_with_names_use_header](/docs/ja/operations/settings/settings-formats.md/#input_format_with_names_use_header)の設定が1に設定されている場合、
入力データのカラムは名前でテーブルのカラムにマッピングされます。未知の名前のカラムは、[input_format_skip_unknown_fields](/docs/ja/operations/settings/settings-formats.md/#input_format_skip_unknown_fields)の設定が1に設定されている場合、スキップされます。
それ以外の場合は、最初の行がスキップされます。
:::

## RowBinaryWithNamesAndTypes {#rowbinarywithnamesandtypes}

[RowBinary](#rowbinary)と類似しますが、ヘッダーが追加されています:

- [LEB128](https://en.wikipedia.org/wiki/LEB128)でエンコードされたカラム数(N)
- N個の`String`がカラム名を指定
- N個の`String`がカラム型を指定

:::note
[settings-formats-input_format_with_names_use_header](/docs/ja/operations/settings/settings-formats.md/#input_format_with_names_use_header)の設定が1に設定されている場合、
入力データのカラムは名前でテーブルのカラムにマッピングされます。未知の名前のカラムは、[input_format_skip_unknown_fields](/docs/ja/operations/settings/settings-formats.md/#input_format_skip_unknown_fields)の設定が1に設定されている場合、スキップされます。
それ以外の場合は、最初の行がスキップされます。
[settings-formats-input_format_with_types_use_header](/docs/ja/operations/settings/settings-formats.md/#input_format_with_types_use_header)の設定が1に設定されている場合、
入力データの型はテーブルの対応するカラムの型と比較されます。そうでない場合、2番目の行はスキップされます。
:::

## RowBinaryWithDefaults {#rowbinarywithdefaults}

[RowBinary](#rowbinary)と類似しますが、各カラムの前にデフォルト値を使用するかどうかを示す追加のバイトがあります。

例:

```sql
:) select * from format('RowBinaryWithDefaults', 'x UInt32 default 42, y UInt32', x'010001000000')

┌──x─┬─y─┐
│ 42 │ 1 │
└────┴───┘
```

カラム`x`の場合、唯一の1バイト`01`がデフォルト値が使用されるべきであることを示し、その後のデータは提供されません。
カラム`y`の場合、データは`00`のバイトで始まり、これはカラムに実際の値があることを示し、その後のデータ`01000000`から読む必要があります。

## RowBinary形式の設定 {#row-binary-format-settings}

- [format_binary_max_string_size](/docs/ja/operations/settings/settings-formats.md/#format_binary_max_string_size) - RowBinary形式でのStringに対する最大許容サイズ。デフォルト値 - `1GiB`。
- [output_format_binary_encode_types_in_binary_format](/docs/ja/operations/settings/settings-formats.md/#output_format_binary_encode_types_in_binary_format) - ヘッダー内での型の名前を持つ文字列の代わりに、[バイナリエンコーディング](/docs/ja/sql-reference/data-types/data-types-binary-encoding.md)を使用してRowBinaryWithNamesAndTypes出力形式で型を記述します。デフォルト値 - `false`。
- [input_format_binary_encode_types_in_binary_format](/docs/ja/operations/settings/settings-formats.md/#input_format_binary_encode_types_in_binary_format) - RowBinaryWithNamesAndTypes入力形式で型名を文字列としてではなく、[バイナリエンコーディング](/docs/ja/sql-reference/data-types/data-types-binary-encoding.md)を使用してヘッダー内の型を読み取ることを許可します。デフォルト値 - `false`.
- [output_format_binary_write_json_as_string](/docs/ja/operations/settings/settings-formats.md/#output_format_binary_write_json_as_string) - RowBinary出力形式で[JSON](/docs/ja/sql-reference/data-types/newjson.md)データ型の値をJSON [String](/docs/ja/sql-reference/data-types/string.md)値として書き込むことを許可します。デフォルト値 - `false`.
- [input_format_binary_read_json_as_string](/docs/ja/operations/settings/settings-formats.md/#input_format_binary_read_json_as_string) - RowBinary入力形式で[JSON](/docs/ja/sql-reference/data-types/newjson.md)データ型の値をJSON [String](/docs/ja/sql-reference/data-types/string.md)値として読み取ることを許可します。デフォルト値 - `false`.

## 値 {#data-format-values}

各行を括弧で囲んで出力します。行はカンマで区切られ、最後の行の後にはカンマはありません。括弧内の値もカンマで区切られています。数値は引用符なしの10進形式で出力されます。配列は角括弧で出力されます。文字列、日付、および時間を伴う日付は引用符で出力されます。エスケープルールと解析は[TabSeparated](#tabseparated)形式と似ています。フォーマット中、余分なスペースは挿入されませんが、解析中には許可されスキップされます（配列値内のスペースを除く、これは許可されません）。[NULL](/docs/ja/sql-reference/syntax.md)は`NULL`として表現されます。

Values形式でデータを渡す際にエスケープが必要な最小限の文字セット：シングルクオートとバックスラッシュ。

この形式は`INSERT INTO t VALUES ...`で使用されますが、クエリの結果をフォーマットするために使用することもできます。

## 値形式の設定 {#values-format-settings}

- [input_format_values_interpret_expressions](/docs/ja/operations/settings/settings-formats.md/#input_format_values_interpret_expressions) - ストリーミングパーサがフィールドを解析できない場合、SQLパーサを実行し、SQL式として解釈しようとします。デフォルト値 - `true`.
- [input_format_values_deduce_templates_of_expressions](/docs/ja/operations/settings/settings-formats.md/#input_format_values_deduce_templates_of_expressions) - ストリーミングパーサがフィールドを解析できない場合、SQLパーサを実行し、SQL式のテンプレートを導出し、テンプレートを使用してすべての行を解析し、すべての行に対して式を解釈しようとします。デフォルト値 - `true`.
- [input_format_values_accurate_types_of_literals](/docs/ja/operations/settings/settings-formats.md/#input_format_values_accurate_types_of_literals) - テンプレートを使用して式を解析し解釈する際に、リテラルの実際の型をチェックして、オーバーフローや精度の問題を回避します。デフォルト値 - `true`.

## 垂直 {#vertical}

各値を指定されたカラム名とともに別の行に出力します。この形式は、各行が多数のカラムで構成されている場合に、1行または数行だけを出力するのに便利です。

[NULL](/docs/ja/sql-reference/syntax.md)は`ᴺᵁᴸᴸ`として出力されます。

例:

``` sql
SELECT * FROM t_null FORMAT Vertical
```

``` response
Row 1:
──────
x: 1
y: ᴺᵁᴸᴸ
```

Vertical形式では行はエスケープされません:

``` sql
SELECT 'string with \'quotes\' and \t with some special \n characters' AS test FORMAT Vertical
```

``` response
Row 1:
──────
test: string with 'quotes' and      with some special
 characters
```

この形式はクエリ結果の出力にのみ適していますが、テーブルに挿入するデータを解析する（取得する）ためには適していません。

## XML {#xml}

XML形式は出力専用であり、解析には適していません。例:

``` xml
<?xml version='1.0' encoding='UTF-8' ?>
<result>
        <meta>
                <columns>
                        <column>
                                <name>SearchPhrase</name>
                                <type>String</type>
                        </column>
                        <column>
                                <name>count()</name>
                                <type>UInt64</type>
                        </column>
                </columns>
        </meta>
        <data>
                <row>
                        <SearchPhrase></SearchPhrase>
                        <field>8267016</field>
                </row>
                <row>
                        <SearchPhrase>bathroom interior design</SearchPhrase>
                        <field>2166</field>
                </row>
                <row>
                        <SearchPhrase>clickhouse</SearchPhrase>
                        <field>1655</field>
                </row>
                <row>
                        <SearchPhrase>2014 spring fashion</SearchPhrase>
                        <field>1549</field>
                </row>
                <row>
                        <SearchPhrase>freeform photos</SearchPhrase>
                        <field>1480</field>
                </row>
                <row>
                        <SearchPhrase>angelina jolie</SearchPhrase>
                        <field>1245</field>
                </row>
                <row>
                        <SearchPhrase>omsk</SearchPhrase>
                        <field>1112</field>
                </row>
                <row>
                        <SearchPhrase>photos of dog breeds</SearchPhrase>
                        <field>1091</field>
                </row>
                <row>
                        <SearchPhrase>curtain designs</SearchPhrase>
                        <field>1064</field>
                </row>
                <row>
                        <SearchPhrase>baku</SearchPhrase>
                        <field>1000</field>
                </row>
        </data>
        <rows>10</rows>
        <rows_before_limit_at_least>141137</rows_before_limit_at_least>
</result>
```

カラム名が受け入れ可能な形式でない場合、単に‘field’が要素名として使用されます。一般に、XML構造はJSON構造に従います。JSONと同様に、無効なUTF-8シーケンスは置換文字�に変更されるため、出力テキストは有効なUTF-8シーケンスで構成されます。

文字列値内の文字`<`と`&`はそれぞれ`<`と`&`としてエスケープされます。

配列は`<array><elem>Hello</elem><elem>World</elem>...</array>`として、タプルは`<tuple><elem>Hello</elem><elem>World</elem>...</tuple>`として出力されます。

## CapnProto {#capnproto}

CapnProtoは、[Protocol Buffers](https://developers.google.com/protocol-buffers/)や[Thrift](https://en.wikipedia.org/wiki/Apache_Thrift)に似たバイナリメッセージ形式ですが、[JSON](#json)や[MessagePack](https://msgpack.org/)とは異なります。

CapnProtoメッセージは厳密に型付けされており、自己記述的ではありません。したがって、外部スキーマの記述が必要です。スキーマはクエリごとにキャッシュされその場で適用されます。

また、[Format Schema](#formatschema)も参照してください。

### データ型のマッチング {#data_types-matching-capnproto}

以下の表は、`INSERT`および`SELECT`クエリにおけるClickHouseの[データ型](/docs/ja/sql-reference/data-types/index.md)と対応するサポートされているデータ型を示しています。

| CapnProto データ型 (`INSERT`)                      | ClickHouse データ型                                                                                                                                                           | CapnProto データ型 (`SELECT`)                      |
|----------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------|
| `UINT8`, `BOOL`                                    | [UInt8](/docs/ja/sql-reference/data-types/int-uint.md)                                                                                                                         | `UINT8`                                            |
| `INT8`                                             | [Int8](/docs/ja/sql-reference/data-types/int-uint.md)                                                                                                                          | `INT8`                                             |
| `UINT16`                                           | [UInt16](/docs/ja/sql-reference/data-types/int-uint.md), [Date](/docs/ja/sql-reference/data-types/date.md)                                                                    | `UINT16`                                           |
| `INT16`                                            | [Int16](/docs/ja/sql-reference/data-types/int-uint.md)                                                                                                                         | `INT16`                                            |
| `UINT32`                                           | [UInt32](/docs/ja/sql-reference/data-types/int-uint.md), [DateTime](/docs/ja/sql-reference/data-types/datetime.md)                                                             | `UINT32`                                           |
| `INT32`                                            | [Int32](/docs/ja/sql-reference/data-types/int-uint.md), [Decimal32](/docs/ja/sql-reference/data-types/decimal.md)                                                              | `INT32`                                            |
| `UINT64`                                           | [UInt64](/docs/ja/sql-reference/data-types/int-uint.md)                                                                                                                        | `UINT64`                                           |
| `INT64`                                            | [Int64](/docs/ja/sql-reference/data-types/int-uint.md), [DateTime64](/docs/ja/sql-reference/data-types/datetime.md), [Decimal64](/docs/ja/sql-reference/data-types/decimal.md) | `INT64`                                            |
| `FLOAT32`                                          | [Float32](/docs/ja/sql-reference/data-types/float.md)                                                                                                                          | `FLOAT32`                                          |
| `FLOAT64`                                          | [Float64](/docs/ja/sql-reference/data-types/float.md)                                                                                                                          | `FLOAT64`                                          |
| `TEXT, DATA`                                       | [String](/docs/ja/sql-reference/data-types/string.md), [FixedString](/docs/ja/sql-reference/data-types/fixedstring.md)                                                         | `TEXT, DATA`                                       |
| `union(T, Void), union(Void, T)`                   | [Nullable(T)](/docs/ja/sql-reference/data-types/date.md)                                                                                                                       | `union(T, Void), union(Void, T)`                   |
| `ENUM`                                             | [Enum(8/16)](/docs/ja/sql-reference/data-types/enum.md)                                                                                                                        | `ENUM`                                             |
| `LIST`                                             | [Array](/docs/ja/sql-reference/data-types/array.md)                                                                                                                            | `LIST`                                             |
| `STRUCT`                                           | [Tuple](/docs/ja/sql-reference/data-types/tuple.md)                                                                                                                            | `STRUCT`                                           |
| `UINT32`                                           | [IPv4](/docs/ja/sql-reference/data-types/ipv4.md)                                                                                                                              | `UINT32`                                           |
| `DATA`                                             | [IPv6](/docs/ja/sql-reference/data-types/ipv6.md)                                                                                                                              | `DATA`                                             |
| `DATA`                                             | [Int128/UInt128/Int256/UInt256](/docs/ja/sql-reference/data-types/int-uint.md)                                                                                                 | `DATA`                                             |
| `DATA`                                             | [Decimal128/Decimal256](/docs/ja/sql-reference/data-types/decimal.md)                                                                                                          | `DATA`                                             |
| `STRUCT(entries LIST(STRUCT(key Key, value Value)))` | [Map](/docs/ja/sql-reference/data-types/map.md)                                                                                                                                | `STRUCT(entries LIST(STRUCT(key Key, value Value)))` |

整数型は入力/出力の際に相互に変換可能です。

CapnProto形式で`Enum`を扱うには[format_capn_proto_enum_comparising_mode](/docs/ja/operations/settings/settings-formats.md/#format_capn_proto_enum_comparising_mode)設定を使用してください。

配列はネスト可能であり、`Nullable`型を引数として持つことができます。`Tuple`と`Map`型もネストできます。

### データの挿入と選択 {#inserting-and-selecting-data-capnproto}

ファイルからCapnProtoデータをClickHouseテーブルに挿入するには、以下のコマンドを使用します:

``` bash
$ cat capnproto_messages.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_schema = 'schema:Message' FORMAT CapnProto"
```

ここで、`schema.capnp`は以下のようになります:

``` capnp
struct Message {
  SearchPhrase @0 :Text;
  c @1 :Uint64;
}
```

ClickHouseテーブルからデータを選択し、CapnProto形式でファイルに保存するには、以下のコマンドを使用します:

``` bash
$ clickhouse-client --query = "SELECT * FROM test.hits FORMAT CapnProto SETTINGS format_schema = 'schema:Message'"
```

### 自動生成スキーマの使用 {#using-autogenerated-capn-proto-schema}

データの外部CapnProtoスキーマがない場合でも、自動生成スキーマを使用してCapnProto形式でデータを入力/出力することができます。例:

```sql
SELECT * FROM test.hits format CapnProto SETTINGS format_capn_proto_use_autogenerated_schema=1
```

この場合、ClickHouseはテーブルの構造に従ってCapnProtoスキーマを自動生成し、このスキーマを使用してCapnProto形式でデータをシリアル化します。

自動生成スキーマでCapnProtoファイルを読み込むこともできます（この場合、ファイルは同じスキーマを使用して作成する必要があります）:

```bash
$ cat hits.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_capn_proto_use_autogenerated_schema=1 FORMAT CapnProto"
```

設定[format_capn_proto_use_autogenerated_schema](../operations/settings/settings-formats.md#format_capn_proto_use_autogenerated_schema)はデフォルトで有効になっており、[format_schema](../operations/settings/settings-formats.md#formatschema-format-schema)が設定されていない場合に適用されます。

入力/出力中に自動生成スキーマをファイルに保存することもでき、設定[output_format_schema](../operations/settings/settings-formats.md#outputformatschema-output-format-schema)を使用します。例:

```sql
SELECT * FROM test.hits format CapnProto SETTINGS format_capn_proto_use_autogenerated_schema=1, output_format_schema='path/to/schema/schema.capnp'
```

この場合、自動生成されたCapnProtoスキーマはファイル`path/to/schema/schema.capnp`に保存されます。

## Prometheus {#prometheus}

[Prometheusのテキスト形式](https://prometheus.io/docs/instrumenting/exposition_formats/#text-based-format)でメトリックを公開します。

出力テーブルには適切な構造が必要です。
カラム`name`（[String](/docs/ja/sql-reference/data-types/string.md)）と`value`（数値）は必須です。
行にはオプションで`help`（[String](/docs/ja/sql-reference/data-types/string.md)）と`timestamp`（数値）を含めることができます。
カラム`type`（[String](/docs/ja/sql-reference/data-types/string.md)）は`counter`、`gauge`、`histogram`、`summary`、`untyped`または空です。
各メトリックの値にはいくつかの`labels`（[Map(String, String)](/docs/ja/sql-reference/data-types/map.md)）を持たせることができます。
いくつかの連続した行は、異なるラベルで1つのメトリックに参照される場合があります。テーブルは（例：`ORDER BY name`で）メトリック名でソートする必要があります。

[`histogram`]と[`summary`]のラベルには特別な要件があります。詳細は[Prometheus文書](https://prometheus.io/docs/instrumenting/exposition_formats/#histograms-and-summaries)を参照してください。ラベルが`{'count':''}`および`{'sum':''}`である行には特別なルールが適用され、それぞれ`<metric_name>_count`と`<metric_name>_sum`に変換されます。

**例:**

```
┌─name────────────────────────────────┬─type──────┬─help──────────────────────────────────────┬─labels─────────────────────────┬────value─┬─────timestamp─┐
│ http_request_duration_seconds       │ histogram │ A histogram of the request duration.      │ {'le':'0.05'}                  │    24054 │             0 │
│ http_request_duration_seconds       │ histogram │                                           │ {'le':'0.1'}                   │    33444 │             0 │
│ http_request_duration_seconds       │ histogram │                                           │ {'le':'0.2'}                   │   100392 │             0 │
│ http_request_duration_seconds       │ histogram │                                           │ {'le':'0.5'}                   │   129389 │             0 │
│ http_request_duration_seconds       │ histogram │                                           │ {'le':'1'}                     │   133988 │             0 │
│ http_request_duration_seconds       │ histogram │                                           │ {'le':'+Inf'}                  │   144320 │             0 │
│ http_request_duration_seconds       │ histogram │                                           │ {'sum':''}                     │    53423 │             0 │
│ http_requests_total                 │ counter   │ Total number of HTTP requests             │ {'method':'post','code':'200'} │     1027 │ 1395066363000 │
│ http_requests_total                 │ counter   │                                           │ {'method':'post','code':'400'} │        3 │ 1395066363000 │
│ metric_without_timestamp_and_labels │           │                                           │ {}                             │    12.47 │             0 │
│ rpc_duration_seconds                │ summary   │ A summary of the RPC duration in seconds. │ {'quantile':'0.01'}            │     3102 │             0 │
│ rpc_duration_seconds                │ summary   │                                           │ {'quantile':'0.05'}            │     3272 │             0 │
│ rpc_duration_seconds                │ summary   │                                           │ {'quantile':'0.5'}             │     4773 │             0 │
│ rpc_duration_seconds                │ summary   │                                           │ {'quantile':'0.9'}             │     9001 │             0 │
│ rpc_duration_seconds                │ summary   │                                           │ {'quantile':'0.99'}            │    76656 │             0 │
│ rpc_duration_seconds                │ summary   │                                           │ {'count':''}                   │     2693 │             0 │
│ rpc_duration_seconds                │ summary   │                                           │ {'sum':''}                     │ 17560473 │             0 │
│ something_weird                     │           │                                           │ {'problem':'division by zero'} │      inf │      -3982045 │
└─────────────────────────────────────┴───────────┴───────────────────────────────────────────┴────────────────────────────────┴──────────┴───────────────┘
```

次のようにフォーマットされます:

```
# HELP http_request_duration_seconds A histogram of the request duration.
# TYPE http_request_duration_seconds histogram
http_request_duration_seconds_bucket{le="0.05"} 24054
http_request_duration_seconds_bucket{le="0.1"} 33444
http_request_duration_seconds_bucket{le="0.5"} 129389
http_request_duration_seconds_bucket{le="1"} 133988
http_request_duration_seconds_bucket{le="+Inf"} 144320
http_request_duration_seconds_sum 53423
http_request_duration_seconds_count 144320

# HELP http_requests_total Total number of HTTP requests
# TYPE http_requests_total counter
http_requests_total{code="200",method="post"} 1027 1395066363000
http_requests_total{code="400",method="post"} 3 1395066363000

metric_without_timestamp_and_labels 12.47

# HELP rpc_duration_seconds A summary of the RPC duration in seconds.
# TYPE rpc_duration_seconds summary
rpc_duration_seconds{quantile="0.01"} 3102
rpc_duration_seconds{quantile="0.05"} 3272
rpc_duration_seconds{quantile="0.5"} 4773
rpc_duration_seconds{quantile="0.9"} 9001
rpc_duration_seconds{quantile="0.99"} 76656
rpc_duration_seconds_sum 17560473
rpc_duration_seconds_count 2693

something_weird{problem="division by zero"} +Inf -3982045
```

## Protobuf {#protobuf}

Protobufとは、[Protocol Buffers](https://protobuf.dev/)形式です。

この形式は外部フォーマットスキーマが必要です。スキーマはクエリ間でキャッシュされます。
ClickHouseは`proto2`と`proto3`の両方のシンタックスをサポートしています。繰り返し/オプション/必須フィールドがサポートされています。

使用例:

``` sql
SELECT * FROM test.table FORMAT Protobuf SETTINGS format_schema = 'schemafile:MessageType'
```

``` bash
cat protobuf_messages.bin | clickhouse-client --query "INSERT INTO test.table SETTINGS format_schema='schemafile:MessageType' FORMAT Protobuf"
```

ファイル`schemafile.proto`は以下のようになります:

``` capnp
syntax = "proto3";

message MessageType {
  string name = 1;
  string surname = 2;
  uint32 birthDate = 3;
  repeated string phoneNumbers = 4;
};
```

テーブルカラムとProtocol Buffersのメッセージ型のフィールドの間の対応を見つけるために、ClickHouseはその名前を比較します。
この比較は大文字と小文字を区別せず、`_`（アンダースコア）と`.`（ドット）は等しいとみなされます。
カラムとProtocol Buffersのメッセージのフィールドの型が異なる場合は、必要な変換が適用されます。

ネストされたメッセージがサポートされています。以下のメッセージタイプでフィールド `z` の場合

``` capnp
message MessageType {
  message XType {
    message YType {
      int32 z;
    };
    repeated YType y;
  };
  XType x;
};
```

ClickHouseは`x.y.z`（または`x_y_z`や`X.y_Z`など）という名前のカラムを探そうとします。
ネストされたメッセージは、[ネストされたデータ構造](/docs/ja/sql-reference/data-types/nested-data-structures/index.md)を入力または出力するのに適しています。

プロトコルバッファのスキーマで次のように定義されたデフォルト値

``` capnp
syntax = "proto2";

message MessageType {
  optional int32 result_per_page = 3 [default = 10];
}
```

は適用されず、[テーブルデフォルト](/docs/ja/sql-reference/statements/create/table.md/#create-default-values)が代わりに使用されます。

ClickHouseは`length-delimited`形式でプロトコルバッファメッセージを入出力します。
つまり、各メッセージの前に、その長さが[varint](https://developers.google.com/protocol-buffers/docs/encoding#varints)として書かれている必要があります。
また、[一般的な言語で長さ区切りのプロトコルバッファメッセージを読み書きする方法](https://cwiki.apache.org/confluence/display/GEODE/Delimiting+Protobuf+Messages)も参照してください。

### 自動生成スキーマの使用 {#using-autogenerated-protobuf-schema}

データの外部プロトコルバッファスキーマがない場合でも、自動生成スキーマを使用してプロトコルバッファ形式でデータを入力/出力することができます。例:

```sql
SELECT * FROM test.hits format Protobuf SETTINGS format_protobuf_use_autogenerated_schema=1
```

この場合、ClickHouseはテーブルの構造に従ってプロトコルバッファスキーマを自動生成し、このスキーマを使用してプロトコルバッファ形式でデータをシリアル化します。

自動生成スキーマでプロトコルバッファファイルを読み込むこともできます（この場合、ファイルは同じスキーマを使用して作成する必要があります）:

```bash
$ cat hits.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_protobuf_use_autogenerated_schema=1 FORMAT Protobuf"
```

設定[format_protobuf_use_autogenerated_schema](../operations/settings/settings-formats.md#format_protobuf_use_autogenerated_schema)はデフォルトで有効になっており、[format_schema](../operations/settings/settings-formats.md#formatschema-format-schema)が設定されていない場合に適用されます。

入力/出力中に自動生成スキーマをファイルに保存することもでき、設定[output_format_schema](../operations/settings/settings-formats.md#outputformatschema-output-format-schema)を使用します。例:

```sql
SELECT * FROM test.hits format Protobuf SETTINGS format_protobuf_use_autogenerated_schema=1, output_format_schema='path/to/schema/schema.proto'
```

この場合、自動生成されたプロトコルバッファスキーマはファイル`path/to/schema/schema.capnp`に保存されます。

### プロトコルバッファキャッシュの削除

[format_schema_path](../operations/server-configuration-parameters/settings.md/#format_schema_path)からロードしたプロトコルバッファスキーマをリロードするには、[SYSTEM DROP ... FORMAT CACHE](../sql-reference/statements/system.md/#system-drop-schema-format)ステートメントを使用します。

```sql
SYSTEM DROP FORMAT SCHEMA CACHE FOR Protobuf
```

## ProtobufSingle {#protobufsingle}

[Protobuf](#protobuf)と同様ですが、長さ区切りがない単一のプロトコルバッファメッセージを保存/解析するためのものです。

## ProtobufList {#protobuflist}

Protobufに似ていますが、行は固定名が「Envelope」であるメッセージ内のサブメッセージのシーケンスとして表現されます。

使用例:

``` sql
SELECT * FROM test.table FORMAT ProtobufList SETTINGS format_schema = 'schemafile:MessageType'
```

``` bash
cat protobuflist_messages.bin | clickhouse-client --query "INSERT INTO test.table FORMAT ProtobufList SETTINGS format_schema='schemafile:MessageType'"
```

ファイル`schemafile.proto`は以下のようになります:

``` capnp
syntax = "proto3";
message Envelope {
  message MessageType {
    string name = 1;
    string surname = 2;
    uint32 birthDate = 3;
    repeated string phoneNumbers = 4;
  };
  MessageType row = 1;
};
```

## Avro {#data-format-avro}

[Apache Avro](https://avro.apache.org/)は、ApacheのHadoopプロジェクト内で開発された行指向のデータシリアル化フレームワークです。

ClickHouseのAvro形式は[Avroデータファイル](https://avro.apache.org/docs/current/spec.html#Object+Container+Files)の読み取りおよび書き込みをサポートしています。

### データ型のマッチング {#data_types-matching}

以下の表は、`INSERT`および`SELECT`クエリにおけるClickHouseの[データ型](/docs/ja/sql-reference/data-types/index.md)とのマッチングを示しています。

| Avro データ型 `INSERT`                           | ClickHouse データ型                                                                                                            | Avro データ型 `SELECT`         |
|-------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|--------------------------------|
| `boolean`, `int`, `long`, `float`, `double`     | [Int(8\16\32)](/docs/ja/sql-reference/data-types/int-uint.md), [UInt(8\16\32)](/docs/ja/sql-reference/data-types/int-uint.md) | `int`                          |
| `boolean`, `int`, `long`, `float`, `double`     | [Int64](/docs/ja/sql-reference/data-types/int-uint.md), [UInt64](/docs/ja/sql-reference/data-types/int-uint.md)               | `long`                         |
| `boolean`, `int`, `long`, `float`, `double`     | [Float32](/docs/ja/sql-reference/data-types/float.md)                                                                         | `float`                        |
| `boolean`, `int`, `long`, `float`, `double`     | [Float64](/docs/ja/sql-reference/data-types/float.md)                                                                         | `double`                       |
| `bytes`, `string`, `fixed`, `enum`              | [String](/docs/ja/sql-reference/data-types/string.md)                                                                         | `bytes` または `string` \*     |
| `bytes`, `string`, `fixed`                      | [FixedString(N)](/docs/ja/sql-reference/data-types/fixedstring.md)                                                            | `fixed(N)`                     |
| `enum`                                          | [Enum(8\16)](/docs/ja/sql-reference/data-types/enum.md)                                                                       | `enum`                         |
| `array(T)`                                      | [Array(T)](/docs/ja/sql-reference/data-types/array.md)                                                                        | `array(T)`                     |
| `map(V, K)`                                     | [Map(V, K)](/docs/ja/sql-reference/data-types/map.md)                                                                         | `map(string, K)`               |
| `union(null, T)`, `union(T, null)`              | [Nullable(T)](/docs/ja/sql-reference/data-types/date.md)                                                                      | `union(null, T)`               |
| `union(T1, T2, …)` \**                          | [Variant(T1, T2, …)](/docs/ja/sql-reference/data-types/variant.md)                                                            | `union(T1, T2, …)` \**         |
| `null`                                          | [Nullable(Nothing)](/docs/ja/sql-reference/data-types/special-data-types/nothing.md)                                          | `null`                         |
| `int (date)` \**\*                              | [Date](/docs/ja/sql-reference/data-types/date.md), [Date32](docs/en/sql-reference/data-types/date32.md)                       | `int (date)` \**\*             |
| `long (timestamp-millis)` \**\*                 | [DateTime64(3)](/docs/ja/sql-reference/data-types/datetime.md)                                                                | `long (timestamp-millis)` \**\*|
| `long (timestamp-micros)` \**\*                 | [DateTime64(6)](/docs/ja/sql-reference/data-types/datetime.md)                                                                | `long (timestamp-micros)` \**\*|
| `bytes (decimal)`  \**\*                        | [DateTime64(N)](/docs/ja/sql-reference/data-types/datetime.md)                                                                | `bytes (decimal)`  \**\*       |
| `int`                                           | [IPv4](/docs/ja/sql-reference/data-types/ipv4.md)                                                                             | `int`                          |
| `fixed(16)`                                     | [IPv6](/docs/ja/sql-reference/data-types/ipv6.md)                                                                             | `fixed(16)`                    |
| `bytes (decimal)` \**\*                         | [Decimal(P, S)](/docs/ja/sql-reference/data-types/decimal.md)                                                                 | `bytes (decimal)` \**\*        |
| `string (uuid)` \**\*                           | [UUID](/docs/ja/sql-reference/data-types/uuid.md)                                                                             | `string (uuid)` \**\*          |
| `fixed(16)`                                     | [Int128/UInt128](/docs/ja/sql-reference/data-types/int-uint.md)                                                               | `fixed(16)`                    |
| `fixed(32)`                                     | [Int256/UInt256](/docs/ja/sql-reference/data-types/int-uint.md)                                                               | `fixed(32)`                    |
| `record`                                        | [Tuple](/docs/ja/sql-reference/data-types/tuple.md)                                                                           | `record`                       |

\* `bytes`はデフォルトです。これは[output_format_avro_string_column_pattern](/docs/ja/operations/settings/settings-formats.md/#output_format_avro_string_column_pattern)によって制御されます。

\** [Variant型](/docs/ja/sql-reference/data-types/variant)はフィールド値として`null`を暗黙的に受け入れるため、例えばAvro `union(T1, T2, null)`は`Variant(T1, T2)`に変換されます。
結果として、ClickHouseからAvroを生成する際には、スキーマ推論中に任意の値が実際に`null`かどうか不明なため、`union`型セットに常に`null`型を含める必要があります。

\**\* [Avro論理型](https://avro.apache.org/docs/current/spec.html#Logical+Types)

サポートされていないAvro論理データ型：`time-millis`, `time-micros`, `duration`

### データ挿入 {#inserting-data-1}

AvroファイルからClickHouseテーブルにデータを挿入するには:

``` bash
$ cat file.avro | clickhouse-client --query="INSERT INTO {some_table} FORMAT Avro"
```

入力Avroファイルのルートスキーマは`record`型でなければなりません。

テーブルカラムとAvroスキーマのフィールドの間の対応を見つけるために、ClickHouseはその名前を比較します。比較は大文字と小文字を区別します。
使用されていないフィールドはスキップされます。

ClickHouseテーブルカラムのデータ型は、挿入されるAvroデータの対応するフィールドの型と異なる可能性があります。データを挿入する際、ClickHouseは上記の表に従ってデータ型を解釈し、その後、ClickHouseテーブルカラムに対応する型にデータを[キャスト](/docs/ja/sql-reference/functions/type-conversion-functions.md/#type_conversion_function-cast)します。

データをインポートする際、スキーマ内でフィールドが見つからず、設定[inform_format_avro_allow_missing_fields](/docs/ja/operations/settings/settings-formats.md/#input_format_avro_allow_missing_fields)が有効の場合、デフォルト値がエラーの代わりに使用されます。

### データ選択 {#selecting-data-1}

ClickHouseテーブルからAvroファイルにデータを選択するには:

``` bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Avro" > file.avro
```

カラム名は以下を満たす必要があります：

- `[A-Za-z_]`で始まる
- 次に`[A-Za-z0-9_]`のみを含む

出力Avroファイルの圧縮と同期インターバルは[output_format_avro_codec](/docs/ja/operations/settings/settings-formats.md/#output_format_avro_codec)と[output_format_avro_sync_interval](/docs/ja/operations/settings/settings-formats.md/#output_format_avro_sync_interval)でそれぞれ設定できます。

### データ例 {#example-data-avro}

ClickHouseの[DESCRIBE](/docs/ja/sql-reference/statements/describe-table)機能を使用して、次の例のようなAvroファイルの推論形式をすばやく表示できます。この例にはClickHouse S3パブリックバケット内の公開アクセス可能なAvroファイルのURLが含まれています：

```
DESCRIBE url('https://clickhouse-public-datasets.s3.eu-central-1.amazonaws.com/hits.avro','Avro);
```
```
┌─name───────────────────────┬─type────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ WatchID                    │ Int64           │              │                    │         │                  │                │
│ JavaEnable                 │ Int32           │              │                    │         │                  │                │
│ Title                      │ String          │              │                    │         │                  │                │
│ GoodEvent                  │ Int32           │              │                    │         │                  │                │
│ EventTime                  │ Int32           │              │                    │         │                  │                │
│ EventDate                  │ Date32          │              │                    │         │                  │                │
│ CounterID                  │ Int32           │              │                    │         │                  │                │
│ ClientIP                   │ Int32           │              │                    │         │                  │                │
│ ClientIP6                  │ FixedString(16) │              │                    │         │                  │                │
│ RegionID                   │ Int32           │              │                    │         │                  │                │
...
│ IslandID                   │ FixedString(16) │              │                    │         │                  │                │
│ RequestNum                 │ Int32           │              │                    │         │                  │                │
│ RequestTry                 │ Int32           │              │                    │         │                  │                │
└────────────────────────────┴─────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

## AvroConfluent {#data-format-avro-confluent}

AvroConfluentは、[Kafka](https://kafka.apache.org/)および[Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html)で一般的に使用されるシングルオブジェクトAvroメッセージのデコードをサポートしています。

各AvroメッセージにはスキーマIDが埋め込まれており、スキーマレジストリの助けを借りて実際のスキーマに解決できます。

スキーマは一度解決されるとキャッシュされます。

スキーマレジストリURLは[format_avro_schema_registry_url](/docs/ja/operations/settings/settings-formats.md/#format_avro_schema_registry_url)で設定されます。

### データ型のマッチング {#data_types-matching-1}

[Avro](#data-format-avro)と同様です。

### 使用方法 {#usage}

スキーマ解決をすばやく確認するには、[kafkacat](https://github.com/edenhill/kafkacat)と[clickhouse-local](/docs/ja/operations/utilities/clickhouse-local.md)を使用できます：

``` bash
$ kafkacat -b kafka-broker  -C -t topic1 -o beginning -f '%s' -c 3 | clickhouse-local --input-format AvroConfluent --format_avro_schema_registry_url 'http://schema-registry' -S "field1 Int64, field2 String"  -q 'select * from table'
1 a
2 b
3 c
```

[Kafka](/docs/ja/engines/table-engines/integrations/kafka.md)で`AvroConfluent`を使用するには：

``` sql
CREATE TABLE topic1_stream
(
    field1 String,
    field2 String
)
ENGINE = Kafka()
SETTINGS
kafka_broker_list = 'kafka-broker',
kafka_topic_list = 'topic1',
kafka_group_name = 'group1',
kafka_format = 'AvroConfluent';

-- デバッグ目的の場合、セッションでformat_avro_schema_registry_urlを設定できます。
-- この方法は本番環境では使用できません。
SET format_avro_schema_registry_url = 'http://schema-registry';

SELECT * FROM topic1_stream;
```

:::note
`format_avro_schema_registry_url`の設定値は再起動後も維持するために`users.xml`で設定する必要があります。また、`Kafka`テーブルエンジンの`format_avro_schema_registry_url`設定を使用することもできます。
:::

## Parquet {#data-format-parquet}

[Apache Parquet](https://parquet.apache.org/)は、Hadoopエコシステムで広く使用されている列指向のストレージ形式です。ClickHouseはこの形式の読み取りおよび書き込み操作をサポートしています。

### データ型のマッチング {#data-types-matching-parquet}

以下の表は、`INSERT`および`SELECT`クエリにおけるClickHouseの[データ型](/docs/ja/sql-reference/data-types/index.md)とのマッチングを示しています。

| Parquet データ型 (`INSERT`)                    | ClickHouse データ型                                                                                       | Parquet データ型 (`SELECT`)  |
|-----------------------------------------------|------------------------------------------------------------------------------------------------------------|-------------------------------|
| `BOOL`                                        | [Bool](/docs/ja/sql-reference/data-types/boolean.md)                                                       | `BOOL`                        |
| `UINT8`, `BOOL`                               | [UInt8](/docs/ja/sql-reference/data-types/int-uint.md)                                                     | `UINT8`                       |
| `INT8`                                        | [Int8](/docs/ja/sql-reference/data-types/int-uint.md)/[Enum8](/docs/ja/sql-reference/data-types/enum.md)   | `INT8`                        |
| `UINT16`                                      | [UInt16](/docs/ja/sql-reference/data-types/int-uint.md)                                                    | `UINT16`                      |
| `INT16`                                       | [Int16](/docs/ja/sql-reference/data-types/int-uint.md)/[Enum16](/docs/ja/sql-reference/data-types/enum.md) | `INT16`                       |
| `UINT32`                                      | [UInt32](/docs/ja/sql-reference/data-types/int-uint.md)                                                    | `UINT32`                      |
| `INT32`                                       | [Int32](/docs/ja/sql-reference/data-types/int-uint.md)                                                     | `INT32`                       |
| `UINT64`                                      | [UInt64](/docs/ja/sql-reference/data-types/int-uint.md)                                                    | `UINT64`                      |
| `INT64`                                       | [Int64](/docs/ja/sql-reference/data-types/int-uint.md)                                                     | `INT64`                       |
| `FLOAT`                                       | [Float32](/docs/ja/sql-reference/data-types/float.md)                                                      | `FLOAT`                       |
| `DOUBLE`                                      | [Float64](/docs/ja/sql-reference/data-types/float.md)                                                      | `DOUBLE`                      |
| `DATE`                                        | [Date32](/docs/ja/sql-reference/data-types/date.md)                                                        | `DATE`                        |
| `TIME (ms)`                                   | [DateTime](/docs/ja/sql-reference/data-types/datetime.md)                                                  | `UINT32`                      |
| `TIMESTAMP`, `TIME (us, ns)`                  | [DateTime64](/docs/ja/sql-reference/data-types/datetime64.md)                                              | `TIMESTAMP`                   |
| `STRING`, `BINARY`                            | [String](/docs/ja/sql-reference/data-types/string.md)                                                      | `BINARY`                      |
| `STRING`, `BINARY`, `FIXED_LENGTH_BYTE_ARRAY` | [FixedString](/docs/ja/sql-reference/data-types/fixedstring.md)                                            | `FIXED_LENGTH_BYTE_ARRAY`     |
| `DECIMAL`                                     | [Decimal](/docs/ja/sql-reference/data-types/decimal.md)                                                    | `DECIMAL`                     |
| `LIST`                                        | [Array](/docs/ja/sql-reference/data-types/array.md)                                                        | `LIST`                        |
| `STRUCT`                                      | [Tuple](/docs/ja/sql-reference/data-types/tuple.md)                                                        | `STRUCT`                      |
| `MAP`                                         | [Map](/docs/ja/sql-reference/data-types/map.md)                                                            | `MAP`                         |
| `UINT32`                                      | [IPv4](/docs/ja/sql-reference/data-types/ipv4.md)                                                          | `UINT32`                      |
| `FIXED_LENGTH_BYTE_ARRAY`, `BINARY`           | [IPv6](/docs/ja/sql-reference/data-types/ipv6.md)                                                          | `FIXED_LENGTH_BYTE_ARRAY`     |
| `FIXED_LENGTH_BYTE_ARRAY`, `BINARY`           | [Int128/UInt128/Int256/UInt256](/docs/ja/sql-reference/data-types/int-uint.md)                             | `FIXED_LENGTH_BYTE_ARRAY`     |

配列はネスト可能であり、`Nullable`型を引数として持つことができます。`Tuple`と`Map`型もネストできます。

サポートされていないParquetデータ型：`FIXED_SIZE_BINARY`, `JSON`, `UUID`, `ENUM`。

ClickHouseテーブルカラムのデータ型は、挿入されるParquetデータの対応するフィールドのデータ型と異なる可能性があります。データを挿入する際、ClickHouseは上記の表に従ってデータ型を解釈し、その後、[キャスト](/docs/ja/sql-reference/functions/type-conversion-functions/#type_conversion_function-cast)を行い、ClickHouseテーブルカラムに設定されたデータ型に変換します。

### データの挿入と選択 {#inserting-and-selecting-data-parquet}

ファイルからParquetデータをClickHouseテーブルに挿入するには、以下のコマンドを使用します:

``` bash
$ cat {filename} | clickhouse-client --query="INSERT INTO {some_table} FORMAT Parquet"
```

ClickHouseテーブルからデータを選択し、それをParquet形式でファイルに保存するには、以下のコマンドを使用します:

``` bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Parquet" > {some_file.pq}
```

Hadoopとのデータ交換には、[HDFSテーブルエンジン](/docs/ja/engines/table-engines/integrations/hdfs.md)を使用できます。

### Parquet形式の設定 {#parquet-format-settings}

- [output_format_parquet_row_group_size](/docs/ja/operations/settings/settings-formats.md/#output_format_parquet_row_group_size) - データ出力の際の行グループサイズ（行単位）。デフォルト値 - `1000000`.
- [output_format_parquet_string_as_string](/docs/ja/operations/settings/settings-formats.md/#output_format_parquet_string_as_string) - Parquet String型を使用してStringカラムをBinaryでなく出力します。デフォルト値 - `false`.
- [input_format_parquet_import_nested](/docs/ja/operations/settings/settings-formats.md/#input_format_parquet_import_nested) - Parquet入力形式でネストされたデータを[Nested](/docs/ja/sql-reference/data-types/nested-data-structures/index.md)テーブルに挿入することを許可します。デフォルト値 - `false`.
- [input_format_parquet_case_insensitive_column_matching](/docs/ja/operations/settings/settings-formats.md/#input_format_parquet_case_insensitive_column_matching) - ParquetカラムとClickHouseカラムの照合を大文字と小文字を区別せずに処理します。デフォルト値 - `false`.
- [input_format_parquet_allow_missing_columns](/docs/ja/operations/settings/settings-formats.md/#input_format_parquet_allow_missing_columns) - Parquetデータを読み取る際にカラムが欠損しても許可します。デフォルト値 - `false`.
- [input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference](/docs/ja/operations/settings/settings-formats.md/#input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference) - Parquet形式のスキーマ推論中にサポートされていない型のカラムをスキップすることを許可します。デフォルト値 - `false`.
- [input_format_parquet_local_file_min_bytes_for_seek](/docs/ja/operations/settings/settings-formats.md/#input_format_parquet_local_file_min_bytes_for_seek) - Parquet入力形式でシークを実行する際に必要な最小バイト数（ローカル読み取りファイル）を定義します。デフォルト値 - `8192`.
- [output_format_parquet_fixed_string_as_fixed_byte_array](/docs/ja/operations/settings/settings-formats.md/#output_format_parquet_fixed_string_as_fixed_byte_array) - FixedStringカラムにParquet FIXED_LENGTH_BYTE_ARRAY型を使用します。デフォルト値 - `true`.
- [output_format_parquet_version](/docs/ja/operations/settings/settings-formats.md/#output_format_parquet_version) - 出力形式で使用されるParquetフォーマットのバージョン。デフォルト値 - `2.latest`.
- [output_format_parquet_compression_method](/docs/ja/operations/settings/settings-formats.md/#output_format_parquet_compression_method) - 出力Parquet形式に使用される圧縮方法。デフォルト値 - `lz4`.
- [input_format_parquet_max_block_size](/docs/ja/operations/settings/settings-formats.md/#input_format_parquet_max_block_size) - Parquetリーダーの最大ブロック行サイズ。デフォルト値 - `65409`.
- [input_format_parquet_prefer_block_bytes](/docs/ja/operations/settings/settings-formats.md/#input_format_parquet_prefer_block_bytes) - Parquetリーダーが出力する平均ブロックバイト数。デフォルト値 - `16744704`.
- [output_format_parquet_write_page_index](/docs/ja/operations/settings/settings-formats.md/#input_format_parquet_max_block_size) - Parquetファイルにページインデックスを書き込む可能性を追加します。現在は`output_format_parquet_use_custom_encoder`を無効にする必要があります。デフォルト値 - `true`.

## ParquetMetadata {data-format-parquet-metadata}

Parquetファイルメタデータ(https://parquet.apache.org/docs/file-format/metadata/)を読むための特別な形式です。常に以下の構造/内容で1行を出力します:
- num_columns - カラムの数
- num_rows - 合計行数
- num_row_groups - 合計行グループ数
- format_version - parquetフォーマットのバージョン、常に1.0または2.6
- total_uncompressed_size - データの総非圧縮バイトサイズ、すべての行グループのtotal_byte_sizeの合計として計算
- total_compressed_size - データの総圧縮バイトサイズ、すべての行グループのtotal_compressed_sizeの合計として計算
- columns - 次の構造を持つカラムメタデータのリスト:
  - name - カラム名
  - path - カラムパス（ネストされたカラムの場合にはnameと異なる）
  - max_definition_level - 最大定義レベル
  - max_repetition_level - 最大繰り返しレベル
  - physical_type - カラムの物理型
  - logical_type - カラムの論理型
- compression - このカラムで使用されている圧縮
- total_uncompressed_size - 行グループすべてのカラムの total_uncompressed_size を合計して得られる、カラムの総非圧縮バイトサイズ
- total_compressed_size - 行グループすべてのカラムの total_compressed_size を合計して得られる、カラムの総圧縮バイトサイズ
- space_saved - 圧縮によって節約されたスペースの割合（パーセント）、(1 - total_compressed_size/total_uncompressed_size)として計算される
- encodings - このカラムで使用されるエンコーディングのリスト
- row_groups - 次の構造を持つ行グループのメタデータリスト:
  - num_columns - 行グループ内のカラム数
  - num_rows - 行グループ内の行数
  - total_uncompressed_size - 行グループの総非圧縮バイトサイズ
  - total_compressed_size - 行グループの総圧縮バイトサイズ
  - columns - 次の構造を持つカラムチャンクメタデータのリスト:
    - name - カラム名
    - path - カラムパス
    - total_compressed_size - カラムの総圧縮バイトサイズ
    - total_uncompressed_size - 行グループの総非圧縮バイトサイズ
    - have_statistics - カラムチャンクメタデータにカラム統計が含まれるかどうかを示す真偽値
    - statistics - カラムチャンクの統計情報 (have_statistics が false の場合、すべてのフィールドは NULL) 次の構造を持つ:
      - num_values - カラムチャンク内の非 NULL 値の数
      - null_count - カラムチャンク内の NULL 値の数
      - distinct_count - カラムチャンク内の異なる値の数
      - min - カラムチャンクの最小値
      - max - カラムチャンクの最大値

例:

```sql
SELECT * FROM file(data.parquet, ParquetMetadata) format PrettyJSONEachRow
```

```json
{
    "num_columns": "2",
    "num_rows": "100000",
    "num_row_groups": "2",
    "format_version": "2.6",
    "metadata_size": "577",
    "total_uncompressed_size": "282436",
    "total_compressed_size": "26633",
    "columns": [
        {
            "name": "number",
            "path": "number",
            "max_definition_level": "0",
            "max_repetition_level": "0",
            "physical_type": "INT32",
            "logical_type": "Int(bitWidth=16, isSigned=false)",
            "compression": "LZ4",
            "total_uncompressed_size": "133321",
            "total_compressed_size": "13293",
            "space_saved": "90.03%",
            "encodings": [
                "RLE_DICTIONARY",
                "PLAIN",
                "RLE"
            ]
        },
        {
            "name": "concat('Hello', toString(modulo(number, 1000)))",
            "path": "concat('Hello', toString(modulo(number, 1000)))",
            "max_definition_level": "0",
            "max_repetition_level": "0",
            "physical_type": "BYTE_ARRAY",
            "logical_type": "None",
            "compression": "LZ4",
            "total_uncompressed_size": "149115",
            "total_compressed_size": "13340",
            "space_saved": "91.05%",
            "encodings": [
                "RLE_DICTIONARY",
                "PLAIN",
                "RLE"
            ]
        }
    ],
    "row_groups": [
        {
            "num_columns": "2",
            "num_rows": "65409",
            "total_uncompressed_size": "179809",
            "total_compressed_size": "14163",
            "columns": [
                {
                    "name": "number",
                    "path": "number",
                    "total_compressed_size": "7070",
                    "total_uncompressed_size": "85956",
                    "have_statistics": true,
                    "statistics": {
                        "num_values": "65409",
                        "null_count": "0",
                        "distinct_count": null,
                        "min": "0",
                        "max": "999"
                    }
                },
                {
                    "name": "concat('Hello', toString(modulo(number, 1000)))",
                    "path": "concat('Hello', toString(modulo(number, 1000)))",
                    "total_compressed_size": "7093",
                    "total_uncompressed_size": "93853",
                    "have_statistics": true,
                    "statistics": {
                        "num_values": "65409",
                        "null_count": "0",
                        "distinct_count": null,
                        "min": "Hello0",
                        "max": "Hello999"
                    }
                }
            ]
        },
        ...
    ]
}
```

## Arrow {#data-format-arrow}

[Apache Arrow](https://arrow.apache.org/)には2つの組み込みの列指向ストレージフォーマットがあります。ClickHouseはこれらのフォーマットの読み書きをサポートしています。

`Arrow`は、Apache Arrowの"file mode"フォーマットです。これはインメモリランダムアクセス用に設計されています。

### データ型のマッチング {#data-types-matching-arrow}

下記の表は、`INSERT`および`SELECT`クエリにおいてサポートされるデータ型とClickHouse [データ型](/docs/ja/sql-reference/data-types/index.md)のマッチングの方法を示しています。

| Arrow データ型 (`INSERT`)              | ClickHouse データ型                                                                                       | Arrow データ型 (`SELECT`) |
|-----------------------------------------|------------------------------------------------------------------------------------------------------------|----------------------------|
| `BOOL`                                  | [Bool](/docs/ja/sql-reference/data-types/boolean.md)                                                       | `BOOL`                     |
| `UINT8`, `BOOL`                         | [UInt8](/docs/ja/sql-reference/data-types/int-uint.md)                                                     | `UINT8`                    |
| `INT8`                                  | [Int8](/docs/ja/sql-reference/data-types/int-uint.md)/[Enum8](/docs/ja/sql-reference/data-types/enum.md)   | `INT8`                     |
| `UINT16`                                | [UInt16](/docs/ja/sql-reference/data-types/int-uint.md)                                                    | `UINT16`                   |
| `INT16`                                 | [Int16](/docs/ja/sql-reference/data-types/int-uint.md)/[Enum16](/docs/ja/sql-reference/data-types/enum.md) | `INT16`                    |
| `UINT32`                                | [UInt32](/docs/ja/sql-reference/data-types/int-uint.md)                                                    | `UINT32`                   |
| `INT32`                                 | [Int32](/docs/ja/sql-reference/data-types/int-uint.md)                                                     | `INT32`                    |
| `UINT64`                                | [UInt64](/docs/ja/sql-reference/data-types/int-uint.md)                                                    | `UINT64`                   |
| `INT64`                                 | [Int64](/docs/ja/sql-reference/data-types/int-uint.md)                                                     | `INT64`                    |
| `FLOAT`, `HALF_FLOAT`                   | [Float32](/docs/ja/sql-reference/data-types/float.md)                                                      | `FLOAT32`                  |
| `DOUBLE`                                | [Float64](/docs/ja/sql-reference/data-types/float.md)                                                      | `FLOAT64`                  |
| `DATE32`                                | [Date32](/docs/ja/sql-reference/data-types/date32.md)                                                      | `UINT16`                   |
| `DATE64`                                | [DateTime](/docs/ja/sql-reference/data-types/datetime.md)                                                  | `UINT32`                   |
| `TIMESTAMP`, `TIME32`, `TIME64`         | [DateTime64](/docs/ja/sql-reference/data-types/datetime64.md)                                              | `UINT32`                   |
| `STRING`, `BINARY`                      | [String](/docs/ja/sql-reference/data-types/string.md)                                                      | `BINARY`                   |
| `STRING`, `BINARY`, `FIXED_SIZE_BINARY` | [FixedString](/docs/ja/sql-reference/data-types/fixedstring.md)                                            | `FIXED_SIZE_BINARY`        |
| `DECIMAL`                               | [Decimal](/docs/ja/sql-reference/data-types/decimal.md)                                                    | `DECIMAL`                  |
| `DECIMAL256`                            | [Decimal256](/docs/ja/sql-reference/data-types/decimal.md)                                                 | `DECIMAL256`               |
| `LIST`                                  | [Array](/docs/ja/sql-reference/data-types/array.md)                                                        | `LIST`                     |
| `STRUCT`                                | [Tuple](/docs/ja/sql-reference/data-types/tuple.md)                                                        | `STRUCT`                   |
| `MAP`                                   | [Map](/docs/ja/sql-reference/data-types/map.md)                                                            | `MAP`                      |
| `UINT32`                                | [IPv4](/docs/ja/sql-reference/data-types/ipv4.md)                                                          | `UINT32`                   |
| `FIXED_SIZE_BINARY`, `BINARY`           | [IPv6](/docs/ja/sql-reference/data-types/ipv6.md)                                                          | `FIXED_SIZE_BINARY`        |
| `FIXED_SIZE_BINARY`, `BINARY`           | [Int128/UInt128/Int256/UInt256](/docs/ja/sql-reference/data-types/int-uint.md)                             | `FIXED_SIZE_BINARY`        |

配列はネスト可能であり、引数として`Nullable`型を持つことができます。`Tuple`と`Map`型もネスト可能です。

`INSERT`クエリには`DICTIONARY`型がサポートされており、`SELECT`クエリでは、[LowCardinality](/docs/ja/sql-reference/data-types/lowcardinality.md)型を`DICTIONARY`型として出力することを可能にする設定 [output_format_arrow_low_cardinality_as_dictionary](/docs/ja/operations/settings/settings-formats.md/#output-format-arrow-low-cardinality-as-dictionary)があります。

サポートされていないArrowデータ型: `FIXED_SIZE_BINARY`, `JSON`, `UUID`, `ENUM`.

ClickHouseのテーブルカラムのデータ型は、対応するArrowのデータフィールドと一致する必要はありません。データを挿入する際、ClickHouseは上記の表に従ってデータ型を解釈し、その後、ClickHouseのテーブルカラムに設定されたデータ型に[キャスト](/docs/ja/sql-reference/functions/type-conversion-functions.md/#type_conversion_function-cast)します。

### データ挿入 {#inserting-data-arrow}

ArrowデータをファイルからClickHouseテーブルに挿入するには、次のコマンドを使用します:

``` bash
$ cat filename.arrow | clickhouse-client --query="INSERT INTO some_table FORMAT Arrow"
```

### データの選択 {#selecting-data-arrow}

ClickHouseテーブルからデータを選択し、それをArrowフォーマットのファイルに保存するには、次のコマンドを使用します:

``` bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Arrow" > {filename.arrow}
```

### Arrowフォーマットの設定 {#arrow-format-settings}

- [output_format_arrow_low_cardinality_as_dictionary](/docs/ja/operations/settings/settings-formats.md/#output_format_arrow_low_cardinality_as_dictionary) - ClickHouse LowCardinality型をDictionary Arrow型として出力することを有効にします。デフォルト値は`false`です。
- [output_format_arrow_use_64_bit_indexes_for_dictionary](/docs/ja/operations/settings/settings-formats.md/#output_format_arrow_use_64_bit_indexes_for_dictionary) - Dictionaryのインデックスに64ビット整数型を使用します。デフォルト値は`false`です。
- [output_format_arrow_use_signed_indexes_for_dictionary](/docs/ja/operations/settings/settings-formats.md/#output_format_arrow_use_signed_indexes_for_dictionary) - Dictionaryのインデックスに符号付き整数型を使用します。デフォルト値は`true`です。
- [output_format_arrow_string_as_string](/docs/ja/operations/settings/settings-formats.md/#output_format_arrow_string_as_string) - StringカラムにBinaryではなくArrow String型を使用します。デフォルト値は`false`です。
- [input_format_arrow_case_insensitive_column_matching](/docs/ja/operations/settings/settings-formats.md/#input_format_arrow_case_insensitive_column_matching) - ArrowカラムとClickHouseカラムのマッチングにおいて大文字小文字を無視します。デフォルト値は`false`です。
- [input_format_arrow_allow_missing_columns](/docs/ja/operations/settings/settings-formats.md/#input_format_arrow_allow_missing_columns) - Arrowデータを読み取る際に欠落しているカラムを許可します。デフォルト値は`false`です。
- [input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference](/docs/ja/operations/settings/settings-formats.md/#input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference) - Arrowフォーマットのスキーマ推論時にサポートされていない型のカラムをスキップすることを許可します。デフォルト値は`false`です。
- [output_format_arrow_fixed_string_as_fixed_byte_array](/docs/ja/operations/settings/settings-formats.md/#output_format_arrow_fixed_string_as_fixed_byte_array) - FixedStringカラムにByte/StringではなくArrow FIXED_SIZE_BINARY型を使用します。デフォルト値は`true`です。
- [output_format_arrow_compression_method](/docs/ja/operations/settings/settings-formats.md/#output_format_arrow_compression_method) - 出力Arrowフォーマットに使用される圧縮メソッド。デフォルト値は`lz4_frame`です。

## ArrowStream {#data-format-arrow-stream}

`ArrowStream`はApache Arrowの「stream mode」フォーマットです。これはインメモリストリーム処理用に設計されています。

## ORC {#data-format-orc}

[Apache ORC](https://orc.apache.org/)は[Hadoop](https://hadoop.apache.org/)エコシステムで広く使用されている列指向ストレージフォーマットです。

### データ型のマッチング {#data-types-matching-orc}

下記の表は、`INSERT`および`SELECT`クエリにおいてサポートされるデータ型とClickHouse [データ型](/docs/ja/sql-reference/data-types/index.md)のマッチングの方法を示しています。

| ORC データ型 (`INSERT`)              | ClickHouse データ型                                                                                              | ORC データ型 (`SELECT`) |
|---------------------------------------|-------------------------------------------------------------------------------------------------------------------|--------------------------|
| `Boolean`                             | [UInt8](/docs/ja/sql-reference/data-types/int-uint.md)                                                            | `Boolean`                |
| `Tinyint`                             | [Int8/UInt8](/docs/ja/sql-reference/data-types/int-uint.md)/[Enum8](/docs/ja/sql-reference/data-types/enum.md)    | `Tinyint`                |
| `Smallint`                            | [Int16/UInt16](/docs/ja/sql-reference/data-types/int-uint.md)/[Enum16](/docs/ja/sql-reference/data-types/enum.md) | `Smallint`               |
| `Int`                                 | [Int32/UInt32](/docs/ja/sql-reference/data-types/int-uint.md)                                                     | `Int`                    |
| `Bigint`                              | [Int64/UInt32](/docs/ja/sql-reference/data-types/int-uint.md)                                                     | `Bigint`                 |
| `Float`                               | [Float32](/docs/ja/sql-reference/data-types/float.md)                                                             | `Float`                  |
| `Double`                              | [Float64](/docs/ja/sql-reference/data-types/float.md)                                                             | `Double`                 |
| `Decimal`                             | [Decimal](/docs/ja/sql-reference/data-types/decimal.md)                                                           | `Decimal`                |
| `Date`                                | [Date32](/docs/ja/sql-reference/data-types/date32.md)                                                             | `Date`                   |
| `Timestamp`                           | [DateTime64](/docs/ja/sql-reference/data-types/datetime64.md)                                                     | `Timestamp`              |
| `String`, `Char`, `Varchar`, `Binary` | [String](/docs/ja/sql-reference/data-types/string.md)                                                             | `Binary`                 |
| `List`                                | [Array](/docs/ja/sql-reference/data-types/array.md)                                                               | `List`                   |
| `Struct`                              | [Tuple](/docs/ja/sql-reference/data-types/tuple.md)                                                               | `Struct`                 |
| `Map`                                 | [Map](/docs/ja/sql-reference/data-types/map.md)                                                                   | `Map`                    |
| `Int`                                 | [IPv4](/docs/ja/sql-reference/data-types/int-uint.md)                                                             | `Int`                    |
| `Binary`                              | [IPv6](/docs/ja/sql-reference/data-types/ipv6.md)                                                                 | `Binary`                 |
| `Binary`                              | [Int128/UInt128/Int256/UInt256](/docs/ja/sql-reference/data-types/int-uint.md)                                    | `Binary`                 |
| `Binary`                              | [Decimal256](/docs/ja/sql-reference/data-types/decimal.md)                                                        | `Binary`                 |

その他の型はサポートされていません。

配列はネスト可能で、引数として`Nullable`型を持つことができます。`Tuple`と`Map`型もネスト可能です。

ClickHouseのテーブルカラムのデータ型は、対応するORCデータフィールドと一致する必要はありません。データを挿入する際、ClickHouseは上記の表に従ってデータ型を解釈し、その後、ClickHouseのテーブルカラムに設定されたデータ型に[キャスト](/docs/ja/sql-reference/functions/type-conversion-functions.md/#type_conversion_function-cast)します。

### データ挿入 {#inserting-data-orc}

ORCデータをファイルからClickHouseテーブルに挿入するには、次のコマンドを使用します:

``` bash
$ cat filename.orc | clickhouse-client --query="INSERT INTO some_table FORMAT ORC"
```

### データの選択 {#selecting-data-orc}

ClickHouseテーブルからデータを選択し、それをORCフォーマットのファイルに保存するには、次のコマンドを使用します:

``` bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT ORC" > {filename.orc}
```

### Arrowフォーマットの設定 {#parquet-format-settings}

- [output_format_arrow_string_as_string](/docs/ja/operations/settings/settings-formats.md/#output_format_arrow_string_as_string) - StringカラムにBinaryではなくArrow String型を使用します。デフォルト値は`false`です。
- [output_format_orc_compression_method](/docs/ja/operations/settings/settings-formats.md/#output_format_orc_compression_method) - 出力ORCフォーマットに使用される圧縮メソッド。デフォルト値は`none`です。
- [input_format_arrow_case_insensitive_column_matching](/docs/ja/operations/settings/settings-formats.md/#input_format_arrow_case_insensitive_column_matching) - ArrowカラムとClickHouseカラムのマッチングにおいて大文字小文字を無視します。デフォルト値は`false`です。
- [input_format_arrow_allow_missing_columns](/docs/ja/operations/settings/settings-formats.md/#input_format_arrow_allow_missing_columns) - Arrowデータを読み取る際に欠落しているカラムを許可します。デフォルト値は`false`です。
- [input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference](/docs/ja/operations/settings/settings-formats.md/#input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference) - Arrowフォーマットのスキーマ推論時にサポートされていない型のカラムをスキップすることを許可します。デフォルト値は`false`です。

Hadoopとデータを交換するには、[HDFSテーブルエンジン](/docs/ja/engines/table-engines/integrations/hdfs.md)を使用できます。

## One {#data-format-one}

ファイルからデータを読み取らず、`UInt8`型、名前`dummy`、値`0`を持つカラムを含む行のみを返す特殊な入力フォーマット。
仮想カラム`_file/_path`を使用して、実際のデータを読み取らずにすべてのファイルをリストするために使用できます。

例:

クエリ:
```sql
SELECT _file FROM file('path/to/files/data*', One);
```

結果:
```text
┌─_file────┐
│ data.csv │
└──────────┘
┌─_file──────┐
│ data.jsonl │
└────────────┘
┌─_file────┐
│ data.tsv │
└──────────┘
┌─_file────────┐
│ data.parquet │
└──────────────┘
```

## Npy {#data-format-npy}

この機能は、NumPyの.npyファイルからNumPy配列をClickHouseにロードするように設計されています。NumPyファイルフォーマットは、数値データの配列を効率的に保存するためのバイナリフォーマットです。インポート中、ClickHouseは最上位次元を単一カラムを持つ行の配列として扱います。サポートされているNpyデータ型と、それに対応するClickHouseの型は以下の通りです。

| Npy データ型 (`INSERT`) | ClickHouse データ型                                            | Npy データ型 (`SELECT`) |
|--------------------------|-----------------------------------------------------------------|--------------------------|
| `i1`                     | [Int8](/docs/ja/sql-reference/data-types/int-uint.md)           | `i1`                     |
| `i2`                     | [Int16](/docs/ja/sql-reference/data-types/int-uint.md)          | `i2`                     |
| `i4`                     | [Int32](/docs/ja/sql-reference/data-types/int-uint.md)          | `i4`                     |
| `i8`                     | [Int64](/docs/ja/sql-reference/data-types/int-uint.md)          | `i8`                     |
| `u1`, `b1`               | [UInt8](/docs/ja/sql-reference/data-types/int-uint.md)          | `u1`                     |
| `u2`                     | [UInt16](/docs/ja/sql-reference/data-types/int-uint.md)         | `u2`                     |
| `u4`                     | [UInt32](/docs/ja/sql-reference/data-types/int-uint.md)         | `u4`                     |
| `u8`                     | [UInt64](/docs/ja/sql-reference/data-types/int-uint.md)         | `u8`                     |
| `f2`, `f4`               | [Float32](/docs/ja/sql-reference/data-types/float.md)           | `f4`                     |
| `f8`                     | [Float64](/docs/ja/sql-reference/data-types/float.md)           | `f8`                     |
| `S`, `U`                 | [String](/docs/ja/sql-reference/data-types/string.md)           | `S`                      |
|                          | [FixedString](/docs/ja/sql-reference/data-types/fixedstring.md) | `S`                      |

**Pythonを使用して.npy形式で配列を保存する例**

```Python
import numpy as np
arr = np.array([[[1],[2],[3]],[[4],[5],[6]]])
np.save('example_array.npy', arr)
```

**ClickHouseでNumPyファイルを読む例**

クエリ:
```sql
SELECT *
FROM file('example_array.npy', Npy)
```

結果:
```
┌─array─────────┐
│ [[1],[2],[3]] │
│ [[4],[5],[6]] │
└───────────────┘
```

**データの選択**

ClickHouseテーブルからデータを選択し、それをNpyフォーマットのファイルに保存するには、次のコマンドを使用します:

```bash
$ clickhouse-client --query="SELECT {column} FROM {some_table} FORMAT Npy" > {filename.npy}
```

## LineAsString {#lineasstring}

このフォーマットでは、入力データの各行が単一の文字列値として解釈されます。このフォーマットは、型[String](/docs/ja/sql-reference/data-types/string.md)の単一フィールドを持つテーブルのみ解析できます。他のカラムは[DEFAULT](/docs/ja/sql-reference/statements/create/table.md/#default)または[MATERIALIZED](/docs/ja/sql-reference/statements/create/table.md/#materialized)に設定されるか、省略される必要があります。

**例**

クエリ:

``` sql
DROP TABLE IF EXISTS line_as_string;
CREATE TABLE line_as_string (field String) ENGINE = Memory;
INSERT INTO line_as_string FORMAT LineAsString "I love apple", "I love banana", "I love orange";
SELECT * FROM line_as_string;
```

結果:

``` text
┌─field─────────────────────────────────────────────┐
│ "I love apple", "I love banana", "I love orange"; │
└───────────────────────────────────────────────────┘
```

## Regexp {#data-format-regexp}

インポートされたデータの各行は、正規表現に従って解析されます。

`Regexp`フォーマットを使用する際には、以下の設定を利用できます：

- `format_regexp` — [String](/docs/ja/sql-reference/data-types/string.md)。[re2](https://github.com/google/re2/wiki/Syntax)形式の正規表現を含みます。

- `format_regexp_escaping_rule` — [String](/docs/ja/sql-reference/data-types/string.md)。以下のエスケープルールがサポートされています：

  - CSV (類似 [CSV](#csv))
  - JSON (類似 [JSONEachRow](#jsoneachrow))
  - Escaped (類似 [TSV](#tabseparated))
  - Quoted (類似 [Values](#data-format-values))
  - Raw (サブパターンを丸ごと抽出、エスケープルールなし、[TSVRaw](#tabseparatedraw)に類似)

- `format_regexp_skip_unmatched` — [UInt8](/docs/ja/sql-reference/data-types/int-uint.md)。インポートされたデータが `format_regexp` 式に一致しなかった場合に例外をスローする必要性を定義します。`0`または`1`に設定できます。

**使用方法**

[format_regexp](/docs/ja/operations/settings/settings-formats.md/#format_regexp)設定からの正規表現は、インポートされたデータの各行に適用されます。正規表現のサブパターン数は、インポートされたデータセット内のカラム数と一致しなければなりません。

インポートされたデータの各行は、改行文字 `'\n'` 又は DOSスタイルの改行 `"\r\n"` で区切られる必要があります。

各サブパターンの内容は、[format_regexp_escaping_rule](/docs/ja/operations/settings/settings-formats.md/#format_regexp_escaping_rule)設定に従って、対応するデータ型のメソッドで解析されます。

正規表現が行に一致せず、[format_regexp_skip_unmatched](/docs/ja/operations/settings/settings-formats.md/#format_regexp_escaping_rule)が1に設定されている場合、その行は静かにスキップされます。それ以外の場合、例外がスローされます。

**例**

ファイル data.tsvを考えてみましょう:

```text
id: 1 array: [1,2,3] string: str1 date: 2020-01-01
id: 2 array: [1,2,3] string: str2 date: 2020-01-02
id: 3 array: [1,2,3] string: str3 date: 2020-01-03
```
そしてテーブル:

```sql
CREATE TABLE imp_regex_table (id UInt32, array Array(UInt32), string String, date Date) ENGINE = Memory;
```

インポートコマンド:

```bash
$ cat data.tsv | clickhouse-client  --query "INSERT INTO imp_regex_table SETTINGS format_regexp='id: (.+?) array: (.+?) string: (.+?) date: (.+?)', format_regexp_escaping_rule='Escaped', format_regexp_skip_unmatched=0 FORMAT Regexp;"
```

クエリ:

```sql
SELECT * FROM imp_regex_table;
```

結果:

```text
┌─id─┬─array───┬─string─┬───────date─┐
│  1 │ [1,2,3] │ str1   │ 2020-01-01 │
│  2 │ [1,2,3] │ str2   │ 2020-01-02 │
│  3 │ [1,2,3] │ str3   │ 2020-01-03 │
└────┴─────────┴────────┴────────────┘
```

## フォーマットスキーマ {#formatschema}

フォーマットスキーマを含むファイル名は、設定 `format_schema` で設定されます。
`Cap'n Proto` および `Protobuf` のフォーマットが使用されている場合、この設定の設定が必要です。
フォーマットスキーマは、ファイル名とこのファイル内のメッセージタイプの名前をコロンで区切った組み合わせです、
例えば `schemafile.proto:MessageType` です。
ファイルがフォーマットの標準拡張子（例えば、`Protobuf`の場合は`.proto`）を持っている場合、その拡張子を省略でき、この場合、フォーマットスキーマは `schemafile:MessageType` となります。

[client](/docs/ja/interfaces/cli.md) で [インタラクティブモード](/docs/ja/interfaces/cli.md/#cli_usage) でデータを入出力する場合、フォーマットスキーマに指定されたファイル名にはクライアント上の現在のディレクトリからの絶対パスまたは相対パスを含めることができます。
[バッチモード](/docs/ja/interfaces/cli.md/#cli_usage) でクライアントを使用する場合、セキュリティ上の理由からスキーマへのパスは相対でなければなりません。

[HTTPインターフェイス](/docs/ja/interfaces/http.md)を使用してデータを入出力する場合、フォーマットスキーマに指定されたファイル名は、サーバー設定の[format_schema_path](/docs/ja/operations/server-configuration-parameters/settings.md/#format_schema_path)に指定されたディレクトリに存在する必要があります。

## エラーのスキップ {#skippingerrors}

`CSV`, `TabSeparated`, `TSKV`, `JSONEachRow`, `Template`, `CustomSeparated`, `Protobuf`などの形式では、パースエラーが発生した場合に壊れた行をスキップし、次の行の最初からパースを続行できます。[input_format_allow_errors_num](/docs/ja/operations/settings/settings-formats.md/#input_format_allow_errors_num)と
[input_format_allow_errors_ratio](/docs/ja/operations/settings/settings-formats.md/#input_format_allow_errors_ratio)の設定を参照してください。
制限事項:
- `JSONEachRow`の場合、パースエラー時にすべてのデータを新しい行（またはEOF）までスキップするため、正確にエラーをカウントするには`\\n`で行を区切る必要があります。
- `Template`および`CustomSeparated`は、次の行の開始を見つけるために最後のカラム後の区切り文字と行間の区切り文字を使用するため、少なくともどちらか一つが空でない場合にのみエラーのスキップが機能します。

## RawBLOB {#rawblob}

このフォーマットでは、入力データは単一の値に読み込まれます。型[String](/docs/ja/sql-reference/data-types/string.md)または類似の単一フィールドのテーブルのみを解析することができます。
結果は、バイナリ形式で区切りやエスケープなしで出力されます。複数の値が出力される場合、フォーマットは曖昧になり、データを再度読み取ることができなくなります。

以下は、`RawBLOB`フォーマットと[TabSeparatedRaw](#tabseparatedraw)フォーマットの比較です。

`RawBLOB`:
- データはバイナリフォーマットで出力され、エスケープなし;
- 値間の区切りなし;
- 各値の最後に改行なし。

`TabSeparatedRaw`:
- データはエスケープなしで出力される;
- 行にはタブで区切られた値が含まれる;
- 各行の最後の値の後には改行がある。

以下は`RawBLOB`と[RowBinary](#rowbinary)フォーマットの比較です。

`RawBLOB`:
- Stringフィールドは長さをプレフィックスに持たずに出力される。

`RowBinary`:
- Stringフィールドはvarint形式（符号なし[LEB128](https://ja.wikipedia.org/wiki/LEB128)）で長さが表され、その後に文字列のバイトが続く。

`RawBLOB`入力に空のデータを渡すと、ClickHouseは例外をスローします:

``` text
Code: 108. DB::Exception: No data to insert
```

**例**

``` bash
$ clickhouse-client --query "CREATE TABLE {some_table} (a String) ENGINE = Memory;"
$ cat {filename} | clickhouse-client --query="INSERT INTO {some_table} FORMAT RawBLOB"
$ clickhouse-client --query "SELECT * FROM {some_table} FORMAT RawBLOB" | md5sum
```

結果:

``` text
f9725a22f9191e064120d718e26862a9  -
```

## MsgPack {#msgpack}

ClickHouseは[MessagePack](https://msgpack.org/)データファイルの読み書きをサポートしています。

### データ型のマッチング {#data-types-matching-msgpack}

| MessagePack データ型 (`INSERT`)                                   | ClickHouse データ型                                                                                    | MessagePack データ型 (`SELECT`) |
|--------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------|----------------------------------|
| `uint N`, `positive fixint`                                        | [UIntN](/docs/ja/sql-reference/data-types/int-uint.md)                                                  | `uint N`                         |
| `int N`, `negative fixint`                                         | [IntN](/docs/ja/sql-reference/data-types/int-uint.md)                                                   | `int N`                          |
| `bool`                                                             | [UInt8](/docs/ja/sql-reference/data-types/int-uint.md)                                                  | `uint 8`                         |
| `fixstr`, `str 8`, `str 16`, `str 32`, `bin 8`, `bin 16`, `bin 32` | [String](/docs/ja/sql-reference/data-types/string.md)                                                   | `bin 8`, `bin 16`, `bin 32`      |
| `fixstr`, `str 8`, `str 16`, `str 32`, `bin 8`, `bin 16`, `bin 32` | [FixedString](/docs/ja/sql-reference/data-types/fixedstring.md)                                         | `bin 8`, `bin 16`, `bin 32`      |
| `float 32`                                                         | [Float32](/docs/ja/sql-reference/data-types/float.md)                                                   | `float 32`                       |
| `float 64`                                                         | [Float64](/docs/ja/sql-reference/data-types/float.md)                                                   | `float 64`                       |
| `uint 16`                                                          | [Date](/docs/ja/sql-reference/data-types/date.md)                                                       | `uint 16`                        |
| `int 32`                                                           | [Date32](/docs/ja/sql-reference/data-types/date32.md)                                                   | `int 32`                         |
| `uint 32`                                                          | [DateTime](/docs/ja/sql-reference/data-types/datetime.md)                                               | `uint 32`                        |
| `uint 64`                                                          | [DateTime64](/docs/ja/sql-reference/data-types/datetime.md)                                             | `uint 64`                        |
| `fixarray`, `array 16`, `array 32`                                 | [Array](/docs/ja/sql-reference/data-types/array.md)/[Tuple](/docs/ja/sql-reference/data-types/tuple.md) | `fixarray`, `array 16`, `array 32` |
| `fixmap`, `map 16`, `map 32`                                       | [Map](/docs/ja/sql-reference/data-types/map.md)                                                         | `fixmap`, `map 16`, `map 32`     |
| `uint 32`                                                          | [IPv4](/docs/ja/sql-reference/data-types/ipv4.md)                                                       | `uint 32`                        |
| `bin 8`                                                            | [String](/docs/ja/sql-reference/data-types/string.md)                                                   | `bin 8`                          |
| `int 8`                                                            | [Enum8](/docs/ja/sql-reference/data-types/enum.md)                                                      | `int 8`                          |
| `bin 8`                                                            | [(U)Int128/(U)Int256](/docs/ja/sql-reference/data-types/int-uint.md)                                    | `bin 8`                          |
| `int 32`                                                           | [Decimal32](/docs/ja/sql-reference/data-types/decimal.md)                                               | `int 32`                         |
| `int 64`                                                           | [Decimal64](/docs/ja/sql-reference/data-types/decimal.md)                                               | `int 64`                         |
| `bin 8`                                                            | [Decimal128/Decimal256](/docs/ja/sql-reference/data-types/decimal.md)                                   | `bin 8 `                         |

例:

".msgpk"ファイルへの書き込み:

```sql
$ clickhouse-client --query="CREATE TABLE msgpack (array Array(UInt8)) ENGINE = Memory;"
$ clickhouse-client --query="INSERT INTO msgpack VALUES ([0, 1, 2, 3, 42, 253, 254, 255]), ([255, 254, 253, 42, 3, 2, 1, 0])";
$ clickhouse-client --query="SELECT * FROM msgpack FORMAT MsgPack" > tmp_msgpack.msgpk;
```

### MsgPackフォーマットの設定 {#msgpack-format-settings}

- [input_format_msgpack_number_of_columns](/docs/ja/operations/settings/settings-formats.md/#input_format_msgpack_number_of_columns) - 挿入されたMsgPackデータ内のカラム数。データからの自動スキーマ推論に使用。デフォルト値は`0`。
- [output_format_msgpack_uuid_representation](/docs/ja/operations/settings/settings-formats.md/#output_format_msgpack_uuid_representation) - MsgPackフォーマットでUUIDを出力する方法。デフォルト値は`EXT`。

## MySQLDump {#mysqldump}

ClickHouseはMySQL [ダンプ](https://dev.mysql.com/doc/refman/8.0/en/mysqldump.html)の読み取りをサポートしています。
ダンプ内の1つのテーブルに属するINSERTクエリからすべてのデータを読み取ります。複数のテーブルがある場合、デフォルトでは最初のテーブルからデータを読み取ります。
[input_format_mysql_dump_table_name](/docs/ja/operations/settings/settings-formats.md/#input_format_mysql_dump_table_name)設定を使用して、データを読み取るテーブルの名前を指定できます。
[input_format_mysql_dump_map_columns](/docs/ja/operations/settings/settings-formats.md/#input_format_mysql_dump_map_columns)設定が1に設定されており、指定されたテーブルのCREATEクエリまたはINSERTクエリ内のカラム名がダンプに含まれている場合、入力データのカラムはその名前でテーブルのカラムにマップされます、
未知の名前のカラムは、設定[input_format_skip_unknown_fields](/docs/ja/operations/settings/settings-formats.md/#input_format_skip_unknown_fields)が1に設定されている場合、スキップされます。
このフォーマットはスキーマ推論をサポートしています：ダンプが指定されたテーブルのCREATEクエリを含む場合、構造はそこから抽出されます。そうでなければ、スキーマはINSERTクエリのデータから推論されます。

例:

ファイル dump.sql:
```sql
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `test` (
  `x` int DEFAULT NULL,
  `y` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
INSERT INTO `test` VALUES (1,NULL),(2,NULL),(3,NULL),(3,NULL),(4,NULL),(5,NULL),(6,7);
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `test 3` (
  `y` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
INSERT INTO `test 3` VALUES (1);
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `test2` (
  `x` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
INSERT INTO `test2` VALUES (1),(2),(3);
```

クエリ:

```sql
DESCRIBE TABLE file(dump.sql, MySQLDump) SETTINGS input_format_mysql_dump_table_name = 'test2'
```

```text
┌─name─┬─type────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ x    │ Nullable(Int32) │              │                    │         │                  │                │
└──────┴─────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
SELECT *
FROM file(dump.sql, MySQLDump)
         SETTINGS input_format_mysql_dump_table_name = 'test2'
```

```text
┌─x─┐
│ 1 │
│ 2 │
│ 3 │
└───┘
```

## DWARF {#dwarf}

ELFファイル（実行ファイル、ライブラリ、オブジェクトファイル）からDWARFデバッグシンボルを解析します。`dwarfdump`に似ていますが、はるかに速く（数百MB/秒）、SQLと共に利用できます。.debug_infoセクション内の各デバッグ情報エントリ（DIE）に対して1行を生成します。DIEを終端するためにDWARFエンコーディングが使用する"null"エントリも含まれます。

簡単な背景: `.debug_info` は *ユニット* で構成されており、それぞれがコンパイルユニットに対応します。各ユニットは、`compile_unit` DIEをルートとする *DIE* のツリーです。各DIEは *タグ* を持ち、*属性* のリストを持ちます。各属性は *名前* と *値*（およびその値がどのようにエンコードされているかを指定する *form*）を持ちます。DIEはソースコードからの内容を表し、そのタグがどのような内容であるかを示しています。例えば、関数は（タグ= `subprogram`）、クラス/構造体/列挙型（`class_type`/`structure_type`/`enumeration_type`）、変数（`variable`）、関数の引数（`formal_parameter`）などです。ツリー構造は対応するソースコードを反映しています。例えば、`class_type` DIE はクラスのメソッドを表す `subprogram` DIE を含むことがあります。

以下のカラムを出力します:
 - `offset` - `.debug_info` セクション内のDIEの位置
 - `size` - エンコードされたDIEのバイト数（属性を含む）
 - `tag` - DIEのタイプ; 通常の "DW_TAG_" プレフィックスは省略されます
 - `unit_name` - このDIEを含むコンパイルユニットの名前
 - `unit_offset` - `.debug_info` セクション内でこのDIEを含むコンパイルユニットの位置
 - 現在のDIEのツリー内の先祖のタグを順に含む配列:
    - `ancestor_tags`
    - `ancestor_offsets` - `ancestor_tags`と並行する補Offset
 - 利便性のために属性配列から重複したいくつかの一般的な属性：
   - `name`
   - `linkage_name` - マングルされた完全修飾名; 通常、関数のみがそれを持ちます（すべての関数が持っているわけではありません）
   - `decl_file` - このエンティティが宣言されたソースコードファイルの名前
   - `decl_line` - このエンティティが宣言されたソースコード内の行番号
 - 属性を説明する並行配列：
   - `attr_name` - 属性の名前; 通常の "DW_AT_" プレフィックスは省略
   - `attr_form` - 属性がどのようにエンコードされ、解釈されるか; 通常のDW_FORM_プレフィックスは省略
   - `attr_int` - 属性の整数値; 属性が数値を持たない場合は0
   - `attr_str` - 属性の文字列値; 属性が文字列値を持たない場合は空

例：最も多くの関数定義を持つコンパイルユニットを見つける（テンプレートのインスタンス化やインクルードされたヘッダファイルからの関数を含む）：
```sql
SELECT
    unit_name,
    count() AS c
FROM file('programs/clickhouse', DWARF)
WHERE tag = 'subprogram' AND NOT has(attr_name, 'declaration')
GROUP BY unit_name
ORDER BY c DESC
LIMIT 3
```
```text
┌─unit_name──────────────────────────────────────────────────┬─────c─┐
│ ./src/Core/Settings.cpp                                    │ 28939 │
│ ./src/AggregateFunctions/AggregateFunctionSumMap.cpp       │ 23327 │
│ ./src/AggregateFunctions/AggregateFunctionUniqCombined.cpp │ 22649 │
└────────────────────────────────────────────────────────────┴───────┘

3 rows in set. Elapsed: 1.487 sec. Processed 139.76 million rows, 1.12 GB (93.97 million rows/s., 752.77 MB/s.)
Peak memory usage: 271.92 MiB.
```

## Markdown {#markdown}

結果を[Markdown](https://en.wikipedia.org/wiki/Markdown)フォーマットを利用してエクスポートし、自分の`.md`ファイルに貼り付ける準備を整えることができます：

```sql
SELECT
    number,
    number * 2
FROM numbers(5)
FORMAT Markdown
```
```results
| number | multiply(number, 2) |
|-:|-:|
| 0 | 0 |
| 1 | 2 |
| 2 | 4 |
| 3 | 6 |
| 4 | 8 |
```

Markdownテーブルは自動的に生成され、GithubのようなMarkdown対応プラットフォームで使用することができます。このフォーマットは出力専用です。

## Form {#form}

Formフォーマットは、データを `key1=value1&key2=value2` の形式でフォーマットされた `application/x-www-form-urlencoded` フォーマットで単一のレコードを読み書きするために使用できます。

例:

ユーザーファイルパスに配置されたファイル `data.tmp` には、URLエンコードされたデータがあります:

```text
t_page=116&c.e=ls7xfkpm&c.tti.m=raf&rt.start=navigation&rt.bmr=390%2C11%2C10
```

```sql
SELECT * FROM file(data.tmp, Form) FORMAT vertical;
```

結果:

```text
Row 1:
──────
t_page:   116
c.e:      ls7xfkpm
c.tti.m:  raf
rt.start: navigation
rt.bmr:   390,11,10
```
