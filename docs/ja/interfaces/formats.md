---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 21
toc_title: "\u5165\u529B\u304A\u3088\u3073\u51FA\u529B\u30D5\u30A9\u30FC\u30DE\u30C3\
  \u30C8"
---

# 入出力データのフォーマット {#formats}

ClickHouse受け入れと返信データなフレームワークです。 入力用にサポートされている形式を使用して、指定されたデータを解析できます `INSERT`s、実行する `SELECT`ファイル、URL、HDFSなどのファイルバックアップテーブルから、または外部辞書を読み取ることができます。 出力でサポートされているフォーマットを使用して、
aの結果 `SELECT`、および実行する `INSERT`ファイルバックアップテーブルにs。

のサポートされるフォーマットは:

| 形式                                                            | 入力 | 出力 |
|-----------------------------------------------------------------|------|------|
| [TabSeparated](#tabseparated)                                   | ✔    | ✔    |
| [TabSeparatedRaw](#tabseparatedraw)                             | ✗    | ✔    |
| [TabSeparatedWithNames](#tabseparatedwithnames)                 | ✔    | ✔    |
| [TabSeparatedWithNamesAndTypes](#tabseparatedwithnamesandtypes) | ✔    | ✔    |
| [テンプレー](#format-template)                                  | ✔    | ✔    |
| [TemplateIgnoreSpaces](#templateignorespaces)                   | ✔    | ✗    |
| [CSV](#csv)                                                     | ✔    | ✔    |
| [CSVWithNames](#csvwithnames)                                   | ✔    | ✔    |
| [カスタム区切り](#format-customseparated)                       | ✔    | ✔    |
| [値](#data-format-values)                                       | ✔    | ✔    |
| [垂直](#vertical)                                               | ✗    | ✔    |
| [VerticalRaw](#verticalraw)                                     | ✗    | ✔    |
| [JSON](#json)                                                   | ✗    | ✔    |
| [JSONCompact](#jsoncompact)                                     | ✗    | ✔    |
| [JSONEachRow](#jsoneachrow)                                     | ✔    | ✔    |
| [TSKV](#tskv)                                                   | ✔    | ✔    |
| [プリティ](#pretty)                                             | ✗    | ✔    |
| [プリティコンパクト](#prettycompact)                            | ✗    | ✔    |
| [プリティコンパクトモノブロック](#prettycompactmonoblock)       | ✗    | ✔    |
| [プリティノスケープ](#prettynoescapes)                          | ✗    | ✔    |
| [PrettySpace](#prettyspace)                                     | ✗    | ✔    |
| [プロトブフ](#protobuf)                                         | ✔    | ✔    |
| [アヴロ](#data-format-avro)                                     | ✔    | ✔    |
| [アブロコンフルエント](#data-format-avro-confluent)             | ✔    | ✗    |
| [寄木細工](#data-format-parquet)                                | ✔    | ✔    |
| [ORC](#data-format-orc)                                         | ✔    | ✗    |
| [ローバイナリ](#rowbinary)                                      | ✔    | ✔    |
| [RowBinaryWithNamesAndTypes](#rowbinarywithnamesandtypes)       | ✔    | ✔    |
| [ネイティブ](#native)                                           | ✔    | ✔    |
| [Null](#null)                                                   | ✗    | ✔    |
| [XML](#xml)                                                     | ✗    | ✔    |
| [CapnProto](#capnproto)                                         | ✔    | ✗    |

ClickHouse設定を使用して、いくつかの形式処理パラメータを制御できます。 詳細については、 [設定](../operations/settings/settings.md) セクション

## TabSeparated {#tabseparated}

TabSeparated形式では、データは行ごとに書き込まれます。 各行にはタブで区切られた値が含まれます。 各値の後にはタブが続きますが、行の最後の値の後には改行が続きます。 厳正なUnixラインフィードと仮定します。 最後の行には、最後に改行も含める必要があります。 値は、引用符を囲まずにテキスト形式で書き込まれ、特殊文字はエスケープされます。

この形式は、名前でも使用できます `TSV`.

その `TabSeparated` 形式は便利な加工データをカスタムプログラムやイントロダクションです。 これは、HTTPインターフェイスおよびコマンドラインクライアントのバッチモードで既定で使用されます。 この形式は、異なるDbms間でデータを転送することもできます。 たとえば、MySQLからダンプを取得してClickHouseにアップロードすることも、その逆も可能です。

その `TabSeparated` 書式では、合計値(合計と共に使用する場合)と極値(場合)の出力がサポートされます ‘extremes’ 1)に設定される。 このような場合、合計値と極値がメインデータの後に出力されます。 主な結果、合計値、および極値は、空の行で区切られます。 例:

``` sql
SELECT EventDate, count() AS c FROM test.hits GROUP BY EventDate WITH TOTALS ORDER BY EventDate FORMAT TabSeparated``
```

``` text
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

### データの書式設定 {#data-formatting}

整数は小数点形式で書かれています。 番号を含むことができ追加 “+” 先頭の文字(解析時は無視され、書式設定時は記録されません)。 負でない数値には負符号を含めることはできません。 読み取り時には、空の文字列をゼロとして解析するか、（符号付き型の場合）マイナス記号だけで構成される文字列をゼロとして解析することができ 対応するデータ型に適合しない数値は、エラーメッセージなしで別の数値として解析される可能性があります。

浮動小数点数は小数点形式で記述されます。 ドットは小数点区切り記号として使用されます。 指数エントリがサポートされています。 ‘inf’, ‘+inf’, ‘-inf’,and ‘nan’. 浮動小数点数のエントリは、小数点で始まったり終わったりすることがあります。
書式設定中に、浮動小数点数の精度が失われる可能性があります。
解析中に、最も近いマシン表現可能な番号を読み取ることは厳密には必要ありません。

日付はYYYY-MM-DD形式で記述され、同じ形式で解析されますが、区切り文字として任意の文字が使用されます。
時刻付きの日付は、次の形式で記述されます `YYYY-MM-DD hh:mm:ss` 同じ形式で解析されますが、区切り文字として任意の文字で解析されます。
これはすべて、クライアントまたはサーバーが起動したときのシステムタイムゾーンで発生します(データの形式に応じて)。 時刻のある日付の場合、夏時間は指定されません。 したがって、夏時間の間にダンプに時間がある場合、ダンプはデータと明確に一致しないため、解析は二回のいずれかを選択します。
読み取り操作中に、エラーメッセージなしで、自然なオーバーフローまたはnullの日付と時刻として、不適切な日付と時刻を解析することができます。

例外として、正確に10桁の数字で構成されている場合は、時刻を含む日付の解析もUnixタイムスタンプ形式でサポートされます。 結果はタイムゾーンに依存しません。 YYYY-MM-DD hh:mm:ssとNNNNNNNNNNの形式は自動的に区別されます。

文字列はバックスラッシュでエスケープされた特殊文字で出力されます。 以下のエスケープシーケンスを使用出力: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\'`, `\\`. 解析にも対応し配列 `\a`, `\v`,and `\xHH` (hexエスケープシーケンス)と `\c` シーケンス `c` は任意の文字です(これらのシーケンスは `c`). このように、データを読み込む形式に対応し、改行して書き込み可能で `\n` または `\`、または改行として。 たとえば、文字列 `Hello world` スペースの代わりに単語の間の改行を使用すると、次のいずれかのバリエーションで解析できます:

``` text
Hello\nworld

Hello\
world
```

の変異体でサポートしてMySQLで書く時にタブ区切り捨て場.

TabSeparated形式でデータを渡すときにエスケープする必要がある最小文字セット:tab、改行(LF)、およびバックスラッシュ。

のみの小さなセットの記号は自動的にエスケープされます。 あなたの端末が出力で台無しにする文字列値に簡単に遭遇することができます。

配列は、角かっこで囲まれたコンマ区切りの値のリストとして記述されます。 配列内のNumber項目は、通常どおりに書式設定されます。 `Date` と `DateTime` 型は単一引quotesで記述されます。 文字列は、上記と同じエスケープ規則で一重引quotesで記述されます。

[NULL](../sql-reference/syntax.md) として書式設定される `\N`.

の各要素 [入れ子](../sql-reference/data-types/nested-data-structures/nested.md) 構造体は配列として表されます。

例えば:

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

``` text
1  [1]    ['a']
```

## TabSeparatedRaw {#tabseparatedraw}

とは異なる `TabSeparated` 行がエスケープせずに書き込まれる形式。
この形式は、クエリ結果の出力にのみ適していますが、解析(テーブルに挿入するデータの取得)には適していません。

この形式は、名前でも使用できます `TSVRaw`.

## TabSeparatedWithNames {#tabseparatedwithnames}

とは異なる `TabSeparated` 列名が最初の行に書き込まれる形式。
解析中、最初の行は完全に無視されます。 列名を使用して位置を特定したり、列の正しさを確認したりすることはできません。
（ヘッダー行の解析のサポートは、将来的に追加される可能性があります。)

この形式は、名前でも使用できます `TSVWithNames`.

## TabSeparatedWithNamesAndTypes {#tabseparatedwithnamesandtypes}

とは異なる `TabSeparated` 列名は最初の行に書き込まれ、列タイプは二番目の行に書き込まれます。
解析中、第一行と第二行は完全に無視されます。

この形式は、名前でも使用できます `TSVWithNamesAndTypes`.

## テンプレー {#format-template}

このフォーマットで指定するカスタムフォーマット文字列とプレースホルダーのための値を指定して逃げます。

設定を使用します `format_template_resultset`, `format_template_row`, `format_template_rows_between_delimiter` and some settings of other formats (e.g. `output_format_json_quote_64bit_integers` 使用する場合 `JSON` エスケープ、さらに参照)

設定 `format_template_row` 次の構文で行の書式指定文字列を含むファイルへのパスを指定します:

`delimiter_1${column_1:serializeAs_1}delimiter_2${column_2:serializeAs_2} ... delimiter_N`,

どこに `delimiter_i` 値の区切り文字です (`$` シンボルでき逃してい `$$`),
`column_i` 値が選択または挿入される列の名前またはインデックスを指定します(空の場合、列はスキップされます),
`serializeAs_i` 列値のエスケープ規則です。 以下の脱出ルールに対応:

-   `CSV`, `JSON`, `XML` （同じ名前の形式と同様)
-   `Escaped` （同様に `TSV`)
-   `Quoted` （同様に `Values`)
-   `Raw` （エスケープせずに、同様に `TSVRaw`)
-   `None` （エスケープルールなし、詳細を参照)

エスケープ規則が省略された場合、 `None` 使用されます。 `XML` と `Raw` 出力のためにだけ適しています。

したがって、次の書式文字列の場合:

      `Search phrase: ${SearchPhrase:Quoted}, count: ${c:Escaped}, ad price: $$${price:JSON};`

の値 `SearchPhrase`, `c` と `price` 列は、次のようにエスケープ `Quoted`, `Escaped` と `JSON` その間に（選択のために）印刷されるか、または（挿入のために）期待されます `Search phrase:`, `, count:`, `, ad price: $` と `;` 区切り文字。 例えば:

`Search phrase: 'bathroom interior design', count: 2166, ad price: $3;`

その `format_template_rows_between_delimiter` これは、最後の行を除くすべての行の後に印刷されます（または期待されます）。 (`\n` 既定では)

設定 `format_template_resultset` このパスには、resultsetの書式指定文字列が含まれます。 Resultsetの書式指定文字列は、行の書式指定文字列と同じ構文を持ち、接頭辞、接尾辞、および追加情報を出力する方法を指定できます。 列名の代わりに次のプレースホルダが含まれます:

-   `data` はデータを含む行です `format_template_row` で区切られた形式 `format_template_rows_between_delimiter`. このプレースホルダーの最初のプレースホルダー形式の文字列になります。
-   `totals` は、合計値を持つ行です。 `format_template_row` 書式(合計で使用する場合)
-   `min` は最小値を持つ行です。 `format_template_row` 書式(極値が1に設定されている場合)
-   `max` は最大値を持つ行です。 `format_template_row` 書式(極値が1に設定されている場合)
-   `rows` 出力行の合計数です
-   `rows_before_limit` そこにあったであろう行の最小数は制限なしです。 出力の場合のみを含むクエリを制限します。 クエリにGROUP BYが含まれている場合、rows_before_limit_at_leastは制限なしで行われていた行の正確な数です。
-   `time` リクエストの実行時間を秒単位で指定します
-   `rows_read` 読み込まれた行数です
-   `bytes_read` (圧縮されていない)読み込まれたバイト数です

プレースホルダ `data`, `totals`, `min` と `max` エスケープルールを指定してはいけません `None` 明示的に指定する必要があります)。 残りのプレースホルダはあるの脱出ルールを指定します。
もし `format_template_resultset` 設定は空の文字列です, `${data}` デフォルト値として使用されます。
Insertクエリ形式では、接頭辞または接尾辞の場合は、一部の列または一部のフィールドをスキップできます（例を参照）。

例を選択:

``` sql
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase ORDER BY c DESC LIMIT 5 FORMAT Template SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format', format_template_rows_between_delimiter = '\n    '
```

`/some/path/resultset.format`:

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

`/some/path/row.format`:

``` text
<tr> <td>${0:XML}</td> <td>${1:XML}</td> </tr>
```

結果:

``` html
<!DOCTYPE HTML>
<html> <head> <title>Search phrases</title> </head>
 <body>
  <table border="1"> <caption>Search phrases</caption>
    <tr> <th>Search phrase</th> <th>Count</th> </tr>
    <tr> <td></td> <td>8267016</td> </tr>
    <tr> <td>bathroom interior design</td> <td>2166</td> </tr>
    <tr> <td>yandex</td> <td>1655</td> </tr>
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

挿入例:

``` text
Some header
Page views: 5, User id: 4324182021466249494, Useless field: hello, Duration: 146, Sign: -1
Page views: 6, User id: 4324182021466249494, Useless field: world, Duration: 185, Sign: 1
Total rows: 2
```

``` sql
INSERT INTO UserActivity FORMAT Template SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format'
```

`/some/path/resultset.format`:

``` text
Some header\n${data}\nTotal rows: ${:CSV}\n
```

`/some/path/row.format`:

``` text
Page views: ${PageViews:CSV}, User id: ${UserID:CSV}, Useless field: ${:CSV}, Duration: ${Duration:CSV}, Sign: ${Sign:CSV}
```

`PageViews`, `UserID`, `Duration` と `Sign` 内部のプレースホルダーは、テーブル内の列の名前です。 後の値 `Useless field` 行とその後 `\nTotal rows:` in suffixは無視されます。
すべての区切り文字の入力データを厳密に等しい区切り文字で指定されたフォーマット文字列です。

## TemplateIgnoreSpaces {#templateignorespaces}

この形式は入力にのみ適しています。
に類似した `Template` しかし、入力ストリーム内の区切り文字と値の間の空白文字をスキップします。 ただし、書式指定文字列に空白文字が含まれている場合、これらの文字は入力ストリーム内で使用されます。 でも指定空のプレースホルダー (`${}` または `${:None}`）区切り文字を別々の部分に分割して、それらの間のスペースを無視します。 などのプレースホルダを使用させていただきますの飛び空白文字です。
それは読むことが可能です `JSON` 列の値がすべての行で同じ順序を持つ場合は、この形式を使用します。 たとえば、次のリクエストは、formatの出力例からデータを挿入するために使用できます [JSON](#json):

``` sql
INSERT INTO table_name FORMAT TemplateIgnoreSpaces SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format', format_template_rows_between_delimiter = ','
```

`/some/path/resultset.format`:

``` text
{${}"meta"${}:${:JSON},${}"data"${}:${}[${data}]${},${}"totals"${}:${:JSON},${}"extremes"${}:${:JSON},${}"rows"${}:${:JSON},${}"rows_before_limit_at_least"${}:${:JSON}${}}
```

`/some/path/row.format`:

``` text
{${}"SearchPhrase"${}:${}${phrase:JSON}${},${}"c"${}:${}${cnt:JSON}${}}
```

## TSKV {#tskv}

TabSeparatedに似ていますが、name=value形式で値を出力します。 名前はTabSeparated形式と同じ方法でエスケープされ、=記号もエスケープされます。

``` text
SearchPhrase=   count()=8267016
SearchPhrase=bathroom interior design    count()=2166
SearchPhrase=yandex     count()=1655
SearchPhrase=2014 spring fashion    count()=1549
SearchPhrase=freeform photos       count()=1480
SearchPhrase=angelina jolie    count()=1245
SearchPhrase=omsk       count()=1112
SearchPhrase=photos of dog breeds    count()=1091
SearchPhrase=curtain designs        count()=1064
SearchPhrase=baku       count()=1000
```

[NULL](../sql-reference/syntax.md) として書式設定される `\N`.

``` sql
SELECT * FROM t_null FORMAT TSKV
```

``` text
x=1    y=\N
```

小さな列が多数ある場合、この形式は無効であり、一般的に使用する理由はありません。 それにもかかわらず、それはJSONEachRowよりも悪くありません効率の面で。

Both data output and parsing are supported in this format. For parsing, any order is supported for the values of different columns. It is acceptable for some values to be omitted – they are treated as equal to their default values. In this case, zeros and blank rows are used as default values. Complex values that could be specified in the table are not supported as defaults.

構文解析できるの存在は、追加のフィールド `tskv` 等号または値なし。 この項目は無視されます。

## CSV {#csv}

カンマ区切りの値の形式 ([RFC](https://tools.ietf.org/html/rfc4180)).

書式設定の場合、行は二重引用符で囲まれます。 文字列内の二重引用符は、行の二重引用符として出力されます。 文字をエスケープするルールは他にありません。 Dateとdate-timeは二重引用符で囲みます。 数値は引用符なしで出力されます。 値は区切り文字で区切られます。 `,` デフォルトでは。 区切り文字は設定で定義されています [format_csv_delimiter](../operations/settings/settings.md#settings-format_csv_delimiter). 行は、Unixラインフィード(LF)を使用して区切られます。 最初に、配列はTabSeparated形式のように文字列にシリアル化され、結果の文字列は二重引用符でCSVに出力されます。 CSV形式のタプルは、個別の列としてシリアル化されます(つまり、タプル内の入れ子は失われます)。

``` bash
$ clickhouse-client --format_csv_delimiter="|" --query="INSERT INTO test.csv FORMAT CSV" < data.csv
```

\*デフォルトでは、区切り文字は `,`. を参照。 [format_csv_delimiter](../operations/settings/settings.md#settings-format_csv_delimiter) より多くの情報のための設定。

解析時には、すべての値を引用符の有無にかかわらず解析できます。 二重引用符と単一引quotesの両方がサポートされています。 行は引用符なしで配置することもできます。 この場合、区切り文字または改行（CRまたはLF）まで解析されます。 RFCに違反して、引用符なしで行を解析すると、先頭と末尾のスペースとタブは無視されます。 ラインフィードでは、Unix(LF)、Windows(CR LF)、およびMac OS Classic(CR LF)タイプがすべてサポートされています。

空の引用符で囲まれていない入力値は、それぞれの列のデフォルト値に置き換えられます。
[input_format_defaults_for_omitted_fields](../operations/settings/settings.md#session_settings-input_format_defaults_for_omitted_fields)
有効です。

`NULL` として書式設定される `\N` または `NULL` または空の引用符で囲まれていない文字列(設定を参照 [input_format_csv_unquoted_null_literal_as_null](../operations/settings/settings.md#settings-input_format_csv_unquoted_null_literal_as_null) と [input_format_defaults_for_omitted_fields](../operations/settings/settings.md#session_settings-input_format_defaults_for_omitted_fields)).

CSV形式では、合計と極値の出力は次のようにサポートされます `TabSeparated`.

## CSVWithNames {#csvwithnames}

また、次のようなヘッダー行も出力します `TabSeparatedWithNames`.

## カスタム区切り {#format-customseparated}

に類似した [テンプレー](#format-template) ですが、版画を読み込みまたは全てのカラムを使用脱出ルールからの設定 `format_custom_escaping_rule` 設定から区切り文字 `format_custom_field_delimiter`, `format_custom_row_before_delimiter`, `format_custom_row_after_delimiter`, `format_custom_row_between_delimiter`, `format_custom_result_before_delimiter` と `format_custom_result_after_delimiter` 書式指定文字列からではありません。
また `CustomSeparatedIgnoreSpaces` 形式は次のようになります `TemplateIgnoreSpaces`.

## JSON {#json}

JSON形式でデータを出力します。 データテーブルのほかに、列名と型、および出力行の合計数、制限がない場合に出力される可能性のある行の数などの追加情報も出力します。 例:

``` sql
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase WITH TOTALS ORDER BY c DESC LIMIT 5 FORMAT JSON
```

``` json
{
        "meta":
        [
                {
                        "name": "SearchPhrase",
                        "type": "String"
                },
                {
                        "name": "c",
                        "type": "UInt64"
                }
        ],

        "data":
        [
                {
                        "SearchPhrase": "",
                        "c": "8267016"
                },
                {
                        "SearchPhrase": "bathroom interior design",
                        "c": "2166"
                },
                {
                        "SearchPhrase": "yandex",
                        "c": "1655"
                },
                {
                        "SearchPhrase": "spring 2014 fashion",
                        "c": "1549"
                },
                {
                        "SearchPhrase": "freeform photos",
                        "c": "1480"
                }
        ],

        "totals":
        {
                "SearchPhrase": "",
                "c": "8873898"
        },

        "extremes":
        {
                "min":
                {
                        "SearchPhrase": "",
                        "c": "1480"
                },
                "max":
                {
                        "SearchPhrase": "",
                        "c": "8267016"
                }
        },

        "rows": 5,

        "rows_before_limit_at_least": 141137
}
```

JSONはJavaScriptと互換性があります。 これを確実にするために、一部の文字は追加でエスケープされます。 `/` としてエスケープ `\/`;代替の改行 `U+2028` と `U+2029` ブラウザによってはエスケープされます `\uXXXX`. ASCII制御文字はエスケープされます。 `\b`, `\f`, `\n`, `\r`, `\t` を使用して、00-1F範囲の残りのバイトと同様に `\uXXXX` sequences. Invalid UTF-8 sequences are changed to the replacement character � so the output text will consist of valid UTF-8 sequences. For compatibility with JavaScript, Int64 and UInt64 integers are enclosed in double-quotes by default. To remove the quotes, you can set the configuration parameter [output_format_json_quote_64bit_integers](../operations/settings/settings.md#session_settings-output_format_json_quote_64bit_integers) 0にする。

`rows` – The total number of output rows.

`rows_before_limit_at_least` そこにある行の最小数は制限なしであったでしょう。 出力の場合のみを含むクエリを制限します。
クエリにGROUP BYが含まれている場合、rows_before_limit_at_leastは制限なしで行われていた行の正確な数です。

`totals` – Total values (when using WITH TOTALS).

`extremes` – Extreme values (when extremes are set to 1).

この形式は、クエリ結果の出力にのみ適していますが、解析(テーブルに挿入するデータの取得)には適していません。

ClickHouseサポート [NULL](../sql-reference/syntax.md) として表示されます。 `null` JSON出力で。

も参照。 [JSONEachRow](#jsoneachrow) 形式。

## JSONCompact {#jsoncompact}

JSONとは異なるのは、データ行がオブジェクトではなく配列で出力される点だけです。

例:

``` json
{
        "meta":
        [
                {
                        "name": "SearchPhrase",
                        "type": "String"
                },
                {
                        "name": "c",
                        "type": "UInt64"
                }
        ],

        "data":
        [
                ["", "8267016"],
                ["bathroom interior design", "2166"],
                ["yandex", "1655"],
                ["fashion trends spring 2014", "1549"],
                ["freeform photo", "1480"]
        ],

        "totals": ["","8873898"],

        "extremes":
        {
                "min": ["","1480"],
                "max": ["","8267016"]
        },

        "rows": 5,

        "rows_before_limit_at_least": 141137
}
```

この形式は、クエリ結果の出力にのみ適していますが、解析(テーブルに挿入するデータの取得)には適していません。
も参照。 `JSONEachRow` 形式。

## JSONEachRow {#jsoneachrow}

この形式を使用する場合、ClickHouseは行を区切り、改行区切りのJSONオブジェクトとして出力しますが、データ全体は有効なJSONではありません。

``` json
{"SearchPhrase":"curtain designs","count()":"1064"}
{"SearchPhrase":"baku","count()":"1000"}
{"SearchPhrase":"","count()":"8267016"}
```

データを挿入するときは、各行に個別のJSONオブジェクトを指定する必要があります。

### データの挿入 {#inserting-data}

``` sql
INSERT INTO UserActivity FORMAT JSONEachRow {"PageViews":5, "UserID":"4324182021466249494", "Duration":146,"Sign":-1} {"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

クリックハウス:

-   オブジェクト内のキーと値のペアの任意の順序。
-   いくつかの値を省略します。

ClickHouseを無視した空間要素には、カンマの後にオブジェクト。 すべてのオブジェクトを一行で渡すことができます。 改行で区切る必要はありません。

**省略された値の処理**

ClickHouseは省略された値を対応するデフォルト値に置き換えます [データ型](../sql-reference/data-types/index.md).

もし `DEFAULT expr` に応じて異なる置換規則を使用します。 [input_format_defaults_for_omitted_fields](../operations/settings/settings.md#session_settings-input_format_defaults_for_omitted_fields) 設定。

次の表を考えます:

``` sql
CREATE TABLE IF NOT EXISTS example_table
(
    x UInt32,
    a DEFAULT x * 2
) ENGINE = Memory;
```

-   もし `input_format_defaults_for_omitted_fields = 0` のデフォルト値 `x` と `a` 等しい `0` のデフォルト値として `UInt32` データ型）。
-   もし `input_format_defaults_for_omitted_fields = 1` のデフォルト値 `x` 等しい `0` しかし、デフォルト値は `a` 等しい `x * 2`.

!!! note "警告"
    データを挿入するとき `insert_sample_with_metadata = 1`,ClickHouseは、挿入と比較して、より多くの計算リソースを消費します `insert_sample_with_metadata = 0`.

### データの選択 {#selecting-data}

を考える `UserActivity` 例としての表:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

クエリ `SELECT * FROM UserActivity FORMAT JSONEachRow` ﾂづｩﾂ。:

``` text
{"UserID":"4324182021466249494","PageViews":5,"Duration":146,"Sign":-1}
{"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

とは異なり [JSON](#json) 無効なUTF-8シーケンスの置換はありません。 値は、次の場合と同じ方法でエスケープされます `JSON`.

!!! note "注"
    任意のバイトセットを文字列に出力できます。 使用する `JSONEachRow` テーブル内のデータが情報を失うことなくJSONとしてフォーマットできることが確実な場合は、format。

### 入れ子構造の使用 {#jsoneachrow-nested}

あなたがテーブルを持っている場合 [入れ子](../sql-reference/data-types/nested-data-structures/nested.md) データ型の列には、同じ構造でJSONデータを挿入することができます。 この機能を有効にするには [input_format_import_nested_json](../operations/settings/settings.md#settings-input_format_import_nested_json) 設定。

たとえば、次の表を考えてみましょう:

``` sql
CREATE TABLE json_each_row_nested (n Nested (s String, i Int32) ) ENGINE = Memory
```

で見ることができるように `Nested` ClickHouseは、入れ子構造の各コンポーネントを個別の列として扱います (`n.s` と `n.i` 私達のテーブルのため）。 次の方法でデータを挿入できます:

``` sql
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n.s": ["abc", "def"], "n.i": [1, 23]}
```

挿入データとしての階層JSONオブジェクト [input_format_import_nested_json=1](../operations/settings/settings.md#settings-input_format_import_nested_json).

``` json
{
    "n": {
        "s": ["abc", "def"],
        "i": [1, 23]
    }
}
```

この設定がない場合、ClickHouseは例外をスローします。

``` sql
SELECT name, value FROM system.settings WHERE name = 'input_format_import_nested_json'
```

``` text
┌─name────────────────────────────┬─value─┐
│ input_format_import_nested_json │ 0     │
└─────────────────────────────────┴───────┘
```

``` sql
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
```

``` text
Code: 117. DB::Exception: Unknown field found while parsing JSONEachRow format: n: (at row 1)
```

``` sql
SET input_format_import_nested_json=1
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
SELECT * FROM json_each_row_nested
```

``` text
┌─n.s───────────┬─n.i────┐
│ ['abc','def'] │ [1,23] │
└───────────────┴────────┘
```

## ネイティブ {#native}

最も効率的な形式。 データ書き込みおよび読み込みをブロックのバイナリ形式です。 ブロックごとに、このブロック内の行数、列数、列名と型、および列の一部が次々と記録されます。 つまり、この形式は “columnar” – it doesn't convert columns to rows. This is the format used in the native interface for interaction between servers, for using the command-line client, and for C++ clients.

この形式を使用すると、ClickHouse DBMSでのみ読み取ることができるダンプをすばやく生成できます。 この形式で自分で作業するのは理にかなっていません。

## Null {#null}

何も出力されません。 ただし、クエリは処理され、コマンドラインクライアントを使用する場合、データはクライアントに送信されます。 この使用のための試験、性能試験をします。
明らかに、この形式は出力にのみ適しており、解析には適していません。

## プリティ {#pretty}

出力データとしてのUnicodeトテーブルも用ANSI-エスケープシーケンス設定色の端子です。
テーブルの完全なグリッドが描画され、各行は、端末内の二つの行を占めています。
各結果ブロックは、個別のテーブルとして出力されます。 これは、結果をバッファリングせずにブロックを出力できるようにするために必要です（すべての値の可視幅を事前に計算するためにはバッファ

[NULL](../sql-reference/syntax.md) として出力されます `ᴺᵁᴸᴸ`.

例（以下に示す [プリティコンパクト](#prettycompact) 形式):

``` sql
SELECT * FROM t_null
```

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

行はきれいな\*形式ではエスケープされません。 例はのために示されています [プリティコンパクト](#prettycompact) 形式:

``` sql
SELECT 'String with \'quotes\' and \t character' AS Escaping_test
```

``` text
┌─Escaping_test────────────────────────┐
│ String with 'quotes' and      character │
└──────────────────────────────────────┘
```

端末に大量のデータをダンプするのを避けるために、最初の10,000行だけが印刷されます。 行数が10,000以上の場合、メッセージ “Showed first 10 000” 印刷されます。
この形式は、クエリ結果の出力にのみ適していますが、解析(テーブルに挿入するデータの取得)には適していません。

Pretty形式は、合計値(合計と共に使用する場合)および極値(場合)の出力をサポートします ‘extremes’ 1)に設定される。 このような場合、合計値と極値は、メインデータの後に個別のテーブルで出力されます。 例（以下に示す [プリティコンパクト](#prettycompact) 形式):

``` sql
SELECT EventDate, count() AS c FROM test.hits GROUP BY EventDate WITH TOTALS ORDER BY EventDate FORMAT PrettyCompact
```

``` text
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

## プリティコンパクト {#prettycompact}

とは異なる [プリティ](#pretty) グリッドが行の間に描画され、結果がよりコンパクトになるという点で。
この形式は、対話モードのコマンドラインクライアントで既定で使用されます。

## プリティコンパクトモノブロック {#prettycompactmonoblock}

とは異なる [プリティコンパクト](#prettycompact) 最大10,000行がバッファリングされ、ブロックではなく単一のテーブルとして出力されます。

## プリティノスケープ {#prettynoescapes}

Ansiエスケープシーケンスが使用されない点でPrettyとは異なります。 これは、ブラウザでこの形式を表示するために必要です。 ‘watch’ コマンドライン

例:

``` bash
$ watch -n1 "clickhouse-client --query='SELECT event, value FROM system.events FORMAT PrettyCompactNoEscapes'"
```

HTTPインターフェイスを使用して、ブラウザーで表示できます。

### プリティコンパクトノスケープ {#prettycompactnoescapes}

前の設定と同じです。

### プリティスパセノスケープス {#prettyspacenoescapes}

前の設定と同じです。

## PrettySpace {#prettyspace}

とは異なる [プリティコンパクト](#prettycompact) その中で、グリッドの代わりに空白（空白文字）が使用されます。

## ローバイナリ {#rowbinary}

バイナリ形式で行ごとにデータを書式設定および解析します。 行と値は、区切り文字なしで連続して表示されます。
この形式は行ベースであるため、ネイティブ形式よりも効率的ではありません。

整数は固定長のリトルエンディアン表現を使用します。 たとえば、UInt64は8バイトを使用します。
DateTimeは、Unixタイムスタンプを値として含むUInt32として表されます。
Dateは、値として1970-01-01以降の日数を含むUInt16オブジェクトとして表されます。
文字列はvarintの長さ(符号なし)で表されます [LEB128](https://en.wikipedia.org/wiki/LEB128))の後に文字列のバイトが続きます。
FixedStringは、単にバイトのシーケンスとして表されます。

配列はvarintの長さとして表されます(符号なし [LEB128](https://en.wikipedia.org/wiki/LEB128))の後に、配列の連続する要素が続きます。

のために [NULL](../sql-reference/syntax.md#null-literal) 1または0を含む追加のバイトがそれぞれの前に追加されます [Null可能](../sql-reference/data-types/nullable.md) 値。 1の場合、値は `NULL` このバイトは別の値として解釈されます。 0の場合、バイトの後の値は `NULL`.

## RowBinaryWithNamesAndTypes {#rowbinarywithnamesandtypes}

に類似した [ローバイナリ](#rowbinary),しかし、追加されたヘッダ:

-   [LEB128](https://en.wikipedia.org/wiki/LEB128)-エンコードされた列数(N)
-   N `String`s列名の指定
-   N `String`s列タイプの指定

## 値 {#data-format-values}

すべての行をかっこで表示します。 行はカンマで区切られます。 最後の行の後にコンマはありません。 角かっこ内の値もコンマ区切りです。 数値は、引用符なしで小数点形式で出力されます。 配列は角かっこで出力されます。 文字列、日付、および時刻付きの日付は引用符で出力されます。 ルールのエスケープと解析は [TabSeparated](#tabseparated) 形式。 フォーマット中に余分なスペースは挿入されませんが、解析中には許可され、スキップされます（配列値内のスペースは許可されません）。 [NULL](../sql-reference/syntax.md) と表される。 `NULL`.

The minimum set of characters that you need to escape when passing data in Values ​​format: single quotes and backslashes.

これはで使用される形式です `INSERT INTO t VALUES ...` ただし、クエリ結果の書式設定にも使用できます。

も参照。: [input_format_values_interpret_expressions](../operations/settings/settings.md#settings-input_format_values_interpret_expressions) と [input_format_values_deduce_templates_of_expressions](../operations/settings/settings.md#settings-input_format_values_deduce_templates_of_expressions) 設定。

## 垂直 {#vertical}

列名を指定して、それぞれの値を別々の行に出力します。 この形式は、各行が多数の列で構成されている場合、一つまたは数つの行だけを印刷するのに便利です。

[NULL](../sql-reference/syntax.md) として出力されます `ᴺᵁᴸᴸ`.

例:

``` sql
SELECT * FROM t_null FORMAT Vertical
```

``` text
Row 1:
──────
x: 1
y: ᴺᵁᴸᴸ
```

行は縦書式でエスケープされません:

``` sql
SELECT 'string with \'quotes\' and \t with some special \n characters' AS test FORMAT Vertical
```

``` text
Row 1:
──────
test: string with 'quotes' and      with some special
 characters
```

この形式は、クエリ結果の出力にのみ適していますが、解析(テーブルに挿入するデータの取得)には適していません。

## VerticalRaw {#verticalraw}

に類似した [垂直](#vertical) しかし、エスケープ無効で。 この形式は、クエリ結果の出力にのみ適しており、解析（データの受信とテーブルへの挿入）には適していません。

## XML {#xml}

XML形式は出力にのみ適しており、解析には適していません。 例:

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
                        <SearchPhrase>yandex</SearchPhrase>
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

列名に許容可能な形式がない場合は、次のようにします ‘field’ 要素名として使用されます。 一般に、XML構造はJSON構造に従います。
Just as for JSON, invalid UTF-8 sequences are changed to the replacement character � so the output text will consist of valid UTF-8 sequences.

文字列値では、文字 `<` と `&` としてエスケープ `<` と `&`.

配列は次のように出力される `<array><elem>Hello</elem><elem>World</elem>...</array>`、およびタプルとして `<tuple><elem>Hello</elem><elem>World</elem>...</tuple>`.

## CapnProto {#capnproto}

Cap'n Protoは、プロトコルバッファや倹約に似たバイナリメッセージ形式ですが、JSONやMessagePackのようなものではありません。

Cap'n Protoメッセージは厳密に型指定され、自己記述ではないため、外部スキーマ記述が必要です。 スキーマはその場で適用され、クエリごとにキャッシュされます。

``` bash
$ cat capnproto_messages.bin | clickhouse-client --query "INSERT INTO test.hits FORMAT CapnProto SETTINGS format_schema='schema:Message'"
```

どこに `schema.capnp` このように見えます:

``` capnp
struct Message {
  SearchPhrase @0 :Text;
  c @1 :Uint64;
}
```

逆シリアル化は効果的であり、通常はシステム負荷を増加させません。

も参照。 [スキーマの書式](#formatschema).

## プロトブフ {#protobuf}

Protobufは-です [プロトコル](https://developers.google.com/protocol-buffers/) 形式。

この形式には、外部形式スキーマが必要です。 このスキーマをキャッシュ間のクエリ.
ClickHouseは両方をサポート `proto2` と `proto3` 構文。 繰り返/オプションに必要な分野に対応しています。

使用例:

``` sql
SELECT * FROM test.table FORMAT Protobuf SETTINGS format_schema = 'schemafile:MessageType'
```

``` bash
cat protobuf_messages.bin | clickhouse-client --query "INSERT INTO test.table FORMAT Protobuf SETTINGS format_schema='schemafile:MessageType'"
```

ここで、ファイル `schemafile.proto` このように見えます:

``` capnp
syntax = "proto3";

message MessageType {
  string name = 1;
  string surname = 2;
  uint32 birthDate = 3;
  repeated string phoneNumbers = 4;
};
```

の対応関係はテーブル列-分野のプロトコルバッファのメッセージタイプClickHouseを比較しつけられた名前が使われている。
この比較では、大文字と小文字は区別されません。 `_` (アンダースコア)と `.` （ドット）は等しいと見なされます。
列の型とプロトコルバッファのメッセージのフィールドが異なる場合は、必要な変換が適用されます。

ネストしたメッセージに対応します。 たとえば、フィールドの場合 `z` 次のメッセージの種類

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

ClickHouseは、名前のある列を検索しようとします `x.y.z` （または `x_y_z` または `X.y_Z` というように）。
入れ子になったメッセージは、 [入れ子データ構造](../sql-reference/data-types/nested-data-structures/nested.md).

次のようにprotobufスキーマで定義されたデフォルト値

``` capnp
syntax = "proto2";

message MessageType {
  optional int32 result_per_page = 3 [default = 10];
}
```

は適用されない。 [テーブルの既定値](../sql-reference/statements/create.md#create-default-values) それらの代わりに使用されます。

ClickHouseの入力および出力protobufのメッセージ `length-delimited` 形式。
これは、すべてのメッセージがその長さを書き込まれる前に [varint](https://developers.google.com/protocol-buffers/docs/encoding#varints).
も参照。 [一般的な言語で長さ区切りのprotobufメッセージを読み書きする方法](https://cwiki.apache.org/confluence/display/GEODE/Delimiting+Protobuf+Messages).

## アヴロ {#data-format-avro}

[Apache Avro](http://avro.apache.org/) ApacheのHadoopプロジェクト内で開発された行指向のデータ直列化フレームワークです。

ClickHouse Avro形式は読み書きをサポートします [Avroデータファイル](http://avro.apache.org/docs/current/spec.html#Object+Container+Files).

### データ型の一致 {#data_types-matching}

次の表に、サポートされているデータ型とClickHouseとの一致を示します [データ型](../sql-reference/data-types/index.md) で `INSERT` と `SELECT` クエリ。

| Avroデータ型 `INSERT`                       | ClickHouseデータ型                                                                                                | Avroデータ型 `SELECT`        |
|---------------------------------------------|-------------------------------------------------------------------------------------------------------------------|------------------------------|
| `boolean`, `int`, `long`, `float`, `double` | [Int(8/16/32)](../sql-reference/data-types/int-uint.md), [UInt(8/16/32)](../sql-reference/data-types/int-uint.md) | `int`                        |
| `boolean`, `int`, `long`, `float`, `double` | [Int64](../sql-reference/data-types/int-uint.md), [UInt64](../sql-reference/data-types/int-uint.md)               | `long`                       |
| `boolean`, `int`, `long`, `float`, `double` | [Float32](../sql-reference/data-types/float.md)                                                                   | `float`                      |
| `boolean`, `int`, `long`, `float`, `double` | [Float64](../sql-reference/data-types/float.md)                                                                   | `double`                     |
| `bytes`, `string`, `fixed`, `enum`          | [文字列](../sql-reference/data-types/string.md)                                                                   | `bytes`                      |
| `bytes`, `string`, `fixed`                  | [固定文字列(N)](../sql-reference/data-types/fixedstring.md)                                                       | `fixed(N)`                   |
| `enum`                                      | [Enum（8月16日)](../sql-reference/data-types/enum.md)                                                             | `enum`                       |
| `array(T)`                                  | [配列(T)](../sql-reference/data-types/array.md)                                                                   | `array(T)`                   |
| `union(null, T)`, `union(T, null)`          | [Nullable(T)](../sql-reference/data-types/date.md)                                                                | `union(null, T)`             |
| `null`                                      | [Nullable(Nothing)](../sql-reference/data-types/special-data-types/nothing.md)                                    | `null`                       |
| `int (date)` \*                             | [日付](../sql-reference/data-types/date.md)                                                                       | `int (date)` \*              |
| `long (timestamp-millis)` \*                | [DateTime64(3)](../sql-reference/data-types/datetime.md)                                                          | `long (timestamp-millis)` \* |
| `long (timestamp-micros)` \*                | [DateTime64(6)](../sql-reference/data-types/datetime.md)                                                          | `long (timestamp-micros)` \* |

\* [Avro論理タイプ](http://avro.apache.org/docs/current/spec.html#Logical+Types)

未サポートのAvroデータ型: `record` （非ルート), `map`

サポートされないAvro論理データ型: `uuid`, `time-millis`, `time-micros`, `duration`

### データの挿入 {#inserting-data-1}

AvroファイルからClickHouseテーブルにデータを挿入するには:

``` bash
$ cat file.avro | clickhouse-client --query="INSERT INTO {some_table} FORMAT Avro"
```

入力Avroファ `record` タイプ。

の対応関係はテーブル列分野のアブロスキーマClickHouseを比較しつけられた名前が使われている。 この比較では、大文字と小文字が区別されます。
未使用の項目はスキップされます。

データの種類ClickHouseテーブルの列ができ、対応する分野においてアブロのデータを挿入します。 データを挿入するとき、ClickHouseは上記の表に従ってデータ型を解釈し、次に [キャスト](../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) 対応する列タイプのデータ。

### データの選択 {#selecting-data-1}

ClickHouseテーブルからAvroファイルにデータを選択するには:

``` bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Avro" > file.avro
```

列名は必須です:

-   で始まる `[A-Za-z_]`
-   その後のみ `[A-Za-z0-9_]`

出力Avroファイルの圧縮および同期間隔は以下で設定できます [output_format_avro_codec](../operations/settings/settings.md#settings-output_format_avro_codec) と [output_format_avro_sync_interval](../operations/settings/settings.md#settings-output_format_avro_sync_interval) それぞれ。

## アブロコンフルエント {#data-format-avro-confluent}

AvroConfluent支援復号単一のオブジェクトアブロのメッセージを使用する [カフカ](https://kafka.apache.org/) と [Confluentスキーマレジストリ](https://docs.confluent.io/current/schema-registry/index.html).

各Avroメッセージには、スキーマレジストリを使用して実際のスキーマに解決できるスキーマidが埋め込まれます。

スキーマがキャッシュ一度に解決されます。

スキーマのレジストリのURLは設定され [format_avro_schema_registry_url](../operations/settings/settings.md#settings-format_avro_schema_registry_url)

### データ型の一致 {#data_types-matching-1}

同じ [アヴロ](#data-format-avro)

### 使用法 {#usage}

迅速な検証スキーマ分解能を使用でき [kafkacat](https://github.com/edenhill/kafkacat) と [ﾂつｨﾂ姪"ﾂ債ﾂつｹ](../operations/utilities/clickhouse-local.md):

``` bash
$ kafkacat -b kafka-broker  -C -t topic1 -o beginning -f '%s' -c 3 | clickhouse-local   --input-format AvroConfluent --format_avro_schema_registry_url 'http://schema-registry' -S "field1 Int64, field2 String"  -q 'select *  from table'
1 a
2 b
3 c
```

使用するには `AvroConfluent` と [カフカ](../engines/table-engines/integrations/kafka.md):

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

SET format_avro_schema_registry_url = 'http://schema-registry';

SELECT * FROM topic1_stream;
```

!!! note "警告"
    設定 `format_avro_schema_registry_url` で構成する必要があります `users.xml` 再起動後にその値を維持する。

## 寄木細工 {#data-format-parquet}

[アパッチの寄木細工](http://parquet.apache.org/) Hadoopエコシステムに広く普及している柱状ストレージ形式です。 ClickHouse支援を読み取りと書き込みの操作のためにこの形式です。

### データ型の一致 {#data_types-matching-2}

次の表に、サポートされているデータ型とClickHouseとの一致を示します [データ型](../sql-reference/data-types/index.md) で `INSERT` と `SELECT` クエリ。

| Parquetデータ型 (`INSERT`) | ClickHouseデータ型                                        | Parquetデータ型 (`SELECT`) |
|----------------------------|-----------------------------------------------------------|----------------------------|
| `UINT8`, `BOOL`            | [UInt8](../sql-reference/data-types/int-uint.md)          | `UINT8`                    |
| `INT8`                     | [Int8](../sql-reference/data-types/int-uint.md)           | `INT8`                     |
| `UINT16`                   | [UInt16](../sql-reference/data-types/int-uint.md)         | `UINT16`                   |
| `INT16`                    | [Int16](../sql-reference/data-types/int-uint.md)          | `INT16`                    |
| `UINT32`                   | [UInt32](../sql-reference/data-types/int-uint.md)         | `UINT32`                   |
| `INT32`                    | [Int32](../sql-reference/data-types/int-uint.md)          | `INT32`                    |
| `UINT64`                   | [UInt64](../sql-reference/data-types/int-uint.md)         | `UINT64`                   |
| `INT64`                    | [Int64](../sql-reference/data-types/int-uint.md)          | `INT64`                    |
| `FLOAT`, `HALF_FLOAT`      | [Float32](../sql-reference/data-types/float.md)           | `FLOAT`                    |
| `DOUBLE`                   | [Float64](../sql-reference/data-types/float.md)           | `DOUBLE`                   |
| `DATE32`                   | [日付](../sql-reference/data-types/date.md)               | `UINT16`                   |
| `DATE64`, `TIMESTAMP`      | [DateTime](../sql-reference/data-types/datetime.md)       | `UINT32`                   |
| `STRING`, `BINARY`         | [文字列](../sql-reference/data-types/string.md)           | `STRING`                   |
| —                          | [FixedString](../sql-reference/data-types/fixedstring.md) | `STRING`                   |
| `DECIMAL`                  | [小数点](../sql-reference/data-types/decimal.md)          | `DECIMAL`                  |

ClickHouseは構成可能の精密を支えます `Decimal` タイプ。 その `INSERT` クエリは、寄木細工を扱います `DECIMAL` ClickHouseとして入力します `Decimal128` タイプ。

対応していな寄木細工のデータ種類: `DATE32`, `TIME32`, `FIXED_SIZE_BINARY`, `JSON`, `UUID`, `ENUM`.

データの種類ClickHouseテーブルの列ができ、対応する分野の寄木細工のデータを挿入します。 データを挿入するとき、ClickHouseは上記の表に従ってデータ型を解釈し、次に [キャスト](../query_language/functions/type_conversion_functions/#type_conversion_function-cast) ClickHouseテーブル列に設定されているそのデータ型のデータ。

### データの挿入と選択 {#inserting-and-selecting-data}

次のコマンドで、ファイルからパーケットデータをClickHouseテーブルに挿入できます:

``` bash
$ cat {filename} | clickhouse-client --query="INSERT INTO {some_table} FORMAT Parquet"
```

ClickHouseテーブルからデータを選択し、次のコマンドでParquet形式でファイルに保存できます:

``` bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Parquet" > {some_file.pq}
```

Hadoopとデータを交換するには、以下を使用できます [HDFSテーブルエンジン](../engines/table-engines/integrations/hdfs.md).

## ORC {#data-format-orc}

[Apache ORC](https://orc.apache.org/) Hadoopエコシステムに広く普及している柱状ストレージ形式です。 この形式のデータはClickHouseにのみ挿入できます。

### データ型の一致 {#data_types-matching-3}

次の表に、サポートされているデータ型とClickHouseとの一致を示します [データ型](../sql-reference/data-types/index.md) で `INSERT` クエリ。

| ORCデータ型 (`INSERT`) | ClickHouseデータ型                                  |
|------------------------|-----------------------------------------------------|
| `UINT8`, `BOOL`        | [UInt8](../sql-reference/data-types/int-uint.md)    |
| `INT8`                 | [Int8](../sql-reference/data-types/int-uint.md)     |
| `UINT16`               | [UInt16](../sql-reference/data-types/int-uint.md)   |
| `INT16`                | [Int16](../sql-reference/data-types/int-uint.md)    |
| `UINT32`               | [UInt32](../sql-reference/data-types/int-uint.md)   |
| `INT32`                | [Int32](../sql-reference/data-types/int-uint.md)    |
| `UINT64`               | [UInt64](../sql-reference/data-types/int-uint.md)   |
| `INT64`                | [Int64](../sql-reference/data-types/int-uint.md)    |
| `FLOAT`, `HALF_FLOAT`  | [Float32](../sql-reference/data-types/float.md)     |
| `DOUBLE`               | [Float64](../sql-reference/data-types/float.md)     |
| `DATE32`               | [日付](../sql-reference/data-types/date.md)         |
| `DATE64`, `TIMESTAMP`  | [DateTime](../sql-reference/data-types/datetime.md) |
| `STRING`, `BINARY`     | [文字列](../sql-reference/data-types/string.md)     |
| `DECIMAL`              | [小数点](../sql-reference/data-types/decimal.md)    |

ClickHouseはの構成可能の精密を支えます `Decimal` タイプ。 その `INSERT` クエリはORCを扱います `DECIMAL` ClickHouseとして入力します `Decimal128` タイプ。

未サポートのORCデータ型: `DATE32`, `TIME32`, `FIXED_SIZE_BINARY`, `JSON`, `UUID`, `ENUM`.

ClickHouseテーブル列のデータ型は、対応するORCデータフィールドと一致する必要はありません。 データを挿入するとき、ClickHouseは上記の表に従ってデータ型を解釈し、次に [キャスト](../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) ClickHouseテーブル列のデータ型セットへのデータ。

### データの挿入 {#inserting-data-2}

次のコマンドで、ファイルからOrcデータをClickHouseテーブルに挿入できます:

``` bash
$ cat filename.orc | clickhouse-client --query="INSERT INTO some_table FORMAT ORC"
```

Hadoopとデータを交換するには、以下を使用できます [HDFSテーブルエンジン](../engines/table-engines/integrations/hdfs.md).

## スキーマの書式 {#formatschema}

形式スキーマを含むファイル名は、この設定によって設定されます `format_schema`.
いずれかの形式を使用する場合は、この設定を設定する必要があります `Cap'n Proto` と `Protobuf`.
形式スキーマは、ファイル名とこのファイル内のメッセージ型の名前の組み合わせで、コロンで区切られます,
e.g. `schemafile.proto:MessageType`.
ファイルが形式の標準拡張子を持っている場合（たとえば, `.proto` のために `Protobuf`),
この場合、形式スキーマは次のようになります `schemafile:MessageType`.

を介してデータを入力または出力する場合 [クライアン](../interfaces/cli.md) で [対話モード](../interfaces/cli.md#cli_usage),形式スキーマで指定されたファイル名
を含むことができ、絶対パス名は相対パスを現在のディレクトリのクライアント
クライアントを使用する場合 [バッチモード](../interfaces/cli.md#cli_usage) は、パスのスキーマ"相対的"に指定する必要があります。

を介してデータを入力または出力する場合 [HTTPインターフェ](../interfaces/http.md) 形式スキーマで指定されたファイル名
指定されたディレクトリにあるはずです。 [format_schema_path](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-format_schema_path)
サーバー構成で。

## スキップエラー {#skippingerrors}

次のような形式があります `CSV`, `TabSeparated`, `TSKV`, `JSONEachRow`, `Template`, `CustomSeparated` と `Protobuf` 解析エラーが発生した場合に壊れた行をスキップし、次の行の先頭から解析を続行できます。 見る [input_format_allow_errors_num](../operations/settings/settings.md#settings-input_format_allow_errors_num) と
[input_format_allow_errors_ratio](../operations/settings/settings.md#settings-input_format_allow_errors_ratio) 設定。
制限:
-解析エラーの場合 `JSONEachRow` 新しい行(またはEOF)まですべてのデータをスキップします。 `\n` エラーを正しく数える。
- `Template` と `CustomSeparated` 最後の列の後にdelimiterを使用し、行の間にdelimiterを使用すると、次の行の先頭を見つけることができます。

[元の記事](https://clickhouse.tech/docs/en/interfaces/formats/) <!--hide-->
