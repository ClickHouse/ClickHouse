---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 21
toc_title: "\u5165\u529B\u304A\u3088\u3073\u51FA\u529B\u5F62\u5F0F"
---

# 入力データと出力データの形式 {#formats}

ClickHouse受け入れと返信データなフレームワークです。 入力でサポートされている形式を使用して、指定されたデータを解析できます。 `INSERT`s、実行する `SELECT`ファイル、URL、またはHDFSなどのファイルバックアップテーブルから、または外部辞書を読み取ります。 出力用にサポートされている形式を使用して、
の結果 `SELECT`、および実行する `INSERT`ファイルによって支持される表へのs。

のサポートされるフォーマットは:

| 書式                                                                | 入力 | 出力 |
|---------------------------------------------------------------------|------|------|
| [タブ区切り](#tabseparated)                                         | ✔    | ✔    |
| [TabSeparatedRaw](#tabseparatedraw)                                 | ✗    | ✔    |
| [Tabseparatedwithnamesname](#tabseparatedwithnames)                 | ✔    | ✔    |
| [Tabseparatedwithnamesandtypesname](#tabseparatedwithnamesandtypes) | ✔    | ✔    |
| [テンプレ](#format-template)                                        | ✔    | ✔    |
| [TemplateIgnoreSpaces](#templateignorespaces)                       | ✔    | ✗    |
| [CSV](#csv)                                                         | ✔    | ✔    |
| [Csvwithnamesname](#csvwithnames)                                   | ✔    | ✔    |
| [CustomSeparated](#format-customseparated)                          | ✔    | ✔    |
| [値](#data-format-values)                                           | ✔    | ✔    |
| [垂直](#vertical)                                                   | ✗    | ✔    |
| [VerticalRaw](#verticalraw)                                         | ✗    | ✔    |
| [JSON](#json)                                                       | ✗    | ✔    |
| [JSONCompact](#jsoncompact)                                         | ✗    | ✔    |
| [JSONEachRow](#jsoneachrow)                                         | ✔    | ✔    |
| [TSKV](#tskv)                                                       | ✔    | ✔    |
| [可愛い](#pretty)                                                   | ✗    | ✔    |
| [PrettyCompact](#prettycompact)                                     | ✗    | ✔    |
| [PrettyCompactMonoBlock](#prettycompactmonoblock)                   | ✗    | ✔    |
| [PrettyNoEscapes](#prettynoescapes)                                 | ✗    | ✔    |
| [PrettySpace](#prettyspace)                                         | ✗    | ✔    |
| [Protobuf](#protobuf)                                               | ✔    | ✔    |
| [アブロ](#data-format-avro)                                         | ✔    | ✔    |
| [AvroConfluent](#data-format-avro-confluent)                        | ✔    | ✗    |
| [Parquet張り](#data-format-parquet)                                 | ✔    | ✔    |
| [ORC](#data-format-orc)                                             | ✔    | ✗    |
| [RowBinary](#rowbinary)                                             | ✔    | ✔    |
| [RowBinaryWithNamesAndTypes](#rowbinarywithnamesandtypes)           | ✔    | ✔    |
| [ネイティブ](#native)                                               | ✔    | ✔    |
| [ヌル](#null)                                                       | ✗    | ✔    |
| [XML](#xml)                                                         | ✗    | ✔    |
| [CapnProto](#capnproto)                                             | ✔    | ✗    |

ClickHouseの設定で、一部のフォーマット処理パラメータを制御できます。 詳細については、 [設定](../operations/settings/settings.md) セクション。

## タブ区切り {#tabseparated}

TabSeparated形式では、データは行によって書き込まれます。 各行にはタブで区切られた値が含まれます。 各値の後には、行の最後の値を除くタブが続き、その後に改行が続きます。 厳密にUnixの改行はどこでも想定されます。 最後の行には、最後に改行が含まれている必要があります。 値は、引用符を囲まずにテキスト形式で書き込まれ、特殊文字はエスケープされます。

この形式は、名前の下でも利用できます `TSV`.

その `TabSeparated` 形式は便利な加工データをカスタムプログラムやイントロダクションです。 デフォルトでは、HTTPインターフェイスとコマンドラインクライアントのバッチモードで使用されます。 この形式は、異なるDbms間でデータを転送することもできます。 たとえば、MySQLからダンプを取得してClickHouseにアップロードすることも、その逆にすることもできます。

その `TabSeparated` formatでは、合計値（合計と共に使用する場合）と極端な値（次の場合）の出力をサポートします ‘extremes’ 1)に設定します。 このような場合、メインデータの後に合計値と極値が出力されます。 主な結果、合計値、および極値は、空の行で区切られます。 例えば:

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

0000-00-00      8873898

2014-03-17      1031592
2014-03-23      1406958
```

### データの書式設定 {#data-formatting}

整数は小数で書かれています。 数字には、 “+” 最初の文字(解析時は無視され、書式設定時には記録されません)。 負でない数値には、負の符号を含めることはできません。 読み込むときには、空の文字列をゼロとして解析するか、（符号付きの型の場合）マイナス記号だけをゼロとして含む文字列を解析することができま 対応するデータ型に収まらない数値は、エラーメッセージなしで別の数値として解析できます。

浮動小数点数は小数で書かれています。 ドットは小数点として使用されます。 指数作に対応してい ‘inf’, ‘+inf’, ‘-inf’、と ‘nan’. 浮動小数点数のエントリは、小数点で開始または終了することができます。
書式設定時に、浮動小数点数の精度が失われることがあります。
解析中に、最も近いマシン表現可能な番号を読み取ることは厳密には必要ありません。

日付はyyyy-mm-dd形式で書かれ、同じ形式で解析されますが、任意の文字を区切り文字として使用します。
時刻を含む日付は、次の形式で書き込まれます `YYYY-MM-DD hh:mm:ss` 同じ形式で解析されますが、区切り文字として任意の文字が使用されます。
これはすべて、クライアントまたはサーバーの起動時にシステムタイムゾーンで発生します(データの形式に応じて異なります)。 時刻を含む日付の場合、夏時間は指定されません。 したがって、ダンプが夏時間の間に時間がある場合、ダンプはデータと明確に一致しません。
読み取り操作中に、時間を含む不適切な日付と日付は、エラーメッセージなしで自然なオーバーフローまたはnull日付と時刻として解析できます。

例外として、時刻を含む日付の解析は、unixタイムスタンプ形式でもサポートされています(正確に10桁の数字で構成されている場合)。 結果はタイムゾーンに依存しません。 フォーマットyyyy-mm-dd hh:mm:ssとnnnnnnnnnは自動的に区別されます。

文字列はバックスラッシュでエスケープされた特殊文字で出力されます。 以下のエスケープシーケンスを使用出力: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\'`, `\\`. 解析にも対応し配列 `\a`, `\v`、と `\xHH` (hexエスケープシーケンス)および `\c` シーケンス、ここで `c` は任意の文字です（これらのシーケンスは `c`). このように、データを読み込む形式に対応し、改行して書き込み可能で `\n` または `\`、または改行として。 たとえば、文字列 `Hello world` スペースではなく単語間の改行を使用すると、次のいずれかのバリエーションで解析できます:

``` text
Hello\nworld

Hello\
world
```

これは、mysqlがタブで区切られたダンプを書き込むときに使用するためです。

TabSeparated形式でデータを渡すときにエスケープする必要がある文字の最小セット：tab、改行（LF）、およびバックスラッシュ。

小さなシンボルのみがエスケープされます。 あなたの端末が出力で台無しにする文字列値に簡単につまずくことができます。

配列は、角かっこで囲まれたコンマ区切りの値のリストとして記述されます。 配列内の数値項目は通常どおりに書式設定されます。 `Date` と `DateTime` 型は一重引quotesで書き込まれます。 文字列は、上記と同じエスケープ規則で一重引quotesで書き込まれます。

[NULL](../sql-reference/syntax.md) フォーマットとして `\N`.

の各要素 [ネスト](../sql-reference/data-types/nested-data-structures/nested.md) 構造体は配列として表されます。

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

とは異なります `TabSeparated` エスケープせずに行が書き込まれるという形式です。
この形式は、クエリ結果を出力する場合にのみ適切ですが、解析(テーブルに挿入するデータの取得)には適していません。

この形式は、名前の下でも利用できます `TSVRaw`.

## Tabseparatedwithnamesname {#tabseparatedwithnames}

とは異なり `TabSeparated` 列名が最初の行に書き込まれる形式。
解析中、最初の行は完全に無視されます。 列名を使用して、列の位置を特定したり、列の正確性を確認したりすることはできません。
（ヘッダー行の解析のサポートは、将来追加される可能性があります。)

この形式は、名前の下でも利用できます `TSVWithNames`.

## Tabseparatedwithnamesandtypesname {#tabseparatedwithnamesandtypes}

とは異なり `TabSeparated` 列名が最初の行に書き込まれ、列タイプが次の行に書き込まれるという形式です。
解析時には、最初と二番目の行は完全に無視されます。

この形式は、名前の下でも利用できます `TSVWithNamesAndTypes`.

## テンプレ {#format-template}

このフォーマットで指定するカスタムフォーマット文字列とプレースホルダーのための値を指定して逃げます。

それは設定を使用します `format_template_resultset`, `format_template_row`, `format_template_rows_between_delimiter` and some settings of other formats (e.g. `output_format_json_quote_64bit_integers` 使用する場合 `JSON` エスケープ,さらに見る)

設定 `format_template_row` 次の構文の行の書式文字列を含むファイルへのパスを指定します:

`delimiter_1${column_1:serializeAs_1}delimiter_2${column_2:serializeAs_2} ... delimiter_N`,

どこに `delimiter_i` 値間の区切り文字です (`$` シンボルは `$$`),
`column_i` 値が選択または挿入される列の名前またはインデックスを指定します(空の場合、列はスキップされます),
`serializeAs_i` 列の値のエスケープ規則です。 以下の脱出ルールに対応:

-   `CSV`, `JSON`, `XML` （同じ名前の形式と同様に)
-   `Escaped` （同様に `TSV`)
-   `Quoted` （同様に `Values`)
-   `Raw` （エスケープせずに、同様に `TSVRaw`)
-   `None` (エスケープルールはありません。)

エスケープルールが省略された場合は、 `None` 使用されます。 `XML` と `Raw` 出力にのみ適しています。

したがって、次の書式文字列については:

      `Search phrase: ${SearchPhrase:Quoted}, count: ${c:Escaped}, ad price: $$${price:JSON};`

の値 `SearchPhrase`, `c` と `price` としてエスケープされる列 `Quoted`, `Escaped` と `JSON` (選択のために)印刷されるか、または(挿入のために)その間期待されます `Search phrase:`, `, count:`, `, ad price: $` と `;` それぞれ区切り文字。 例えば:

`Search phrase: 'bathroom interior design', count: 2166, ad price: $3;`

その `format_template_rows_between_delimiter` 最後の行を除くすべての行の後に印刷される（または期待される）行間の区切り文字を指定します (`\n` デフォルトでは)

設定 `format_template_resultset` resultsetの書式文字列を含むファイルへのパスを指定します。 Resultsetの書式文字列は、行の書式文字列と同じ構文を持ち、接頭辞、接尾辞、およびいくつかの追加情報を出力する方法を指定できます。 で以下のプレースホルダの代わりにカラム名:

-   `data` データのある行ですか `format_template_row` フォーマット `format_template_rows_between_delimiter`. このプレースホルダーの最初のプレースホルダー形式の文字列になります。
-   `totals` 合計値が入っている行です `format_template_row` 形式(合計と共に使用する場合)
-   `min` 最小値を持つ行です `format_template_row` フォーマット(極値が1に設定されている場合)
-   `max` は、最大値を持つ行です `format_template_row` フォーマット(極値が1に設定されている場合)
-   `rows` 出力行の合計数です
-   `rows_before_limit` そこにあったであろう行の最小数は制限なしです。 出力の場合のみを含むクエリを制限します。 クエリにGROUP BYが含まれている場合、rows\_before\_limit\_at\_leastは、制限なしで存在していた正確な行数です。
-   `time` リクエストの実行時間を秒単位で指定します
-   `rows_read` 読み取られた行の数です
-   `bytes_read` 読み込まれたバイト数(圧縮されていないバイト数)を指定します

プレースホルダ `data`, `totals`, `min` と `max` 必要な脱出ルールの指定（または `None` 明示的に指定する必要があります)。 残りのプレースホ
この `format_template_resultset` 設定は空の文字列です, `${data}` デフォルト値として使用されます。
Insertクエリ形式では、いくつかの列またはいくつかのフィールドをスキップすることができます。

選択例:

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

`PageViews`, `UserID`, `Duration` と `Sign` 内部のプレースホルダーは、テーブル内の列の名前です。 その後の値 `Useless field` 行とその後 `\nTotal rows:` サフィックスでは無視されます。
すべての区切り文字の入力データを厳密に等しい区切り文字で指定されたフォーマット文字列です。

## TemplateIgnoreSpaces {#templateignorespaces}

この形式は入力にのみ適しています。
に似て `Template` ただし、入力ストリームの区切り文字と値の間の空白文字はスキップします。 ただし、書式指定文字列に空白文字が含まれている場合は、これらの文字が入力ストリームに必要になります。 空のプレースホルダも指定できます (`${}` または `${:None}`)いくつかの区切り文字を別々の部分に分割して、それらの間の空白を無視する。 などのプレースホルダを使用させていただきますの飛び空白文字です。
それは読むことが可能です `JSON` 列の値がすべての行で同じ順序を持つ場合、この形式を使用します。 たとえば、次のリクエストは、formatの出力例からデータを挿入するために使用できます [JSON](#json):

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

TabSeparatedに似ていますが、name=value形式で値を出力します。 名前はTabSeparated形式と同じようにエスケープされ、=記号もエスケープされます。

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

[NULL](../sql-reference/syntax.md) フォーマットとして `\N`.

``` sql
SELECT * FROM t_null FORMAT TSKV
```

``` text
x=1    y=\N
```

多数の小さな列がある場合、この形式は無効であり、一般的にそれを使用する理由はありません。 それにもかかわらず、それは効率の面でjsoneachrowよりも悪くありません。

Both data output and parsing are supported in this format. For parsing, any order is supported for the values of different columns. It is acceptable for some values to be omitted – they are treated as equal to their default values. In this case, zeros and blank rows are used as default values. Complex values that could be specified in the table are not supported as defaults.

解析により、追加フィールドの存在が許可されます `tskv` 等号または値なし。 この項目は無視されます。

## CSV {#csv}

コンマ区切りの値の形式 ([RFC](https://tools.ietf.org/html/rfc4180)).

書式設定の場合、行は二重引用符で囲まれます。 文字列内の二重引用符は、行内の二つの二重引用符として出力されます。 文字をエスケープする他の規則はありません。 日付と日時は二重引用符で囲みます。 数字は引用符なしで出力されます。 値は区切り文字で区切られます。 `,` デフォルトでは。 区切り文字は設定で定義されます [format\_csv\_delimiter](../operations/settings/settings.md#settings-format_csv_delimiter). 行は、Unixの改行(LF)を使用して区切られます。 まず、配列をTabSeparated形式のように文字列にシリアル化し、結果の文字列を二重引用符でCSVに出力します。 CSV形式の組は、別々の列としてシリアル化されます（つまり、組内の入れ子は失われます）。

``` bash
$ clickhouse-client --format_csv_delimiter="|" --query="INSERT INTO test.csv FORMAT CSV" < data.csv
```

\*デフォルトでは、区切り文字は `,`. を見る [format\_csv\_delimiter](../operations/settings/settings.md#settings-format_csv_delimiter) より多くの情報のための設定。

解析時には、すべての値を引用符で囲んで解析することができます。 二重引用符と一重引quotesの両方がサポートされます。 行は、引用符なしで配置することもできます。 この場合、それらは区切り文字または改行（crまたはlf）まで解析されます。 rfcに違反して、引用符なしで行を解析するとき、先頭と末尾のスペースとタブは無視されます。 改行には、unix(lf)、windows(cr lf)、およびmac os classic(cr lf)タイプがすべてサポートされています。

空の引用符で囲まれていない入力値は、それぞれの列のデフォルト値に置き換えられます。
[input\_format\_defaults\_for\_omitted\_fields](../operations/settings/settings.md#session_settings-input_format_defaults_for_omitted_fields)
は有効です。

`NULL` フォーマットとして `\N` または `NULL` または、引用符で囲まれていない空の文字列(“設定”を参照 [input\_format\_csv\_unquoted\_null\_literal\_as\_null](../operations/settings/settings.md#settings-input_format_csv_unquoted_null_literal_as_null) と [input\_format\_defaults\_for\_omitted\_fields](../operations/settings/settings.md#session_settings-input_format_defaults_for_omitted_fields)).

CSV形式は、totalsとextremesの出力を次のようにサポートします `TabSeparated`.

## Csvwithnamesname {#csvwithnames}

また、次のようなヘッダー行も出力します `TabSeparatedWithNames`.

## CustomSeparated {#format-customseparated}

に似て [テンプレ](#format-template) ですが、版画を読み込みまたは全てのカラムを使用脱出ルールからの設定 `format_custom_escaping_rule` 設定からの区切り文字 `format_custom_field_delimiter`, `format_custom_row_before_delimiter`, `format_custom_row_after_delimiter`, `format_custom_row_between_delimiter`, `format_custom_result_before_delimiter` と `format_custom_result_after_delimiter`、書式文字列からではありません。
また、 `CustomSeparatedIgnoreSpaces` フォーマット `TemplateIgnoreSpaces`.

## JSON {#json}

JSON形式でデータを出力します。 データテーブルのほかに、列名と型、およびいくつかの追加情報(出力行の合計数、および制限がない場合に出力される可能性のある行の数)も出力します。 例えば:

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

JSONはJavaScriptと互換性があります。 これを確実にするために、一部の文字は追加でエスケープされます。 `/` としてエスケープ `\/`;代替改行 `U+2028` と `U+2029` いくつかのブラウザを破る、としてエスケープ `\uXXXX`. バックスペース、フォームフィード、ラインフィード、キャリッジリターン、および水平タブがエスケープされます `\b`, `\f`, `\n`, `\r`, `\t` 00-1F範囲の残りのバイトと同様に、 `\uXXXX` sequences. Invalid UTF-8 sequences are changed to the replacement character � so the output text will consist of valid UTF-8 sequences. For compatibility with JavaScript, Int64 and UInt64 integers are enclosed in double-quotes by default. To remove the quotes, you can set the configuration parameter [output\_format\_json\_quote\_64bit\_integers](../operations/settings/settings.md#session_settings-output_format_json_quote_64bit_integers) に0.

`rows` – The total number of output rows.

`rows_before_limit_at_least` そこにある行の最小数は制限なしであったでしょう。 出力の場合のみを含むクエリを制限します。
クエリにgroup byが含まれている場合、rows\_before\_limit\_at\_leastは、制限なしで存在していた正確な行数です。

`totals` – Total values (when using WITH TOTALS).

`extremes` – Extreme values (when extremes are set to 1).

この形式は、クエリ結果を出力する場合にのみ適切ですが、解析(テーブルに挿入するデータの取得)には適していません。

ClickHouse支援 [NULL](../sql-reference/syntax.md) として表示されます `null` JSON出力で。

また、 [JSONEachRow](#jsoneachrow) フォーマット。

## JSONCompact {#jsoncompact}

JSONとは異なり、データ行はオブジェクトではなく配列内に出力されます。

例えば:

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

この形式は、クエリ結果を出力する場合にのみ適切ですが、解析(テーブルに挿入するデータの取得)には適していません。
また、 `JSONEachRow` フォーマット。

## JSONEachRow {#jsoneachrow}

この形式を使用する場合、clickhouseは行を区切られた改行で区切られたjsonオブジェクトとして出力しますが、データ全体が有効なjsonではありません。

``` json
{"SearchPhrase":"curtain designs","count()":"1064"}
{"SearchPhrase":"baku","count()":"1000"}
{"SearchPhrase":"","count()":"8267016"}
```

データを挿入するときは、各行に別々のjsonオブジェクトを指定する必要があります。

### データの挿入 {#inserting-data}

``` sql
INSERT INTO UserActivity FORMAT JSONEachRow {"PageViews":5, "UserID":"4324182021466249494", "Duration":146,"Sign":-1} {"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

クリックハウスは:

-   オブジェクト内のキーと値のペアの順序。
-   いくつかの値を省略する。

ClickHouseを無視した空間要素には、カンマの後にオブジェクト。 すべてのオブジェクトを一行で渡すことができます。 改行で区切る必要はありません。

**省略された値の処理**

ClickHouseは、省略された値を対応するデフォルト値に置き換えます [データ型](../sql-reference/data-types/index.md).

もし `DEFAULT expr` は、ClickHouseはに応じて異なる置換規則を使用して、指定されています [input\_format\_defaults\_for\_omitted\_fields](../operations/settings/settings.md#session_settings-input_format_defaults_for_omitted_fields) 設定。

次の表を考えてみます:

``` sql
CREATE TABLE IF NOT EXISTS example_table
(
    x UInt32,
    a DEFAULT x * 2
) ENGINE = Memory;
```

-   もし `input_format_defaults_for_omitted_fields = 0` のデフォルト値を返します。 `x` と `a` 等しい `0` のデフォルト値として `UInt32` データ型)。
-   もし `input_format_defaults_for_omitted_fields = 1` のデフォルト値を返します。 `x` 等しい `0` しかし、デフォルト値は `a` 等しい `x * 2`.

!!! note "警告"
    データを挿入するとき `insert_sample_with_metadata = 1`、ClickHouseは、より多くの計算リソースを消費します。 `insert_sample_with_metadata = 0`.

### データの選択 {#selecting-data}

考慮する `UserActivity` 例として表:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

クエリ `SELECT * FROM UserActivity FORMAT JSONEachRow` を返します:

``` text
{"UserID":"4324182021466249494","PageViews":5,"Duration":146,"Sign":-1}
{"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

とは異なり [JSON](#json) 形式は、無効なUTF-8シーケンスの置換はありません。 値は、forと同じ方法でエスケープされます `JSON`.

!!! note "メモ"
    任意のバイトセットを文字列に出力することができます。 を使用 `JSONEachRow` テーブル内のデータをJSON形式にすることができると確信している場合は、情報を失うことなく書式設定します。

### 入れ子構造の使用法 {#jsoneachrow-nested}

あなたがテーブルを持っている場合 [ネスト](../sql-reference/data-types/nested-data-structures/nested.md) データ型の列には、同じ構造でJSONデータを挿入することができます。 この機能を有効にするには [input\_format\_import\_nested\_json](../operations/settings/settings.md#settings-input_format_import_nested_json) 設定。

たとえば、次の表を考えてみます:

``` sql
CREATE TABLE json_each_row_nested (n Nested (s String, i Int32) ) ENGINE = Memory
```

あなたが見ることができるように `Nested` データ型の説明、ClickHouseは、入れ子構造の各コンポーネントを個別の列として扱います (`n.s` と `n.i` 私達のテーブルのため）。 次の方法でデータを挿入できます:

``` sql
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n.s": ["abc", "def"], "n.i": [1, 23]}
```

データを階層jsonオブジェクトとして挿入するには、 [input\_format\_import\_nested\_json=1](../operations/settings/settings.md#settings-input_format_import_nested_json).

``` json
{
    "n": {
        "s": ["abc", "def"],
        "i": [1, 23]
    }
}
```

この設定がない場合、clickhouseは例外をスローします。

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

最も効率的な形式。 データ書き込みおよび読み込みをブロックのバイナリ形式です。 各ブロックについて、行数、列数、列名と型、およびこのブロック内の列の一部が次々に記録されます。 つまり、この形式は次のとおりです “columnar” – it doesn’t convert columns to rows. This is the format used in the native interface for interaction between servers, for using the command-line client, and for C++ clients.

この形式を使用すると、clickhouse dbmsでのみ読み取ることができるダンプをすばやく生成できます。 この形式を自分で操作するのは意味がありません。

## ヌル {#null}

何も出力されません。 ただし、クエリが処理され、コマンドラインクライアントを使用すると、データがクライアントに送信されます。 パフォーマンステストを含むテストに使用されます。
明らかに、この形式は出力にのみ適しており、解析には適していません。

## 可愛い {#pretty}

出力データとしてのunicodeトテーブルも用ansi-エスケープシーケンス設定色の端子です。
テーブルの完全なグリッドが描画され、各行は端末内の二行を占めています。
各結果ブロックは、別のテーブルとして出力されます。 これは、結果をバッファリングせずにブロックを出力できるようにするために必要です（すべての値の可視幅を事前に計算するためにバッファリ

[NULL](../sql-reference/syntax.md) として出力されます `ᴺᵁᴸᴸ`.

例(以下に示す [PrettyCompact](#prettycompact) 書式):

``` sql
SELECT * FROM t_null
```

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

行はpretty\*形式でエスケープされません。 例はのために示されています [PrettyCompact](#prettycompact) 書式:

``` sql
SELECT 'String with \'quotes\' and \t character' AS Escaping_test
```

``` text
┌─Escaping_test────────────────────────┐
│ String with 'quotes' and      character │
└──────────────────────────────────────┘
```

ターミナルへのデータのダンプを避けるために、最初の10,000行だけが出力されます。 行数が10,000以上の場合、メッセージは次のようになります “Showed first 10 000” 印刷されます。
この形式は、クエリ結果を出力する場合にのみ適切ですが、解析(テーブルに挿入するデータの取得)には適していません。

かの形式に対応出力の合計値（利用の場合との合計)は、極端な場合 ‘extremes’ 1)に設定します。 このような場合、合計値と極値がメインデータの後に別のテーブルで出力されます。 例(以下に示す [PrettyCompact](#prettycompact) 書式):

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
│ 0000-00-00 │ 8873898 │
└────────────┴─────────┘

Extremes:
┌──EventDate─┬───────c─┐
│ 2014-03-17 │ 1031592 │
│ 2014-03-23 │ 1406958 │
└────────────┴─────────┘
```

## PrettyCompact {#prettycompact}

とは異なります [可愛い](#pretty) グリッドが行間に描画され、結果がよりコンパクトになるという点です。
この形式は、対話モードのコマンドラインクライアントでは既定で使用されます。

## PrettyCompactMonoBlock {#prettycompactmonoblock}

とは異なります [PrettyCompact](#prettycompact) その中で最大10,000行がバッファリングされ、ブロックではなく単一のテーブルとして出力されます。

## PrettyNoEscapes {#prettynoescapes}

Ansi-escapeシーケンスが使用されていない点がPrettyと異なります。 これは、ブラウザでこの形式を表示するためだけでなく、 ‘watch’ コマンドラインユーティ

例えば:

``` bash
$ watch -n1 "clickhouse-client --query='SELECT event, value FROM system.events FORMAT PrettyCompactNoEscapes'"
```

ブラウザに表示するためにhttpインターフェイスを使用できます。

### Prettompactnoescapes {#prettycompactnoescapes}

前の設定と同じです。

### PrettySpaceNoEscapes {#prettyspacenoescapes}

前の設定と同じです。

## PrettySpace {#prettyspace}

とは異なります [PrettyCompact](#prettycompact) その空白（空白文字）では、グリッドの代わりに使用されます。

## RowBinary {#rowbinary}

バイナリ形式の行ごとにデータを書式設定および解析します。 行と値は、区切り文字なしで連続して一覧表示されます。
この形式は、行ベースであるため、ネイティブ形式よりも効率的ではありません。

整数は固定長のリトルエンディアン表現を使用します。 たとえば、uint64は8バイトを使用します。
DateTimeは、Unixタイムスタンプを値として含むUInt32として表されます。
日付はuint16オブジェクトとして表され、このオブジェクトには1970-01-01からの日数が値として含まれます。
Stringは、varintの長さ(符号なし)として表されます [LEB128](https://en.wikipedia.org/wiki/LEB128)その後に文字列のバイトが続きます。
FixedStringは、単純にバイトのシーケンスとして表されます。

配列は、varintの長さ(符号なし)として表されます [LEB128](https://en.wikipedia.org/wiki/LEB128)）、配列の連続した要素が続きます。

のために [NULL](../sql-reference/syntax.md#null-literal) 支援、追加のバイトを含む1または0が追加される前に各 [Nullable](../sql-reference/data-types/nullable.md) 値。 1の場合、値は次のようになります `NULL` このバイトは別の値として解釈されます。 0の場合、バイトの後の値はそうではありません `NULL`.

## RowBinaryWithNamesAndTypes {#rowbinarywithnamesandtypes}

に似て [RowBinary](#rowbinary)、しかし、追加されたヘッダと:

-   [LEB128](https://en.wikipedia.org/wiki/LEB128)-エンコードされた列数(N)
-   N `String`s列名の指定
-   N `String`s列タイプの指定

## 値 {#data-format-values}

版画毎に行ットに固定して使用します。 行はコンマで区切られます。 最後の行の後にコンマはありません。 角かっこ内の値もコンマで区切られます。 数字は引用符なしの小数点形式で出力されます。 配列は角かっこで囲まれて出力されます。 文字列、日付、および時刻を含む日付が引用符で囲まれて出力されます。 ルールのエスケープと解析は、 [タブ区切り](#tabseparated) フォーマット。 書式設定時には、余分なスペースは挿入されませんが、解析時には、それらは許可され、スキップされます（配列値内のスペースは許可されません）。 [NULL](../sql-reference/syntax.md) として表されます `NULL`.

The minimum set of characters that you need to escape when passing data in Values ​​format: single quotes and backslashes.

これは、以下で使用される形式です `INSERT INTO t VALUES ...` ただし、クエリ結果の書式設定にも使用できます。

また見なさい: [input\_format\_values\_interpret\_expressions](../operations/settings/settings.md#settings-input_format_values_interpret_expressions) と [input\_format\_values\_deduce\_templates\_of\_expressions](../operations/settings/settings.md#settings-input_format_values_deduce_templates_of_expressions) 設定。

## 垂直 {#vertical}

各値を、指定された列名とは別の行に出力します。 このフォーマットは、各行が多数の列で構成されている場合に、単一または少数の行だけを印刷する場合に便利です。

[NULL](../sql-reference/syntax.md) として出力されます `ᴺᵁᴸᴸ`.

例えば:

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

この形式は、クエリ結果を出力する場合にのみ適切ですが、解析(テーブルに挿入するデータの取得)には適していません。

## VerticalRaw {#verticalraw}

に似て [垂直](#vertical) しかし、無効にエスケープすると。 この形式は、クエリ結果の出力にのみ適しており、解析（データの受信とテーブルへの挿入）には適していません。

## XML {#xml}

XML形式は出力にのみ適しており、解析には適していません。 例えば:

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

列名に許容可能な形式がない場合は、 ‘field’ 要素名として使用されます。 一般に、XML構造はJSON構造に従います。
Just as for JSON, invalid UTF-8 sequences are changed to the replacement character � so the output text will consist of valid UTF-8 sequences.

文字列値では、文字 `<` と `&` としてエスケープ `<` と `&`.

配列は出力されます `<array><elem>Hello</elem><elem>World</elem>...</array>`、およびタプルとして `<tuple><elem>Hello</elem><elem>World</elem>...</tuple>`.

## CapnProto {#capnproto}

Cap’n Protoは、プロトコルバッファやThriftに似たバイナリメッセージ形式ですが、JSONやMessagePackには似ていません。

Cap’n Protoメッセージは厳密に型付けされており、自己記述型ではありません。 スキーマはその場で適用され、クエリごとにキャッシュされます。

``` bash
$ cat capnproto_messages.bin | clickhouse-client --query "INSERT INTO test.hits FORMAT CapnProto SETTINGS format_schema='schema:Message'"
```

どこに `schema.capnp` このように見える:

``` capnp
struct Message {
  SearchPhrase @0 :Text;
  c @1 :Uint64;
}
```

逆シリアル化は効果的であり、通常はシステムの負荷を増加させません。

また見なさい [書式スキーマ](#formatschema).

## Protobuf {#protobuf}

Protobufは-です [プロトコル](https://developers.google.com/protocol-buffers/) フォーマット。

この形式には、外部形式スキーマが必要です。 このスキーマをキャッシュ間のクエリ.
クリックハウスは、 `proto2` と `proto3` 構文。 繰り返し/省略可能/必須項目がサポートされます。

使用例:

``` sql
SELECT * FROM test.table FORMAT Protobuf SETTINGS format_schema = 'schemafile:MessageType'
```

``` bash
cat protobuf_messages.bin | clickhouse-client --query "INSERT INTO test.table FORMAT Protobuf SETTINGS format_schema='schemafile:MessageType'"
```

ここで、ファイル `schemafile.proto` このように見える:

``` capnp
syntax = "proto3";

message MessageType {
  string name = 1;
  string surname = 2;
  uint32 birthDate = 3;
  repeated string phoneNumbers = 4;
};
```

の対応関係はテーブル列-分野のプロトコルバッファのメッセージタイプclickhouseを比較しつけられた名前が使われている。
この比較では、大文字と小文字は区別されません `_` （アンダースコア）と `.` （ドット）は等しいとみなされます。
列の型とプロトコルバッファのメッセージのフィールドが異なる場合、必要な変換が適用されます。

ネストしたメッセージに対応します。 たとえば、フィールドの場合 `z` 次のメッセージタイプ

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

ClickHouseは、名前の付いた列を検索しようとします `x.y.z` （または `x_y_z` または `X.y_Z` など）。
ネストしたメッセージを入力と出力 [入れ子のデータ構造](../sql-reference/data-types/nested-data-structures/nested.md).

このようなprotobufスキーマで定義されたデフォルト値

``` capnp
syntax = "proto2";

message MessageType {
  optional int32 result_per_page = 3 [default = 10];
}
```

適用されない。 [表のデフォルト](../sql-reference/statements/create.md#create-default-values) それらの代わりに使用されます。

クリックハウスの入力および出力のprotobufメッセージ `length-delimited` フォーマット。
これは、すべてのメッセージがその長さを [varint](https://developers.google.com/protocol-buffers/docs/encoding#varints).
また見なさい [一般的な言語で長さ区切りのprotobufメッセージを読み書きする方法](https://cwiki.apache.org/confluence/display/GEODE/Delimiting+Protobuf+Messages).

## アブロ {#data-format-avro}

[Apache Avro](http://avro.apache.org/) は、列指向データを直列化の枠組みに発展してApache Hadoopのプロジェクト.

ClickHouseアブロ形式の読み書きの支援 [Avroデータファイル](http://avro.apache.org/docs/current/spec.html#Object+Container+Files).

### 一致するデータ型 {#data_types-matching}

下の表に、サポートされているデータの種類とどのように試合clickhouse [データ型](../sql-reference/data-types/index.md) で `INSERT` と `SELECT` クエリ。

| Avroデータ型 `INSERT`                       | ClickHouseデータタイプ                                                                                            | Avroデータ型 `SELECT`        |
|---------------------------------------------|-------------------------------------------------------------------------------------------------------------------|------------------------------|
| `boolean`, `int`, `long`, `float`, `double` | [Int(8/16/32)](../sql-reference/data-types/int-uint.md), [UInt(8/16/32)](../sql-reference/data-types/int-uint.md) | `int`                        |
| `boolean`, `int`, `long`, `float`, `double` | [Int64](../sql-reference/data-types/int-uint.md), [UInt64](../sql-reference/data-types/int-uint.md)               | `long`                       |
| `boolean`, `int`, `long`, `float`, `double` | [Float32](../sql-reference/data-types/float.md)                                                                   | `float`                      |
| `boolean`, `int`, `long`, `float`, `double` | [Float64](../sql-reference/data-types/float.md)                                                                   | `double`                     |
| `bytes`, `string`, `fixed`, `enum`          | [文字列](../sql-reference/data-types/string.md)                                                                   | `bytes`                      |
| `bytes`, `string`, `fixed`                  | [FixedString(N)](../sql-reference/data-types/fixedstring.md)                                                      | `fixed(N)`                   |
| `enum`                                      | [Enum(8/16)](../sql-reference/data-types/enum.md)                                                                 | `enum`                       |
| `array(T)`                                  | [配列(t)](../sql-reference/data-types/array.md)                                                                   | `array(T)`                   |
| `union(null, T)`, `union(T, null)`          | [Nullable(T)](../sql-reference/data-types/date.md)                                                                | `union(null, T)`             |
| `null`                                      | [Nullable(何もなし)](../sql-reference/data-types/special-data-types/nothing.md)                                   | `null`                       |
| `int (date)` \*                             | [日付](../sql-reference/data-types/date.md)                                                                       | `int (date)` \*              |
| `long (timestamp-millis)` \*                | [DateTime64(3)](../sql-reference/data-types/datetime.md)                                                          | `long (timestamp-millis)` \* |
| `long (timestamp-micros)` \*                | [DateTime64(6)](../sql-reference/data-types/datetime.md)                                                          | `long (timestamp-micros)` \* |

\* [Avro論理型](http://avro.apache.org/docs/current/spec.html#Logical+Types)

未サポートのavroデータ型: `record` (非ルート), `map`

サポートされないavro論理データ型: `uuid`, `time-millis`, `time-micros`, `duration`

### データの挿入 {#inserting-data-1}

AvroファイルのデータをClickHouseテーブルに挿入するには:

``` bash
$ cat file.avro | clickhouse-client --query="INSERT INTO {some_table} FORMAT Avro"
```

入力avroファ `record` タイプ。

AvroスキーマClickHouseのテーブル列とフィールド間の対応を検索するには、その名前を比較します。 この比較では、大文字と小文字が区別されます。
未使用の項目はスキップされます。

データの種類clickhouseテーブルの列ができ、対応する分野においてアブロのデータを挿入します。 データを挿入するとき、clickhouseは上記の表に従ってデータ型を解釈します [キャスト](../query_language/functions/type_conversion_functions/#type_conversion_function-cast) 対応する列タイプのデータ。

### データの選択 {#selecting-data-1}

ClickHouseテーブルからAvroファイルにデータを選択するには:

``` bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Avro" > file.avro
```

列名は必須です:

-   始める `[A-Za-z_]`
-   その後のみ `[A-Za-z0-9_]`

出力avroファイル圧縮およびsync間隔はと形成することができます [output\_format\_avro\_codec](../operations/settings/settings.md#settings-output_format_avro_codec) と [output\_format\_avro\_sync\_interval](../operations/settings/settings.md#settings-output_format_avro_sync_interval) それぞれ。

## AvroConfluent {#data-format-avro-confluent}

AvroConfluent支援復号単一のオブジェクトアブロのメッセージを使用する [カフカname](https://kafka.apache.org/) と [Confluentスキーマレジストリ](https://docs.confluent.io/current/schema-registry/index.html).

各avroメッセージには、スキーマレジストリを使用して実際のスキーマに解決できるスキーマidが埋め込まれます。

スキーマがキャッシュ一度に解決されます。

スキーマレジスト [format\_avro\_schema\_registry\_url](../operations/settings/settings.md#settings-format_avro_schema_registry_url)

### 一致するデータ型 {#data_types-matching-1}

と同じ [アブロ](#data-format-avro)

### 使い方 {#usage}

使用できるスキーマ解決をすばやく検証するには [kafkacat](https://github.com/edenhill/kafkacat) と [ﾂつ"ﾂづ按つｵﾂ！](../operations/utilities/clickhouse-local.md):

``` bash
$ kafkacat -b kafka-broker  -C -t topic1 -o beginning -f '%s' -c 3 | clickhouse-local   --input-format AvroConfluent --format_avro_schema_registry_url 'http://schema-registry' -S "field1 Int64, field2 String"  -q 'select *  from table'
1 a
2 b
3 c
```

使用するには `AvroConfluent` と [カフカname](../engines/table-engines/integrations/kafka.md):

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
    設定 `format_avro_schema_registry_url` で構成する必要があります `users.xml` 再起動後にその価値を維持する。

## Parquet張り {#data-format-parquet}

[Apacheの寄木細工](http://parquet.apache.org/) Hadoopエコシステムでは柱状のストレージ形式が普及しています。 ClickHouse支援を読み取りと書き込みの操作のためにこの形式です。

### 一致するデータ型 {#data_types-matching-2}

下の表に、サポートされているデータの種類とどのように試合clickhouse [データ型](../sql-reference/data-types/index.md) で `INSERT` と `SELECT` クエリ。

| Parquetデータ型 (`INSERT`) | ClickHouseデータタイプ                                    | Parquetデータ型 (`SELECT`) |
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
| `DECIMAL`                  | [小数](../sql-reference/data-types/decimal.md)            | `DECIMAL`                  |

ClickHouseは構成可能の精密をの支えます `Decimal` タイプ。 その `INSERT` クエリは寄木細工を扱います `DECIMAL` クリックハウスとして入力 `Decimal128` タイプ。

Parquetデータ型: `DATE32`, `TIME32`, `FIXED_SIZE_BINARY`, `JSON`, `UUID`, `ENUM`.

データの種類clickhouseテーブルの列ができ、対応する分野の寄木細工のデータを挿入します。 データを挿入するとき、clickhouseは上記の表に従ってデータ型を解釈します [キャスト](../query_language/functions/type_conversion_functions/#type_conversion_function-cast) ClickHouseテーブル列に設定されているデータ型へのデータ。

### データの挿入と選択 {#inserting-and-selecting-data}

次のコマンドを使用して、ファイルのparquetデータをclickhouseテーブルに挿入できます:

``` bash
$ cat {filename} | clickhouse-client --query="INSERT INTO {some_table} FORMAT Parquet"
```

次のコマンドを実行すると、clickhouseテーブルからデータを選択し、parquet形式のファイルに保存することができます:

``` bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Parquet" > {some_file.pq}
```

Hadoopとデータを交換するには、次のようにします [HDFSテーブルエンジン](../engines/table-engines/integrations/hdfs.md).

## ORC {#data-format-orc}

[Apache ORC](https://orc.apache.org/) Hadoopエコシステムでは柱状のストレージ形式が普及しています。 この形式のデータのみをClickHouseに挿入できます。

### 一致するデータ型 {#data_types-matching-3}

下の表に、サポートされているデータの種類とどのように試合clickhouse [データ型](../sql-reference/data-types/index.md) で `INSERT` クエリ。

| ORCデータ型 (`INSERT`) | ClickHouseデータタイプ                              |
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
| `DECIMAL`              | [小数](../sql-reference/data-types/decimal.md)      |

ClickHouseはの構成可能の精密を支えます `Decimal` タイプ。 その `INSERT` クエリはORCを処理します `DECIMAL` クリックハウスとして入力 `Decimal128` タイプ。

サポートされていな: `DATE32`, `TIME32`, `FIXED_SIZE_BINARY`, `JSON`, `UUID`, `ENUM`.

ClickHouseテーブル列のデータ型は、対応するORCデータフィールドと一致する必要はありません。 データを挿入するとき、ClickHouseは上記の表に従ってデータ型を解釈します [キャスト](../query_language/functions/type_conversion_functions/#type_conversion_function-cast) ClickHouseテーブル列のデータ型セットに対するデータ。

### データの挿入 {#inserting-data-2}

次のコマンドを実行すると、ファイルのorcデータをclickhouseテーブルに挿入できます:

``` bash
$ cat filename.orc | clickhouse-client --query="INSERT INTO some_table FORMAT ORC"
```

Hadoopとデータを交換するには、次のようにします [HDFSテーブルエンジン](../engines/table-engines/integrations/hdfs.md).

## 書式スキーマ {#formatschema}

ファイルのファイル名を含む形式スキーマを設定し、設定 `format_schema`.
いずれかの形式を使用する場合は、この設定を行う必要があります `Cap'n Proto` と `Protobuf`.
フォーマット-スキーマは、このファイル内のファイル名とメッセージ-タイプ名の組み合わせで、コロンで区切られます,
e.g. `schemafile.proto:MessageType`.
ファイルにフォーマットの標準拡張子がある場合(例えば, `.proto` のために `Protobuf`),
これは省略することができ、この場合、形式スキーマは次のようになります `schemafile:MessageType`.

データを入力するか、または出力すれば [お客様](../interfaces/cli.md) で [対話モード](../interfaces/cli.md#cli_usage)、フォーマットスキーマで指定されたファイル名
クライアン
でクライアントを使用する場合 [バッチモード](../interfaces/cli.md#cli_usage) は、パスのスキーマ“相対的”に指定する必要があります。

データを入力するか、または出力すれば [HTTPインター](../interfaces/http.md) フォーマットスキーマで指定したファイル名
に指定されたディレクトリにあるはずです。 [format\_schema\_path](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-format_schema_path)
サーバー構成で。

## エラーのスキップ {#skippingerrors}

以下のようないくつかの形式 `CSV`, `TabSeparated`, `TSKV`, `JSONEachRow`, `Template`, `CustomSeparated` と `Protobuf` キ壊れた列が構文解析エラーが発生したときの解析から初めます。 見る [input\_format\_allow\_errors\_num](../operations/settings/settings.md#settings-input_format_allow_errors_num) と
[input\_format\_allow\_errors\_ratio](../operations/settings/settings.md#settings-input_format_allow_errors_ratio) 設定。
制限:
-解析エラーの場合 `JSONEachRow` 新しい行（またはEOF）まですべてのデータをスキップするので、行は次のように区切られます `\n` エラーを正確にカウントする。
- `Template` と `CustomSeparated` 最後の列の後にdelimiterを使用し、次の行の先頭を見つけるために行間の区切り文字を使用するので、エラーをスキップすると、少なくとも一方が空でない

[元の記事](https://clickhouse.tech/docs/en/interfaces/formats/) <!--hide-->
