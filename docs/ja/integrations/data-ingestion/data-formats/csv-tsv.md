---
sidebar_label: CSV と TSV
slug: /ja/integrations/data-formats/csv-tsv
---

# ClickHouseでのCSVとTSVデータの操作

ClickHouseは、CSVからのデータのインポートおよびエクスポートをサポートしています。CSVファイルはヘッダ行、カスタムデリミタ、エスケープシンボルなど、さまざまなフォーマット仕様を持つ場合があります。そのため、ClickHouseは各ケースに効率的に対応するためのフォーマットと設定を提供しています。

## CSVファイルからのデータのインポート

データをインポートする前に、次のような関連する構造のテーブルを作成します:

```sql
CREATE TABLE sometable
(
    `path` String,
    `month` Date,
    `hits` UInt32
)
ENGINE = MergeTree
ORDER BY tuple(month, path)
```

この[CSVファイル](assets/data_small.csv)から`sometable`テーブルにデータをインポートするには、ファイルを直接clickhouse-clientにパイプします：

```bash
clickhouse-client -q "INSERT INTO sometable FORMAT CSV" < data_small.csv
```

[FORMAT CSV](/docs/ja/interfaces/formats.md/#csv)を使用して、CSVフォーマットのデータを取り込むことをClickHouseに知らせます。別の方法として、ローカルファイルからデータをロードする場合は、[FROM INFILE](/docs/ja/sql-reference/statements/insert-into.md/#inserting-data-from-a-file)句を使用します：

```sql
INSERT INTO sometable
FROM INFILE 'data_small.csv'
FORMAT CSV
```

ここでは、`FORMAT CSV`句を使用して、ClickHouseがファイルフォーマットを理解できるようにしています。URLから直接データをロードするには[url()](/docs/ja/sql-reference/table-functions/url.md/)関数や、S3ファイルからは[s3()](/docs/ja/sql-reference/table-functions/s3.md/)関数を使用できます。

:::tip
`file()`および`INFILE`/`OUTFILE`に対して明示的なフォーマット設定をスキップできます。
その場合、ClickHouseはファイルの拡張子に基づいてフォーマットを自動的に検出します。
:::

### ヘッダ付きのCSVファイル

[ヘッダ付きのCSVファイル](assets/data_small_headers.csv)があると仮定します：

```bash
head data-small-headers.csv
```
```response
"path","month","hits"
"Akiba_Hebrew_Academy","2017-08-01",241
"Aegithina_tiphia","2018-02-01",34
```

このファイルからデータをインポートするには、[CSVWithNames](/docs/ja/interfaces/formats.md/#csvwithnames)フォーマットを使用します：

```bash
clickhouse-client -q "INSERT INTO sometable FORMAT CSVWithNames" < data_small_headers.csv
```

この場合、ClickHouseはファイルの最初の行をスキップしてデータをインポートします。

:::tip
23.1[バージョン](https://github.com/ClickHouse/ClickHouse/releases)以降では、`CSV`タイプを使用した場合にClickHouseがCSVファイルのヘッダを自動的に検出するようになるので、`CSVWithNames`や`CSVWithNamesAndTypes`を使用する必要がありません。
:::

### カスタムデリミタ付きのCSVファイル

CSVファイルがカンマ以外のデリミタを使用する場合は、[format_csv_delimiter](/docs/ja/operations/settings/settings-formats.md/#format_csv_delimiter)オプションを使用して関連するシンボルを設定できます：

```sql
SET format_csv_delimiter = ';'
```

これで、CSVファイルからのインポート時にカンマの代わりに`;`シンボルがデリミタとして使用されます。

### CSVファイルの行のスキップ

時には、CSVファイルからのデータインポート時に特定の行数をスキップすることがあります。これは[input_format_csv_skip_first_lines](/docs/ja/operations/settings/settings-formats.md/#input_format_csv_skip_first_lines)オプションを使用して行うことができます：

```sql
SET input_format_csv_skip_first_lines = 10
```

この場合、CSVファイルの最初の10行をスキップします：

```sql
SELECT count(*) FROM file('data-small.csv', CSV)
```
```response
┌─count()─┐
│     990 │
└─────────┘
```

[ファイル](assets/data_small.csv)には1k行がありますが、最初の10行をスキップするように指示したため、ClickHouseは990行のみをロードしました。

:::tip
`file()`関数を使用する場合、ClickHouse Cloudでは、ファイルが存在するマシン上で`clickhouse client`を実行する必要があります。ローカルでファイルを探索するには[`clickhouse-local`](/docs/ja/operations/utilities/clickhouse-local.md)を使用することもできます。
:::

### CSVファイルでのNULL値の扱い

NULL値は、ファイルを生成したアプリケーションに応じて異なる方法でエンコードされることがあります。デフォルトでは、ClickHouseはCSVで`\N`をNULL値として使用しますが、[format_csv_null_representation](/docs/ja/operations/settings/settings-formats.md/#format_tsv_null_representation)オプションを使用して変更できます。

次のCSVファイルがあるとします：

```bash
> cat nulls.csv
Donald,90
Joe,Nothing
Nothing,70
```

このファイルからデータをロードすると、ClickHouseは`Nothing`を文字列として扱います（これが正しい）：

```sql
SELECT * FROM file('nulls.csv')
```
```response
┌─c1──────┬─c2──────┐
│ Donald  │ 90      │
│ Joe     │ Nothing │
│ Nothing │ 70      │
└─────────┴─────────┘
```

ClickHouseに`Nothing`を`NULL`として扱わせたい場合は、次のオプションを使用して定義できます：

```sql
SET format_csv_null_representation = 'Nothing'
```

これで、期待通りに`NULL`が入ります：

```sql
SELECT * FROM file('nulls.csv')
```
```response
┌─c1─────┬─c2───┐
│ Donald │ 90   │
│ Joe    │ ᴺᵁᴸᴸ │
│ ᴺᵁᴸᴸ   │ 70   │
└────────┴──────┘
```

## TSV（タブ区切り）ファイル

タブ区切りデータフォーマットは、データ交換フォーマットとして広く使用されているフォーマットです。[TSVファイル](assets/data_small.tsv)からClickHouseにデータをロードするには、[TabSeparated](/docs/ja/interfaces/formats.md/#tabseparated)フォーマットを使用します：

```bash
clickhouse-client -q "INSERT INTO sometable FORMAT TabSeparated" < data_small.tsv
```

ヘッダを持つTSVファイルを操作するための[TabSeparatedWithNames](/docs/ja/interfaces/formats.md/#tabseparatedwithnames)フォーマットもあります。また、CSVと同様に、[input_format_tsv_skip_first_lines](/docs/ja/operations/settings/settings-formats.md/#input_format_tsv_skip_first_lines)オプションを使用して最初のX行をスキップすることもできます。

### Raw TSV

時折、TSVファイルはタブや改行をエスケープせずに保存されることがあります。そのようなファイルを扱うためには、[TabSeparatedRaw](/docs/ja/interfaces/formats.md/#tabseparatedraw)を使用するべきです。

## CSVへのエクスポート

前の例のいずれかのフォーマットは、データをエクスポートするためにも使用できます。テーブル（またはクエリ）からCSVフォーマットにデータをエクスポートするには、同じ`FORMAT`句を使用します：

```sql
SELECT *
FROM sometable
LIMIT 5
FORMAT CSV
```
```response
"Akiba_Hebrew_Academy","2017-08-01",241
"Aegithina_tiphia","2018-02-01",34
"1971-72_Utah_Stars_season","2016-10-01",1
"2015_UEFA_European_Under-21_Championship_qualification_Group_8","2015-12-01",73
"2016_Greater_Western_Sydney_Giants_season","2017-05-01",86
```

CSVファイルにヘッダを追加するには、[CSVWithNames](/docs/ja/interfaces/formats.md/#csvwithnames)フォーマットを使用します：

```sql
SELECT *
FROM sometable
LIMIT 5
FORMAT CSVWithNames
```
```response
"path","month","hits"
"Akiba_Hebrew_Academy","2017-08-01",241
"Aegithina_tiphia","2018-02-01",34
"1971-72_Utah_Stars_season","2016-10-01",1
"2015_UEFA_European_Under-21_Championship_qualification_Group_8","2015-12-01",73
"2016_Greater_Western_Sydney_Giants_season","2017-05-01",86
```

### エクスポートしたデータをCSVファイルに保存

エクスポートしたデータをファイルに保存するには、[INTO…OUTFILE](/docs/ja/sql-reference/statements/select/into-outfile.md)句を使用します：

```sql
SELECT *
FROM sometable
INTO OUTFILE 'out.csv'
FORMAT CSVWithNames
```
```response
36838935 rows in set. Elapsed: 1.304 sec. Processed 36.84 million rows, 1.42 GB (28.24 million rows/s., 1.09 GB/s.)
```

36百万行をCSVファイルに保存するのにClickHouseが約**1**秒かかりました。

### カスタムデリミタでのCSVエクスポート

カンマ以外のデリミタを使用したい場合は、[format_csv_delimiter](/docs/ja/operations/settings/settings-formats.md/#format_csv_delimiter)設定オプションを使用できます：

```sql
SET format_csv_delimiter = '|'
```

これで、ClickHouseはCSVフォーマットで`|`をデリミタとして使用します：

```sql
SELECT *
FROM sometable
LIMIT 5
FORMAT CSV
```
```response
"Akiba_Hebrew_Academy"|"2017-08-01"|241
"Aegithina_tiphia"|"2018-02-01"|34
"1971-72_Utah_Stars_season"|"2016-10-01"|1
"2015_UEFA_European_Under-21_Championship_qualification_Group_8"|"2015-12-01"|73
"2016_Greater_Western_Sydney_Giants_season"|"2017-05-01"|86
```

### Windows向けのCSVエクスポート

Windows環境で問題なく動作するCSVファイルが必要な場合は、[output_format_csv_crlf_end_of_line](/docs/ja/operations/settings/settings-formats.md/#output_format_csv_crlf_end_of_line)オプションを有効にすることを検討すべきです。これにより、改行として`\n`の代わりに`\r\n`が使用されます：

```sql
SET output_format_csv_crlf_end_of_line = 1;
```

## CSVファイルのスキーマ推論

多くの場合、不明なCSVファイルを操作しなければならないため、カラムにどのデータ型を使用するかを探索する必要があります。ClickHouseはデフォルトで、指定されたCSVファイルの分析に基づいてデータ型を推測しようとします。これは「スキーマ推論」として知られています。検出されたデータ型は、[file()](/docs/ja/sql-reference/table-functions/file.md)関数と組み合わせて`DESCRIBE`ステートメントを使用して探索できます：

```sql
DESCRIBE file('data-small.csv', CSV)
```
```response
┌─name─┬─type─────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ c1   │ Nullable(String) │              │                    │         │                  │                │
│ c2   │ Nullable(Date)   │              │                    │         │                  │                │
│ c3   │ Nullable(Int64)  │              │                    │         │                  │                │
└──────┴──────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

ここで、ClickHouseは効率的にCSVファイルのカラム型を推測できました。ClickHouseに推測させたくない場合は、次のオプションを使用して無効にできます：

```sql
SET input_format_csv_use_best_effort_in_schema_inference = 0
```

すべてのカラムタイプは、この場合`String`として扱われます。

### カラムタイプを明示的に指定したCSVのエクスポートとインポート {#exporting-and-importing-csv-with-explicit-column-types}

ClickHouseは、[CSVWithNamesAndTypes](/docs/ja/interfaces/formats.md/#csvwithnamesandtypes)（および他の*WithNamesフォーマットファミリー）を使用してデータをエクスポートする際にカラムタイプを明示的に設定することもできます：

```sql
SELECT *
FROM sometable
LIMIT 5
FORMAT CSVWithNamesAndTypes
```
```response
"path","month","hits"
"String","Date","UInt32"
"Akiba_Hebrew_Academy","2017-08-01",241
"Aegithina_tiphia","2018-02-01",34
"1971-72_Utah_Stars_season","2016-10-01",1
"2015_UEFA_European_Under-21_Championship_qualification_Group_8","2015-12-01",73
"2016_Greater_Western_Sydney_Giants_season","2017-05-01",86
```

このフォーマットでは、カラム名を持つ1行とカラム型を持つもう1行の2つのヘッダ行が含まれます。これにより、ClickHouse（および他のアプリ）は[そのようなファイル](assets/data_csv_types.csv)からデータをロードする際にカラムタイプを識別できます：

```sql
DESCRIBE file('data_csv_types.csv', CSVWithNamesAndTypes)
```
```response
┌─name──┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ path  │ String │              │                    │         │                  │                │
│ month │ Date   │              │                    │         │                  │                │
│ hits  │ UInt32 │              │                    │         │                  │                │
└───────┴────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

これで、ClickHouseは推測の代わりに、（2番目の）ヘッダ行に基づいてカラム型を識別します。

## カスタムデリミタ、セパレータ、およびエスケープルール

複雑なケースでは、テキストデータは非常にカスタムな形式でフォーマットされ、それでも構造を持つことがあります。このようなケースには、カスタムエスケープルール、デリミタ、行セパレータ、開始/終了シンボルを設定できる特別な[CustomSeparated](/docs/ja/interfaces/formats.md/#format-customseparated)フォーマットがあります。

次のデータがファイルにあると仮定します：

```
row('Akiba_Hebrew_Academy';'2017-08-01';241),row('Aegithina_tiphia';'2018-02-01';34),...
```

個別の行が`row()`で囲まれ、行は`,`で分離され、個別の値は`;`で区切られていることがわかります。この場合、次の設定を使用してこのファイルからデータを読み込むことができます：

```sql
SET format_custom_row_before_delimiter = 'row(';
SET format_custom_row_after_delimiter = ')';
SET format_custom_field_delimiter = ';';
SET format_custom_row_between_delimiter = ',';
SET format_custom_escaping_rule = 'Quoted';
```

これで、カスタムフォーマット[ファイル](assets/data_small_custom.txt)からデータをロードできます：

```sql
SELECT *
FROM file('data_small_custom.txt', CustomSeparated)
LIMIT 3
```
```response
┌─c1────────────────────────┬─────────c2─┬──c3─┐
│ Akiba_Hebrew_Academy      │ 2017-08-01 │ 241 │
│ Aegithina_tiphia          │ 2018-02-01 │  34 │
│ 1971-72_Utah_Stars_season │ 2016-10-01 │   1 │
└───────────────────────────┴────────────┴─────┘
```

[CustomSeparatedWithNames](/docs/ja/interfaces/formats.md/#customseparatedwithnames)を使用して、ヘッダを正しくエクスポートおよびインポートすることもできます。さらに複雑なケースに対処するために、正規表現やテンプレートフォーマットについては[regexとテンプレート](templates-regex.md)を探ってください。

## 大容量CSVファイルの操作

CSVファイルは大容量になることがあり、ClickHouseはどのようなサイズのファイルでも効率的に操作します。大容量のファイルは通常圧縮されており、ClickHouseは処理前に解凍する必要はありません。Insert中に`COMPRESSION`句を使用できます：

```sql
INSERT INTO sometable
FROM INFILE 'data_csv.csv.gz'
COMPRESSION 'gzip' FORMAT CSV
```

`COMPRESSION`句を省略した場合、ClickHouseはファイルの拡張子に基づいて圧縮形式を推測しようとします。同じアプローチを使用して、ファイルを直接圧縮フォーマットにエクスポートすることもできます：

```sql
SELECT *
FROM for_csv
INTO OUTFILE 'data_csv.csv.gz'
COMPRESSION 'gzip' FORMAT CSV
```

これにより、圧縮された`data_csv.csv.gz`ファイルが作成されます。

## その他のフォーマット

ClickHouseは、さまざまなシナリオやプラットフォームをカバーするために、多くのフォーマット、テキストおよびバイナリー形式のサポートを導入しています。次の記事で、これらのフォーマットおよびそれらを使用する方法についてさらに探ってください：

- **CSVとTSVフォーマット**
- [Parquet](parquet.md)
- [JSONフォーマット](/docs/ja/integrations/data-ingestion/data-formats/json/intro.md)
- [正規表現とテンプレート](templates-regex.md)
- [ネイティブおよびバイナリーフォーマット](binary.md)
- [SQLフォーマット](sql.md)

また、ローカル/リモートファイルでClickhouseサーバーを必要とせずに作業を行うための携帯型完全集合ツールである[clickhouse-local](https://clickhouse.com/blog/extracting-converting-querying-local-files-with-sql-clickhouse-local)もチェックしてください。
