---
slug: /ja/operations/utilities/clickhouse-local
sidebar_position: 60
sidebar_label: clickhouse-local
---

# clickhouse-local

## 関連コンテンツ

- ブログ: [clickhouse-localを使用してローカルファイルのデータを抽出、変換、およびクエリ](https://clickhouse.com/blog/extracting-converting-querying-local-files-with-sql-clickhouse-local)

## clickhouse-localとClickHouseのどちらを使用するか

`clickhouse-local` は、SQLを使用してローカルおよびリモートファイルを高速に処理する必要がある開発者にとって、完全なデータベースサーバーをインストールすることなく使用できる、簡単なClickHouseのバージョンです。`clickhouse-local` を使用することで、開発者はコマンドラインから直接SQLコマンドを使用することができ、完全なClickHouseのインストールを必要とせずにClickHouse機能にアクセスするための簡単で効率的な方法を提供します。`clickhouse-local` の主な利点のひとつは、[clickhouse-client](https://clickhouse.com/docs/ja/integrations/sql-clients/clickhouse-client-local) をインストールする際にすでに含まれていることです。これにより、複雑なインストールプロセスを必要とせずに、開発者はすぐに `clickhouse-local` を使用開始できます。

`clickhouse-local` は開発とテスト目的、およびファイル処理に関して非常に便利なツールですが、エンドユーザーやアプリケーション向けのサーバーとしては適していません。これらのシナリオでは、オープンソースの [ClickHouse](https://clickhouse.com/docs/ja/install) の使用が推奨されます。ClickHouseは大規模な分析ワークロードを処理するよう設計された強力なOLAPデータベースであり、大規模データセットに対する複雑なクエリの迅速かつ効率的な処理を提供し、ハイパフォーマンスが重要な本番環境での使用に最適です。さらに、ClickHouseはレプリケーション、シャーディング、高可用性など、アプリケーションをサポートするためにスケールアップするために不可欠な機能を幅広く提供しています。より大きなデータセットを扱ったり、エンドユーザーやアプリケーションをサポートする必要がある場合は、`clickhouse-local` の代わりにオープンソースのClickHouseを使用することをお勧めします。

以下のドキュメントを参照して、`clickhouse-local` の使用例（[ローカルファイルへのクエリ](#query_data_in_file) や [S3のParquetファイルを読み取る](#query-data-in-a-parquet-file-in-aws-s3)）をご覧ください。

## clickhouse-localのダウンロード

`clickhouse-local` はClickHouseサーバーと `clickhouse-client` を実行する同じ `clickhouse` バイナリを使用して実行されます。最新バージョンをダウンロードする最も簡単な方法は、次のコマンドを使用することです。

```bash
curl https://clickhouse.com/ | sh
```

:::note
ダウンロードしたばかりのバイナリは、さまざまなClickHouseツールとユーティリティを実行できます。データベースサーバーとしてClickHouseを実行したい場合は、[クイックスタート](../../quick-start.mdx)を参照してください。
:::

## ファイル内のデータにSQLでクエリを実行する {#query_data_in_file}

`clickhouse-local` の一般的な使用方法は、ファイルに対してアドホッククエリを実行することです。データをテーブルに挿入する必要はありません。`clickhouse-local` は、ファイルからデータを一時テーブルにストリームし、SQLを実行することができます。

ファイルが `clickhouse-local` と同じマシン上にある場合、ロードするファイルを指定するだけで済みます。以下の `reviews.tsv` ファイルには、Amazon製品レビューのサンプルが含まれています。

```bash
./clickhouse local -q "SELECT * FROM 'reviews.tsv'"
```

このコマンドは、次のコマンドのショートカットです。

```bash
./clickhouse local -q "SELECT * FROM file('reviews.tsv')"
```

ClickHouseはファイルの拡張子からタブ区切り形式を知っています。形式を明示的に指定する必要がある場合、[ClickHouseの多くの入力形式](../../interfaces/formats.md)のいずれかを追加するだけで済みます。

```bash
./clickhouse local -q "SELECT * FROM file('reviews.tsv', 'TabSeparated')"
```

`file` テーブル関数はテーブルを作成し、`DESCRIBE` を使用して推測されたスキーマを見ることができます。

```bash
./clickhouse local -q "DESCRIBE file('reviews.tsv')"
```

:::tip
ファイル名でグロブを使用することが許可されています（[グロブの置換](/docs/ja/sql-reference/table-functions/file.md/#globs-in-path) を参照）。

例:

```bash
./clickhouse local -q "SELECT * FROM 'reviews*.jsonl'"
./clickhouse local -q "SELECT * FROM 'review_?.csv'"
./clickhouse local -q "SELECT * FROM 'review_{1..3}.csv'"
```

:::

```response
marketplace	Nullable(String)
customer_id	Nullable(Int64)
review_id	Nullable(String)
product_id	Nullable(String)
product_parent	Nullable(Int64)
product_title	Nullable(String)
product_category	Nullable(String)
star_rating	Nullable(Int64)
helpful_votes	Nullable(Int64)
total_votes	Nullable(Int64)
vine	Nullable(String)
verified_purchase	Nullable(String)
review_headline	Nullable(String)
review_body	Nullable(String)
review_date	Nullable(Date)
```

最高評価の製品を見つけましょう。

```bash
./clickhouse local -q "SELECT
    argMax(product_title,star_rating),
    max(star_rating)
FROM file('reviews.tsv')"
```

```response
Monopoly Junior Board Game	5
```

## AWS S3のParquetファイルにクエリを実行する

S3にファイルがある場合、`clickhouse-local` と `s3` テーブル関数を使用して、ファイルをClickHouseテーブルに挿入せずにクエリを実行できます。イギリスで売られた不動産の価格情報を含む `house_0.parquet` という名前のファイルが公開バケットにあります。行数を確認してみましょう。

```bash
./clickhouse local -q "
SELECT count()
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/house_parquet/house_0.parquet')"
```

このファイルには270万行あります。

```response
2772030
```

ClickHouseがファイルから推測するスキーマを見ることは常に有用です。

```bash
./clickhouse local -q "DESCRIBE s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/house_parquet/house_0.parquet')"
```

```response
price	Nullable(Int64)
date	Nullable(UInt16)
postcode1	Nullable(String)
postcode2	Nullable(String)
type	Nullable(String)
is_new	Nullable(UInt8)
duration	Nullable(String)
addr1	Nullable(String)
addr2	Nullable(String)
street	Nullable(String)
locality	Nullable(String)
town	Nullable(String)
district	Nullable(String)
county	Nullable(String)
```

最も高価な地域を見てみましょう。

```bash
./clickhouse local -q "
SELECT
    town,
    district,
    count() AS c,
    round(avg(price)) AS price,
    bar(price, 0, 5000000, 100)
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/house_parquet/house_0.parquet')
GROUP BY
    town,
    district
HAVING c >= 100
ORDER BY price DESC
LIMIT 10"
```

```response
LONDON	CITY OF LONDON	886	2271305	█████████████████████████████████████████████▍
LEATHERHEAD	ELMBRIDGE	206	1176680	███████████████████████▌
LONDON	CITY OF WESTMINSTER	12577	1108221	██████████████████████▏
LONDON	KENSINGTON AND CHELSEA	8728	1094496	█████████████████████▉
HYTHE	FOLKESTONE AND HYTHE	130	1023980	████████████████████▍
CHALFONT ST GILES	CHILTERN	113	835754	████████████████▋
AMERSHAM	BUCKINGHAMSHIRE	113	799596	███████████████▉
VIRGINIA WATER	RUNNYMEDE	356	789301	███████████████▊
BARNET	ENFIELD	282	740514	██████████████▊
NORTHWOOD	THREE RIVERS	184	731609	██████████████▋
```

:::tip
ファイルをClickHouseに挿入する準備が整ったら、ClickHouseサーバーを起動し、ファイルと `s3` テーブル関数の結果を `MergeTree` テーブルに挿入します。詳細については、[クイックスタート](../../quick-start.mdx)を参照してください。
:::

## フォーマットの変換

`clickhouse-local` を使用して、データを異なる形式に変換できます。例：

``` bash
$ clickhouse-local --input-format JSONLines --output-format CSV --query "SELECT * FROM table" < data.json > data.csv
```

形式はファイルの拡張子から自動検出されます：

``` bash
$ clickhouse-local --query "SELECT * FROM table" < data.json > data.csv
```

ショートカットとして `--copy` 引数を使用して以下のように書くことができます。

``` bash
$ clickhouse-local --copy < data.json > data.csv
```

## 使用法 {#usage}

デフォルトでは、`clickhouse-local` は同じホスト上のClickHouseサーバーのデータにアクセスでき、サーバーの設定には依存しません。また、`--config-file` 引数を使用してサーバー設定を読み込むこともサポートしています。テンポラリデータには、デフォルトで一意のテンポラリデータディレクトリが作成されます。

基本的な使用法 (Linux):

``` bash
$ clickhouse-local --structure "table_structure" --input-format "format_of_incoming_data" --query "query"
```

基本的な使用法 (Mac):

``` bash
$ ./clickhouse local --structure "table_structure" --input-format "format_of_incoming_data" --query "query"
```

:::note
`clickhouse-local` はWSL2を介してWindowsでもサポートされています。
:::

引数：

- `-S`, `--structure` — 入力データのテーブル構造。
- `--input-format` — 入力フォーマット、デフォルトは `TSV`。
- `-F`, `--file` — データへのパス、デフォルトは `stdin`。
- `-q`, `--query` — `;` で区切られる実行するクエリ。`--query` は複数回指定可能、例: `--query "SELECT 1" --query "SELECT 2"`。`--queries-file`と同時に使用することはできません。
- `--queries-file` - 実行するクエリを持つファイルパス。`--queries-file` は複数回指定可能、例: `--query queries1.sql --query queries2.sql`。`--query` と同時に使用することはできません。
- `--multiquery, -n` – このオプションが指定されている場合、セミコロンで区切られた複数のクエリを `--query` オプションの後にリストすることができます。便利な点として、`--query` を省略し、`--multiquery` の直後にクエリを直接渡すことが可能です。
- `-N`, `--table` — 出力データを置くテーブル名、デフォルトは `table`。
- `-f`, `--format`, `--output-format` — 出力フォーマット、デフォルトは `TSV`。
- `-d`, `--database` — デフォルトデータベース、デフォルトは `_local`。
- `--stacktrace` — 例外の際にデバッグ出力をダンプするかどうか。
- `--echo` — 実行前にクエリを表示。
- `--verbose` — クエリ実行に関する詳細情報。
- `--logger.console` — コンソールへのログ。
- `--logger.log` — ログファイル名。
- `--logger.level` — ログレベル。
- `--ignore-error` — クエリが失敗しても処理を停止しない。
- `-c`, `--config-file` — ClickHouseサーバー用のものと同じ形式の設定ファイルのパス。デフォルトは設定なし。
- `--no-system-tables` — システムテーブルをアタッチしない。
- `--help` — `clickhouse-local` の引数リファレンス。
- `-V`, `--version` — バージョン情報を表示して終了。

また、より一般的に使用されるClickHouseの設定変数用の引数もあります。

## 例 {#examples}

``` bash
$ echo -e "1,2\n3,4" | clickhouse-local --structure "a Int64, b Int64" \
    --input-format "CSV" --query "SELECT * FROM table"
Read 2 rows, 32.00 B in 0.000 sec., 5182 rows/sec., 80.97 KiB/sec.
1   2
3   4
```

前述の例は以下と同じです。

``` bash
$ echo -e "1,2\n3,4" | clickhouse-local -n --query "
    CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin);
    SELECT a, b FROM table;
    DROP TABLE table;"
Read 2 rows, 32.00 B in 0.000 sec., 4987 rows/sec., 77.93 KiB/sec.
1   2
3   4
```

`stdin` や `--file` 引数を使用する必要はなく、[`file` テーブル関数](../../sql-reference/table-functions/file.md)を使用して任意の数のファイルを開くことができます。

``` bash
$ echo 1 | tee 1.tsv
1

$ echo 2 | tee 2.tsv
2

$ clickhouse-local --query "
    select * from file('1.tsv', TSV, 'a int') t1
    cross join file('2.tsv', TSV, 'b int') t2"
1	2
```

次に、各Unixユーザーのメモリユーザーを出力してみましょう。

クエリ：

``` bash
$ ps aux | tail -n +2 | awk '{ printf("%s\t%s\n", $1, $4) }' \
    | clickhouse-local --structure "user String, mem Float64" \
        --query "SELECT user, round(sum(mem), 2) as memTotal
            FROM table GROUP BY user ORDER BY memTotal DESC FORMAT Pretty"
```

結果：

``` text
Read 186 rows, 4.15 KiB in 0.035 sec., 5302 rows/sec., 118.34 KiB/sec.
┏━━━━━━━━━━┳━━━━━━━━━━┓
┃ user     ┃ memTotal ┃
┡━━━━━━━━━━╇━━━━━━━━━━┩
│ bayonet  │    113.5 │
├──────────┼──────────┤
│ root     │      8.8 │
├──────────┼──────────┤
...
```

## 関連コンテンツ

- [clickhouse-localを使用してローカルファイルのデータを抽出、変換、およびクエリ](https://clickhouse.com/blog/extracting-converting-querying-local-files-with-sql-clickhouse-local)
- [ClickHouseへのデータ投入 - パート1](https://clickhouse.com/blog/getting-data-into-clickhouse-part-1)
- [巨大な実世界のデータセットを探索する: 100年以上の気象記録をClickHouseで](https://clickhouse.com/blog/real-world-data-noaa-climate-data)
