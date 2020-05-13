---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 60
toc_title: "\uFF82\u3064\"\uFF82\u3065\u6309\u3064\uFF75\uFF82\uFF01"
---

# ﾂつ"ﾂづ按つｵﾂ！ {#clickhouse-local}

その `clickhouse-local` プログラムは、展開し、ClickHouseサーバーを構成することなく、ローカルファイルに高速処理を実行できます。

データを受け入れを表すテーブル、クエリを利用して [クリックハウスsql方言](../../sql-reference/index.md).

`clickhouse-local` ClickHouse serverと同じコアを使用するため、ほとんどの機能と同じフォーマットとテーブルエンジンをサポートします。

デフォルトでは `clickhouse-local` 同じホスト上のデータにアクセスすることはできませんが、 `--config-file` 引数。

!!! warning "警告"
    るおそれがあります。負荷生産サーバの設定に `clickhouse-local` 人為的なミスの場合、データが破損する可能性があるためです。

## 使い方 {#usage}

基本的な使用法:

``` bash
$ clickhouse-local --structure "table_structure" --input-format "format_of_incoming_data" -q "query"
```

引数:

-   `-S`, `--structure` — table structure for input data.
-   `-if`, `--input-format` — input format, `TSV` デフォルトでは。
-   `-f`, `--file` — path to data, `stdin` デフォルトでは。
-   `-q` `--query` — queries to execute with `;` デリメートルとして
-   `-N`, `--table` — table name where to put output data, `table` デフォルトでは。
-   `-of`, `--format`, `--output-format` — output format, `TSV` デフォルトでは。
-   `--stacktrace` — whether to dump debug output in case of exception.
-   `--verbose` — more details on query execution.
-   `-s` — disables `stderr` ログ記録。
-   `--config-file` — path to configuration file in same format as for ClickHouse server, by default the configuration empty.
-   `--help` — arguments references for `clickhouse-local`.

また、より一般的に使用される各clickhouse構成変数の引数があります `--config-file`.

## 例 {#examples}

``` bash
$ echo -e "1,2\n3,4" | clickhouse-local -S "a Int64, b Int64" -if "CSV" -q "SELECT * FROM table"
Read 2 rows, 32.00 B in 0.000 sec., 5182 rows/sec., 80.97 KiB/sec.
1   2
3   4
```

前の例は次のようになります:

``` bash
$ echo -e "1,2\n3,4" | clickhouse-local -q "CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin); SELECT a, b FROM table; DROP TABLE table"
Read 2 rows, 32.00 B in 0.000 sec., 4987 rows/sec., 77.93 KiB/sec.
1   2
3   4
```

次に、各unixユーザーのメモリユーザーを出力します:

``` bash
$ ps aux | tail -n +2 | awk '{ printf("%s\t%s\n", $1, $4) }' | clickhouse-local -S "user String, mem Float64" -q "SELECT user, round(sum(mem), 2) as memTotal FROM table GROUP BY user ORDER BY memTotal DESC FORMAT Pretty"
```

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

[元の記事](https://clickhouse.tech/docs/en/operations/utils/clickhouse-local/) <!--hide-->
