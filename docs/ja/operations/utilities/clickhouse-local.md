---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 60
toc_title: "\uFF82\u3064\uFF68\uFF82\u59EA\"\uFF82\u50B5\uFF82\u3064\uFF79"
---

# ﾂつｨﾂ姪"ﾂ債ﾂつｹ {#clickhouse-local}

その `clickhouse-local` プログラムで行う高速処理が地元のファイルを展開に合わせて構成されていますClickHouseサーバーです。

データを受け入れを表すテーブル、クエリを利用して [クリックハウスSQL方言](../../sql-reference/index.md).

`clickhouse-local` ClickHouse serverと同じコアを使用するため、ほとんどの機能と同じフォーマットとテーブルエンジンのセットをサポートします。

既定では `clickhouse-local` なアクセスデータを同じホストにも対応して搭載サーバの設定を使用 `--config-file` 引数。

!!! warning "警告"
    運用サーバーの構成をロードすることはお勧めしません `clickhouse-local` ヒューマンエラーの場合にデータが破損する可能性があるため。

## 使用法 {#usage}

基本的な使用法:

``` bash
$ clickhouse-local --structure "table_structure" --input-format "format_of_incoming_data" -q "query"
```

引数:

-   `-S`, `--structure` — table structure for input data.
-   `-if`, `--input-format` — input format, `TSV` デフォルトでは。
-   `-f`, `--file` — path to data, `stdin` デフォルトでは。
-   `-q` `--query` — queries to execute with `;` デリメーターとして。
-   `-N`, `--table` — table name where to put output data, `table` デフォルトでは。
-   `-of`, `--format`, `--output-format` — output format, `TSV` デフォルトでは。
-   `--stacktrace` — whether to dump debug output in case of exception.
-   `--verbose` — more details on query execution.
-   `-s` — disables `stderr` ロギング
-   `--config-file` — path to configuration file in same format as for ClickHouse server, by default the configuration empty.
-   `--help` — arguments references for `clickhouse-local`.

また、ClickHouse設定変数ごとに引数があります。 `--config-file`.

## 例 {#examples}

``` bash
$ echo -e "1,2\n3,4" | clickhouse-local -S "a Int64, b Int64" -if "CSV" -q "SELECT * FROM table"
Read 2 rows, 32.00 B in 0.000 sec., 5182 rows/sec., 80.97 KiB/sec.
1   2
3   4
```

前の例と同じです:

``` bash
$ echo -e "1,2\n3,4" | clickhouse-local -q "CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin); SELECT a, b FROM table; DROP TABLE table"
Read 2 rows, 32.00 B in 0.000 sec., 4987 rows/sec., 77.93 KiB/sec.
1   2
3   4
```

次に、各Unixユーザのメモリユーザを出力しましょう:

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

[元の記事](https://clickhouse.com/docs/en/operations/utils/clickhouse-local/) <!--hide-->
