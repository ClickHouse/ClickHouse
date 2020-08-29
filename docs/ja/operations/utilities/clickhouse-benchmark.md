---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 61
toc_title: "clickhouse-\u30D9\u30F3\u30C1\u30DE\u30FC\u30AF"
---

# clickhouse-ベンチマーク {#clickhouse-benchmark}

ClickHouseサーバーに接続し、指定したクエリを繰り返し送信します。

構文:

``` bash
$ echo "single query" | clickhouse-benchmark [keys]
```

または

``` bash
$ clickhouse-benchmark [keys] <<< "single query"
```

一連のクエリを送信する場合は、テキストファイルを作成し、各クエリをこのファイル内の個々の文字列に配置します。 例えば:

``` sql
SELECT * FROM system.numbers LIMIT 10000000
SELECT 1
```

次に、このファイルを標準入力に渡します。 `clickhouse-benchmark`.

``` bash
clickhouse-benchmark [keys] < queries_file
```

## キー {#clickhouse-benchmark-keys}

-   `-c N`, `--concurrency=N` — Number of queries that `clickhouse-benchmark` 同時に送信します。 デフォルト値:1。
-   `-d N`, `--delay=N` — Interval in seconds between intermediate reports (set 0 to disable reports). Default value: 1.
-   `-h WORD`, `--host=WORD` — Server host. Default value: `localhost`. のために [比較モード](#clickhouse-benchmark-comparison-mode) 複数を使用できます `-h` 鍵だ
-   `-p N`, `--port=N` — Server port. Default value: 9000. For the [比較モード](#clickhouse-benchmark-comparison-mode) 複数を使用できます `-p` 鍵だ
-   `-i N`, `--iterations=N` — Total number of queries. Default value: 0.
-   `-r`, `--randomize` — Random order of queries execution if there is more then one input query.
-   `-s`, `--secure` — Using TLS connection.
-   `-t N`, `--timelimit=N` — Time limit in seconds. `clickhouse-benchmark` 指定された時間制限に達すると、クエリの送信を停止します。 デフォルト値:0(制限時間無効)。
-   `--confidence=N` — Level of confidence for T-test. Possible values: 0 (80%), 1 (90%), 2 (95%), 3 (98%), 4 (99%), 5 (99.5%). Default value: 5. In the [比較モード](#clickhouse-benchmark-comparison-mode) `clickhouse-benchmark` を実行します。 [独立した二つのサンプル学生のtテスト](https://en.wikipedia.org/wiki/Student%27s_t-test#Independent_two-sample_t-test) これらの分布が選択された信頼度と異ならないかどうかを判定します。
-   `--cumulative` — Printing cumulative data instead of data per interval.
-   `--database=DATABASE_NAME` — ClickHouse database name. Default value: `default`.
-   `--json=FILEPATH` — JSON output. When the key is set, `clickhouse-benchmark` 指定されたJSONファイルにレポートを出力します。
-   `--user=USERNAME` — ClickHouse user name. Default value: `default`.
-   `--password=PSWD` — ClickHouse user password. Default value: empty string.
-   `--stacktrace` — Stack traces output. When the key is set, `clickhouse-bencmark` 出力スタックトレースの例外をスローしました。
-   `--stage=WORD` — Query processing stage at server. ClickHouse stops query processing and returns answer to `clickhouse-benchmark` 指定された段階で。 可能な値: `complete`, `fetch_columns`, `with_mergeable_state`. デフォルト値: `complete`.
-   `--help` — Shows the help message.

あなたはいくつかを適用したい場合 [設定](../../operations/settings/index.md) クエリの場合は、キーとして渡します `--<session setting name>= SETTING_VALUE`. 例えば, `--max_memory_usage=1048576`.

## 出力 {#clickhouse-benchmark-output}

既定では, `clickhouse-benchmark` 各レポート `--delay` インターバル

レポートの例:

``` text
Queries executed: 10.

localhost:9000, queries 10, QPS: 6.772, RPS: 67904487.440, MiB/s: 518.070, result RPS: 67721584.984, result MiB/s: 516.675.

0.000%      0.145 sec.
10.000%     0.146 sec.
20.000%     0.146 sec.
30.000%     0.146 sec.
40.000%     0.147 sec.
50.000%     0.148 sec.
60.000%     0.148 sec.
70.000%     0.148 sec.
80.000%     0.149 sec.
90.000%     0.150 sec.
95.000%     0.150 sec.
99.000%     0.150 sec.
99.900%     0.150 sec.
99.990%     0.150 sec.
```

このレポートでは:

-   クエリの数 `Queries executed:` フィールド

-   ステータス文字列を含む(順):

    -   ClickHouseサーバーのエンドポイント。
    -   処理されたクエリの数。
    -   QPS:QPS:で指定された期間に毎秒実行されるクエリサーバーの数 `--delay` 引数。
    -   RPS:サーバーが指定された期間に毎秒読み取った行数 `--delay` 引数。
    -   MiB/s:指定された期間に毎秒読み取られるmebibytesサーバーの数 `--delay` 引数。
    -   結果RPS:サーバーによって指定された期間内にクエリの結果に対して毎秒どのくらいの行が配置されますか `--delay` 引数。
    -   クエリの結果に対してサーバによって指定された期間内に秒あたりのメビバイト数 `--delay` 引数。

-   クエリの実行時間の百分位数。

## 比較モード {#clickhouse-benchmark-comparison-mode}

`clickhouse-benchmark` ﾂつｨﾂ姪"ﾂつ"ﾂ債ﾂづｭﾂつｹ

比較モードを使用するには、両方のサーバーのエンドポイントを `--host`, `--port` 鍵だ 引数リスト内の位置によって一致するキーは、最初の `--host` 最初のものと一致します `--port` など。 `clickhouse-benchmark` 両方のサーバーへの接続を確立し、クエリを送信します。 ランダムに選択されたサーバー宛の各クエリ。 結果は、各サーバーごとに個別に表示されます。

## 例 {#clickhouse-benchmark-example}

``` bash
$ echo "SELECT * FROM system.numbers LIMIT 10000000 OFFSET 10000000" | clickhouse-benchmark -i 10
```

``` text
Loaded 1 queries.

Queries executed: 6.

localhost:9000, queries 6, QPS: 6.153, RPS: 123398340.957, MiB/s: 941.455, result RPS: 61532982.200, result MiB/s: 469.459.

0.000%      0.159 sec.
10.000%     0.159 sec.
20.000%     0.159 sec.
30.000%     0.160 sec.
40.000%     0.160 sec.
50.000%     0.162 sec.
60.000%     0.164 sec.
70.000%     0.165 sec.
80.000%     0.166 sec.
90.000%     0.166 sec.
95.000%     0.167 sec.
99.000%     0.167 sec.
99.900%     0.167 sec.
99.990%     0.167 sec.



Queries executed: 10.

localhost:9000, queries 10, QPS: 6.082, RPS: 121959604.568, MiB/s: 930.478, result RPS: 60815551.642, result MiB/s: 463.986.

0.000%      0.159 sec.
10.000%     0.159 sec.
20.000%     0.160 sec.
30.000%     0.163 sec.
40.000%     0.164 sec.
50.000%     0.165 sec.
60.000%     0.166 sec.
70.000%     0.166 sec.
80.000%     0.167 sec.
90.000%     0.167 sec.
95.000%     0.170 sec.
99.000%     0.172 sec.
99.900%     0.172 sec.
99.990%     0.172 sec.
```
