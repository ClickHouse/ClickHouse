---
slug: /ja/operations/utilities/clickhouse-benchmark
sidebar_position: 61
sidebar_label: clickhouse-benchmark
---

# clickhouse-benchmark 

ClickHouseサーバーに接続し、指定されたクエリを繰り返し送信します。

**構文**

``` bash
$ clickhouse-benchmark --query ["single query"] [keys]
```

または

``` bash
$ echo "single query" | clickhouse-benchmark [keys]
```

または

``` bash
$ clickhouse-benchmark [keys] <<< "single query"
```

クエリのセットを送信したい場合は、テキストファイルを作成し、このファイルに各クエリを個別の行として配置します。例：

``` sql
SELECT * FROM system.numbers LIMIT 10000000;
SELECT 1;
```

次に、このファイルを`clickhouse-benchmark`の標準入力に渡します：

``` bash
clickhouse-benchmark [keys] < queries_file;
```

## キー {#clickhouse-benchmark-keys}

- `--query=QUERY` — 実行するクエリ。このパラメーターが渡されない場合、`clickhouse-benchmark`は標準入力からクエリを読み取ります。
- `-c N`, `--concurrency=N` — `clickhouse-benchmark`が同時に送信するクエリの数。デフォルト値: 1。
- `-d N`, `--delay=N` — 中間レポート間の秒単位の間隔（レポートを無効にするには0を設定）。デフォルト値: 1。
- `-h HOST`, `--host=HOST` — サーバーホスト。デフォルト値: `localhost`。[比較モード](#clickhouse-benchmark-comparison-mode)では複数の`-h`キーを使用できます。
- `-i N`, `--iterations=N` — 総クエリ数。デフォルト値: 0（無限に繰り返す）。
- `-r`, `--randomize` — 複数の入力クエリがある場合、クエリ実行の順序をランダムにします。
- `-s`, `--secure` — `TLS`接続の使用。
- `-t N`, `--timelimit=N` — 秒単位の時間制限。指定された時間制限に達すると`clickhouse-benchmark`はクエリの送信を停止します。デフォルト値: 0（時間制限なし）。
- `--port=N` — サーバーポート。デフォルト値: 9000。[比較モード](#clickhouse-benchmark-comparison-mode)では複数の`--port`キーを使用できます。
- `--confidence=N` — T検定の信頼レベル。可能な値: 0 (80%), 1 (90%), 2 (95%), 3 (98%), 4 (99%), 5 (99.5%)。デフォルト値: 5。[比較モード](#clickhouse-benchmark-comparison-mode)では、`clickhouse-benchmark`は指定された信頼レベルで[独立二標本のスチューデントのt検定](https://en.wikipedia.org/wiki/Student%27s_t-test#Independent_two-sample_t-test)を実行して、2つの分布に違いがないかを判断します。
- `--cumulative` — 各間隔のデータの代わりに累積データを表示します。
- `--database=DATABASE_NAME` — ClickHouseデータベース名。デフォルト値: `default`。
- `--user=USERNAME` — ClickHouseユーザー名。デフォルト値: `default`。
- `--password=PSWD` — ClickHouseユーザーパスワード。デフォルト値: 空文字列。
- `--stacktrace` — スタックトレースの出力。このキーが設定されている場合、`clickhouse-benchmark`は例外のスタックトレースを出力します。
- `--stage=WORD` — サーバーでのクエリ処理段階。ClickHouseは指定された段階でクエリ処理を停止し、`clickhouse-benchmark`に回答を返します。可能な値: `complete`, `fetch_columns`, `with_mergeable_state`。デフォルト値: `complete`。
- `--help` — ヘルプメッセージを表示します。

クエリにいくつかの[設定](../../operations/settings/index.md)を適用したい場合は、`--<session setting name>=SETTING_VALUE`というキーとしてそれらを渡します。例えば、`--max_memory_usage=1048576`。

## 出力 {#clickhouse-benchmark-output}

デフォルトでは、`clickhouse-benchmark`は各`--delay`インターバルのレポートを表示します。

レポートの例：

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

レポートでは以下が確認できます：

- `Queries executed:`フィールドにおけるクエリ数。

- ステータス文字列には以下の情報が含まれます（順番に）：

    - ClickHouseサーバーのエンドポイント。
    - 処理されたクエリの数。
    - QPS: `--delay`引数で指定された期間中、サーバーが1秒あたりに実行したクエリ数。
    - RPS: `--delay`引数で指定された期間中、サーバーが1秒あたりに読み取った行数。
    - MiB/s: `--delay`引数で指定された期間中、サーバーが1秒あたりに読み取ったMebibytes数。
    - result RPS: `--delay`引数で指定された期間中、サーバーがクエリの結果に1秒あたりに配置した行数。
    - result MiB/s: `--delay`引数で指定された期間中、サーバーがクエリの結果に1秒あたりに配置したMebibytes数。

- クエリ実行時間のパーセンタイル。

## 比較モード {#clickhouse-benchmark-comparison-mode}

`clickhouse-benchmark`は2つの稼働中のClickHouseサーバーのパフォーマンスを比較できます。

比較モードを使用するには、両方のサーバーのエンドポイントを`--host`、`--port`キーの2組で指定します。キーは引数リスト内での位置によって組み合わされます。最初の`--host`は最初の`--port`と組み合わされる、というように。`clickhouse-benchmark`は両方のサーバーに接続を確立し、クエリを送信します。各クエリはランダムに選ばれたサーバーに送信されます。結果は表として表示されます。

## 例 {#clickhouse-benchmark-example}

``` bash
$ echo "SELECT * FROM system.numbers LIMIT 10000000 OFFSET 10000000" | clickhouse-benchmark --host=localhost --port=9001 --host=localhost --port=9000 -i 10
```

``` text
Loaded 1 queries.

Queries executed: 5.

localhost:9001, queries 2, QPS: 3.764, RPS: 75446929.370, MiB/s: 575.614, result RPS: 37639659.982, result MiB/s: 287.168.
localhost:9000, queries 3, QPS: 3.815, RPS: 76466659.385, MiB/s: 583.394, result RPS: 38148392.297, result MiB/s: 291.049.

0.000%          0.258 sec.      0.250 sec.
10.000%         0.258 sec.      0.250 sec.
20.000%         0.258 sec.      0.250 sec.
30.000%         0.258 sec.      0.267 sec.
40.000%         0.258 sec.      0.267 sec.
50.000%         0.273 sec.      0.267 sec.
60.000%         0.273 sec.      0.267 sec.
70.000%         0.273 sec.      0.267 sec.
80.000%         0.273 sec.      0.269 sec.
90.000%         0.273 sec.      0.269 sec.
95.000%         0.273 sec.      0.269 sec.
99.000%         0.273 sec.      0.269 sec.
99.900%         0.273 sec.      0.269 sec.
99.990%         0.273 sec.      0.269 sec.

No difference proven at 99.5% confidence
```
