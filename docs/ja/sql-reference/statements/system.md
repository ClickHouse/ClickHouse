---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 37
toc_title: SYSTEM
---

# システムクエリ {#query-language-system}

-   [RELOAD DICTIONARIES](#query_language-system-reload-dictionaries)
-   [RELOAD DICTIONARY](#query_language-system-reload-dictionary)
-   [DROP DNS CACHE](#query_language-system-drop-dns-cache)
-   [DROP MARK CACHE](#query_language-system-drop-mark-cache)
-   [FLUSH LOGS](#query_language-system-flush_logs)
-   [RELOAD CONFIG](#query_language-system-reload-config)
-   [SHUTDOWN](#query_language-system-shutdown)
-   [KILL](#query_language-system-kill)
-   [STOP DISTRIBUTED SENDS](#query_language-system-stop-distributed-sends)
-   [FLUSH DISTRIBUTED](#query_language-system-flush-distributed)
-   [START DISTRIBUTED SENDS](#query_language-system-start-distributed-sends)
-   [STOP MERGES](#query_language-system-stop-merges)
-   [START MERGES](#query_language-system-start-merges)

## RELOAD DICTIONARIES {#query_language-system-reload-dictionaries}

以前に正常に読み込まれたすべての辞書を再読み込みします。
デフォルトでは、辞書は遅延して読み込まれます [dictionaries\_lazy\_load](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-dictionaries_lazy_load)）したがって、起動時に自動的にロードされるのではなく、dictGet関数による最初のアクセス時に初期化されるか、ENGINE=Dictionaryを使用してテーブルから選択されます。 その `SYSTEM RELOAD DICTIONARIES` クエバなどの辞書(ロード).
常に戻ります `Ok.` 辞書の更新の結果に関係なく。

## 辞書Dictionary\_nameを再読み込み {#query_language-system-reload-dictionary}

辞書を完全に再読み込みします `dictionary_name` ディクショナリの状態に関係なく(LOADED/NOT\_LOADED/FAILED)。
常に戻ります `Ok.` 辞書の更新の結果に関係なく。
ディクショナリのステータスは、 `system.dictionaries` テーブル。

``` sql
SELECT name, status FROM system.dictionaries;
```

## DROP DNS CACHE {#query_language-system-drop-dns-cache}

ClickHouseの内部DNSキャッシュをリセットします。 場合によっては（古いClickHouseバージョンの場合）、インフラストラクチャを変更するとき（別のClickHouseサーバーまたは辞書で使用されているサーバーのIPアドレスを変更す

より便利な(自動)キャッシュ管理については、"disable\_internal\_dns\_cache,dns\_cache\_update\_periodパラメーター"を参照してください。

## DROP MARK CACHE {#query_language-system-drop-mark-cache}

リセットをマークします。 ClickHouseおよび性能試験の開発で使用される。

## FLUSH LOGS {#query_language-system-flush_logs}

Flushes buffers of log messages to system tables (e.g. system.query\_log). Allows you to not wait 7.5 seconds when debugging.

## RELOAD CONFIG {#query_language-system-reload-config}

ClickHouse構成を再読み込みします。 設定がZookeeperに格納されている場合に使用されます。

## SHUTDOWN {#query_language-system-shutdown}

通常はClickHouseをシャットダウンします `service clickhouse-server stop` / `kill {$pid_clickhouse-server}`)

## KILL {#query_language-system-kill}

クリックハウスプロセスを中止します `kill -9 {$ pid_clickhouse-server}`)

## 分散テーブルの管理 {#query-language-system-distributed}

ClickHouseは管理できます [分散](../../engines/table-engines/special/distributed.md) テーブル ユーザーがこれらのテーブルにデータを挿入すると、ClickHouseはまずクラスターノードに送信するデータのキューを作成し、それを非同期に送信します。 キュー処理を管理するには [STOP DISTRIBUTED SENDS](#query_language-system-stop-distributed-sends), [FLUSH DISTRIBUTED](#query_language-system-flush-distributed),and [START DISTRIBUTED SENDS](#query_language-system-start-distributed-sends) クエリ。 分散データを同期して挿入することもできます。 `insert_distributed_sync` 設定。

### STOP DISTRIBUTED SENDS {#query_language-system-stop-distributed-sends}

を無効にした背景データの分布を挿入する際、データを配布します。

``` sql
SYSTEM STOP DISTRIBUTED SENDS [db.]<distributed_table_name>
```

### FLUSH DISTRIBUTED {#query_language-system-flush-distributed}

ClickHouseが強制的にクラスターノードにデータを同期的に送信します。 使用できないノードがある場合、ClickHouseは例外をスローし、クエリの実行を停止します。 これは、すべてのノードがオンラインに戻ったときに発生します。

``` sql
SYSTEM FLUSH DISTRIBUTED [db.]<distributed_table_name>
```

### START DISTRIBUTED SENDS {#query_language-system-start-distributed-sends}

を背景データの分布を挿入する際、データを配布します。

``` sql
SYSTEM START DISTRIBUTED SENDS [db.]<distributed_table_name>
```

### STOP MERGES {#query_language-system-stop-merges}

MergeTreeファミリ内のテーブルのバックグラウンドマージを停止できます:

``` sql
SYSTEM STOP MERGES [[db.]merge_tree_family_table_name]
```

!!! note "注"
    `DETACH / ATTACH` 以前にすべてのMergeTreeテーブルに対してマージが停止された場合でも、tableはテーブルのバックグラウンドマージを開始します。

### START MERGES {#query_language-system-start-merges}

MergeTreeファミリ内のテーブルのバックグラウンドマージを開始できます:

``` sql
SYSTEM START MERGES [[db.]merge_tree_family_table_name]
```

[元の記事](https://clickhouse.tech/docs/en/query_language/system/) <!--hide-->
