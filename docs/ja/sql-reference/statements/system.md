---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
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

前に正常に読み込まれたすべての辞書を再読み込みします。
デフォルトでは、辞書を取り込みの遅延を参照 [dictionaries\_lazy\_load](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-dictionaries_lazy_load)）、起動時に自動的にロードされるのではなく、dictGet関数を使用して最初のアクセス時に初期化されるか、ENGINE=Dictionaryテーブルから選択されます。 その `SYSTEM RELOAD DICTIONARIES` クエバなどの辞書(ロード).
常に戻る `Ok.` 辞書の更新の結果にかかわらず。

## 辞書dictionary\_nameを再読み込み {#query_language-system-reload-dictionary}

辞書を完全に再読み込みする `dictionary_name` 辞書の状態にかかわらず（LOADED/NOT\_LOADED/FAILED）。
常に戻る `Ok.` 辞書を更新した結果にかかわらず。
ディクショナリのステータスは以下のクエリで確認できます。 `system.dictionaries` テーブル。

``` sql
SELECT name, status FROM system.dictionaries;
```

## DROP DNS CACHE {#query_language-system-drop-dns-cache}

ClickHouseの内部DNSキャッシュをリセットします。 場合によっては（古いClickHouseバージョンの場合）、インフラストラクチャを変更するとき（別のClickHouseサーバーのIPアドレスまたは辞書で使用されるサーバーを変更する

より便利な(自動)キャッシュ管理については、“disable\_internal\_dns\_cache,dns\_cache\_update\_periodパラメータ”を参照してください。

## DROP MARK CACHE {#query_language-system-drop-mark-cache}

リセットをマークします。 clickhouseおよび性能試験の開発で使用される。

## FLUSH LOGS {#query_language-system-flush_logs}

Flushes buffers of log messages to system tables (e.g. system.query\_log). Allows you to not wait 7.5 seconds when debugging.

## RELOAD CONFIG {#query_language-system-reload-config}

ClickHouse構成を再読み込みします。 設定がZooKeeeperに格納されている場合に使用されます。

## SHUTDOWN {#query_language-system-shutdown}

通常シャットダウンclickhouse(のような `service clickhouse-server stop` / `kill {$pid_clickhouse-server}`)

## KILL {#query_language-system-kill}

異常終了しclickhouse工程など `kill -9 {$ pid_clickhouse-server}`)

## 分散テーブルの管理 {#query-language-system-distributed}

ClickHouse管理 [分散](../../engines/table-engines/special/distributed.md) テーブル。 ユーザーがこれらのテーブルにデータを挿入すると、ClickHouseはまずクラスターノードに送信するデータのキューを作成し、次に非同期に送信します。 キューの処理を管理することができます [STOP DISTRIBUTED SENDS](#query_language-system-stop-distributed-sends), [FLUSH DISTRIBUTED](#query_language-system-flush-distributed)、と [START DISTRIBUTED SENDS](#query_language-system-start-distributed-sends) クエリ。 また、分散データを同期的に挿入することもできます。 `insert_distributed_sync` 設定。

### STOP DISTRIBUTED SENDS {#query_language-system-stop-distributed-sends}

を無効にした背景データの分布を挿入する際、データを配布します。

``` sql
SYSTEM STOP DISTRIBUTED SENDS [db.]<distributed_table_name>
```

### FLUSH DISTRIBUTED {#query_language-system-flush-distributed}

クラスタノードにデータを同期送信するようにclickhouseを強制します。 ノードが使用できない場合、clickhouseは例外をスローし、クエリの実行を停止します。 これは、すべてのノードがオンラインに戻ったときに発生します。

``` sql
SYSTEM FLUSH DISTRIBUTED [db.]<distributed_table_name>
```

### START DISTRIBUTED SENDS {#query_language-system-start-distributed-sends}

を背景データの分布を挿入する際、データを配布します。

``` sql
SYSTEM START DISTRIBUTED SENDS [db.]<distributed_table_name>
```

### STOP MERGES {#query_language-system-stop-merges}

提供可能停止を背景に合併したテーブルのmergetree家族:

``` sql
SYSTEM STOP MERGES [[db.]merge_tree_family_table_name]
```

!!! note "メモ"
    `DETACH / ATTACH` テーブルは、以前にすべてのMergeTreeテーブルのマージが停止された場合でも、テーブルのバックグラウンドマージを開始します。

### START MERGES {#query_language-system-start-merges}

の提供が開始背景に合併したテーブルのmergetree家族:

``` sql
SYSTEM START MERGES [[db.]merge_tree_family_table_name]
```

[元の記事](https://clickhouse.tech/docs/en/query_language/system/) <!--hide-->
