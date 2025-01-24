---
slug: /ja/operations/settings/merge-tree-settings
title: "MergeTree テーブルの設定"
---

システムテーブル `system.merge_tree_settings` には、グローバルに設定された MergeTree の設定が表示されます。

MergeTree の設定はサーバーの設定ファイルの `merge_tree` セクションで設定できます。また、`CREATE TABLE` ステートメントの `SETTINGS` 句で個々の `MergeTree` テーブルに対して指定することもできます。

`max_suspicious_broken_parts` の設定をカスタマイズする例：

サーバー設定ファイルで、すべての `MergeTree` テーブルに対するデフォルトを設定します。

``` text
<merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</merge_tree>
```

特定のテーブルに対して設定する場合:

``` sql
CREATE TABLE tab
(
    `A` Int64
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS max_suspicious_broken_parts = 500;
```

特定のテーブルの設定を `ALTER TABLE ... MODIFY SETTING` で変更する場合:

```sql
ALTER TABLE tab MODIFY SETTING max_suspicious_broken_parts = 100;

-- グローバルなデフォルト（system.merge_tree_settings からの値）にリセット
ALTER TABLE tab RESET SETTING max_suspicious_broken_parts;
```

## index_granularity

インデックスのマーク間のデータ行の最大数。

デフォルト値: 8192。

## index_granularity_bytes

データ粒度の最大サイズ（バイト単位）。

デフォルト値: 10485760 (約10 MiB)。

行数のみで粒度サイズを制限するには、0に設定します（推奨されません）。

## min_index_granularity_bytes

許容されるデータ粒度の最小サイズ（バイト単位）。

デフォルト値: 1024バイト。

非常に低い index_granularity_bytes を持つテーブルの誤作成を防ぐための保護手段です。

## enable_mixed_granularity_parts

`index_granularity_bytes` 設定で粒度サイズを管理するための遷移を有効または無効にします。バージョン 19.11 以前では、粒度サイズを制限するために `index_granularity` 設定のみがありました。`index_granularity_bytes` 設定は、巨大な行（数十メガバイト、数百メガバイト）があるテーブルからデータを選択する際の ClickHouse のパフォーマンスを向上させます。大型の行を持つテーブルがある場合、この設定をテーブルに対して有効にすると、`SELECT` クエリの効率性が向上します。

## use_minimalistic_part_header_in_zookeeper

ZooKeeper におけるデータパーツヘッダーの格納方法。これを有効にすると、ZooKeeper はより少ないデータを保存します。詳細は[こちら](../server-configuration-parameters/settings.md/#server-settings-use_minimalistic_part_header_in_zookeeper)を参照してください。

## min_merge_bytes_to_use_direct_io

ストレージディスクへのダイレクトI/Oアクセスを使用するために必要なマージ操作の最小データ量。データパーツをマージする際、ClickHouse はマージするすべてのデータの総ストレージ量を計算します。
もし量が `min_merge_bytes_to_use_direct_io` バイトを超える場合、ClickHouse はデータをストレージディスクに読み書きします。このとき、ダイレクトI/Oインターフェースを使用します（`O_DIRECT` オプション）。
もし `min_merge_bytes_to_use_direct_io = 0` 場合、ダイレクトI/Oは無効になります。

デフォルト値: `10 * 1024 * 1024 * 1024` バイト。

## merge_with_ttl_timeout

有効期限 (TTL) による削除と共にマージを繰り返すまでの最小遅延時間（秒単位）。

デフォルト値: `14400` 秒（4時間）。

## merge_with_recompression_ttl_timeout

再圧縮 TTL を使用するマージを繰り返すまでの最小遅延時間（秒単位）。

デフォルト値: `14400` 秒（4時間）。

## write_final_mark

データパートの最後（最終バイト後）に最終インデックスマークを書き込むかどうかを設定します。

デフォルト値: 1。

変更しないでください。悪いことが起こります。

## storage_policy

ストレージポリシー。

## min_bytes_for_wide_part

`Wide` フォーマットで保存できるデータパートに含まれる最小のバイト/行数。
これらの設定の一つ、両方、またはどちらも設定できます。

## max_compress_block_size

テーブルに書き込む前に圧縮される未圧縮データのブロックの最大サイズ。
この設定はグローバル設定にも指定できます（[max_compress_block_size](/docs/ja/operations/settings/settings.md/#max-compress-block-size)設定を参照）。
テーブル作成時に指定した値が、この設定のグローバル値を上書きします。

## min_compress_block_size

次のマークを記録する際に圧縮が必要な未圧縮データのブロックの最小サイズ。
この設定はグローバル設定にも指定できます（[min_compress_block_size](/docs/ja/operations/settings/settings.md/#min-compress-block-size)設定を参照）。
テーブル作成時に指定した値が、この設定のグローバル値を上書きします。

## max_suspicious_broken_parts

単一のパーティション内で破損したパーツの数が `max_suspicious_broken_parts` の値を超えると、 自動削除が拒否されます。

許容される値:

- 任意の正の整数。

デフォルト値: 100。

## parts_to_throw_insert {#parts-to-throw-insert}

単一のパーティション内のアクティブなパーツの数が `parts_to_throw_insert` の値を超えると、`INSERT` は `Too many parts (N). Merges are processing significantly slower than inserts` という例外で中断されます。

許容される値:

- 任意の正の整数。

デフォルト値: 3000。

`SELECT` クエリの最大パフォーマンスを達成するには、処理されるパーツの数を最小限に抑える必要があります。詳細は [Merge Tree](../../development/architecture.md#merge-tree) を参照してください。

バージョン 23.6 より前では、この設定は 300 に設定されていました。異なる高い値を設定することもできますが、`Too many parts` エラーの発生確率を低くする一方で、`SELECT` のパフォーマンスが低下する可能性があります。また、マージの問題が発生した場合（例: ディスク容量の不足）、元の 300 での設定よりも気づくのが遅れます。

## parts_to_delay_insert {#parts-to-delay-insert}

単一のパーティション内のアクティブなパーツの数が `parts_to_delay_insert` の値を超えると、`INSERT` が意図的に遅延されます。

許容される値:

- 任意の正の整数。

デフォルト値: 1000。

ClickHouse は、バックグラウンドのマージプロセスがパーツを追加するよりも速くマージできるように、`INSERT` を遅延（「スリープ」を追加）して実行します。

## inactive_parts_to_throw_insert {#inactive-parts-to-throw-insert}

単一のパーティションにおける非アクティブなパーツの数が `inactive_parts_to_throw_insert` の値を超える場合、`INSERT` は "Too many inactive parts (N). Parts cleaning are processing significantly slower than inserts" という例外で中断されます。

許容される値:

- 任意の正の整数。

デフォルト値: 0（無制限）。

## inactive_parts_to_delay_insert {#inactive-parts-to-delay-insert}

単一のパーティションにおける非アクティブなパーツの数が `inactive_parts_to_delay_insert` の値に達すると、`INSERT` は意図的に遅延されます。これは、サーバーがパーツを速やかにクリーンアップできない場合に役立ちます。

許容される値:

- 任意の正の整数。

デフォルト値: 0（無制限）。

## max_delay_to_insert {#max-delay-to-insert}

単一のパーティション内のアクティブなパーツの数が [parts_to_delay_insert](#parts-to-delay-insert) の値を超えた場合に `INSERT` 遅延を計算するために使用される秒単位の値。

許容される値:

- 任意の正の整数。

デフォルト値: 1。

`INSERT` の遅延（ミリ秒単位）は次の式で計算されます:
```code
max_k = parts_to_throw_insert - parts_to_delay_insert
k = 1 + parts_count_in_partition - parts_to_delay_insert
delay_milliseconds = pow(max_delay_to_insert * 1000, k / max_k)
```
例えば、パーティションに 299 のアクティブなパーツがあり、parts_to_throw_insert = 300, parts_to_delay_insert = 150, max_delay_to_insert = 1 の場合、`INSERT` は `pow( 1 * 1000, (1 + 299 - 150) / (300 - 150) ) = 1000` ミリ秒遅延されます。

バージョン 23.1 から式が変更されました:
```code
allowed_parts_over_threshold = parts_to_throw_insert - parts_to_delay_insert
parts_over_threshold = parts_count_in_partition - parts_to_delay_insert + 1
delay_milliseconds = max(min_delay_to_insert_ms, (max_delay_to_insert * 1000) * parts_over_threshold / allowed_parts_over_threshold)
```
例えば、パーティションに 224 のアクティブなパーツがあり、parts_to_throw_insert = 300, parts_to_delay_insert = 150, max_delay_to_insert = 1, min_delay_to_insert_ms = 10 の場合、`INSERT` は `max( 10, 1 * 1000 * (224 - 150 + 1) / (300 - 150) ) = 500` ミリ秒遅延されます。

## max_parts_in_total {#max-parts-in-total}

テーブルのすべてのパーティションのアクティブなパーツの総数が `max_parts_in_total` の値を超えた場合、`INSERT` は `Too many parts (N)` という例外で中断されます。

許容される値:

- 任意の正の整数。

デフォルト値: 100000。

テーブル中のパーツの数が多いと、ClickHouse クエリのパフォーマンスが低下し、ClickHouse の起動時間が長くなります。通常、これはパーティション戦略の選択ミス（あまりに小さなパーティション）が原因で誤った設計の結果です。

## simultaneous_parts_removal_limit {#simultaneous-parts-removal-limit}

古いパーツが大量にある場合、クリーンスレッドは同じイテレーションで `simultaneous_parts_removal_limit` パーツを削除しようとします。
`simultaneous_parts_removal_limit` が `0` に設定されている場合は無制限を意味します。

デフォルト値: 0。

## replicated_deduplication_window {#replicated-deduplication-window}

ClickHouse Keeper が重複をチェックするためにハッシュを保存する最近挿入されたブロックの数。

許容される値:

- 任意の正の整数。
- 0（重複排除を無効にする）

デフォルト値: 1000。

`Insert` コマンドは1つ以上のブロック（パーツ）を作成します。レプリケートされたテーブルに書き込む際の[挿入重複排除](../../engines/table-engines/mergetree-family/replication.md)のために、ClickHouse は作成されたパーツのハッシュをClickHouse Keeperに書き込みます。ハッシュは、最新の `replicated_deduplication_window` ブロックのみに保存されます。古いハッシュはClickHouse Keeperから削除されます。
`replicated_deduplication_window` が多いほど、`Inserts` の実行が遅くなります。なぜなら、より多くのエントリと比較する必要があるからです。
ハッシュはフィールド名と型の組成および挿入されたパーツのデータ（バイトのストリーム）から計算されます。

## non_replicated_deduplication_window {#non-replicated-deduplication-window}

重複をチェックするためにハッシュが保存される、非レプリケート[MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) テーブルの最近挿入されたブロックの数。

許容される値:

- 任意の正の整数。
- 0（重複排除を無効にする）。

デフォルト値: 0。

レプリケートされたテーブルに似た重複排除機構を使用します（[replicated_deduplication_window](#replicated-deduplication-window)の設定を参照）。作成されたパーツのハッシュはディスク上のローカルファイルに書き込まれます。

## replicated_deduplication_window_seconds {#replicated-deduplication-window-seconds}

挿入されたブロックのハッシュがClickHouse Keeperから削除されるまでの秒数。

許容される値:

- 任意の正の整数。

デフォルト値: 604800 (1週間)。

[replicated_deduplication_window](#replicated-deduplication-window)に似ていますが、`replicated_deduplication_window_seconds` は挿入重複排除のためのブロックハッシュを保存する期間を指定します。`replicated_deduplication_window_seconds`より古いハッシュは、`replicated_deduplication_window`未満であってもClickHouse Keeperから削除されます。

時間は最新のレコードの時間に対して相対的であり、実際の時間に対してではありません。もしそれが唯一のレコードであれば、永遠に保存されます。

## replicated_deduplication_window_for_async_inserts {#replicated-deduplication-window-for-async-inserts}

ClickHouse Keeper が重複をチェックするためにハッシュを保存する最近の非同期挿入ブロックの数。

許容される値:

- 任意の正の整数。
- 0（非同期挿入の重複排除を無効にする）

デフォルト値: 10000。

[非同期インサート](./settings.md#async-insert) コマンドは一つ以上のブロック（パーツ）にキャッシュされていきます。レプリケートされたテーブルに書き込む際の [挿入重複排除](../../engines/table-engines/mergetree-family/replication.md) のために、ClickHouse は各挿入のハッシュをClickHouse Keeperに書き込みます。ハッシュは、最新の `replicated_deduplication_window_for_async_inserts` ブロックのみに保存されます。古いハッシュはClickHouse Keeperから削除されます。
`replicated_deduplication_window_for_async_inserts` が多いほど、`Async Inserts` の実行が遅くなります。なぜなら、より多くのエントリと比較する必要があるからです。
ハッシュはフィールド名と型の組成および挿入データ（バイトのストリーム）から計算されます。

## replicated_deduplication_window_seconds_for_async_inserts {#replicated-deduplication-window-seconds-for-async_inserts}

非同期の挿入のハッシュがClickHouse Keeperから削除されるまでの秒数。

許容される値:

- 任意の正の整数。

デフォルト値: 604800 (1週間)。

[replicated_deduplication_window_for_async_inserts](#replicated-deduplication-window-for-async-inserts) に似ており、非同期挿入重複排除のためのブロックのハッシュを保存する期間を指定します。`replicated_deduplication_window_seconds_for_async_inserts` より古いハッシュは、`replicated_deduplication_window_for_async_inserts` より少なくても ClickHouse Keeper から削除されます。

時間は最新のレコードの時間に対して相対的であり、実際の時間に対してではありません。もしそれが唯一のレコードであれば、永遠に保存されます。

## use_async_block_ids_cache {#use-async-block-ids-cache}

`true` の場合、非同期挿入のハッシュをキャッシュします。

許容される値:

- true, false

デフォルト値: false。

複数の非同期挿入を持つブロックは、複数のハッシュを生成します。一部の挿入が重複している場合、Keeper は1つのRPCで1つの重複したハッシュのみを返し、不要なRPCリトライを引き起こします。このキャッシュはKeeperのハッシュパスを監視します。Keeperで更新が監視されている場合、キャッシュは可能な限り早く更新され、メモリ内で重複した挿入をフィルタリングできるようになります。

## async_block_ids_cache_min_update_interval_ms

`use_async_block_ids_cache` を更新するための最小間隔（ミリ秒単位）

許容される値:

- 任意の正の整数。

デフォルト値: 100。

通常、`use_async_block_ids_cache` は監視されているKeeperパスで更新があるとすぐに更新されます。ただし、キャッシュの更新が頻繁すぎると負担がかかる可能性があります。この最小間隔がキャッシュの更新が過度に速くなるのを防ぎます。この値を長く設定しすぎると、重複した挿入を含むブロックのリトライ時間が長くなる可能性があります。

## max_replicated_logs_to_keep

非アクティブなレプリカがある場合、ClickHouse Keeper のログに存在できる最大記録数。非アクティブなレプリカは、この数を超えると失われます。

許容される値:

- 任意の正の整数。

デフォルト値: 1000

## min_replicated_logs_to_keep

ZooKeeper ログにある最後の記録をこの数まで保持します。テーブルの作業には影響を与えません: ZooKeeper ログのクリーンアップ前に診断するためだけに使用されます。

許容される値:

- 任意の正の整数。

デフォルト値: 10

## prefer_fetch_merged_part_time_threshold

レプリケーションログ（ClickHouse Keeper または ZooKeeper）のエントリ作成からの経過時間が指定されたしきい値を超え、パーツのサイズの合計が `prefer_fetch_merged_part_size_threshold` を超える場合、ローカルでマージを行う代わりに、レプリカからマージされたパーツをフェッチすることが推奨されます。これにより非常に長いマージを高速化します。

許容される値:

- 任意の正の整数。

デフォルト値: 3600

## prefer_fetch_merged_part_size_threshold

パーツサイズの合計がこのしきい値を超え、レプリケーションログエントリの作成からの経過時間が `prefer_fetch_merged_part_time_threshold` を超える場合、ローカルでマージを行う代わりにレプリカからマージされたパーツをフェッチすることが推奨されます。これにより非常に長いマージを高速化します。

許容される値:

- 任意の正の整数。

デフォルト値: 10,737,418,240

## execute_merges_on_single_replica_time_threshold

この設定の値がゼロより大きい場合、1つのレプリカのみがすぐにマージを開始し、他のレプリカはその結果をダウンロードするためにその時間まで待ちます。その時間内に選ばれたレプリカがマージを完了しない場合は、標準の動作にフォールバックします。

許容される値:

- 任意の正の整数。

デフォルト値: 0（秒）

## remote_fs_execute_merges_on_single_replica_time_threshold

この設定の値がゼロより大きい場合、マージされたパーツが共有ストレージ上にあり、`allow_remote_fs_zero_copy_replication` が有効になっている場合、1つのレプリカのみがマージをすぐに開始します。

:::note ゼロコピー レプリケーションはまだ本番環境では使用できません
ゼロコピー レプリケーションは ClickHouse バージョン 22.8 以降でデフォルトで無効になっています。この機能は本番環境での使用を推奨しません。
:::

許容される値:

- 任意の正の整数。

デフォルト値: 10800

## try_fetch_recompressed_part_timeout

再圧縮を伴うマージを開始する前のタイムアウト（秒単位）。この期間、新しい圧縮パーツを取得しようとします。この期間中、再圧縮を伴うマージをクリックハウスが割り当てられている場合のレプリカからフェッチしようとします。

再圧縮は多くの場合遅く動作するため、このタイムアウトまで再圧縮を伴うマージを開始することなく、再圧縮された部分をレプリカからフェッチしようとします。

許容される値:

- 任意の正の整数。

デフォルト値: 7200

## always_fetch_merged_part

`true` の場合、このレプリカはパーツをマージせず、常に他のレプリカからマージされたパーツをダウンロードします。

許容される値:

- true, false

デフォルト値: false

## max_suspicious_broken_parts

最大破損パーツ数、これを超えると自動削除が拒否されます。

許容される値:

- 任意の正の整数。

デフォルト値: 100

## max_suspicious_broken_parts_bytes

全部の破損パーツの最大サイズ、これを超える場合、自動削除が拒否されます。

許容される値:

- 任意の正の整数。

デフォルト値: 1,073,741,824

## max_files_to_modify_in_alter_columns

ファイルの変更（削除、追加）数がこの設定値を超える場合 `ALTER` を適用しません。

許容される値:

- 任意の正の整数。

デフォルト値: 75

## max_files_to_remove_in_alter_columns

削除ファイル数がこの設定値を超える場合、 `ALTER` を適用しません。

許容される値:

- 任意の正の整数。

デフォルト値: 50

## replicated_max_ratio_of_wrong_parts

不正なパーツの比率が総パーツ数を基準としてこの値を下回っている場合に開始を許可します。

許容される値:

- 浮動小数点数、0.0 - 1.0

デフォルト値: 0.5

## replicated_max_parallel_fetches_for_host

エンドポイントからの並列フェッチの制限（実際のプールサイズ）。

許容される値:

- 任意の正の整数。

デフォルト値: 15

## replicated_fetches_http_connection_timeout

部分フェッチリクエストの HTTP 接続タイムアウト。デフォルトプロファイル `http_connection_timeout` より継承され、明示的に設定されていない場合。

許容される値:

- 任意の正の整数。

デフォルト値: 明示的に設定されていない場合はデフォルトプロファイル `http_connection_timeout` より継承されます。

## replicated_can_become_leader

`true` の場合、このノード上のレプリカーテーブルのレプリカがリーダーシップを取得しようとします。

許容される値:

- true, false

デフォルト値: true

## zookeeper_session_expiration_check_period

ZooKeeper セッションの有効期限チェック期間、秒単位。

許容される値:

- 任意の正の整数。

デフォルト値: 60

## detach_old_local_parts_when_cloning_replica

失われたレプリカを修復する際に古いローカルパーツを削除しません。

許容される値:

- true, false

デフォルト値: true

## replicated_fetches_http_connection_timeout {#replicated_fetches_http_connection_timeout}

部分フェッチリクエストの HTTP 接続タイムアウト（秒単位）。明示的に設定されていない場合は、デフォルトプロファイル[http_connection_timeout](./settings.md#http_connection_timeout)より継承されます。

許容される値:

- 任意の正の整数。
- 0 - `http_connection_timeout` の値を使用します。

デフォルト値: 0。

## replicated_fetches_http_send_timeout {#replicated_fetches_http_send_timeout}

部分フェッチリクエストの HTTP 送信タイムアウト（秒単位）。明示的に設定されていない場合は、デフォルトプロファイル[http_send_timeout](./settings.md#http_send_timeout)より継承されます。

許容される値:

- 任意の正の整数。
- 0 - `http_send_timeout` の値を使用します。

デフォルト値: 0。

## replicated_fetches_http_receive_timeout {#replicated_fetches_http_receive_timeout}

フェッチ部分リクエストの HTTP 受信タイムアウト（秒単位）。明示的に設定されていない場合は、デフォルトプロファイル[http_receive_timeout](./settings.md#http_receive_timeout)より継承されます。

許容される値:

- 任意の正の整数。
- 0 - `http_receive_timeout` の値を使用します。

デフォルト値: 0。

## max_replicated_fetches_network_bandwidth {#max_replicated_fetches_network_bandwidth}

[レプリケートされた](../../engines/table-engines/mergetree-family/replication.md) フェッチのためのネットワークの最大速度を秒あたりのバイト数で制限します。この設定は特定のテーブルに適用され、[max_replicated_fetches_network_bandwidth_for_server](settings.md#max_replicated_fetches_network_bandwidth_for_server) 設定はサーバに対して適用されます。

サーバーネットワークと特定のテーブル用ネットワークは両方設定することができますが、このためにはテーブルレベル設定の値がサーバレベルのものよりも小さくなければなりません。そうでない場合、サーバーは`max_replicated_fetches_network_bandwidth_for_server` 設定だけを考慮します。

この設定は正確に順守されていません。

許容される値:

- 正の整数。
- 0 — 無制限。

デフォルト値: `0`.

**使用方法**

新しいノードを追加または置換する際にデータをレプリケートする速度を制限するために使用される。

## max_replicated_sends_network_bandwidth {#max_replicated_sends_network_bandwidth}

[レプリケートされた](../../engines/table-engines/mergetree-family/replication.md) 送信のためのネットワークの最大速度を秒あたりのバイト数で制限します。この設定は特定のテーブルに適用され、[max_replicated_sends_network_bandwidth_for_server](settings.md#max_replicated_sends_network_bandwidth_for_server) 設定はサーバに対して適用されます。

サーバーネットワークと特定のテーブル用ネットワークは両方設定することができますが、このためにはテーブルレベル設定の値がサーバレベルのものよりも小さくなければなりません。そうでない場合、サーバーは`max_replicated_sends_network_bandwidth_for_server` 設定だけを考慮します。

この設定は正確に順守されていません。

許容される値:

- 正の整数。
- 0 — 無制限。

デフォルト値: `0`.

**使用方法**

新しいノードを追加または置換する際にデータをレプリケートする速度を制限するために使用される。

## old_parts_lifetime {#old-parts-lifetime}

サーバの予期しない再起動中のデータ損失を防ぐための非アクティブパーツの保持時間（秒単位）。

許容される値:

- 任意の正の整数。

デフォルト値: 480。

いくつかのパーツを新しいパーツにマージした後、ClickHouse は元のパーツを非アクティブとしてマークし、`old_parts_lifetime` 秒後にのみそれらを削除します。
非アクティブパーツは現在のクエリで使用されていない場合に削除されます。すなわち、パーツの `refcount` が 1 の場合です。

新しいパーツには `fsync` が呼び出されないため、新しいパーツは一時的にサーバのRAM（OSキャッシュ）にのみ存在します。サーバが予期せず再起動された場合、新しいパーツが失われたり損傷を受けたりする可能性があります。
データを保護するために、非アクティブパーツはすぐに削除されません。

起動時、ClickHouse はパーツの整合性を確認します。
マージされたパーツが損傷している場合、ClickHouse は非アクティブパーツをアクティブリストに戻し、それらを再びマージします。その後、損傷したパーツはリネームされ（`broken_` 接頭辞が追加）、`detached` フォルダに移動されます。
マージされたパーツが損傷していない場合、元の非アクティブパーツはリネームされ（`ignored_` 接頭辞が追加）、`detached` フォルダに移動されます。

デフォルトの `dirty_expire_centisecs` 値（Linux カーネル設定）は 30 秒（書き込まれたデータがRAMにのみ保存される最大時間）ですが、ディスクシステムに負荷がかかると、データははるか後に書き込まれます。エクスペリメンタルに、`old_parts_lifetime` の値として 480 秒が選ばれ、新しいパーツがディスクに確実に書き込まれる時間となります。

## max_bytes_to_merge_at_max_space_in_pool {#max-bytes-to-merge-at-max-space-in-pool}

十分なリソースが利用可能な場合に1つのパーツにマージされる最大トータルパーツサイズ（バイト単位）。`max_bytes_to_merge_at_max_space_in_pool`は、自動バックグラウンドマージによって作成される最大可能パーツサイズにおおよそ対応しています。

許容される値:

- 任意の正の整数。

デフォルト値: 161061273600 (150 GB)。

マージスケジューラは定期的にパーティション内のパーツサイズと数を分析し、プール内に十分な空きリソースがある場合、バックグラウンドマージを開始します。マージは、ソースパーツの総サイズが `max_bytes_to_merge_at_max_space_in_pool` を超えるまで行われます。

[OPTIMIZE FINAL](../../sql-reference/statements/optimize.md)によって開始されたマージは、`max_bytes_to_merge_at_max_space_in_pool` を無視し、利用可能なリソース（空きディスク容量）を考慮して、パーティション内に1つのパーツが残るまでパーツをマージします。

## max_bytes_to_merge_at_min_space_in_pool {#max-bytes-to-merge-at-min-space-in-pool}

バックグラウンドプールで最小の利用可能リソースで1つのパーツにマージされる最大トータルパーツサイズ（バイト単位）。

許容される値:

- 任意の正の整数。

デフォルト値: 1048576 (1 MB)

`max_bytes_to_merge_at_min_space_in_pool`は、ディスク空き容量不足の状況でも小さなパーツの数を減らすために、最大トータルパーツサイズを定義します。これにより、小さなパーツの数を減らし、`Too many parts` エラーの可能性を減らします。
マージは、マージされたトータルパーツサイズを2倍にすることでディスクスペースを予約します。したがって、ディスク空きスペースが少ない場合、フリースペースは多くありますが、このスペースは進行中の大規模なマージによって既に予約されており、他のマージを開始できないため、小さなパーツの数が増え続けます。

## merge_max_block_size {#merge-max-block-size}

メモリに読み込まれるパーツからの行数。

可能な値:

- 任意の正の整数。

デフォルト値: 8192

マージが `merge_max_block_size` 行単位のブロックで行を読み取って、マージおよび新しいパーツに書き込みます。読み取りブロックはRAM内に配置されるため、`merge_max_block_size` はマージのために必要とされるRAMのサイズに影響を与えます。このため、マージは非常に広い行を持つテーブルに対して大量のRAMを消費する可能性があります（平均行サイズが100kbの場合、10個のパーツをマージするとき、(100kb * 10 * 8192) = 約 8GBのRAM）。`merge_max_block_size` を減少させることによって、必要とされるRAMの量を減少させることができますが、マージを遅くします。

## number_of_free_entries_in_pool_to_lower_max_size_of_merge {#number-of-free-entries-in-pool-to-lower-max-size-of-merge}

プール（またはレプリケートされたキュー）に指定された数値以下の空きエントリがある場合、処理する最大マージサイズを縮小し始めます（もしくはキューに投入します）。
これは長時間実行されるマージでプールを埋め尽くさないように、小さなマージを処理させるためです。

可能な値:

- 任意の正の整数。

デフォルト値: 8

## number_of_free_entries_in_pool_to_execute_mutation {#number-of-free-entries-in-pool-to-execute-mutation}

プールに指定された数値以下の空きエントリがある場合、パーツの変異を実行しません。
これは通常のマージのための空きスレッドを残すため、"パーツが多すぎる"のを避けるためです。

可能な値:

- 任意の正の整数。

デフォルト値: 20

**使用方法**

`number_of_free_entries_in_pool_to_execute_mutation` の設定値は [background_pool_size](/docs/ja/operations/server-configuration-parameters/settings.md/#background_pool_size) * [background_merges_mutations_concurrency_ratio](/docs/ja/operations/server-configuration-parameters/settings.md/#background_merges_mutations_concurrency_ratio) の値未満である必要があります。そうでない場合、ClickHouse は例外をスローします。

## max_part_loading_threads {#max-part-loading-threads}

ClickHouse が起動する際にパーツを読み込む最大スレッド数。

可能な値:

- 任意の正の整数。

デフォルト値: 自動（CPUコア数）。

起動時、ClickHouse はすべてのテーブルのすべてのパーツを読み取ります（パーツのメタデータを持つファイルを読み取る）し、メモリにすべてのパーツのリストを作成します。このプロセスは、パーツの数が多いシステムでは時間がかかることがあり、`max_part_loading_threads` を増やすことにより、このプロセスを短縮することができます（この処理がCPUとディスクI/Oによって制約されていない場合）。

## max_partitions_to_read {#max-partitions-to-read}

一つのクエリでアクセスできる最大パーティション数を制限します。

テーブル作成時に指定した設定値はクエリレベルの設定で上書きできます。

可能な値:

- 任意の正の整数。

デフォルト値: -1（無制限）。

また、クエリ複雑性設定 [max_partitions_to_read](query-complexity#max-partitions-to-read) をクエリ / セッション / プロファイルレベルで指定できます。

## min_age_to_force_merge_seconds {#min_age_to_force_merge_seconds}

すべてのパートが `min_age_to_force_merge_seconds` の値より古い場合にパーツをマージします。

可能な値:

- 正の整数。

デフォルト値: 0 — 無効。

## min_age_to_force_merge_on_partition_only {#min_age_to_force_merge_on_partition_only}

`min_age_to_force_merge_seconds` をパーティション全体にのみ適用し、部分セットには適用しないかどうか。

可能な値:

- true, false

デフォルト値: false

## number_of_free_entries_in_pool_to_execute_optimize_entire_partition {#number_of_free_entries_in_pool_to_execute_optimize_entire_partition}

プールに指定された数よりも少ない空きエントリがある場合、バックグラウンドでパーティション全体を最適化する操作を実行しません（`min_age_to_force_merge_seconds` を設定し、`min_age_to_force_merge_on_partition_only` を有効にした場合に生成されるタスク）。これにより通常のマージ用の空きスレッドを残し、"パーツが多すぎる"のを避けるためです。

可能な値:

- 正の整数。

デフォルト値: 25

`number_of_free_entries_in_pool_to_execute_optimize_entire_partition` 設定の値は [background_pool_size](/docs/ja/operations/server-configuration-parameters/settings.md/#background_pool_size) * [background_merges_mutations_concurrency_ratio](/docs/ja/operations/server-configuration-parameters/settings.md/#background_merges_mutations_concurrency_ratio) の値より少なくする必要があります。そうでない場合、ClickHouse は例外をスローします。

## allow_floating_point_partition_key {#allow_floating_point_partition_key}

パーティションキーとして浮動小数点数を許可するかどうかを設定します。

可能な値:

- 0 — 浮動小数点パーティションキーを許可しない。
- 1 — 浮動小数点パーティションキーを許可する。

デフォルト値: `0`.

## check_sample_column_is_correct {#check_sample_column_is_correct}

テーブル作成時にサンプリング用のカラムまたはサンプリング式のデータタイプが正しいかどうかを確認するチェックを有効にします。データタイプは、`UInt8`、`UInt16`、`UInt32`、`UInt64` のいずれかの符号なし [整数型](../../sql-reference/data-types/int-uint.md)でなければなりません。

可能な値:

- true  — チェックが有効です。
- false — テーブル作成時のチェックが無効です。

デフォルト値: `true`.

デフォルトでは、ClickHouse サーバはテーブル作成時にサンプリング用のカラムまたはサンプリング式のデータタイプをチェックします。すでに不正なサンプリング式を持つテーブルがあり、サーバが起動中に例外を発生させたくない場合は `check_sample_column_is_correct` を `false` に設定してください。

## min_bytes_to_rebalance_partition_over_jbod {#min-bytes-to-rebalance-partition-over-jbod}

新しい大型パーツをボリュームディスク [JBOD](https://en.wikipedia.org/wiki/Non-RAID_drive_architectures) 上に分散させる際のバランシングを有効にするための最小バイト数を設定します。

可能な値:

- 正の整数。
- 0 — バランシングが無効です。

デフォルト値: `0`.

**使用例**

`min_bytes_to_rebalance_partition_over_jbod` 設定の値は [max_bytes_to_merge_at_max_space_in_pool](../../operations/settings/merge-tree-settings.md#max-bytes-to-merge-at-max-space-in-pool) / 1024 の値より多くなければなりません。そうでない場合、 ClickHouse は例外をスローします。

## detach_not_byte_identical_parts {#detach_not_byte_identical_parts}

マージまたは変異後、他のレプリカのデータパーツとバイト単位で一致しない場合に、データパーツをデタッチするかどうかを設定します。無効な場合、データパーツは削除されます。この設定を有効にすると、後でそのようなパーツを分析することができます。

この設定は、[データレプリケーション](../../engines/table-engines/mergetree-family/replication.md)が有効な `MergeTree` テーブルに適用されます。

可能な値:

- 0 — パーツは削除されます。
- 1 — パーツはデタッチされます。

デフォルト値: `0`.

## merge_tree_clear_old_temporary_directories_interval_seconds {#setting-merge-tree-clear-old-temporary-directories-interval-seconds}

古い一時ディレクトリのクリーンアップを実行するための秒単位での間隔をClickHouseに設定します。

可能な値:

- 任意の正の整数。

デフォルト値: `60` 秒。

## merge_tree_clear_old_parts_interval_seconds {#setting-merge-tree-clear-old-parts-interval-seconds}

古いパーツ、WAL、および変異のクリーンアップを実行するための秒単位での間隔をClickHouseに設定します。

可能な値:

- 任意の正の整数。

デフォルト値: `1` 秒。

## max_concurrent_queries {#max-concurrent-queries}

MergeTree テーブルに関連する最大同時実行クエリ数。クエリは他の `max_concurrent_queries` 設定でも制限されます。

可能な値:

- 正の整数。
- 0 — 制限なし。

デフォルト値: `0`（無制限）。

**例**

``` xml
<max_concurrent_queries>50</max_concurrent_queries>
```

## min_marks_to_honor_max_concurrent_queries {#min-marks-to-honor-max-concurrent-queries}

クエリが [max_concurrent_queries](#max-concurrent-queries) 設定を適用できるようにするために読み取る必要のある最小マーク数。クエリは他の `max_concurrent_queries` 設定でも制限されることに注意してください。

可能な値:

- 正の整数。
- 0 — 無効（`max_concurrent_queries` 制限はクエリには適用されません）。

デフォルト値: `0`（制限は適用されません）。

**例**

``` xml
<min_marks_to_honor_max_concurrent_queries>10</min_marks_to_honor_max_concurrent_queries>
```

## ratio_of_defaults_for_sparse_serialization {#ratio-of-defaults-for-sparse-serialization}

カラム内の *_all_ 値に対する _default_ 値の比率の最小限を設定します。この値を設定することで、カラムがスパースなシリアライゼーションを使用して保存されます。

カラムがスパース（ほとんどゼロを含む）の場合、ClickHouse はそれをスパースな形式でエンコードすることができ、計算が自動的に最適化されます。これは、クエリ中にデータが完全に展開される必要がないためです。スパースなシリアライゼーションを有効にするためには、`ratio_of_defaults_for_sparse_serialization` 設定を 1.0 未満に定義します。値が 1.0 以上の場合、通常の完全シリアライゼーションを使用して常に書き込みが行われます。

可能な値:

- 0 と 1 の間の浮動小数点数、スパースなシリアライゼーションを有効にする
- 1.0（またはそれ以上）、スパースなシリアライゼーションを使用したくない場合

デフォルト値: `0.9375`

**例**

次のテーブル内の `s` カラムには、行の95％が空の文字列です。`my_regular_table` ではスパースなシリアライゼーションを使用せず、`my_sparse_table` では `ratio_of_defaults_for_sparse_serialization` を 0.95 に設定します。

```sql
CREATE TABLE my_regular_table
(
    `id` UInt64,
    `s` String
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO my_regular_table
SELECT
    number AS id,
    number % 20 = 0 ? toString(number): '' AS s
FROM
    numbers(10000000);


CREATE TABLE my_sparse_table
(
    `id` UInt64,
    `s` String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.95;

INSERT INTO my_sparse_table
SELECT
    number,
    number % 20 = 0 ? toString(number): ''
FROM
    numbers(10000000);
```

`my_sparse_table` の `s` カラムがディスク上でより少ないストレージスペースを使用していることに注意してください。

```sql
SELECT table, name, data_compressed_bytes, data_uncompressed_bytes FROM system.columns
WHERE table LIKE 'my_%_table';
```

```response
┌─table────────────┬─name─┬─data_compressed_bytes─┬─data_uncompressed_bytes─┐
│ my_regular_table │ id   │              37790741 │                75488328 │
│ my_regular_table │ s    │               2451377 │                12683106 │
│ my_sparse_table  │ id   │              37790741 │                75488328 │
│ my_sparse_table  │ s    │               2283454 │                 9855751 │
└──────────────────┴──────┴───────────────────────┴─────────────────────────┘
```

カラムがスパースなエンコーディングを使用しているかどうかを確認するには、`system.parts_columns` テーブルの `serialization_kind` カラムを表示します。

```sql
SELECT column, serialization_kind FROM system.parts_columns
WHERE table LIKE 'my_sparse_table';
```

スパースなシリアライゼーションを使用してストレージされた `s` の部分を見ることができます。

```response
┌─column─┬─serialization_kind─┐
│ id     │ Default            │
│ s      │ Default            │
│ id     │ Default            │
│ s      │ Default            │
│ id     │ Default            │
│ s      │ Sparse             │
│ id     │ Default            │
│ s      │ Sparse             │
│ id     │ Default            │
│ s      │ Sparse             │
│ id     │ Default            │
│ s      │ Sparse             │
│ id     │ Default            │
│ s      │ Sparse             │
│ id     │ Default            │
│ s      │ Sparse             │
│ id     │ Default            │
│ s      │ Sparse             │
└────────┴────────────────────┘
```

## replace_long_file_name_to_hash {#replace_long_file_name_to_hash}
カラムのファイル名が `max_file_name_length` バイトを超える場合、SipHash128 に置き換えます。デフォルト値: `false`.

## max_file_name_length {#max_file_name_length}

ハッシュを適用せずに元のままにしておくファイル名の最大長。`replace_long_file_name_to_hash` が有効になっている場合のみ影響します。この設定の値はファイル拡張子の長さを含みません。したがって、ファイルシステムのエラーを避けるために、通常 255 バイト以下の最大ファイル名長よりもある程度の余裕を持って設定することをお勧めします。デフォルト値: 127。

## allow_experimental_block_number_column

マージにおいて仮想カラム `_block_number` を永続化します。

デフォルト値: false.

## exclude_deleted_rows_for_part_size_in_merge {#exclude_deleted_rows_for_part_size-in-merge}

有効にすると、`DELETE FROM` を使用して削除された行を除外して、データパーツの実際の推定サイズを使用してパーツを選択してマージを行います。この動作は、この設定が有効化された `DELETE FROM` の影響を受けるデータパーツにのみトリガーされることに注意してください。

可能な値:

- true, false

デフォルト値: false

**関連項目**

- [load_existing_rows_count_for_old_parts](#load_existing_rows_count-for-old-parts) 設定

## load_existing_rows_count_for_old_parts {#load_existing_rows_count_for_old_parts}

[exclude_deleted_rows_for_part_size_in_merge](#exclude_deleted_rows_for_part_size_in_merge) とともに有効にされている場合、既存のデータパーツの削除された行数がテーブルの起動時に計算されます。これは、テーブルの起動時に遅延が発生する可能性があります。

可能な値:

- true, false

デフォルト値: false

**関連項目**

- [exclude_deleted_rows_for_part_size_in_merge](#exclude_deleted_rows_for_part_size_in_merge) 設定

## use_compact_variant_discriminators_serialization {#use_compact_variant_discriminators_serialization}

Variant データ型でのバイナリシリアライゼーションのディスクリミネータのためのコンパクトモードを有効化します。
このモードでは、大多数が1つのバリアントまたは多くのNULL値がある場合に、パーツにディスクリミネータが保存されるために大幅にメモリを節約できます。

デフォルト値: true

## merge_workload

マージおよび他のワークロード間でのリソース利用と共有を調整するために使用されます。特定のテーブルのバックグラウンドマージのための `workload` 設定値として使用します。指定されていない場合（空の文字列）、サーバー設定 `merge_workload` が代わりに使用されます。

デフォルト値: 空の文字列

**関連項目**
- [ワークロードスケジューリング](/docs/ja/operations/workload-scheduling.md)

## mutation_workload

変異および他のワークロード間でのリソース利用と共有を調整するために使用されます。特定のテーブルのバックグラウンド変異のための `workload` 設定値として使用します。指定されていない場合（空の文字列）、サーバー設定 `mutation_workload` が代わりに使用されます。

デフォルト値: 空の文字列

**関連項目**
- [ワークロードスケジューリング](/docs/ja/operations/workload-scheduling.md)

### optimize_row_order

新しく挿入されたテーブルパートの圧縮性を向上させるために行順序を最適化すべきかどうかを制御します。

これは通常の MergeTree エンジン テーブルにのみ影響します。専用の MergeTree エンジンテーブル（例: CollapsingMergeTree）には影響しません。

MergeTree テーブルは（任意で）[圧縮コーデック](../../sql-reference/statements/create/table.md#column_compression_codec)を使用して圧縮されます。
LZ4 や ZSTD などの一般的な圧縮コーデックは、データがパターンを示す場合、最大の圧縮率を達成します。
同じ値の長く続くランは通常非常によく圧縮されます。

この設定が有効になっている場合、ClickHouse は新しく挿入されたパーツ内のデータを、テーブルの新しいパーツのカラム全体で等値ランの数を最小化するような行順序で保存しようとします。
言い換えれば、少ない数の等値ランは個々のランが長くなり、よく圧縮されることを意味します。

最適な行順序を見つけることは計算上不可能（NPハード）です。
そのため、ClickHouse は依然として元の行順序よりも圧縮率を改善する行順序を迅速に見つけるためにヒューリスティックスを使用します。

<details markdown="1">

<summary>行順序を見つけるためのヒューリスティックス</summary>

SQLでは、異なる行順序の同じテーブル（テーブルパート）が同等であると見なされるため、テーブル（またはテーブルパート）に対して自由に行をシャッフルすることが一般に可能です。

テーブルの主キーが定義されている場合、この行のシャッフルの自由度は制限されます。
ClickHouse では、主キー `C1, C2, ..., CN` は、テーブルの行により `C1`, `C2`, ..., `Cn` のカラムでソートされていることが求められます（[クラスタインデックス](https://en.wikipedia.org/wiki/Database_index#Clustered)）。
その結果、プライマリキーカラムと同じ値を持つ行、すなわち行の"等値クラス"内でのみ行のシャッフルが許可されます。
直感的には、高カードのプライマリキー、例: `DateTime64` タイムスタンプカラムを含む主キーは多数の小さな等値クラスを生じます。
それに対して、低カードのプライマリキーを持つテーブルは少数の大きな等値クラスを生成します。
主キーなしのテーブルは、すべての行を一つの等値クラスとして表現する極端なケースです。

等値クラスが少なく、大きいほど、行の再シャッフル時の自由度が高くなります。

それぞれの等値クラスにおける最適な行順序を見つけるヒューリスティックスは、D. Lemire, O. Kaserが[Reordering columns for smaller indexes](https://doi.org/10.1016/j.ins.2011.02.002)で提案し、主キー以外のカラムの昇順で行を並べ替えます。
次の3つの手順を実行します。
1. 主キーカラムの行値に基づいてすべての等値クラスを見つけます。
2. 各等値クラスに対して、主キー以外のカラムのカーディナリティを計算（通常は推定）します。
3. 各等値クラスに対して、主キー以外のカラムのカーディナリティの昇順で行を並べ替えます。

</details>

有効にすると、新しいデータの行順序を分析および最適化するための追加のCPUコストが発生します。
INSERTの実行は、データ特性に応じて30〜50％長くなることが予想されます。
LZ4 または ZSTD の圧縮率は平均20〜40％改善します。

この設定は、低カードの主キーを持つテーブルに最適です。すなわち、少数の異なる主キー値を持つテーブルです。
高カードの主キー、例: `DateTime64` 型のタイムスタンプカラムを含む主キーは、この設定からの利益を期待されません。

## lightweight_mutation_projection_mode

デフォルトでは、論理削除の `DELETE` は、プロジェクションを持つテーブルには機能しません。これは、プロジェクション中の行が `DELETE` 操作によって影響を受ける可能性があるためです。したがって、デフォルト値は `throw` となります。
しかし、このオプションにより動作を変更できます。`drop` または `rebuild` のいずれかの値を使用した場合、削除がプロジェクションに伴います。`drop` はプロジェクションを削除するため、プロジェクションが削除されたがために現在のクエリで高速ですが、将来のクエリで遅くなる可能性があります。
`rebuild` はプロジェクションを再構築し、現在のクエリのパフォーマンスに影響を与えますが、将来のクエリのスピードアップが期待されます。良いことは、これらのオプションは部分レベルでのみ動作するため、
タッチしない部分のプロジェクションは、削除や再構築のアクションをトリガーすることなくそのまま残ります。

可能な値:

- throw, drop, rebuild

デフォルト値: throw

## deduplicate_merge_projection_mode

非クラシック MergeTree で非クラシック MergeTree を持つテーブルのためにプロジェクションを作成することを許可するかどうか。無視するオプションは、純粋に互換性のためのもので、誤った回答をもたらす可能性があります。許可される場合、マージプロジェクションのアクション（削除または再構築）です。そのため、クラシック MergeTree はこの設定を無視します。
`OPTIMIZE DEDUPLICATE` も制御し、すべての MergeTree ファミリメンバーに影響を及ぼします。`lightweight_mutation_projection_mode` オプションと同様に、部分レベルです。

可能な値:

- ignore, throw, drop, rebuild

デフォルト値: throw

## min_free_disk_bytes_to_perform_insert

データを挿入するためにディスクスペースに空いているべき最小バイト数。利用可能なバイトが `min_free_disk_bytes_to_perform_insert` 未満の場合は例外がスローされ、挿入が実行されません。この設定は以下を念頭に置いています。
- `keep_free_space_bytes` 設定を考慮します。
- `INSERT` 操作によって書き込まれるデータ量は考慮しません。
- 正の（ゼロでない）バイト数が指定された場合にのみチェックされます

可能な値:

- 任意の正の整数。

デフォルト値: 0 バイト。

`min_free_disk_bytes_to_perform_insert` と `min_free_disk_ratio_to_perform_insert` の両方が指定されている場合、ClickHouse は空きメモリの大きさで入力を許可する値を考慮します。

## min_free_disk_ratio_to_perform_insert

`INSERT` を実行するためのディスクスペースの最小の空き対合計比率。0と1の間の浮動小数点値である必要があります。この設定は以下を考慮します。
- `keep_free_space_bytes` 設定を考慮します。
- `INSERT` 操作によって書き込まれるデータ量は考慮しません。
- 正の（ゼロでない）比率が指定された場合にのみチェックされます

可能な値:

- 浮動小数点、0.0 - 1.0

デフォルト値: 0.0

`min_free_disk_ratio_to_perform_insert` と `min_free_disk_bytes_to_perform_insert` の両方が指定されている場合、ClickHouse は空きメモリの大きな部分で挿入を実行することを許可する値を考慮します。
