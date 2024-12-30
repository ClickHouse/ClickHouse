---
slug: /ja/engines/table-engines/mergetree-family/custom-partitioning-key
sidebar_position: 30
sidebar_label: カスタムパーティショニングキー
---

# カスタムパーティショニングキー

:::note
ほとんどの場合、パーティションキーは必要なく、その他多くの場合も月単位以上に詳細なパーティションキーは不要です。

あまりに詳細なパーティショニングは絶対に避けてください。データをクライアントIDや名前でパーティショニングしないでください。代わりに、`ORDER BY` 式の最初のカラムとしてクライアントIDや名前を設定してください。
:::

パーティショニングは、[MergeTree ファミリーのテーブル](../../../engines/table-engines/mergetree-family/mergetree.md)で利用可能です。これには、[レプリケートテーブル](../../../engines/table-engines/mergetree-family/replication.md)や[マテリアライズドビュー](../../../sql-reference/statements/create/view.md#materialized-view)も含まれます。

パーティションとは、指定された基準によってテーブル内のレコードを論理的に組み合わせたものです。月単位、日単位、またはイベントタイプ別など、任意の基準でパーティションを設定できます。それぞれのパーティションは個別に保存され、このデータの操作を簡素化します。データにアクセスする際、ClickHouse は可能な限り最小のパーティションサブセットを使用します。パーティションキーを含むクエリのパフォーマンスは、パーティションによって向上します。ClickHouse はパーツやグラニュールを選択する前に、そのパーティションをフィルタ処理します。

パーティションは、[テーブル作成時](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)の `PARTITION BY expr` 句で指定されます。パーティションキーはテーブルのカラムから任意の式を用いることができます。例えば、月別のパーティショニングを指定するには、`toYYYYMM(date_column)` という式を使用します。

``` sql
CREATE TABLE visits
(
    VisitDate Date,
    Hour UInt8,
    ClientID UUID
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(VisitDate)
ORDER BY Hour;
```

パーティションキーは、複数の式のタプルにもできます（[主キー](../../../engines/table-engines/mergetree-family/mergetree.md#primary-keys-and-indexes-in-queries)と似ています）。例えば：

``` sql
ENGINE = ReplicatedCollapsingMergeTree('/clickhouse/tables/name', 'replica1', Sign)
PARTITION BY (toMonday(StartDate), EventType)
ORDER BY (CounterID, StartDate, intHash32(UserID));
```

この例では、当週中に発生したイベントタイプでパーティショニングを設定しています。

デフォルトでは、浮動小数点のパーティションキーはサポートされていません。使用するには、設定 [allow_floating_point_partition_key](../../../operations/settings/merge-tree-settings.md#allow_floating_point_partition_key) を有効にしてください。

新しいデータをテーブルに挿入する際、このデータは主キーでソートされた個別のパーツ（チャンク）として保存されます。挿入から10-15分後に、同じパーティションのパーツが統合されます。

:::info
統合はパーティショニング式が同じ値を持つデータパーツに対してのみ機能します。これは、**過剰に詳細なパーティションを作成しないでください**（約1,000パーティション以上）。さもないと、ファイルシステム上のファイル数が不当に多く、オープンファイルディスクリプタが膨大になるため `SELECT` クエリのパフォーマンスが悪くなります。
:::

[system.parts](../../../operations/system-tables/parts.md#system_tables-parts) テーブルを使用してテーブルのパーツとパーティションを表示できます。たとえば、月別パーティショニングの `visits` テーブルを持っていると仮定します。`system.parts` テーブルに対して `SELECT` クエリを実行してみましょう。

``` sql
SELECT
    partition,
    name,
    active
FROM system.parts
WHERE table = 'visits'
```

``` text
┌─partition─┬─name──────────────┬─active─┐
│ 201901    │ 201901_1_3_1      │      0 │
│ 201901    │ 201901_1_9_2_11   │      1 │
│ 201901    │ 201901_8_8_0      │      0 │
│ 201901    │ 201901_9_9_0      │      0 │
│ 201902    │ 201902_4_6_1_11   │      1 │
│ 201902    │ 201902_10_10_0_11 │      1 │
│ 201902    │ 201902_11_11_0_11 │      1 │
└───────────┴───────────────────┴────────┘
```

`partition` カラムにはパーティションの名前が含まれます。この例では、`201901` と `201902` の2つのパーティションがあります。このカラムの値を使用して、[ALTER ... PARTITION](../../../sql-reference/statements/alter/partition.md) クエリでパーティション名を指定できます。

`name` カラムにはパーティションデータパーツの名前が含まれます。このカラムを使用して、[ALTER ATTACH PART](../../../sql-reference/statements/alter/partition.md#alter_attach-partition) クエリでパーツの名前を指定できます。

パーツ名 `201901_1_9_2_11` の内訳は以下の通りです。

- `201901` はパーティション名です。
- `1` はデータブロックの最小番号です。
- `9` はデータブロックの最大番号です。
- `2` はチャンクレベル（生成される MergeTree の深さ）です。
- `11` は変更バージョン（パーツが変更された場合）

:::info
古いタイプのテーブルパーツ名は次の形式です：`20190117_20190123_2_2_0`（最小日付 - 最大日付 - 最小ブロック番号 - 最大ブロック番号 - レベル）。
:::

`active` カラムはパーツの状態を示します。`1` はアクティブ、`0` は非アクティブです。非アクティブなパーツは、例えば、より大きなパーツに統合された後に残るソースパーツです。破損したデータパーツも非アクティブとして示されます。

例で示すように、同じパーティションに属するいくつかの分離されたパーツがあります（例えば、`201901_1_3_1` と `201901_1_9_2`）。これはまだ統合されていない状態を示しています。ClickHouse はデータの挿入から約15分後に、挿入されたデータパーツを定期的に統合します。さらに、[OPTIMIZE](../../../sql-reference/statements/optimize.md) クエリを使用して非スケジュールの統合を行うこともできます。例：

``` sql
OPTIMIZE TABLE visits PARTITION 201902;
```

``` text
┌─partition─┬─name─────────────┬─active─┐
│ 201901    │ 201901_1_3_1     │      0 │
│ 201901    │ 201901_1_9_2_11  │      1 │
│ 201901    │ 201901_8_8_0     │      0 │
│ 201901    │ 201901_9_9_0     │      0 │
│ 201902    │ 201902_4_6_1     │      0 │
│ 201902    │ 201902_4_11_2_11 │      1 │
│ 201902    │ 201902_10_10_0   │      0 │
│ 201902    │ 201902_11_11_0   │      0 │
└───────────┴──────────────────┴────────┘
```

非アクティブなパーツは統合から約10分後に削除されます。

パーツとパーティションのセットを見る別の方法として、テーブルのディレクトリにアクセスする方法があります：`/var/lib/clickhouse/data/<database>/<table>/`。例：

``` bash
/var/lib/clickhouse/data/default/visits$ ls -l
total 40
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  1 16:48 201901_1_3_1
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201901_1_9_2_11
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 15:52 201901_8_8_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 15:52 201901_9_9_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201902_10_10_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201902_11_11_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:19 201902_4_11_2_11
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 12:09 201902_4_6_1
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  1 16:48 detached
```

フォルダー ‘201901_1_1_0’, ‘201901_1_7_1’ などはパーツのディレクトリです。各パーツは対応するパーティションに属し、特定の月（この例のテーブルは月単位でパーティショニングされています）のデータのみを含みます。

`detached` ディレクトリには、[DETACH](../../../sql-reference/statements/alter/partition.md#alter_detach-partition) クエリを使用してテーブルから切り離されたパーツが含まれます。破損したパーツも削除の代わりにこのディレクトリに移動されます。サーバーは `detached` ディレクトリのパーツを使用しません。このディレクトリ内のデータはいつでも追加、削除、変更できますが、サーバーは [ATTACH](../../../sql-reference/statements/alter/partition.md#alter_attach-partition) クエリを実行するまでこのことを認識しません。

動作中のサーバーでは、ファイルシステム上のパーツやそのデータのセットを手動で変更することはできません。サーバーはそれを認識しないためです。非レプリケートテーブルの場合、サーバーが停止しているときにこれを行うことができますが、お勧めしません。レプリケートテーブルの場合、パーツのセットを変更することはできません。

ClickHouse はパーティションに対して操作を行うことができます：削除、別のテーブルへのコピー、バックアップの作成など。すべての操作のリストは[パーティションとパーツの操作](../../../sql-reference/statements/alter/partition.md#alter_manipulations-with-partitions)セクションをご覧ください。

## パーティションキーを使用した GROUP BY 最適化

テーブルのパーティションキーとクエリの GROUP BY キーの組み合わせによっては、各パーティションごとに独立して集計処理を実行できる可能性があります。
この場合、すべての実行スレッドの部分的に集計されたデータを最終的にマージする必要がなくなります。
これは、各 GROUP BY キーの値が異なるスレッドの作業セットに現れないという保証があるためです。

典型的な例は以下の通りです：

``` sql
CREATE TABLE session_log
(
    UserID UInt64,
    SessionID UUID
)
ENGINE = MergeTree
PARTITION BY sipHash64(UserID) % 16
ORDER BY tuple();

SELECT
    UserID,
    COUNT()
FROM session_log
GROUP BY UserID;
```

:::note
このようなクエリのパフォーマンスは、テーブルのレイアウトに大きく依存します。そのため、この最適化はデフォルトでは有効になっていません。
:::

良好なパフォーマンスのための重要な要素：

- クエリに関与するパーティションの数が十分に大きいこと（`max_threads / 2` より多い）、そうでないとクエリはマシンを十分に利用できません
- パーティションが小さすぎてはいけません。そうでないと、バッチ処理が行ごとの処理に退化します
- パーティションは大きさが比較可能であるべきです。そうすると、すべてのスレッドがほぼ同等の作業量を行います

:::info
データを均等にパーティション間で分散させるためには、`partition by` 句のカラムにいくつかのハッシュ関数を適用することをお勧めします。
:::

関連する設定は次のとおりです：

- `allow_aggregate_partitions_independently` - 最適化の使用が有効かどうかを制御します
- `force_aggregate_partitions_independently` - 正しさの観点から使用可能な場合だが、内部ロジックによりその実行性が評価されて無効化されている場合に使用を強制します
- `max_number_of_partitions_for_independent_aggregation` - テーブルが保持することができるパーティションの最大数に対する制限

