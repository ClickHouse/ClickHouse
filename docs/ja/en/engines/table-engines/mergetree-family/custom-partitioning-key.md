---
description: 'MergeTree テーブルにカスタムのパーティションキーを追加する方法を学びます。'
sidebar_label: 'カスタムパーティションキー'
sidebar_position: 30
slug: /engines/table-engines/mergetree-family/custom-partitioning-key
title: 'カスタムパーティションキー'
doc_type: 'guide'
---

:::note
ほとんどの場合、パーティションキーは必要ありません。また、オブザーバビリティのユースケースで日単位のパーティション化が一般的な場合を除き、月単位より細かいパーティションキーもたいてい不要です。

細かすぎるパーティション化は決して使用しないでください。データをクライアント識別子や名前でパーティション化しないでください。代わりに、クライアント識別子または名前を ORDER BY 式の先頭のカラムにしてください。
:::

パーティション化は、[MergeTree family tables](../../../engines/table-engines/mergetree-family/mergetree.md) ([レプリケートテーブル](../../../engines/table-engines/mergetree-family/replication.md) や [materialized views](/ja/sql-reference/statements/create/view#materialized-view) を含む) で使用できます。

パーティションは、指定した基準に基づいてテーブル内のレコードを論理的にまとめたものです。月単位、日単位、イベントタイプ単位など、任意の基準で設定できます。各パーティションは、このデータを扱いやすくするために個別に保存されます。データにアクセスする際、ClickHouse は可能な限り最小のパーティションのサブセットを使用します。パーティション化キーを含むクエリでは、ClickHouse はパーティション内のパーツや granules を選択する前にそのパーティションでフィルタリングを行うため、パーティション化によってパフォーマンスが向上します。

パーティションは、[テーブルを作成する](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)際の `PARTITION BY expr` 句で指定します。パーティションキーには、テーブルのカラムに基づく任意の式を使用できます。たとえば、月単位のパーティション化を指定するには、`toYYYYMM(date_column)` 式を使用します。

```sql
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

パーティションキーには、式のタプルを指定することもできます ([主キー](../../../engines/table-engines/mergetree-family/mergetree.md#primary-keys-and-indexes-in-queries)と同様です) 。例:

```sql
ENGINE = ReplicatedCollapsingMergeTree('/clickhouse/tables/name', 'replica1', Sign)
PARTITION BY (toMonday(StartDate), EventType)
ORDER BY (CounterID, StartDate, intHash32(UserID));
```

この例では、現在の週に発生したイベントタイプごとにパーティション化を設定します。

デフォルトでは、浮動小数点のパーティションキーはサポートされていません。使用するには、設定 [allow&#95;floating&#95;point&#95;partition&#95;key](../../../operations/settings/merge-tree-settings.md#allow_floating_point_partition_key) を有効にします。

新しいデータをテーブルに挿入すると、そのデータは主キーでソートされた個別のパーツ (chunk) として保存されます。挿入から 10〜15 分後、同じパーティション内のパーツは 1 つの完全なパーツにマージされます。

:::info
マージが機能するのは、パーティション化式の値が同じデータパーツに対してのみです。つまり、**パーティションを過度に細かく分けるべきではありません** (パーティション数はおよそ 1,000 個以下に抑えてください) 。そうしないと、ファイルシステム内のファイル数や開いているファイルディスクリプタの数が過剰になり、`SELECT` クエリのパフォーマンスが低下します。
:::

テーブルパーツとパーティションを確認するには、[system.parts](../../../operations/system-tables/parts.md) テーブルを使用します。たとえば、月単位でパーティション化された `visits` テーブルがあるとします。`system.parts` テーブルに対して `SELECT` クエリを実行してみましょう。

```sql
SELECT
    partition,
    name,
    active
FROM system.parts
WHERE table = 'visits'
```

```text
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

`partition` カラムにはパーティション名が含まれます。この例では、`201901` と `201902` の 2 つのパーティションがあります。このカラムの値を使用すると、[ALTER ... PARTITION](../../../sql-reference/statements/alter/partition.md) クエリでパーティション名を指定できます。

`name` カラムにはパーティションのデータパーツ名が含まれます。このカラムを使用すると、[ALTER ATTACH PART](/ja/sql-reference/statements/alter/partition#attach-partitionpart) クエリでパーツ名を指定できます。

パーツ名 `201901_1_9_2_11` を分解してみましょう。

* `201901` はパーティション名です。
* `1` は data block の最小番号です。
* `9` は data block の最大番号です。
* `2` は chunk レベルです (このパーツの元になったマージツリーの深さ) 。
* `11` は mutation バージョンです (パーツが mutation 済みの場合) 。

:::info
旧形式のテーブルのパーツ名は `20190117_20190123_2_2_0` です (最小日付 - 最大日付 - 最小ブロック番号 - 最大ブロック番号 - レベル) 。
:::

`active` カラムはパーツのステータスを示します。`1` はアクティブ、`0` は非アクティブです。非アクティブなパーツには、たとえば、より大きなパーツへマージされた後に残る元のパーツがあります。破損したデータパーツも非アクティブとして示されます。

例を見ると、同じパーティションに複数の別個のパーツがあります (たとえば、`201901_1_3_1` と `201901_1_9_2`) 。これは、これらのパーツがまだマージされていないことを意味します。ClickHouse は、データ挿入後およそ 15 分で、挿入されたデータパーツを定期的にマージします。さらに、[OPTIMIZE](../../../sql-reference/statements/optimize.md) クエリを使用して、スケジュールされていないマージを実行することもできます。例:

```sql
OPTIMIZE TABLE visits PARTITION 201902;
```

```text
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

非アクティブなパーツは、マージ後およそ10分で削除されます。

パーツとパーティションの一式を確認するもう1つの方法は、テーブルのディレクトリ `/var/lib/clickhouse/data/<database>/<table>/` を見ることです。例えば:

```bash
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

フォルダ `'201901_1_1_0'`、`'201901_1_7_1'` などは、パーツのディレクトリです。各パーツは対応するパーティションに対応しており、特定の月のデータだけを含みます (この例のテーブルは月単位でパーティション化されています) 。

`detached` ディレクトリには、[DETACH](/ja/sql-reference/statements/detach) クエリを使用してテーブルからデタッチされたパーツが格納されます。破損したパーツも、削除される代わりにこのディレクトリへ移動されます。サーバーは `detached` ディレクトリ内のパーツを使用しません。このディレクトリ内のデータはいつでも追加、削除、変更できますが、[ATTACH](/ja/sql-reference/statements/alter/partition#attach-partitionpart) クエリを実行するまでは、サーバーはそのことを認識しません。

稼働中のサーバーでは、ファイルシステム上でパーツの集合やそのデータを手動で変更することはできない点に注意してください。サーバーがその変更を認識しないためです。非レプリケートテーブルの場合、サーバーの停止中であればこれを行えますが、推奨されません。レプリケートテーブルの場合、いかなる場合でもパーツの集合は変更できません。

ClickHouse では、パーティションに対して操作を実行できます。削除したり、あるテーブルから別のテーブルへコピーしたり、バックアップを作成したりできます。すべての操作の一覧については、[Manipulations With Partitions and Parts](/ja/sql-reference/statements/alter/partition) のセクションを参照してください。

<div id="group-by-optimisation-using-partition-key">
  ## パーティションキーを使用した Group By の最適化
</div>

テーブルのパーティションキーとクエリの Group By キーの組み合わせによっては、各パーティションごとに独立して集計を実行できる場合があります。
その場合、最後にすべての実行スレッドで部分的に集計されたデータをマージする必要はありません。
これは、各 Group By キーの値が 2 つの異なるスレッドのワーキングセットにまたがって現れないことが保証されるためです。

典型的な例は次のとおりです。

```sql
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
このようなクエリのパフォーマンスは、テーブルのレイアウトに大きく左右されます。そのため、この最適化はデフォルトでは有効になっていません。
:::

高いパフォーマンスを得るための主な条件は次のとおりです。

* クエリに含まれるパーティション数が十分に多いこと (`max_threads / 2` より多いこと) 。そうでないと、クエリがマシンを十分に活用できません
* パーティションが小さすぎないこと。小さすぎると、バッチ処理が行単位の処理に近くなってしまいます
* パーティションのサイズが同程度であること。そうすることで、すべてのスレッドがほぼ同じ量の処理を行えます

:::info
データを各パーティションに均等に分散するため、`partition by` 句のカラムに何らかのハッシュ関数を適用することを推奨します。
:::

関連する設定は次のとおりです。

* `allow_aggregate_partitions_independently` - この最適化を有効にするかどうかを制御します
* `force_aggregate_partitions_independently` - 正しさの観点では適用可能であっても、その有効性を見積もる内部ロジックによって無効化される場合に、この最適化の使用を強制します
* `max_number_of_partitions_for_independent_aggregation` - テーブルが持てるパーティション数の上限を定めるハード制限