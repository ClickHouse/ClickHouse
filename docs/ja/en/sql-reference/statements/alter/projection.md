---
description: 'プロジェクションの操作に関するドキュメント'
sidebar_label: 'PROJECTION'
sidebar_position: 49
slug: /sql-reference/statements/alter/projection
title: 'プロジェクション'
doc_type: 'reference'
---

このページでは、プロジェクションの概要、使用方法、およびプロジェクションを操作するための各種オプションについて説明します。

<div id="overview">
  ## プロジェクションの概要
</div>

プロジェクションは、クエリ実行を最適化するフォーマットでデータを格納します。この機能は、次のような用途で役立ちます。

* 主キーに含まれていないカラムに対してクエリを実行する
* カラムを事前集計し、計算量と I/O の両方を削減する

テーブルには 1 つ以上のプロジェクションを定義でき、クエリ分析時には、スキャン対象のデータ量が最も少ないプロジェクションが、ユーザーが指定したクエリを変更することなく ClickHouse によって選択されます。

:::note[ディスク使用量]
プロジェクションは内部的に新しい非表示テーブルを作成するため、より多くの I/O とディスク容量が必要になります。
たとえば、プロジェクションで異なる主キーを定義した場合、元のテーブルのすべてのデータが複製されます。
:::

プロジェクションが内部的にどのように動作するかについての、より技術的な詳細は、この[ページ](/ja/guides/best-practices/sparse-primary-indexes.md/#option-3-projections)で確認できます。

<div id="examples">
  ## プロジェクションを使用する
</div>

<div id="example-filtering-without-using-primary-keys">
  ### 主キーを使わないフィルタリングの例
</div>

テーブルを作成します。

```sql
CREATE TABLE visits_order
(
   `user_id` UInt64,
   `user_name` String,
   `pages_visited` Nullable(Float64),
   `user_agent` String
)
ENGINE = MergeTree()
PRIMARY KEY user_agent
```

`ALTER TABLE` を使用すると、既存のテーブルにプロジェクションを追加できます。

```sql
ALTER TABLE visits_order ADD PROJECTION user_name_projection (
    SELECT *
    ORDER BY user_name
)

ALTER TABLE visits_order MATERIALIZE PROJECTION user_name_projection
```

データの挿入:

```sql
INSERT INTO visits_order SELECT
    number,
    'test',
    1.5 * (number / 2),
    'Android'
FROM numbers(1, 100);
```

プロジェクションを使用すると、元のテーブルで `user_name` が `PRIMARY_KEY` として定義されていなくても、`user_name` による絞り込みを高速に行えます。
クエリ実行時には、データが `user_name` の順に並んでいるため、プロジェクションを使用したほうが処理されるデータ量が少なくなると ClickHouse が判断します。

```sql
SELECT
    *
FROM visits_order
WHERE user_name='test'
LIMIT 2
```

クエリでプロジェクションが使用されているかどうかを確認するには、`system.query_log` テーブルを確認します。`projections` フィールドには、使用されたプロジェクションの名前が入り、使用されていない場合は空になります。

```sql
SELECT query, projections FROM system.query_log WHERE query_id='<query_id>'
```

<div id="example-pre-aggregation-query">
  ### 事前集計クエリの例
</div>

プロジェクション `projection_visits_by_user` を持つテーブルを作成します:

```sql
CREATE TABLE visits
(
   `user_id` UInt64,
   `user_name` String,
   `pages_visited` Nullable(Float64),
   `user_agent` String,
   PROJECTION projection_visits_by_user
   (
       SELECT
           user_agent,
           sum(pages_visited)
       GROUP BY user_id, user_agent
   )
)
ENGINE = MergeTree()
ORDER BY user_agent
```

データを挿入します:

```sql
INSERT INTO visits SELECT
    number,
    'test',
    1.5 * (number / 2),
    'Android'
FROM numbers(1, 100);
```

```sql
INSERT INTO visits SELECT
    number,
    'test',
    1. * (number / 2),
   'IOS'
FROM numbers(100, 500);
```

`GROUP BY` を使用して、フィールド `user_agent` で最初のクエリを実行します。
事前集計の内容が一致しないため、このクエリでは定義したプロジェクションは使用されません。

```sql
SELECT
    user_agent,
    count(DISTINCT user_id)
FROM visits
GROUP BY user_agent
```

プロジェクションを利用するには、事前集計のフィールドと `GROUP BY` フィールドの一部またはすべてを選択するクエリを実行できます。

```sql
SELECT
    user_agent
FROM visits
WHERE user_id > 50 AND user_id < 150
GROUP BY user_agent
```

```sql
SELECT
    user_agent,
    sum(pages_visited)
FROM visits
GROUP BY user_agent
```

前述のとおり、プロジェクションが使用されたかどうかは、`system.query_log` テーブルを確認することで把握できます。
`projections` フィールドには、使用されたプロジェクションの名前が表示されます。
プロジェクションが使用されていない場合は、空になります。

```sql
SELECT query, projections FROM system.query_log WHERE query_id='<query_id>'
```

<div id="projection-indexes">
  ### プロジェクション索引の作成と利用
</div>

[プロジェクション索引](../../../engines/table-engines/mergetree-family/mergetree.md#projection-index)を作成するには:

```sql
CREATE TABLE events
(
    `event_time` DateTime,
    `event_id` UInt64,
    `user_id` UInt64,
    `huge_string` String,
    PROJECTION order_by_user_id INDEX user_id TYPE basic
)
ENGINE = MergeTree()
ORDER BY (event_id);
```

<details markdown="1">
  <summary>明示的な `_part_offset` フィールドを使用したプロジェクションの作成</summary>

  プロジェクション索引は、次の構文でも作成できます (非推奨) :

  ```sql
  CREATE TABLE events
  (
      `event_time` DateTime,
      `event_id` UInt64,
      `user_id` UInt64,
      `huge_string` String,
      PROJECTION order_by_user_id
      (
          SELECT
              _part_offset
          ORDER BY user_id
      )
  )
  ENGINE = MergeTree()
  ORDER BY (event_id);
  ```
</details>

いくつかのサンプルデータを挿入します:

```sql
INSERT INTO events SELECT * FROM generateRandom() LIMIT 100000;
```

`_part_offset` フィールドは、マージやミューテーションを経てもその値が保持されるため、セカンダリ索引に有用です。これをクエリで活用できます:

```sql
SELECT
    count()
FROM events
WHERE _part_starting_offset + _part_offset IN (
    SELECT _part_starting_offset + _part_offset
    FROM events
    WHERE user_id = 42
)
SETTINGS enable_shared_storage_snapshot_in_query = 1
```

<div id="example-projection-with-where">
  ### `WHERE` 句を使ったプロジェクションの例
</div>

プロジェクションには `WHERE` 句を含めて、一部の行だけを格納できます。これは、クエリが特定の条件で頻繁に絞り込まれる場合に便利です。プロジェクションは条件に一致する行だけをマテリアライズするため、ストレージ使用量を削減し、クエリパフォーマンスを向上できます。

テーブルを作成し、フィルタ付きのプロジェクションを追加します:

```sql
CREATE TABLE events
(
    `event_type` String,
    `time` DateTime,
    `message` String
)
ENGINE = MergeTree()
ORDER BY time;

ALTER TABLE events ADD PROJECTION proj_pageview (
    SELECT event_type, time, message
    WHERE event_type = 'pageview'
    ORDER BY time
);

ALTER TABLE events MATERIALIZE PROJECTION proj_pageview;
```

データの挿入:

```sql
INSERT INTO events VALUES
    ('pageview', '2024-01-01', 'homepage'),
    ('click', '2024-01-02', 'button'),
    ('pageview', '2024-01-03', 'about');
```

クエリの `WHERE` 句がプロジェクションの `WHERE` 句を**含意している**場合 (つまり、プロジェクションのフィルター内のすべての条件がクエリのフィルターにも含まれている場合) 、オプティマイザは、それが有利だと判断すれば、そのプロジェクションを自動的に使用できます：

```sql
-- This query implies the projection's WHERE, so the projection may be used:
SELECT time, message FROM events WHERE event_type = 'pageview';

-- A stricter query also implies the projection's WHERE:
SELECT time, message FROM events WHERE event_type = 'pageview' AND time > '2024-01-01';

-- This query does NOT imply the projection, so the base table is scanned:
SELECT time, message FROM events WHERE event_type = 'click';
```

含意チェックは保守的です — 式の正規形に対して連言項の完全一致で判定するため、有効な最適化の機会 (たとえば範囲の含意) を一部見逃す可能性はありますが、誤った結果を生成することはありません。

<div id="manipulating-projections">
  ## プロジェクションの操作
</div>

[プロジェクション](/ja/engines/table-engines/mergetree-family/mergetree.md/#projections) では、次の操作を行えます。

<div id="add-projection">
  ### ADD PROJECTION
</div>

テーブルのメタデータにプロジェクションの定義を追加するには、以下のステートメントを使用します。

```sql
-- Normal projection (supports WHERE)
ALTER TABLE [db.]name [ON CLUSTER cluster] ADD PROJECTION [IF NOT EXISTS] name ( SELECT <COLUMN LIST EXPR> [WHERE <expr>] [ORDER BY] ) [WITH SETTINGS ( setting_name1 = setting_value1, setting_name2 = setting_value2, ...)]

-- Aggregate projection (supports WHERE)
ALTER TABLE [db.]name [ON CLUSTER cluster] ADD PROJECTION [IF NOT EXISTS] name ( SELECT <COLUMN LIST EXPR> [WHERE <expr>] [GROUP BY] ) [WITH SETTINGS ( setting_name1 = setting_value1, setting_name2 = setting_value2, ...)]
```

:::note
プロジェクションで `WHERE` 句を定義すると、その条件に一致する行のみが実体化されます。オプティマイザは、クエリの `WHERE` がプロジェクションの `WHERE` を論理的に含意し、かつそのプロジェクションがクエリプラン上有利な場合に、そのプロジェクションを使用できます。これは、通常のプロジェクションと集約プロジェクションの両方に当てはまります。
:::

<div id="with-settings">
  #### `WITH SETTINGS` 節
</div>

`WITH SETTINGS` は **プロジェクションレベルの設定** を定義し、プロジェクションでデータをどのように格納するかをカスタマイズします (たとえば、`index_granularity` や `index_granularity_bytes`) 。
これらは **MergeTree テーブル設定** に直接対応しますが、**このプロジェクションにのみ** 適用されます。

例:

```sql
ALTER TABLE t
ADD PROJECTION p (
    SELECT x ORDER BY x
) WITH SETTINGS (
    index_granularity = 4096,
    index_granularity_bytes = 1048576
);
```

プロジェクション設定は、そのプロジェクションに適用される実効テーブル設定を上書きします。ただし、検証ルールに従い (たとえば、無効または互換性のない上書きは拒否されます) 。

<div id="drop-projection">
  ### DROP PROJECTION
</div>

以下のステートメントを使用して、テーブルのメタデータからプロジェクション定義を削除し、ディスク上のプロジェクションファイルを削除します。
これは[ミューテーション](/ja/sql-reference/statements/alter/index.md#mutations)として実装されています。

```sql
ALTER TABLE [db.]name [ON CLUSTER cluster] DROP PROJECTION [IF EXISTS] name
```

<div id="materialize-projection">
  ### MATERIALIZE PROJECTION
</div>

以下のステートメントを使用して、パーティション `partition_name` 内のプロジェクション `name` を再構築できます。
これは[mutation](/ja/sql-reference/statements/alter/index.md#mutations)として実装されています。

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] MATERIALIZE PROJECTION [IF EXISTS] name [IN PARTITION partition_name]
```

<div id="clear-projection">
  ### CLEAR PROJECTION
</div>

以下のステートメントを使用すると、定義を削除せずにディスク上のプロジェクションファイルを削除できます。
これは [mutation](/ja/sql-reference/statements/alter/index.md#mutations) として実装されています。

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] CLEAR PROJECTION [IF EXISTS] name [IN PARTITION partition_name]
```

`ADD`、`DROP`、`CLEAR` コマンドは、メタデータの変更やファイルの削除しか行わないという意味で軽量です。
また、これらはレプリケートされ、ClickHouse Keeper または ZooKeeper を介してプロジェクションのメタデータが同期されます。

:::note
プロジェクションの操作は、[`*MergeTree`](/ja/engines/table-engines/mergetree-family/mergetree.md) エンジンのテーブル ([レプリケーション対応](/ja/engines/table-engines/mergetree-family/replication.md) バリアントを含む) でのみサポートされています。
:::

<div id="control-projections-merges">
  ### projection のマージ動作を制御する
</div>

クエリを実行すると、ClickHouse は元のテーブルとその projection のいずれかから読み取るかを選択します。
元のテーブルとその projection のいずれから読み取るかの判断は、各テーブルパートごとに個別に行われます。
ClickHouse は一般に、できるだけ少ないデータを読み取ることを目指しており、たとえばパートの主キーをサンプリングするなど、読み取り元として最適なパートを特定するためのいくつかの工夫を行います。
場合によっては、ソーステーブルのパーツに対応する projection part が存在しないことがあります。
これはたとえば、SQL でテーブルに projection を作成する処理がデフォルトで「遅延」的であり、新しく挿入されたデータにのみ影響し、既存のパーツは変更しないために起こります。

projection の 1 つには事前計算済みの集約値がすでに含まれているため、ClickHouse はクエリ実行時に再度集約を行わずに済むよう、対応する projection part から読み取ろうとします。特定のパートに対応する projection part がない場合、クエリ実行は元のパートにフォールバックします。

では、元のテーブル内の行が、単純ではない data part のバックグラウンドマージによって単純ではない形で変化した場合はどうなるでしょうか。
たとえば、テーブルが `ReplacingMergeTree` table engine を使用して格納されているとします。
マージ中に同じ行が複数の入力パートで検出された場合、最新の行バージョン (最後に挿入されたパートのもの) だけが保持され、それ以前のすべてのバージョンは破棄されます。

同様に、テーブルが `AggregatingMergeTree` table engine を使用して格納されている場合、マージ操作によって入力パーツ内の同じ行が (主キーの値に基づいて) 1 つの行にまとめられ、部分集約状態が更新されることがあります。

ClickHouse v24.8 より前では、projection part は黙ってメインデータと同期しなくなるか、あるいは update や delete のような特定の操作をまったく実行できませんでした。これは、テーブルに projection がある場合、データベースが自動的に例外を送出していたためです。

v24.8 以降では、新しいテーブルレベル設定 [`deduplicate_merge_projection_mode`](/ja/operations/settings/merge-tree-settings#deduplicate_merge_projection_mode) により、前述の単純ではないバックグラウンドマージ操作が元のテーブルのパーツで発生した場合の動作を制御できます。

Delete mutations も、元のテーブルのパーツから行を削除する part merge operations の一例です。v24.7 以降では、論理削除 によってトリガーされる delete mutations に関する動作を制御する設定もあります: [`lightweight_mutation_projection_mode`](/ja/operations/settings/merge-tree-settings#deduplicate_merge_projection_mode)。

以下は、`deduplicate_merge_projection_mode` と `lightweight_mutation_projection_mode` の両方で設定可能な値です。

* `throw` (デフォルト): 例外が送出され、projection part が同期しなくなるのを防ぎます。
* `drop`: 影響を受けた projection table parts は削除されます。影響を受けた projection part については、クエリは元の table part にフォールバックします。
* `rebuild`: 影響を受けた projection part は、元の table part 内のデータとの整合性を保つよう再構築されます。

<div id="limitations">
  ## 制限事項
</div>

プロジェクションの`ORDER BY`句では、`ALIAS`カラムを使用できません。例えば:

```sql
CREATE TABLE t
(
    id UInt64,
    a UInt32,
    ab_sum UInt64 ALIAS a + 1,
--highlight-next-line
    PROJECTION p (SELECT a ORDER BY ab_sum)
)
ENGINE = MergeTree ORDER BY id;
-- Fails with UNKNOWN_IDENTIFIER
```

`ALIAS` カラムは物理的に保存されず、クエリ時にオンザフライで計算されるため、ソート式が評価される projection part の書き込みパスでは使用できません。

代わりに、`MATERIALIZED` カラムを使用するか、式を直接インラインで記述してください：

```sql
-- using MATERIALIZED column
CREATE TABLE t
(
    id UInt64,
    a UInt32,
    ab_sum UInt64 MATERIALIZED a + 1,
    PROJECTION p (SELECT a ORDER BY ab_sum)
)
ENGINE = MergeTree ORDER BY id;

-- using an inline expression
CREATE TABLE t
(
    id UInt64,
    a UInt32,
    PROJECTION p (SELECT a ORDER BY a + 1)
)
ENGINE = MergeTree ORDER BY id;
```

<div id="see-also">
  ## 関連項目
</div>

* [&quot;マージ中のプロジェクションの制御&quot; (ブログ記事) ](https://clickhouse.com/blog/clickhouse-release-24-08#control-of-projections-during-merges)
* [&quot;プロジェクション&quot; (ガイド) ](/ja/data-modeling/projections#using-projections-to-speed-up-UK-price-paid)
* [&quot;materialized view とプロジェクション&quot;](https://clickhouse.com/docs/managing-data/materialized-views-versus-projections)