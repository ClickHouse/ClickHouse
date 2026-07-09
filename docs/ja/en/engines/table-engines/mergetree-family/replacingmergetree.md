---
description: '同じソートキーの値（`PRIMARY KEY` ではなく、テーブル定義の `ORDER BY` セクション）を持つ重複エントリを削除する点で、MergeTree と異なります。'
sidebar_label: 'ReplacingMergeTree'
sidebar_position: 40
slug: /engines/table-engines/mergetree-family/replacingmergetree
title: 'ReplacingMergeTree テーブルエンジン'
doc_type: 'reference'
---

このエンジンは、同じ[ソートキー](../../../engines/table-engines/mergetree-family/mergetree.md)の値 (`PRIMARY KEY` ではなく、テーブル定義の `ORDER BY` セクション) を持つ重複エントリを削除する点で、[MergeTree](/ja/engines/table-engines/mergetree-family/mergetree) と異なります。

データの重複排除は、マージ中にのみ発生します。マージは不明なタイミングでバックグラウンドで実行されるため、それを見込んで計画することはできません。データの一部は未処理のまま残る可能性があります。`OPTIMIZE` クエリを使用して予定外のマージを実行することはできますが、`OPTIMIZE` クエリは大量のデータを読み書きするため、これに頼るべきではありません。

したがって、`ReplacingMergeTree` は容量を節約するためにバックグラウンドで重複データを除去する用途には適していますが、重複が存在しないことを保証するものではありません。

:::note
ベストプラクティスやパフォーマンス最適化の方法を含む ReplacingMergeTree の詳細なガイドは、[こちら](/ja/guides/replacing-merge-tree) を参照してください。
:::

<div id="creating-a-table">
  ## テーブルの作成
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = ReplacingMergeTree([ver [, is_deleted]])
[PARTITION BY expr]
[ORDER BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

リクエストパラメータの説明については、[ステートメントの説明](../../../sql-reference/statements/create/table.md)を参照してください。

:::note
行の一意性は、`PRIMARY KEY` ではなく、`ORDER BY` テーブルセクションによって決まります。
:::

<div id="replacingmergetree-parameters">
  ## ReplacingMergeTree のパラメータ
</div>

<div id="ver">
  ### `ver`
</div>

`ver` — バージョン番号を持つカラム。型は `UInt*`、`Date`、`DateTime`、`DateTime64` のいずれかです。省略可能なパラメータです。

マージ時には、`ReplacingMergeTree` は同じソートキーを持つすべての行の中から 1 行だけを残します。

* `ver` が設定されていない場合は、selection 内の最後の行です。selection とは、マージに参加する一連のパーツに含まれる行の集合です。もっとも最近作成されたパーツ (最後の insert) が、selection 内で最後になります。したがって、重複排除後は、一意なソートキーごとに、もっとも新しい insert で追加された最後の行が残ります。
* `ver` が指定されている場合は、最大のバージョンを持つ行です。複数の行で `ver` が同じ場合は、それらには &quot;`ver` が指定されていない場合&quot; のルールが適用されます。つまり、もっとも最近挿入された行が残ります。

例:

```sql
-- without ver - the last inserted 'wins'
CREATE TABLE myFirstReplacingMT
(
    `key` Int64,
    `someCol` String,
    `eventTime` DateTime
)
ENGINE = ReplacingMergeTree
ORDER BY key;

INSERT INTO myFirstReplacingMT Values (1, 'first', '2020-01-01 01:01:01');
INSERT INTO myFirstReplacingMT Values (1, 'second', '2020-01-01 00:00:00');

SELECT * FROM myFirstReplacingMT FINAL;

┌─key─┬─someCol─┬───────────eventTime─┐
│   1 │ second  │ 2020-01-01 00:00:00 │
└─────┴─────────┴─────────────────────┘


-- with ver - the row with the biggest ver 'wins'
CREATE TABLE mySecondReplacingMT
(
    `key` Int64,
    `someCol` String,
    `eventTime` DateTime
)
ENGINE = ReplacingMergeTree(eventTime)
ORDER BY key;

INSERT INTO mySecondReplacingMT Values (1, 'first', '2020-01-01 01:01:01');
INSERT INTO mySecondReplacingMT Values (1, 'second', '2020-01-01 00:00:00');

SELECT * FROM mySecondReplacingMT FINAL;

┌─key─┬─someCol─┬───────────eventTime─┐
│   1 │ first   │ 2020-01-01 01:01:01 │
└─────┴─────────┴─────────────────────┘
```

<div id="is_deleted">
  ### `is_deleted`
</div>

`is_deleted` — マージ中に、この行のデータが state を表すのか、それとも削除対象なのかを判断するために使われるカラム名です。`1` は &quot;deleted&quot; 行、`0` は &quot;state&quot; 行を表します。

カラムのデータ型 — `UInt8`。

:::note
`is_deleted` は、`ver` が使用されている場合にのみ有効にできます。

データに対してどのような操作を行う場合でも、バージョンは増やす必要があります。挿入された 2 つの行のバージョン番号が同じ場合は、最後に挿入された行が保持されます。

デフォルトでは、ClickHouse は、その行が削除行であっても、あるキーに対する最後の行を保持します。これは、今後それより低いバージョンの行を
安全に挿入でき、削除行も引き続き適用されるようにするためです。

このような削除行を完全に削除するには、テーブル設定 `allow_experimental_replacing_merge_with_cleanup` を有効にして、次のいずれかを実行します。

1. テーブル設定 `enable_replacing_merge_with_cleanup_for_min_age_to_force_merge`、`min_age_to_force_merge_on_partition_only`、`min_age_to_force_merge_seconds` を設定します。パーティション内のすべてのパーツが `min_age_to_force_merge_seconds` より古い場合、ClickHouse はそれらを
   すべて 1 つのパーツにマージし、削除行を取り除きます。

2. `OPTIMIZE TABLE table [PARTITION partition | PARTITION ID 'partition_id'] FINAL CLEANUP` を手動で実行します。
   :::

例:

```sql
-- with ver and is_deleted
CREATE OR REPLACE TABLE myThirdReplacingMT
(
    `key` Int64,
    `someCol` String,
    `eventTime` DateTime,
    `is_deleted` UInt8
)
ENGINE = ReplacingMergeTree(eventTime, is_deleted)
ORDER BY key
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;

INSERT INTO myThirdReplacingMT Values (1, 'first', '2020-01-01 01:01:01', 0);
INSERT INTO myThirdReplacingMT Values (1, 'first', '2020-01-01 01:01:01', 1);

select * from myThirdReplacingMT final;

0 rows in set. Elapsed: 0.003 sec.

-- delete rows with is_deleted
OPTIMIZE TABLE myThirdReplacingMT FINAL CLEANUP;

INSERT INTO myThirdReplacingMT Values (1, 'first', '2020-01-01 00:00:00', 0);

select * from myThirdReplacingMT final;

┌─key─┬─someCol─┬───────────eventTime─┬─is_deleted─┐
│   1 │ first   │ 2020-01-01 00:00:00 │          0 │
└─────┴─────────┴─────────────────────┴────────────┘
```

<div id="query-clauses">
  ## クエリ句
</div>

`ReplacingMergeTree` テーブルの作成時には、`MergeTree` テーブルの作成時と同じ[句](../../../engines/table-engines/mergetree-family/mergetree.md)が必要です。

<details markdown="1">
  <summary>テーブル作成の非推奨の方法</summary>

  :::note
  新しいプロジェクトではこの方法を使用しないでください。可能であれば、既存のプロジェクトは上記で説明した方法に切り替えてください。
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] ReplacingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [ver])
  ```

  `ver` を除くすべてのパラメータは、`MergeTree` の場合と同じ意味です。

  * `ver` - バージョンを格納するカラム。オプションのパラメータです。詳細については、上記の説明を参照してください。
</details>

<div id="query-time-de-duplication--final">
  ## クエリ時の重複排除 &amp; FINAL
</div>

マージ時には、ReplacingMergeTree は `ORDER BY` カラム (テーブルの作成時に使用) の値を一意の識別子として重複行を特定し、最も高いバージョンだけを保持します。ただし、これで得られるのはあくまで最終的な整合性にすぎません。行が重複排除されることは保証されないため、これに依存すべきではありません。その結果、更新行や削除行もクエリ対象に含まれるため、クエリが不正確な結果を返すことがあります。

正しい結果を得るには、バックグラウンドマージに加えて、クエリ時の重複排除と削除済み行の除去を行う必要があります。これは `FINAL` 演算子を使用することで実現できます。たとえば、次の例を考えてみましょう。

```sql
CREATE TABLE rmt_example
(
    `number` UInt16
)
ENGINE = ReplacingMergeTree
ORDER BY number

INSERT INTO rmt_example SELECT floor(randUniform(0, 100)) AS number
FROM numbers(1000000000)

0 rows in set. Elapsed: 19.958 sec. Processed 1.00 billion rows, 8.00 GB (50.11 million rows/s., 400.84 MB/s.)
```

`FINAL` を付けずにクエリを実行すると、カウントが不正確になります (正確な結果はマージの状況によって異なります) ：

```sql
SELECT count()
FROM rmt_example

┌─count()─┐
│     200 │
└─────────┘

1 row in set. Elapsed: 0.002 sec.
```

FINAL を追加すると、正しい結果が得られます：

```sql
SELECT count()
FROM rmt_example
FINAL

┌─count()─┐
│     100 │
└─────────┘

1 row in set. Elapsed: 0.002 sec.
```

`FINAL` の詳細や、`FINAL` のパフォーマンスを最適化する方法については、[ReplacingMergeTree の詳細ガイド](/ja/guides/replacing-merge-tree)を参照することをお勧めします。