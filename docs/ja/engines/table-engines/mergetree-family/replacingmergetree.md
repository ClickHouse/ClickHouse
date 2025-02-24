---
slug: /ja/engines/table-engines/mergetree-family/replacingmergetree
sidebar_position: 40
sidebar_label:  ReplacingMergeTree
---

# ReplacingMergeTree

このエンジンは、[MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md#table_engines-mergetree)と異なり、同じ[ソートキー](../../../engines/table-engines/mergetree-family/mergetree.md)の値を持つ重複エントリを削除します（`PRIMARY KEY`ではなく、テーブルの`ORDER BY`セクション）。

データの重複排除はマージの際にのみ発生します。マージは背景で不明な時点で発生するため、計画することはできません。一部のデータは未処理のまま残る可能性があります。`OPTIMIZE`クエリを使用して予定外のマージを実行することができますが、大量のデータを読み書きするため、これに依存しないでください。

したがって、`ReplacingMergeTree`は重複データを背景で削除し、スペースを節約するのに適していますが、重複が存在しないことを保証するものではありません。

:::note
ReplacingMergeTreeの詳細ガイドにはベストプラクティスやパフォーマンスの最適化方法も含まれており、[こちら](/docs/ja/guides/replacing-merge-tree)で利用可能です。
:::

## テーブルの作成 {#creating-a-table}

``` sql
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
行の一意性は`PRIMARY KEY`ではなく、テーブルの`ORDER BY`セクションによって決定されます。
:::

## ReplacingMergeTree パラメーター

### ver

`ver` — バージョン番号を持つカラム。タイプは `UInt*`、`Date`、`DateTime`または`DateTime64`です。オプションのパラメーターです。

マージ時に、`ReplacingMergeTree`は同じソートキーを持つ行の中から1つだけを残します：

- `ver`が設定されていない場合、選択されたものの最後のもの。選択は、マージに参加しているパーツの集合からなる行の集合です。最も最近に作成されたパーツ（最後の挿入）が選択の最後になります。したがって、重複処理後、最も最近の挿入から来る最後の行が各ユニークなソートキーに残ります。
- `ver`が指定されている場合は、最大のバージョン。もし複数の行で`ver`が同じであれば、「`ver`が指定されていない場合」のルールが適用されます、すなわち、最も最近に挿入された行が残ります。

例：

```sql
-- verなし - 最後に挿入された行が残る
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


-- ver使用 - 最大のverを持つ行が残る
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

### is_deleted

`is_deleted` — この行のデータが状態を表しているか削除されるべきかを決定するためにマージ中に使用されるカラムの名前：`1`は"削除"された行、`0`は"状態"の行。

カラムデータ型 — `UInt8`。

:::note
`is_deleted`は`ver`を使用している場合にのみ有効にできます。

行は`OPTIMIZE ... FINAL CLEANUP`でのみ削除されます。この`CLEANUP`特別キーワードは、デフォルトでは許可されていませんが、`allow_experimental_replacing_merge_with_cleanup` MergeTree設定を有効にすると使用できます。

データに対する操作が何であれ、バージョンは増加しなければなりません。同じバージョン番号を持つ二つの挿入された行がある場合、最後に挿入された行が残ります。

:::

例：
```sql
-- verとis_deletedで
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

-- is_deletedを持つ行を削除
OPTIMIZE TABLE myThirdReplacingMT FINAL CLEANUP;

INSERT INTO myThirdReplacingMT Values (1, 'first', '2020-01-01 00:00:00', 0);

select * from myThirdReplacingMT final;

┌─key─┬─someCol─┬───────────eventTime─┬─is_deleted─┐
│   1 │ first   │ 2020-01-01 00:00:00 │          0 │
└─────┴─────────┴─────────────────────┴────────────┘
```

## クエリの条項

`ReplacingMergeTree`テーブルを作成する際には、`MergeTree`テーブルを作成する場合と同じ[条項](../../../engines/table-engines/mergetree-family/mergetree.md)が必要です。

<details markdown="1">

<summary>非推奨のテーブル作成方法</summary>

:::note
新しいプロジェクトではこの方法を使用せず、可能であれば古いプロジェクトを上記の方法に切り替えてください。
:::

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] ReplacingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [ver])
```

`ver`を除くすべてのパラメーターは、`MergeTree`の場合と同じ意味を持ちます。

- `ver` - バージョンを持つカラム。オプションのパラメーター。説明については上記のテキストをご覧ください。

</details>

## クエリ時間の重複排除 & FINAL

マージ時に、ReplacingMergeTreeは`ORDER BY`カラムの値をユニークな識別子として使用して重複行を識別し、最高のバージョンのみを保持します。しかしながら、これは事後的に正しさを提供するのみであり、行が重複排除されることを保証するものではないため、これに頼るべきではありません。したがって、クエリは問題が発生する可能性があるため、適切な回答を得るためにはバックグラウンドでのマージとクエリ時間での重複排除と削除の排除を補完する必要があります。これは`FINAL`演算子を使用して可能です。以下の例を考えてみてください：

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
`FINAL`なしでクエリを実行すると、不正確な数が出力されます（正確な結果はマージ状況によって異なります）：

```sql
SELECT count()
FROM rmt_example

┌─count()─┐
│     200 │
└─────────┘

1 row in set. Elapsed: 0.002 sec.
```

`FINAL`を追加すると、正しい結果が得られます：

```sql
SELECT count()
FROM rmt_example
FINAL

┌─count()─┐
│     100 │
└─────────┘

1 row in set. Elapsed: 0.002 sec.
```

`FINAL`およびその最適化方法についてより詳細な情報は、[ReplacingMergeTreeの詳細ガイド](/docs/ja/guides/replacing-merge-tree)を参照することをお勧めします。
