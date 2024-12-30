---
slug: /ja/engines/table-engines/mergetree-family/collapsingmergetree
sidebar_position: 70
sidebar_label: CollapsingMergeTree
---

# CollapsingMergeTree

このエンジンは [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md)から継承しており、行の折り畳みロジックをデータパーツのマージアルゴリズムに追加しています。

`CollapsingMergeTree` は、ソートキー (`ORDER BY`)内の特定のフィールド`Sign`がすべてのフィールドと等価である場合に、1 と -1 の値を持っている場合に、行のペアを非同期的に削除（折り畳み）します。ペアがない行は保持されます。詳細は、本ドキュメントの [Collapsing](#table_engine-collapsingmergetree-collapsing) セクションを参照してください。

このエンジンにより、ストレージのボリュームが大幅に削減され、`SELECT`クエリの効率が向上します。

## テーブルの作成 {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = CollapsingMergeTree(sign)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

クエリパラメータの説明については、[クエリ説明](../../../sql-reference/statements/create/table.md)を参照してください。

## CollapsingMergeTree パラメータ

### sign

`sign` — 行のタイプを示すカラムの名前: `1` は「状態」の行で、`-1` は「キャンセル」行です。

    カラムのデータ型 — `Int8`.

## クエリ句

`CollapsingMergeTree`テーブルを作成する場合、`MergeTree`テーブルを作成するときと同じ[クエリ句](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)が必要です。

<details markdown="1">

<summary>推奨されない方法でのテーブル作成</summary>

:::note
新しいプロジェクトではこの方法を使用せず、可能であれば上記で説明した方法に古いプロジェクトを切り替えてください。
:::

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] CollapsingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, sign)
```

`sign`を除くすべてのパラメータは`MergeTree`と同じ意味を持ちます。

- `sign` — 行のタイプを示すカラム名: `1` — 「状態」の行, `-1` — 「キャンセル」行。

    カラムデータ型 — `Int8`.

</details>

## Collapsing {#table_engine-collapsingmergetree-collapsing}

### データ {#data}

特定のオブジェクトの継続的に変化するデータを保存する必要がある状況を考えてみてください。オブジェクトの1行を持ち、変更時にそれを更新するのが論理的に聞こえるかもしれませんが、更新操作はストレージ内のデータの書き換えを必要とするため、DBMSにとって時間と費用のかかる操作です。データを迅速に書き込む必要がある場合、更新は容認できませんが、次のようにオブジェクトの変更を順次書き込むことができます。

特定のカラム `Sign` を使用します。`Sign = 1` の場合、行はオブジェクトの状態を示し、「状態」行と呼びます。`Sign = -1` の場合、同じ属性を持つオブジェクトの状態のキャンセルを示し、「キャンセル」行と呼びます。

例えば、あるサイトでユーザーがチェックしたページとそこでの滞在時間を計算したいとします。ある時点でユーザーのアクティビティ状態を次の行で記録します。

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

後のある時点で、ユーザーのアクティビティの変化を登録し、次の2行で記録します。

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

最初の行はオブジェクト（ユーザー）の以前の状態をキャンセルします。キャンセルされた状態のソートキーのフィールドを Sign を除いてコピーする必要があります。

2番目の行には現在の状態が含まれています。

私たちはユーザーの活動の最後の状態のみを必要としているので、次の行

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │         5 │      146 │   -1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

はオブジェクトの無効な（古い）状態を折り畳んで削除できます。`CollapsingMergeTree` はデータパーツのマージ中にこれを行います。

なぜすべての変更に対して2行が必要なのか、[アルゴリズム](#table_engine-collapsingmergetree-collapsing-algorithm)の段落で説明します。

**このアプローチの特異な特性**

1. データを書くプログラムは、オブジェクトをキャンセルできるようにその状態を記憶しておく必要があります。「キャンセル」文字列は「状態」文字列のソートキーのフィールドのコピーと反対の`Sign` を含むべきです。初期のストレージサイズを増大させますが、データをすばやく書き込むことができます。
2. カラム内の長い増加する配列は、書き込み負荷のため、エンジンの効率を低下させます。より単純なデータの方が効率は高くなります。
3. `SELECT`の結果はオブジェクト変更履歴の一貫性に大きく依存します。挿入用データを準備する際には正確である必要があります。一貫性のないデータでは、例えばセッション深度のような非負のメトリックに対して負の値を得るといった予測不可能な結果が生じる可能性があります。

### アルゴリズム {#table_engine-collapsingmergetree-collapsing-algorithm}

ClickHouseがデータパーツをマージする際、同じソートキー (`ORDER BY`)を持つ連続する行の各グループは、`Sign = 1` の行（「状態」行）と `Sign = -1` の行（「キャンセル」行）を持つ2行以下に減少します。つまり、エントリは崩壊します。

結果となるデータパーツの各々について、ClickHouse は次のことを行います：

1. 「状態」行と「キャンセル」行の数が一致し、最後の行が「状態」行の場合、最初の「キャンセル」行と最後の「状態」行を保持します。
2. 「状態」行が「キャンセル」行より多い場合、最後の「状態」行を保持します。
3. 「キャンセル」行が「状態」行より多い場合、最初の「キャンセル」行を保持します。
4. 他のすべての場合、どの行も保持しません。

また、「状態」行が「キャンセル」行より少なくとも2つ以上多い場合、または「キャンセル」行が「状態」行より少なくとも2つ以上多い場合、マージは続行されますが、ClickHouse はこの状況を論理的エラーとして扱い、サーバーログに記録します。このエラーは、同じデータが複数回挿入された場合に発生することがあります。

したがって、崩壊は統計計算の結果を変更すべきではありません。
変更は徐々に折り畳まれ、最終的にはほぼすべてのオブジェクトの最後の状態のみが残ります。

`Sign` は必須です。なぜなら、マージアルゴリズムは同じソートキーを持つすべての行が同じ結果のデータパーツや同じ物理サーバー上にあることを保証しないからです。ClickHouse は複数のスレッドで`SELECT`クエリを処理し、結果中の行の順序を予測することはできません。`CollapsingMergeTree` テーブルから完全に「折り畳まれた」データを取得する必要がある場合、集計が必要です。

最終的な崩壊を行うには、`GROUP BY` 句と符号を考慮に入れた集計関数を含むクエリを書きます。たとえば、数量を計算する場合は`count()`の代わりに`sum(Sign)`を使用します。何かの合計を計算する場合は`sum(x)`の代わりに`sum(Sign * x)`を使用するなどし、さらに`HAVING sum(Sign) > 0`を追加します。

この方法で`count`、`sum`、および`avg`を計算することができます。オブジェクトが折り畳まれない状態を少なくとも1つ持っている場合、`uniq`を計算できます。`CollapsingMergeTree`が折り畳まれた状態の値の履歴を保存しないため、`min`および`max`を計算することはできません。

集計なしでデータを抽出する必要がある場合（たとえば、最新の値が特定の条件と一致する行が存在するかどうかを確認する場合）、`FROM`句に`FINAL`修飾子を使用できます。このアプローチは非常に効率が悪いです。

## 使用例 {#example-of-use}

サンプルデータ:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

テーブルの作成:

``` sql
CREATE TABLE UAct
(
    UserID UInt64,
    PageViews UInt8,
    Duration UInt8,
    Sign Int8
)
ENGINE = CollapsingMergeTree(Sign)
ORDER BY UserID
```

データの挿入:

``` sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, 1)
```

``` sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, -1),(4324182021466249494, 6, 185, 1)
```

我々は2つの`INSERT`クエリを使用して2つの異なるデータパーツを作成しました。1つのクエリでデータを挿入する場合は、ClickHouseは1つのデータパーツを作成し、マージは発生しません。

データの取得:

``` sql
SELECT * FROM UAct
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

何を見て、どこで折り畳みが行われるのか？

2つの`INSERT`クエリを通じて、2つのデータパーツを作成しました。`SELECT`クエリは2つのスレッドで実行され、我々は行のランダムな順序を得ました。折り畳みは発生しませんでした。なぜなら、まだデータパーツのマージが行われていないからです。ClickHouseは予測できない瞬間にデータパーツをマージします。

したがって、集計が必要です：

``` sql
SELECT
    UserID,
    sum(PageViews * Sign) AS PageViews,
    sum(Duration * Sign) AS Duration
FROM UAct
GROUP BY UserID
HAVING sum(Sign) > 0
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┐
│ 4324182021466249494 │         6 │      185 │
└─────────────────────┴───────────┴──────────┘
```

集計が必要なくて、強制的に折り畳みを行いたい場合は、`FROM`句に`FINAL`修飾子を使用できます。

``` sql
SELECT * FROM UAct FINAL
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

このデータの選択方法は非常に非効率的です。大きなテーブルに対しては使用しないでください。

## 別のアプローチの使用例 {#example-of-another-approach}

サンプルデータ:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │        -5 │     -146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

マージはキーフィールドのみを考慮するというアイデアです。「キャンセル」行では、`Sign` カラムを使用せずに合計するときに行の前バージョンを相殺する負の値を指定できます。このアプローチでは、負の値を格納できるように`PageViews`,`Duration`のデータ型をUInt8からInt16に変更する必要があります。

``` sql
CREATE TABLE UAct
(
    UserID UInt64,
    PageViews Int16,
    Duration Int16,
    Sign Int8
)
ENGINE = CollapsingMergeTree(Sign)
ORDER BY UserID
```

このアプローチをテストしましょう：

``` sql
insert into UAct values(4324182021466249494,  5,  146,  1);
insert into UAct values(4324182021466249494, -5, -146, -1);
insert into UAct values(4324182021466249494,  6,  185,  1);

select * from UAct final; // 本番環境ではfinalの使用を避けてください（テストや小さなテーブルの場合のみ）
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

``` sql
SELECT
    UserID,
    sum(PageViews) AS PageViews,
    sum(Duration) AS Duration
FROM UAct
GROUP BY UserID
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┐
│ 4324182021466249494 │         6 │      185 │
└─────────────────────┴───────────┴──────────┘
```

``` sql
select count() FROM UAct
```

``` text
┌─count()─┐
│       3 │
└─────────┘
```

``` sql
optimize table UAct final;

select * FROM UAct
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```
