---
slug: /ja/engines/table-engines/mergetree-family/versionedcollapsingmergetree
sidebar_position: 80
sidebar_label: VersionedCollapsingMergeTree
---

# VersionedCollapsingMergeTree

このエンジンは以下を提供します:

- 常に変化するオブジェクト状態を迅速に記録することができます。
- 古いオブジェクト状態をバックグラウンドで削除します。これによりストレージ容量を大幅に削減します。

詳細については[Collapsing](#table_engines_versionedcollapsingmergetree)セクションを参照してください。

このエンジンは[MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md#table_engines-mergetree)から継承され、データパーツのマージアルゴリズムに行を折りたたむロジックを追加します。`VersionedCollapsingMergeTree`は[CollapsingMergeTree](../../../engines/table-engines/mergetree-family/collapsingmergetree.md)と同じ目的を果たしますが、異なる折りたたみアルゴリズムを使用して、複数のスレッドで任意の順序でデータを挿入することができます。特に、`Version`カラムを使用することで、誤った順序で挿入されたとしても行を適切に折りたたむことができます。これに対し、`CollapsingMergeTree`は、厳密に連続した挿入のみを許容します。

## テーブルの作成 {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = VersionedCollapsingMergeTree(sign, version)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

クエリパラメータの説明については、[クエリの説明](../../../sql-reference/statements/create/table.md)を参照してください。

### エンジンのパラメータ

``` sql
VersionedCollapsingMergeTree(sign, version)
```

#### sign

`sign` — 行のタイプを示すカラムの名前: `1` は「状態」行、 `-1` は「キャンセル」行を示します。

カラムのデータ型は`Int8`である必要があります。

#### version

`version` — オブジェクト状態のバージョンを示すカラムの名前。

カラムのデータ型は`UInt*`である必要があります。

### クエリ節

`VersionedCollapsingMergeTree`テーブルを作成する際には、`MergeTree`テーブルを作成する際と同じ[節](../../../engines/table-engines/mergetree-family/mergetree.md)が必要です。

<details markdown="1">

<summary>非推奨のテーブル作成方法</summary>

:::note
新しいプロジェクトではこの方法を使用しないでください。可能であれば、古いプロジェクトを上記の方法に切り替えてください。
:::

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] VersionedCollapsingMergeTree(date-column [, samp#table_engines_versionedcollapsingmergetreeling_expression], (primary, key), index_granularity, sign, version)
```

`sign`および`version`以外のすべてのパラメータは`MergeTree`と同じ意味を持ちます。

- `sign` — 行のタイプを示すカラムの名前: `1` は「状態」行、 `-1` は「キャンセル」行を示します。

    カラムのデータ型 — `Int8`。

- `version` — オブジェクト状態のバージョンを示すカラムの名前。

    カラムのデータ型は`UInt*`である必要があります。

</details>

## Collapsing {#table_engines_versionedcollapsingmergetree}

### データ {#data}

あるオブジェクトの継続的に変わるデータを保存する必要がある状況を考えてみましょう。オブジェクトにつき1行を持ち、変更があるたびに行を更新するのが理にかなっています。しかし、更新操作はDBMSにとってデータをストレージに書き換える必要があるため高価で遅いです。データを迅速に書き込む必要がある場合には更新は受け入れられませんが、以下のようにオブジェクトへの変更を順次書き込むことができます。

行を書き込む際には`Sign`カラムを使用します。`Sign = 1`であれば、行はオブジェクトの状態を示します（これを「状態」行と呼びます）。`Sign = -1`を指定すると、同じ属性を持つオブジェクトの状態がキャンセルされたことを示します（これを「キャンセル」行と呼びます）。また、各オブジェクトの状態を別々の番号で識別するために`Version`カラムを使用します。

たとえば、あるサイトでユーザーが訪れたページ数と滞在時間を計算したいとします。ある時点で、以下のユーザー活動状態の行を記録します。

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

しばらくして、ユーザー活動の変更を記録し、以下の2行を書き込みます。

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
│ 4324182021466249494 │         6 │      185 │    1 │       2 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

最初の行はオブジェクト（ユーザー）の以前の状態をキャンセルします。キャンセルされた状態のすべてのフィールドを`Sign`を除いてコピーする必要があります。

2行目には現在の状態が含まれています。

ユーザー活動の最後の状態だけを必要とするため、

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

という行は、オブジェクトの無効な（古い）状態を折りたたむことができ、`VersionedCollapsingMergeTree`はデータパーツのマージ中にこれを行います。

なぜ各変更に2行必要かについては、[アルゴリズム](#table_engines-versionedcollapsingmergetree-algorithm)を参照してください。

**使用上の注意**

1. データを書き込むプログラムは、オブジェクトの状態を覚えておき、それをキャンセルできるようにする必要があります。「キャンセル」文字列は、主キーのフィールドと「状態」文字列のバージョンおよび反対の`Sign`を含む必要があります。これにより初期のストレージサイズが増加しますが、データを迅速に書き込むことができます。
2. カラム内の長い配列の成長は、書き込み時の負荷のためにエンジンの効率性を低下させます。データがシンプルであるほど効率性は高まります。
3. `SELECT`の結果は、オブジェクト変更の履歴の一貫性に強く依存します。データを挿入する準備をする際に注意してください。不整合なデータでは、セッションの深さのような非負のメトリクスに負の値が発生するなど、予測不可能な結果を得ることがあります。

### アルゴリズム {#table_engines-versionedcollapsingmergetree-algorithm}

ClickHouseがデータパーツをマージするとき、同じ主キーとバージョンをもち、`Sign`が異なる行のペアを削除します。行の順序は関係ありません。

ClickHouseがデータを挿入するとき、行を主キーで順序付けます。もし`Version`カラムが主キーに含まれていない場合、ClickHouseはこのカラムを最後のフィールドとして主キーに暗黙的に追加し、順序付けに使用します。

## データの選択 {#selecting-data}

ClickHouseは、同じ主キーを持つすべての行が同じ結果データパーツまたは同じ物理サーバーに存在することを保証しません。これはデータを書き込む際と後続のデータパーツのマージの両方で当てはまります。さらに、ClickHouseは`SELECT`クエリを複数のスレッドで処理し、結果の行の順序を予測できません。このため、`VersionedCollapsingMergeTree`テーブルから完全に「折りたたまれた」データを取得する必要がある場合には集計が必要です。

折りたたみを完了するには、`GROUP BY`句と`Sign`を考慮した集計関数を使用したクエリを記述します。たとえば、数量を計算するには`count()`の代わりに`sum(Sign)`を使用します。何かの合計を計算するには、`sum(x)`の代わりに`sum(Sign * x)`を使用し、`HAVING sum(Sign) > 0`を追加します。

`count`、`sum`、`avg`といった集計はこの方法で計算できます。オブジェクトが少なくとも1つの非折りたたまれた状態を持っている場合、`uniq`も計算できます。`min`および`max`は計算できません。なぜなら`VersionedCollapsingMergeTree`は折りたたまれた状態の値の履歴を保存しないからです。

折りたたみを施すが集計を行わないデータを抽出したい場合（たとえば最新の値が特定の条件に一致する行が存在するかを確認するために）、`FROM`句に`FINAL`修飾子を使用できます。この方法は効率が悪く、大きなテーブルには使用すべきではありません。

## 使用例 {#example-of-use}

サンプルデータ:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
│ 4324182021466249494 │         6 │      185 │    1 │       2 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

テーブルの作成:

``` sql
CREATE TABLE UAct
(
    UserID UInt64,
    PageViews UInt8,
    Duration UInt8,
    Sign Int8,
    Version UInt8
)
ENGINE = VersionedCollapsingMergeTree(Sign, Version)
ORDER BY UserID
```

データの挿入:

``` sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, 1, 1)
```

``` sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, -1, 1),(4324182021466249494, 6, 185, 1, 2)
```

異なるデータパーツを作成するために2つの`INSERT`クエリを使用します。1つのクエリでデータを挿入すると、ClickHouseは1つのデータパーツを作成し、一度もマージを行いません。

データの取得:

``` sql
SELECT * FROM UAct
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │   -1 │       1 │
│ 4324182021466249494 │         6 │      185 │    1 │       2 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

ここで私たちが見ているものと折りたたまれた部分はどこにあるのでしょうか？
2つの`INSERT`クエリを使用して2つのデータパーツを作成しました。`SELECT`クエリは2つのスレッドで実行され、結果は行のランダムな順序になっています。
折りたたみが発生しなかったのは、データパーツがまだマージされていないためです。ClickHouseはデータパーツを予測できない時点でマージします。

このため、集計が必要です:

``` sql
SELECT
    UserID,
    sum(PageViews * Sign) AS PageViews,
    sum(Duration * Sign) AS Duration,
    Version
FROM UAct
GROUP BY UserID, Version
HAVING sum(Sign) > 0
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Version─┐
│ 4324182021466249494 │         6 │      185 │       2 │
└─────────────────────┴───────────┴──────────┴─────────┘
```

集計が不要で折りたたみを強制したい場合は、`FROM`句に`FINAL`修飾子を使用できます。

``` sql
SELECT * FROM UAct FINAL
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         6 │      185 │    1 │       2 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

これはデータを選択する際の非常に非効率的な方法です。大きなテーブルでは使用しないでください。
