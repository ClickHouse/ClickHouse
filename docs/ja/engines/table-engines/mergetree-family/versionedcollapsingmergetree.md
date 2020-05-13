---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 37
toc_title: VersionedCollapsingMergeTree
---

# Versionedcollapsingmergetree {#versionedcollapsingmergetree}

このエンジン:

-   では迅速書き込みオブジェクトとは常に変化しています。
-   バックグラウン これを大幅に削減量に保管します。

セクションを見る [折りたたみ](#table_engines_versionedcollapsingmergetree) 詳細については。

エンジンは [MergeTree](mergetree.md#table_engines-mergetree) 追加した論理崩壊行のアルゴリズムのための統合データ部品です。 `VersionedCollapsingMergeTree` と同じ目的を果たす [CollapsingMergeTree](collapsingmergetree.md) が異なる崩壊のアルゴリズムを挿入し、データを任意の順番で複数のスレッド）。 特に、 `Version` 列は、間違った順序で挿入されていても、行を適切に折りたたむのに役立ちます。 対照的に, `CollapsingMergeTree` 厳密に連続した挿入のみを許可します。

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

説明のクエリパラメータの [クエリの説明](../../../sql-reference/statements/create.md).

**エンジン変数**

``` sql
VersionedCollapsingMergeTree(sign, version)
```

-   `sign` — Name of the column with the type of row: `1` は “state” 行, `-1` は “cancel” 行

    列データ型は次のようになります `Int8`.

-   `version` — Name of the column with the version of the object state.

    列データ型は次のようになります `UInt*`.

**クエリ句**

作成するとき `VersionedCollapsingMergeTree` テーブル、同じ [句](mergetree.md) を作成するときに必要です。 `MergeTree` テーブル。

<details markdown="1">

<summary>テーブルを作成する非推奨の方法</summary>

!!! attention "注意"
    用途では使用しないでください方法で新規プロジェクト. 可能であれば、古いプロジェクトを上記の方法に切り替えます。

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] VersionedCollapsingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, sign, version)
```

以下を除くすべてのパラメータ `sign` と `version` と同じ意味を持つ `MergeTree`.

-   `sign` — Name of the column with the type of row: `1` は “state” 行, `-1` は “cancel” 行

    Column Data Type — `Int8`.

-   `version` — Name of the column with the version of the object state.

    列データ型は次のようになります `UInt*`.

</details>

## 折りたたみ {#table_engines_versionedcollapsingmergetree}

### データ {#data}

あるオブジェクトのデータを継続的に変更する必要がある状況を考えてみましょう。 オブジェクトに対して一つの行を持ち、変更があるときはいつでも行を更新するのが妥当です。 ただし、dbmsでは、ストレージ内のデータの書き換えが必要なため、更新操作は高価で低速です。 データをすばやく書き込む必要がある場合は更新できませんが、オブジェクトに変更を順番に書き込むことができます。

を使用 `Sign` 行を書き込むときの列。 もし `Sign = 1` これは、行がオブジェクトの状態であることを意味します（それを呼び出しましょう “state” 行）。 もし `Sign = -1` これは、同じ属性を持つオブジェクトの状態の取り消しを示します（ “cancel” 行）。 また、 `Version` 別の番号を持つオブジェクトの各状態を識別する必要がある列。

たとえば、ユーザーがいくつかのサイトで訪問したページの数と、そこにいた期間を計算したいとします。 ある時点で、ユーザーアクティビティの状態で次の行を書きます:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

ある時点で、ユーザーアクティビティの変更を登録し、次の二つの行を書き込みます。

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
│ 4324182021466249494 │         6 │      185 │    1 │       2 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

最初の行は、オブジェクト（user）の前の状態を取り消します。 でコピーのすべての分野を中止状態を除く `Sign`.

次の行には、現在の状態が含まれています。

ユーザーアクティビティの最後の状態だけが必要なので、行

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

オブジェクトの無効な（古い）状態を削除して、削除することができます。 `VersionedCollapsingMergeTree` これは、データ部分をマージ中に行います。

そんなしが必要であるが、行の変更を参照 [Algorithm](#table_engines-versionedcollapsingmergetree-algorithm).

**使用上の注意**

1.  プログラムを書き込み、データ意のオブジェクトを解除します。 その “cancel” 文字列は、 “state” 反対の文字列 `Sign`. この増加の初期サイズでの保存が可能なデータを書き込む。
2.  列の長い配列は、書き込みの負荷のためにエンジンの効率を低下させます。 データがより簡単になればなるほど、効率は向上します。
3.  `SELECT` 結果は、オブジェクト変更の履歴の一貫性に強く依存します。 挿入するデータを準備するときは正確です。 セッションの深さなどの非負の指標の負の値など、一貫性のないデータで予測不可能な結果を得ることができます。

### Algorithm {#table_engines-versionedcollapsingmergetree-algorithm}

ClickHouseは、データパーツをマージするときに、同じ主キーとバージョンが異なる行の各ペアを削除します `Sign`. 行の順序は関係ありません。

ClickHouseがデータを挿入すると、主キーで行を並べ替えます。 この `Version` 列は主キーにはなく、ClickHouseはそれを主キーに暗黙的に最後のフィールドとして追加し、それを順序付けに使用します。

## データの選択 {#selecting-data}

ClickHouseは、同じ主キーを持つすべての行が同じ結果のデータ部分にあるか、同じ物理サーバー上にあることを保証するものではありません。 これは、データの書き込みとそれに続くデータ部分のマージの両方に当てはまります。 さらに、ClickHouseプロセス `SELECT` 複数のスレッドを持つクエリは、結果の行の順序を予測することはできません。 これは、完全に取得する必要がある場合に集約が必要であることを意味します “collapsed” からのデータ `VersionedCollapsingMergeTree` テーブル。

折りたたみを完了するには、次のようにクエリを記述します `GROUP BY` この符号を考慮する句および集計関数。 たとえば、数量を計算するには、以下を使用します `sum(Sign)` 代わりに `count()`. 何かの合計を計算するには、次のようにします `sum(Sign * x)` 代わりに `sum(x)`、と追加 `HAVING sum(Sign) > 0`.

凝集体 `count`, `sum` と `avg` この方法で計算できます。 合計 `uniq` オブジェクトに折りたたまれていない状態がある場合に計算できます。 凝集体 `min` と `max` 計算できないのは `VersionedCollapsingMergeTree` 折りたたまれた状態の値の履歴は保存されません。

データを抽出する必要がある場合 “collapsing” な集計（例えば、確認列が存在する最新の値に一致条件)を使用できます `FINAL` のための修飾語 `FROM` 句。 このアプローチは非効率で利用すべきではありませんの大きます。

## 使用例 {#example-of-use}

データ例:

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

私たちは二つを使う `INSERT` 二つの異なるデータ部分を作成するクエリ。 単一のクエリでデータを挿入すると、ClickHouseは単一のデータ部分を作成し、マージは実行しません。

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

ここでは何が見え、折りたたまれた部分はどこにありますか？
二つのデータパーツを二つ作成しました `INSERT` クエリ。 その `SELECT` クエリは二つのスレッドで実行され、結果は行のランダムな順序です。
デー clickhouseは、予測できない未知の時点でデータパーツをマージします。

これが集約が必要な理由です:

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

集約を必要とせず、強制的に崩壊させたい場合は、 `FINAL` のための修飾語 `FROM` 句。

``` sql
SELECT * FROM UAct FINAL
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         6 │      185 │    1 │       2 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

これは、データを選択する非常に非効率的な方法です。 大きなテーブルには使用しないでください。

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/versionedcollapsingmergetree/) <!--hide-->
