---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 37
toc_title: "\u30D0\u30FC\u30B8\u30E7\u30CB\u30F3\u30B0\u30B3\u30E9\u30D7\u30B7\u30F3\
  \u30B0\u30DE\u30FC\u30B2\u30C3\u30C8\u30EA\u30FC"
---

# バージョニングコラプシングマーゲットリー {#versionedcollapsingmergetree}

このエンジン:

-   では迅速書き込みオブジェクトとは常に変化しています。
-   削除古いオブジェクト状態の背景になります。 これにより、保管量が大幅に削減されます。

セクションを参照 [崩壊](#table_engines_versionedcollapsingmergetree) 詳細については.

エンジンはから継承します [メルゲツリー](mergetree.md#table_engines-mergetree) 追加した論理崩壊行のアルゴリズムのための統合データ部品です。 `VersionedCollapsingMergeTree` と同じ目的を果たしています [折りたたみマージツリー](collapsingmergetree.md) が異なる崩壊のアルゴリズムを挿入し、データを任意の順番で複数のスレッド）。 特に、 `Version` 列は、間違った順序で挿入されても、行を適切に折りたたむのに役立ちます。 対照的に, `CollapsingMergeTree` 厳密に連続した挿入のみを許可します。

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

クエリパラメータの説明については、 [クエリの説明](../../../sql-reference/statements/create.md).

**エンジン変数**

``` sql
VersionedCollapsingMergeTree(sign, version)
```

-   `sign` — Name of the column with the type of row: `1` は “state” 行, `-1` は “cancel” ロウ

    列のデータ型は次のとおりです `Int8`.

-   `version` — Name of the column with the version of the object state.

    列のデータ型は次のとおりです `UInt*`.

**クエリ句**

を作成するとき `VersionedCollapsingMergeTree` テーブル、同じ [句](mergetree.md) を作成するときに必要です。 `MergeTree` テーブル。

<details markdown="1">

<summary>推奨されていません法テーブルを作成する</summary>

!!! attention "注意"
    用途では使用しないでください方法で新規プロジェクト. 可能であれば、古いプロジェクトを上記の方法に切り替えます。

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] VersionedCollapsingMergeTree(date-column [, samp#table_engines_versionedcollapsingmergetreeling_expression], (primary, key), index_granularity, sign, version)
```

以下を除くすべてのパラメータ `sign` と `version` と同じ意味を持つ `MergeTree`.

-   `sign` — Name of the column with the type of row: `1` は “state” 行, `-1` は “cancel” ロウ

    Column Data Type — `Int8`.

-   `version` — Name of the column with the version of the object state.

    列のデータ型は次のとおりです `UInt*`.

</details>

## 崩壊 {#table_engines_versionedcollapsingmergetree}

### データ {#data}

うな状況を考える必要がある場合の保存継続的に変化するデータのオブジェクトです。 オブジェクトに対して一つの行を持ち、変更があるたびにその行を更新するのが妥当です。 ただし、DBMSではストレージ内のデータを書き換える必要があるため、更新操作はコストがかかり、時間がかかります。 更新はできませんが必要な場合は書き込みデータはすでに書き込み、変更オブジェクトの順にしております。

使用する `Sign` 行を書き込むときの列。 もし `Sign = 1` これは、行がオブジェクトの状態であることを意味します（ “state” 行）。 もし `Sign = -1` これは、同じ属性を持つオブジェクトの状態の取り消しを示します（ “cancel” 行）。 また、 `Version` オブジェクトの各状態を個別の番号で識別する必要があります。

たとえば、ユーザーがいくつのサイトにアクセスしたのか、どのくらいの時間があったのかを計算します。 ある時点で、次の行をユーザーアクティビティの状態で記述します:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

後のある時点で、ユーザーアクティビティの変更を登録し、次の二つの行でそれを書きます。

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
│ 4324182021466249494 │         6 │      185 │    1 │       2 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

最初の行は、オブジェクト(ユーザー)の以前の状態を取り消します。 キャンセルされた状態のすべてのフィールドをコピーします。 `Sign`.

次の行には現在の状態が含まれます。

ユーザーアクティビティの最後の状態だけが必要なので、行

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

オブジェクトの無効な（古い）状態を折りたたんで、削除することができます。 `VersionedCollapsingMergeTree` このことを融合させたデータの部品です。

変更ごとに二つの行が必要な理由については、以下を参照してください [アルゴリズ](#table_engines-versionedcollapsingmergetree-algorithm).

**使用上の注意**

1.  プログラムを書き込み、データ意のオブジェクトを解除します。 その “cancel” のコピーでなければなりません。 “state” 反対の文字列 `Sign`. この増加の初期サイズでの保存が可能なデータを書き込む。
2.  列の長い成長配列は、書き込みの負荷のためにエンジンの効率を低下させる。 より簡単なデータ、より良い効率。
3.  `SELECT` 結果は、オブジェクト変更の履歴の一貫性に強く依存します。 挿入のためのデータを準備するときは正確です。 セッションの深さなど、負でない指標の負の値など、不整合なデータで予測不可能な結果を得ることができます。

### アルゴリズ {#table_engines-versionedcollapsingmergetree-algorithm}

ClickHouseは、データパーツをマージするときに、同じ主キーとバージョンと異なる行の各ペアを削除します `Sign`. 行の順序は重要ではありません。

ClickHouseはデータを挿入するとき、主キーによって行を並べ替えます。 もし `Version` 列が主キーにない場合、ClickHouseはそれを最後のフィールドとして暗黙的に主キーに追加し、順序付けに使用します。

## データの選択 {#selecting-data}

ClickHouseは、同じ主キーを持つすべての行が同じ結果のデータ部分または同じ物理サーバー上にあることを保証するものではありません。 これは、データの書き込みとその後のデータ部分のマージの両方に当てはまります。 さらに、ClickHouseプロセス `SELECT` 複数のスレッドを使用してクエリを実行し、結果の行の順序を予測することはできません。 これは、完全に取得する必要がある場合に集約が必要であることを意味します “collapsed” aからのデータ `VersionedCollapsingMergeTree` テーブル。

折りたたみを終了するには、 `GROUP BY` 符号を説明する句および集計関数。 たとえば、数量を計算するには、次を使用します `sum(Sign)` 代わりに `count()`. 何かの合計を計算するには、次のようにします `sum(Sign * x)` 代わりに `sum(x)` を追加します。 `HAVING sum(Sign) > 0`.

集計 `count`, `sum` と `avg` この方法で計算できます。 集計 `uniq` オブジェクトが少なくとも一つの非折りたたみ状態を持つ場合に計算できます。 集計 `min` と `max` 計算できないのは `VersionedCollapsingMergeTree` 折りたたまれた状態の値の履歴は保存されません。

データを抽出する必要がある場合は “collapsing” な集計（例えば、確認列が存在する最新の値に一致条件)を使用できます `FINAL` モディファイア `FROM` 句。 このアプローチは非効率で利用すべきではありませんの大きます。

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

我々は二つを使用します `INSERT` 二つの異なるデータ部分を作成するクエリ。 単一のクエリでデータを挿入すると、ClickHouseは一つのデータパーツを作成し、マージを実行することはありません。

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

私たちはここで何を見て、崩壊した部分はどこにありますか？
二つのデータパーツを作成しました `INSERT` クエリ。 その `SELECT` クエリは二つのスレッドで実行され、結果は行のランダムな順序です。
崩壊の発生はありませんので、データのパーツがなされております。 ClickHouseは、予測できない未知の時点でデータパーツをマージします。

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

集約を必要とせず、強制的に折りたたみたいなら、以下を使うことができます `FINAL` モディファイア `FROM` 句。

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
