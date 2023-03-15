---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 36
toc_title: "\u6298\u308A\u305F\u305F\u307F\u30DE\u30FC\u30B8\u30C4\u30EA\u30FC"
---

# 折りたたみマージツリー {#table_engine-collapsingmergetree}

エンジンはから継承します [メルゲツリー](mergetree.md) 加算の論理行の崩壊データ部品の統合アルゴリズムです。

`CollapsingMergeTree` 並べ替えキー内のすべてのフィールドの場合、行のペアを非同期に削除(折りたたみ)します (`ORDER BY`）は、特定のフィールドを除いて同等です `Sign` これは `1` と `-1` 値。 ペアのない行は保持されます。 詳細については、 [崩壊](#table_engine-collapsingmergetree-collapsing) 文書のセクション。

エンジンはかなり貯蔵の容積を減らし、効率をの高めるかもしれません `SELECT` 結果としてのクエリ。

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

クエリパラメータの説明については、 [クエリの説明](../../../sql-reference/statements/create.md).

**CollapsingMergeTreeパラメータ**

-   `sign` — Name of the column with the type of row: `1` は “state” 行, `-1` は “cancel” ロウ

    Column data type — `Int8`.

**クエリ句**

を作成するとき `CollapsingMergeTree` テーブル、同じ [クエリ句](mergetree.md#table_engine-mergetree-creating-a-table) を作成するときのように必要です。 `MergeTree` テーブル。

<details markdown="1">

<summary>推奨されていません法テーブルを作成する</summary>

!!! attention "注意"
    可能であれば、古いプロジェクトを上記の方法に切り替えてください。

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] CollapsingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, sign)
```

以下を除くすべてのパラメータ `sign` と同じ意味を持つ `MergeTree`.

-   `sign` — Name of the column with the type of row: `1` — “state” 行, `-1` — “cancel” ロウ

    Column Data Type — `Int8`.

</details>

## 崩壊 {#table_engine-collapsingmergetree-collapsing}

### データ {#data}

考える必要がある状況などが保存継続的に変化するデータのオブジェクトです。 オブジェクトの行を一つ持ち、変更時に更新するのは論理的ですが、DBMSではストレージ内のデータを書き換える必要があるため、更新操作はコストがかか が必要な場合にデータを書き込むには、迅速に更新できませんが、きの変化をオブジェクトの順にしております。

特定の列を使用する `Sign`. もし `Sign = 1` これは、行がオブジェクトの状態であることを意味します。 “state” ロウ もし `Sign = -1` これは、同じ属性を持つオブジェクトの状態の取り消しを意味します。 “cancel” ロウ

たとえば、ユーザーがあるサイトでチェックしたページ数と、そこにいた時間を計算したいとします。 ある時点で、ユーザーアクティビティの状態で次の行を書きます:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

ある時点で、後で我々は、ユーザーの活動の変更を登録し、次の二つの行でそれを書きます。

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

最初の行は、オブジェクト(ユーザー)の以前の状態を取り消します。 キャンセルされた状態の並べ替えキーフィールドをコピーします。 `Sign`.

次の行には現在の状態が含まれます。

ユーザーアクティビティの最後の状態だけが必要なので、行

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │         5 │      146 │   -1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

オブジェクトの無効な（古い）状態を折りたたんで削除することができます。 `CollapsingMergeTree` データパーツのマージ中にこれを行います。

なぜ私たちは、読み込まれた各変更のための2行が必要です [アルゴリズ](#table_engine-collapsingmergetree-collapsing-algorithm) 段落。

**このようなアプローチ**

1.  プログラムを書き込み、データ意のオブジェクトをキャンセルはできます。 “Cancel” 文字列はコピーのソートキーの分野において “state” 文字列とその逆 `Sign`. この増加の初期サイズでの保存が可能なデータを書き込む。
2.  列の長い成長配列は、書き込みの負荷によるエンジンの効率を低下させます。 より簡単なデータ、より高い効率。
3.  その `SELECT` 結果は、オブジェクト変更履歴の一貫性に強く依存します。 挿入のためのデータを準備するときは正確です。 たとえば、セッションの深さなど、負でない指標の負の値など、不整合なデータで予測不可能な結果を得ることができます。

### アルゴリズ {#table_engine-collapsingmergetree-collapsing-algorithm}

ClickHouseがデータパーツをマージすると、同じ並べ替えキーを持つ連続した行の各グループ (`ORDER BY`）は、以下の二つの行に縮小される。 `Sign = 1` (“state” 行）と別の `Sign = -1` (“cancel” 行）。 つまり、エントリは崩壊します。

各り、その結果得られたデータは部分ClickHouse省:

1.  最初の “cancel” そして最後の “state” 行の数が “state” と “cancel” 行は一致し、最後の行は “state” ロウ
2.  最後の “state” より多くがあれば、行 “state” 行より “cancel” 行。
3.  最初の “cancel” より多くがあれば、行 “cancel” 行より “state” 行。
4.  他のすべての場合には、行のいずれも。

また、少なくとも2以上がある場合 “state” 行より “cancel” 行、または少なくとも2以上 “cancel” 次の行 “state” マージは続行されますが、ClickHouseはこの状況を論理エラーとして扱い、サーバーログに記録します。 同じデータが複数回挿入された場合、このエラーが発生する可能性があります。

したがって、崩壊は統計の計算結果を変えてはならない。
変更は徐々に崩壊し、最終的にはほぼすべてのオブジェクトの最後の状態だけが残った。

その `Sign` マージアルゴリズムでは、同じ並べ替えキーを持つすべての行が同じ結果データ部分にあり、同じ物理サーバー上にあることを保証するものではないため、必 ClickHouseプロセス `SELECT` 複数のスレッドでクエリを実行し、結果の行の順序を予測することはできません。 完全に取得する必要がある場合は、集計が必要です “collapsed” データから `CollapsingMergeTree` テーブル。

折りたたみを終了するには、次のクエリを記述します `GROUP BY` 符号を説明する句および集計関数。 たとえば、数量を計算するには、次を使用します `sum(Sign)` 代わりに `count()`. 何かの合計を計算するには、次のようにします `sum(Sign * x)` 代わりに `sum(x)`、というように、また、追加 `HAVING sum(Sign) > 0`.

集計 `count`, `sum` と `avg` この方法で計算できます。 集計 `uniq` 算出できる場合にはオブジェクトは、少なくとも一つの状態はまだ崩れていない。 集計 `min` と `max` 計算できませんでした。 `CollapsingMergeTree` 折りたたまれた状態の値の履歴は保存されません。

集計なしでデータを抽出する必要がある場合(たとえば、特定の条件に一致する最新の値を持つ行が存在するかどうかをチェックする場合)には、次の `FINAL` モディファイア `FROM` 句。 このアプローチは、大幅に低効率です。

## 使用例 {#example-of-use}

データ例:

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

我々は二つを使用します `INSERT` 二つの異なるデータ部分を作成するクエリ。 一つのクエリでデータを挿入すると、ClickHouseは一つのデータ部分を作成し、マージを実行しません。

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

私たちは何を見て、どこで崩壊していますか？

二つと `INSERT` クエリーを作成し、2つのデータ部品です。 その `SELECT` クエリは2つのスレッドで実行され、行のランダムな順序が得られました。 データパーツのマージがまだないため、折りたたみは発生しませんでした。 ClickHouseは予測できない未知の瞬間にデータ部分をマージします。

したがって、集約が必要です:

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

集約を必要とせず、強制的に崩壊させたい場合は、以下を使用できます `FINAL` 修飾子のための `FROM` 句。

``` sql
SELECT * FROM UAct FINAL
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

データを選択するこの方法は非常に非効率的です。 大きなテーブルには使わないでください。

## 別のアプローチの例 {#example-of-another-approach}

データ例:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │        -5 │     -146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

アイデアは、マージが考慮にキーフィールドのみを取ることです。 そして、 “Cancel” 行我々は、符号列を使用せずに合計するときに行の以前のバージョンを等しく負の値を指定することができます。 この方法では、データ型を変更する必要があります `PageViews`,`Duration` uint8-\>Int16の負の値を格納する。

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

ましょう試験のアプローチ:

``` sql
insert into UAct values(4324182021466249494,  5,  146,  1);
insert into UAct values(4324182021466249494, -5, -146, -1);
insert into UAct values(4324182021466249494,  6,  185,  1);

select * from UAct final; // avoid using final in production (just for a test or small tables)
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
```text
┌──────────────UserID─┬─PageViews─┬─Duration─┐
│ 4324182021466249494 │         6 │      185 │
└─────────────────────┴───────────┴──────────┘
```

``` sqk
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

[元の記事](https://clickhouse.com/docs/en/operations/table_engines/collapsingmergetree/) <!--hide-->
