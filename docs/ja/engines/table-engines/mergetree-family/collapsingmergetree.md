---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 36
toc_title: CollapsingMergeTree
---

# Collapsingmergetree {#table_engine-collapsingmergetree}

エンジンは [MergeTree](mergetree.md) 加算の論理行の崩壊データ部品の統合アルゴリズムです。

`CollapsingMergeTree` 並べ替えキー内のすべてのフィールドの場合、行のペアを非同期的に削除(折りたたみ)します (`ORDER BY`)は、特定のフィールドを除いて同等です `Sign` これは `1` と `-1` 値。 ペアのない行は保持されます。 詳細については、 [折りたたみ](#table_engine-collapsingmergetree-collapsing) 文書のセクション。

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

説明のクエリパラメータは、 [クエリの説明](../../../sql-reference/statements/create.md).

**CollapsingMergeTreeパラメータ**

-   `sign` — Name of the column with the type of row: `1` は “state” 行, `-1` は “cancel” 行

    Column data type — `Int8`.

**クエリ句**

作成するとき `CollapsingMergeTree` テーブル、同じ [クエリ句](mergetree.md#table_engine-mergetree-creating-a-table) 作成するときと同じように、必須です。 `MergeTree` テーブル。

<details markdown="1">

<summary>テーブルを作成する非推奨の方法</summary>

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

すべてのパラメーターを除く `sign` と同じ意味を持つ `MergeTree`.

-   `sign` — Name of the column with the type of row: `1` — “state” 行, `-1` — “cancel” 行

    Column Data Type — `Int8`.

</details>

## 折りたたみ {#table_engine-collapsingmergetree-collapsing}

### データ {#data}

あるオブジェクトのデータを継続的に変更する必要がある状況を考えてみましょう。 これは、オブジェクトのための一つの行を持っており、任意の変更でそれを更新する論理的に聞こえるが、更新操作は、ストレージ内のデータの書き換え が必要な場合にデータを書き込むには、迅速に更新できませんが、きの変化をオブジェクトの順にしております。

特定の列を使用する `Sign`. もし `Sign = 1` これは、行がオブジェクトの状態であることを意味します。 “state” 行 もし `Sign = -1` これは、同じ属性を持つオブジェクトの状態の取り消しを意味し、それを呼び出しましょう “cancel” 行

例えば、しい計算のページのユーザーかサイトは、長くご愛用いただけると思いがあります。 ある時点で、ユーザーアクティビティの状態で次の行を書きます:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

しばらくして、ユーザーアクティビティの変更を登録し、次の二つの行を書き込みます。

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

最初の行は、オブジェクト（user）の前の状態を取り消します。 取り消された状態の並べ替えキーフィールドをコピーします `Sign`.

次の行には、現在の状態が含まれています。

ユーザーアクティビティの最後の状態だけが必要なので、行

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │         5 │      146 │   -1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

オブジェクトの無効な（古い）状態を崩壊させることを削除することができます。 `CollapsingMergeTree` これは、データ部分のマージ中に行います。

なぜ私たちは必要2行の各変更のために読む [Algorithm](#table_engine-collapsingmergetree-collapsing-algorithm) 段落。

**そのようなアプローチ**

1.  プログラムを書き込み、データ意のオブジェクトをキャンセルはできます。 “Cancel” 文字列はコピーのソートキーの分野において “state” 文字列とその逆 `Sign`. この増加の初期サイズでの保存が可能なデータを書き込む。
2.  列の長い配列は、書き込みの負荷によるエンジンの効率を低下させます。 より簡単なデータ、より高い効率。
3.  その `SELECT` 結果は、オブジェクトの変更履歴の整合性に強く依存します。 挿入するデータを準備するときは正確です。 たとえば、セッションの深さなどの負でない指標の負の値などです。

### Algorithm {#table_engine-collapsingmergetree-collapsing-algorithm}

ClickHouseがデータパーツをマージすると、同じソートキーを持つ連続した行の各グループ (`ORDER BY` これは、以下の二つの行に縮小されています。 `Sign = 1` (“state” 行）と別の `Sign = -1` (“cancel” 行）。 言い換えれば、エントリは崩壊する。

それぞれの結果のデータ部分clickhouse保存:

1.  最初の “cancel” そして最後の “state” 行の数が “state” と “cancel” 行は一致し、最後の行はaです “state” 行

2.  最後の “state” 行、より多くのがある場合 “state” 行よりも “cancel” 行。

3.  最初の “cancel” 行、より多くのがある場合 “cancel” 行よりも “state” 行。

4.  他のすべてのケースでは、行はありません。

また、少なくとも2以上がある場合 “state” 行よりも “cancel” 行、または少なくとも2以上 “cancel” その後の行 “state” ただし、ClickHouseはこの状況を論理エラーとして扱い、サーバーログに記録します。 このエラーは、同じデータが複数回挿入された場合に発生します。

したがって、崩壊は統計の計算結果を変更すべきではありません。
変更は徐々に崩壊し、最終的にはほぼすべてのオブジェクトの最後の状態だけが残った。

その `Sign` マージアルゴリズムは、同じソートキーを持つすべての行が同じ結果のデータ部分にあり、同じ物理サーバー上にあることを保証するものではないため、必須で ClickHouse過程 `SELECT` 複数のスレッドを持つクエリは、結果の行の順序を予測することはできません。 完全に取得する必要がある場合は、集計が必要です “collapsed” からのデータ `CollapsingMergeTree` テーブル。

折りたたみを完了するには、次のクエリを記述します `GROUP BY` この符号を考慮する句および集計関数。 たとえば、数量を計算するには、以下を使用します `sum(Sign)` 代わりに `count()`. 何かの合計を計算するには、次のようにします `sum(Sign * x)` 代わりに `sum(x)`、など、とも追加 `HAVING sum(Sign) > 0`.

凝集体 `count`, `sum` と `avg` この方法で計算できます。 合計 `uniq` 算出できる場合にはオブジェクトは、少なくとも一つの状態はまだ崩れていない。 凝集体 `min` と `max` 以下の理由で計算できませんでした `CollapsingMergeTree` 折りたたまれた状態の値の履歴は保存されません。

集計せずにデータを抽出する必要がある場合(たとえば、最新の値が特定の条件に一致する行が存在するかどうかをチェックする場合)は、次のように `FINAL` のための修飾語 `FROM` 句。 このアプローチは大幅に少ない効率的です。

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

私たちは二つを使う `INSERT` 二つの異なるデータ部分を作成するクエリ。 また、データの挿入につクエリClickHouseを一つのデータ部分を行いませんが合併します。

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

二つの `INSERT` クエリーを作成し、2つのデータ部品です。 その `SELECT` クエリは2つのスレッドで実行され、ランダムな行の順序が得られました。 データ部分のマージがまだなかったため、折りたたみは発生しません。 ClickHouseは予測できない未知の瞬間にデータ部分をマージします。

このようにして集計:

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

集約を必要とせず、強制的に崩壊させたい場合は、以下を使用できます `FINAL` の修飾子 `FROM` 句。

``` sql
SELECT * FROM UAct FINAL
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

データを選択するこの方法は非常に非効率的です。 大きなテーブルには使用しないでください。

## 別のアプローチの例 {#example-of-another-approach}

データ例:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │        -5 │     -146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

アイデアは、マージが唯一のキーフィールドを考慮することです。 そして、 “Cancel” 行符号列を使用せずに合計すると、行の以前のバージョンを等しくする負の値を指定できます。 この方法では、データ型を変更する必要があります `PageViews`,`Duration` UInt8-\>Int16の負の値を格納します。

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

アプローチをテストしよう:

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

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/collapsingmergetree/) <!--hide-->
