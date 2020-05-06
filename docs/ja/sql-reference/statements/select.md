---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 33
toc_title: SELECT
---

# クエリ構文の選択 {#select-queries-syntax}

`SELECT` データ検索を実行します。

``` sql
[WITH expr_list|(subquery)]
SELECT [DISTINCT] expr_list
[FROM [db.]table | (subquery) | table_function] [FINAL]
[SAMPLE sample_coeff]
[ARRAY JOIN ...]
[GLOBAL] [ANY|ALL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER] JOIN (subquery)|table USING columns_list
[PREWHERE expr]
[WHERE expr]
[GROUP BY expr_list] [WITH TOTALS]
[HAVING expr]
[ORDER BY expr_list]
[LIMIT [offset_value, ]n BY columns]
[LIMIT [n, ]m]
[UNION ALL ...]
[INTO OUTFILE filename]
[FORMAT format]
```

SELECT直後の必須の式リストを除き、すべての句はオプションです。
以下の句は、クエリ実行コンベアとほぼ同じ順序で記述されています。

クエリが省略された場合、 `DISTINCT`, `GROUP BY` と `ORDER BY` 句および `IN` と `JOIN` サブクエリでは、クエリはO（1）量のRAMを使用して完全にストリーム処理されます。
それ以外の場合、適切な制限が指定されていない場合、クエリは大量のramを消費する可能性があります: `max_memory_usage`, `max_rows_to_group_by`, `max_rows_to_sort`, `max_rows_in_distinct`, `max_bytes_in_distinct`, `max_rows_in_set`, `max_bytes_in_set`, `max_rows_in_join`, `max_bytes_in_join`, `max_bytes_before_external_sort`, `max_bytes_before_external_group_by`. 詳細については、以下を参照してください “Settings”. 外部ソート（一時テーブルをディスクに保存する）と外部集約を使用することが可能です。 `The system does not have "merge join"`.

### With句 {#with-clause}

このセクション支援のための共通表現 ([CTE](https://en.wikipedia.org/wiki/Hierarchical_and_recursive_queries_in_SQL)）、いくつかの制限があります:
1. 再帰的な問合せには対応していない
2. サブクエリがsection内で使用される場合、その結果は正確に一つの行を持つスカラーになります
3. 式の結果はサブクエリでは使用できません
WITH句の式の結果は、SELECT句の中で使用できます。

例1:定数式をasとして使用する “variable”

``` sql
WITH '2019-08-01 15:23:00' as ts_upper_bound
SELECT *
FROM hits
WHERE
    EventDate = toDate(ts_upper_bound) AND
    EventTime <= ts_upper_bound
```

例2:select句の列リストからsum(bytes)式の結果を削除する

``` sql
WITH sum(bytes) as s
SELECT
    formatReadableSize(s),
    table
FROM system.parts
GROUP BY table
ORDER BY s
```

例3:結果のスカラサブクエリ

``` sql
/* this example would return TOP 10 of most huge tables */
WITH
    (
        SELECT sum(bytes)
        FROM system.parts
        WHERE active
    ) AS total_disk_usage
SELECT
    (sum(bytes) / total_disk_usage) * 100 AS table_disk_usage,
    table
FROM system.parts
GROUP BY table
ORDER BY table_disk_usage DESC
LIMIT 10
```

例4:サブクエリでの式の再利用
サブクエリでの式の現在の使用制限の回避策として、複製することができます。

``` sql
WITH ['hello'] AS hello
SELECT
    hello,
    *
FROM
(
    WITH ['hello'] AS hello
    SELECT hello
)
```

``` text
┌─hello─────┬─hello─────┐
│ ['hello'] │ ['hello'] │
└───────────┴───────────┘
```

### FROM句 {#select-from}

FROM句が省略された場合、データは `system.one` テーブル。
その `system.one` このテーブルは、他のDbmsで見つかったデュアルテーブルと同じ目的を果たします。

その `FROM` 句は、データを読み取るソースを指定します:

-   テーブル
-   サブクエリ
-   [テーブル機能](../table-functions/index.md#table-functions)

`ARRAY JOIN` そして、定期的に `JOIN` また、（下記参照）が含まれていてもよいです。

テーブルの代わりに、 `SELECT` サブクエリは、かっこで指定できます。
標準sqlとは対照的に、サブクエリの後にシノニムを指定する必要はありません。

実行をクエリに対して、すべての列をクエリを取り出しに適します。 任意の列は不要のため、外部クエリはスローされ、サブクエリ.
クエリで列がリストされない場合(たとえば, `SELECT count() FROM t`）行の数を計算するために、いくつかの列がテーブルから抽出されます（最小の列が優先されます）。

#### 最終修飾子 {#select-from-final}

が異なる場合は、それぞれの選定からデータをテーブルからの [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)-エンジンファミリー以外 `GraphiteMergeTree`. とき `FINAL` 指定されている場合、ClickHouseは結果を返す前にデータを完全にマージするため、指定されたテーブルエンジンのマージ中に発生するすべてのデータ変換を実行し

また、:
- [複製された](../../engines/table-engines/mergetree-family/replication.md) のバージョン `MergeTree` エンジン
- [ビュー](../../engines/table-engines/special/view.md), [バッファ](../../engines/table-engines/special/buffer.md), [分散](../../engines/table-engines/special/distributed.md)、と [MaterializedView](../../engines/table-engines/special/materializedview.md) 他のエンジンを操作するエンジンは、それらが上に作成された提供 `MergeTree`-エンジンテーブル。

使用するクエリ `FINAL` そうでない類似のクエリと同じくらい速く実行されません。:

-   クエリは単一のスレッドで実行され、クエリの実行中にデータがマージされます。
-   とのクエリ `FINAL` クエリで指定された列に加えて、主キー列を読み取ります。

ほとんどの場合、使用を避けます `FINAL`.

### サンプル句 {#select-sample-clause}

その `SAMPLE` 句は、近似クエリ処理を可能にします。

データサンプリングを有効にすると、すべてのデータに対してクエリは実行されず、特定のデータ(サンプル)に対してのみ実行されます。 たとえば、すべての訪問の統計を計算する必要がある場合は、すべての訪問の1/10分のクエリを実行し、その結果に10を掛けるだけで十分です。

近似クエリ処理は、次の場合に役立ちます:

-   厳密なタイミング要件（\<100ms）がありますが、それらを満たすために追加のハードウェアリソースのコストを正当化できない場合。
-   生データが正確でない場合、近似は品質を著しく低下させません。
-   ビジネス要件は、おおよその結果（費用対効果のため、または正確な結果をプレミアムユーザーに販売するため）を対象とします。

!!! note "メモ"
    サンプリングを使用できるのは、次の表のみです [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) テーブル作成時にサンプリング式が指定された場合にのみ [MergeTreeエンジン](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)).

データサンプリングの機能を以下に示します:

-   データサンプリングは確定的なメカニズムです。 同じの結果 `SELECT .. SAMPLE` クエリは常に同じです。
-   サンプリン テーブルに単一のサンプリングキーは、サンプルと同じ係数を常に選択と同じサブセットのデータです。 たとえば、ユーザー idのサンプルでは、異なるテーブルのすべてのユーザー idのサブセットが同じ行になります。 これは、サブクエリでサンプルを使用できることを意味します [IN](#select-in-operators) 句。 また、以下を使用してサンプルを結合できます [JOIN](#select-join) 句。
-   サンプリングで読み下からのデータディスク。 サンプリングキーを正しく指定する必要があります。 詳細については、 [MergeTreeテーブルの作成](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table).

のための `SAMPLE` 句次の構文がサポートされています:

| SAMPLE Clause Syntax | 説明                                                                                                                                                                                                                                  |
|----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `SAMPLE k`           | ここに `k` 0から1までの数値です。</br>クエリが実行される `k` データの割合。 例えば, `SAMPLE 0.1` データの10%に対してクエリを実行します。 [もっと読む](#select-sample-k)                                                               |
| `SAMPLE n`           | ここに `n` は十分に大きい整数です。</br>クエリは、少なくとものサンプルで実行されます `n` 行（しかし、これ以上のものではない）。 例えば, `SAMPLE 10000000` 最小10,000,000行に対してクエリを実行します。 [もっと読む](#select-sample-n) |
| `SAMPLE k OFFSET m`  | ここに `k` と `m` 0から1までの数字です。</br>クエリは次のサンプルで実行されます `k` データの割合。 に使用されるデータのサンプルと相殺することにより `m` 分数。 [もっと読む](#select-sample-offset)                                    |

#### SAMPLE K {#select-sample-k}

ここに `k` は0から1までの数値です(小数表記と小数表記の両方がサポートされています)。 例えば, `SAMPLE 1/2` または `SAMPLE 0.5`.

で `SAMPLE k` 句は、サンプルから取られます `k` データの割合。 例を以下に示します:

``` sql
SELECT
    Title,
    count() * 10 AS PageViews
FROM hits_distributed
SAMPLE 0.1
WHERE
    CounterID = 34
GROUP BY Title
ORDER BY PageViews DESC LIMIT 1000
```

この例では、0.1(10%)のデータのサンプルでクエリが実行されます。 集計関数の値は自動的には修正されないので、おおよその結果を得るには値 `count()` 手動で10倍します。

#### SAMPLE N {#select-sample-n}

ここに `n` は十分に大きい整数です。 例えば, `SAMPLE 10000000`.

この場合、クエリは少なくともサンプルで実行されます `n` 行（しかし、これ以上のものではない）。 例えば, `SAMPLE 10000000` 最小10,000,000行に対してクエリを実行します。

データ読み取りのための最小単位は一つの顆粒であるため（そのサイズは `index_granularity` それは、顆粒のサイズよりもはるかに大きいサンプルを設定することは理にかなっています。

を使用する場合 `SAMPLE n` 句、データの相対パーセントが処理されたかわからない。 したがって、集計関数に掛ける係数はわかりません。 を使用 `_sample_factor` おおよその結果を得るための仮想列。

その `_sample_factor` 列には、動的に計算される相対係数が含まれます。 この列は、次の場合に自動的に作成されます [作成](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) 指定したサンプリングキーを持つテーブル。 の使用例 `_sample_factor` 列は以下の通りです。

テーブルを考えてみましょう `visits` これには、サイト訪問に関する統計情報が含まれます。 最初の例は、ページビューの数を計算する方法を示しています:

``` sql
SELECT sum(PageViews * _sample_factor)
FROM visits
SAMPLE 10000000
```

次の例では、訪問回数の合計を計算する方法を示します:

``` sql
SELECT sum(_sample_factor)
FROM visits
SAMPLE 10000000
```

以下の例は、平均セッション期間を計算する方法を示しています。 相対係数を使用して平均値を計算する必要はありません。

``` sql
SELECT avg(Duration)
FROM visits
SAMPLE 10000000
```

#### SAMPLE K OFFSET M {#select-sample-offset}

ここに `k` と `m` は0から1までの数字です。 例を以下に示す。

**例1**

``` sql
SAMPLE 1/10
```

この例では、サンプルはすべてのデータの1/10thです:

`[++------------]`

**例2**

``` sql
SAMPLE 1/10 OFFSET 1/2
```

ここでは、データの後半から10％のサンプルを採取します。

`[------++------]`

### 配列結合句 {#select-array-join-clause}

実行を許可する `JOIN` 配列または入れ子になったデータ構造。 その意図は、 [arrayJoin](../../sql-reference/functions/array-join.md#functions_arrayjoin) 機能が、その機能はより広いです。

``` sql
SELECT <expr_list>
FROM <left_subquery>
[LEFT] ARRAY JOIN <array>
[WHERE|PREWHERE <expr>]
...
```

単一のみを指定できます `ARRAY JOIN` クエリ内の句。

実行時にクエリの実行順序が最適化されます `ARRAY JOIN`. が `ARRAY JOIN` の前に必ず指定する必要があります。 `WHERE/PREWHERE` のいずれかを実行することができます。 `WHERE/PREWHERE` （結果がこの節で必要な場合）、またはそれを完了した後（計算量を減らすため）。 処理順序はクエリオプティマイザによって制御されます。

サポートされる種類の `ARRAY JOIN` は以下の通りです:

-   `ARRAY JOIN` -この場合、空の配列は結果に含まれません `JOIN`.
-   `LEFT ARRAY JOIN` -結果の `JOIN` 空の配列を含む行を含みます。 空の配列の値は、配列要素タイプのデフォルト値に設定されます(通常は0、空の文字列またはNULL)。

以下の例は、以下の使用例を示しています。 `ARRAY JOIN` と `LEFT ARRAY JOIN` 句。 テーブルを作成してみましょう [配列](../../sql-reference/data-types/array.md) 列を入力して値を挿入します:

``` sql
CREATE TABLE arrays_test
(
    s String,
    arr Array(UInt8)
) ENGINE = Memory;

INSERT INTO arrays_test
VALUES ('Hello', [1,2]), ('World', [3,4,5]), ('Goodbye', []);
```

``` text
┌─s───────────┬─arr─────┐
│ Hello       │ [1,2]   │
│ World       │ [3,4,5] │
│ Goodbye     │ []      │
└─────────────┴─────────┘
```

以下の例では、 `ARRAY JOIN` 句:

``` sql
SELECT s, arr
FROM arrays_test
ARRAY JOIN arr;
```

``` text
┌─s─────┬─arr─┐
│ Hello │   1 │
│ Hello │   2 │
│ World │   3 │
│ World │   4 │
│ World │   5 │
└───────┴─────┘
```

次の例では、 `LEFT ARRAY JOIN` 句:

``` sql
SELECT s, arr
FROM arrays_test
LEFT ARRAY JOIN arr;
```

``` text
┌─s───────────┬─arr─┐
│ Hello       │   1 │
│ Hello       │   2 │
│ World       │   3 │
│ World       │   4 │
│ World       │   5 │
│ Goodbye     │   0 │
└─────────────┴─────┘
```

#### エイリアスの使用 {#using-aliases}

配列のエイリアスを指定することができます。 `ARRAY JOIN` 句。 この場合、配列項目はこのエイリアスでアクセスできますが、配列自体は元の名前でアクセスされます。 例えば:

``` sql
SELECT s, arr, a
FROM arrays_test
ARRAY JOIN arr AS a;
```

``` text
┌─s─────┬─arr─────┬─a─┐
│ Hello │ [1,2]   │ 1 │
│ Hello │ [1,2]   │ 2 │
│ World │ [3,4,5] │ 3 │
│ World │ [3,4,5] │ 4 │
│ World │ [3,4,5] │ 5 │
└───────┴─────────┴───┘
```

別名を使用すると、次の操作を実行できます `ARRAY JOIN` 外部配列を使用する。 例えば:

``` sql
SELECT s, arr_external
FROM arrays_test
ARRAY JOIN [1, 2, 3] AS arr_external;
```

``` text
┌─s───────────┬─arr_external─┐
│ Hello       │            1 │
│ Hello       │            2 │
│ Hello       │            3 │
│ World       │            1 │
│ World       │            2 │
│ World       │            3 │
│ Goodbye     │            1 │
│ Goodbye     │            2 │
│ Goodbye     │            3 │
└─────────────┴──────────────┘
```

複数の配列をコンマで区切ることができます。 `ARRAY JOIN` 句。 この場合, `JOIN` それらと同時に実行されます（直積ではなく、直積）。 すべての配列は同じサイズでなければなりません。 例えば:

``` sql
SELECT s, arr, a, num, mapped
FROM arrays_test
ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num, arrayMap(x -> x + 1, arr) AS mapped;
```

``` text
┌─s─────┬─arr─────┬─a─┬─num─┬─mapped─┐
│ Hello │ [1,2]   │ 1 │   1 │      2 │
│ Hello │ [1,2]   │ 2 │   2 │      3 │
│ World │ [3,4,5] │ 3 │   1 │      4 │
│ World │ [3,4,5] │ 4 │   2 │      5 │
│ World │ [3,4,5] │ 5 │   3 │      6 │
└───────┴─────────┴───┴─────┴────────┘
```

以下の例では、 [arrayEnumerate](../../sql-reference/functions/array-functions.md#array_functions-arrayenumerate) 機能:

``` sql
SELECT s, arr, a, num, arrayEnumerate(arr)
FROM arrays_test
ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num;
```

``` text
┌─s─────┬─arr─────┬─a─┬─num─┬─arrayEnumerate(arr)─┐
│ Hello │ [1,2]   │ 1 │   1 │ [1,2]               │
│ Hello │ [1,2]   │ 2 │   2 │ [1,2]               │
│ World │ [3,4,5] │ 3 │   1 │ [1,2,3]             │
│ World │ [3,4,5] │ 4 │   2 │ [1,2,3]             │
│ World │ [3,4,5] │ 5 │   3 │ [1,2,3]             │
└───────┴─────────┴───┴─────┴─────────────────────┘
```

#### 配列の参加入れ子データ構造 {#array-join-with-nested-data-structure}

`ARRAY`また、“参加”で動作します [入れ子のデータ構造](../../sql-reference/data-types/nested-data-structures/nested.md). 例えば:

``` sql
CREATE TABLE nested_test
(
    s String,
    nest Nested(
    x UInt8,
    y UInt32)
) ENGINE = Memory;

INSERT INTO nested_test
VALUES ('Hello', [1,2], [10,20]), ('World', [3,4,5], [30,40,50]), ('Goodbye', [], []);
```

``` text
┌─s───────┬─nest.x──┬─nest.y─────┐
│ Hello   │ [1,2]   │ [10,20]    │
│ World   │ [3,4,5] │ [30,40,50] │
│ Goodbye │ []      │ []         │
└─────────┴─────────┴────────────┘
```

``` sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN nest;
```

``` text
┌─s─────┬─nest.x─┬─nest.y─┐
│ Hello │      1 │     10 │
│ Hello │      2 │     20 │
│ World │      3 │     30 │
│ World │      4 │     40 │
│ World │      5 │     50 │
└───────┴────────┴────────┘
```

ネストされたデータ構造の名前を指定する場合 `ARRAY JOIN`、意味は同じです `ARRAY JOIN` それが構成されているすべての配列要素。 例を以下に示します:

``` sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN `nest.x`, `nest.y`;
```

``` text
┌─s─────┬─nest.x─┬─nest.y─┐
│ Hello │      1 │     10 │
│ Hello │      2 │     20 │
│ World │      3 │     30 │
│ World │      4 │     40 │
│ World │      5 │     50 │
└───────┴────────┴────────┘
```

この変化はまた意味を成している:

``` sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN `nest.x`;
```

``` text
┌─s─────┬─nest.x─┬─nest.y─────┐
│ Hello │      1 │ [10,20]    │
│ Hello │      2 │ [10,20]    │
│ World │      3 │ [30,40,50] │
│ World │      4 │ [30,40,50] │
│ World │      5 │ [30,40,50] │
└───────┴────────┴────────────┘
```

エイリアスは、ネストされたデータ構造のために使用することができます。 `JOIN` 結果またはソース配列。 例えば:

``` sql
SELECT s, `n.x`, `n.y`, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN nest AS n;
```

``` text
┌─s─────┬─n.x─┬─n.y─┬─nest.x──┬─nest.y─────┐
│ Hello │   1 │  10 │ [1,2]   │ [10,20]    │
│ Hello │   2 │  20 │ [1,2]   │ [10,20]    │
│ World │   3 │  30 │ [3,4,5] │ [30,40,50] │
│ World │   4 │  40 │ [3,4,5] │ [30,40,50] │
│ World │   5 │  50 │ [3,4,5] │ [30,40,50] │
└───────┴─────┴─────┴─────────┴────────────┘
```

使用例 [arrayEnumerate](../../sql-reference/functions/array-functions.md#array_functions-arrayenumerate) 機能:

``` sql
SELECT s, `n.x`, `n.y`, `nest.x`, `nest.y`, num
FROM nested_test
ARRAY JOIN nest AS n, arrayEnumerate(`nest.x`) AS num;
```

``` text
┌─s─────┬─n.x─┬─n.y─┬─nest.x──┬─nest.y─────┬─num─┐
│ Hello │   1 │  10 │ [1,2]   │ [10,20]    │   1 │
│ Hello │   2 │  20 │ [1,2]   │ [10,20]    │   2 │
│ World │   3 │  30 │ [3,4,5] │ [30,40,50] │   1 │
│ World │   4 │  40 │ [3,4,5] │ [30,40,50] │   2 │
│ World │   5 │  50 │ [3,4,5] │ [30,40,50] │   3 │
└───────┴─────┴─────┴─────────┴────────────┴─────┘
```

### JOIN句 {#select-join}

通常のデータを結合します [SQL JOIN](https://en.wikipedia.org/wiki/Join_(SQL)) センス

!!! info "メモ"
    関連しない [ARRAY JOIN](#select-array-join-clause).

``` sql
SELECT <expr_list>
FROM <left_subquery>
[GLOBAL] [ANY|ALL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER] JOIN <right_subquery>
(ON <expr_list>)|(USING <column_list>) ...
```

テーブル名は次の代わりに指定できます `<left_subquery>` と `<right_subquery>`. これは、 `SELECT * FROM table` サブクエリは、テーブルが次のものを持つ特殊な場合を除きます。 [参加](../../engines/table-engines/special/join.md) engine – an array prepared for joining.

#### サポートされる種類の `JOIN` {#select-join-types}

-   `INNER JOIN` （または `JOIN`)
-   `LEFT JOIN` （または `LEFT OUTER JOIN`)
-   `RIGHT JOIN` （または `RIGHT OUTER JOIN`)
-   `FULL JOIN` （または `FULL OUTER JOIN`)
-   `CROSS JOIN` （または `,` )

標準を参照してください [SQL JOIN](https://en.wikipedia.org/wiki/Join_(SQL)) 説明。

#### 複数の結合 {#multiple-join}

クエリを実行すると、clickhouseはマルチテーブル結合を二つのテーブル結合のシーケンスに書き換えます。 たとえば、clickhouseに参加するための四つのテーブルがある場合は、最初と二番目のテーブルを結合し、その結果を三番目のテーブルに結合し、最後のステップで

クエリが含まれている場合、 `WHERE` 句、ClickHouseはこの句から中間結合を介してプッシュダウンフィルターを試行します。 各中間結合にフィルタを適用できない場合、ClickHouseはすべての結合が完了した後にフィルタを適用します。

私たちはお勧め `JOIN ON` または `JOIN USING` クエリを作成するための構文。 例えば:

``` sql
SELECT * FROM t1 JOIN t2 ON t1.a = t2.a JOIN t3 ON t1.a = t3.a
```

テーブルのコンマ区切りリストを使用することができます `FROM` 句。 例えば:

``` sql
SELECT * FROM t1, t2, t3 WHERE t1.a = t2.a AND t1.a = t3.a
```

これらの構文を混在させないでください。

ClickHouseはカンマで構文を直接サポートしていないので、使用することはお勧めしません。 このアルゴ `CROSS JOIN` と `INNER JOIN` クエリ処理に進みます。 クエリを書き換えるとき、ClickHouseはパフォーマンスとメモリ消費の最適化を試みます。 デフォルトでは、ClickHouseはコンマを `INNER JOIN` 句と変換 `INNER JOIN` に `CROSS JOIN` アルゴリズムが保証できない場合 `INNER JOIN` 必要なデータを返します。

#### 厳密さ {#select-join-strictness}

-   `ALL` — If the right table has several matching rows, ClickHouse creates a [デカルト積](https://en.wikipedia.org/wiki/Cartesian_product) 一致する行から。 これが標準です `JOIN` SQLでの動作。
-   `ANY` — If the right table has several matching rows, only the first one found is joined. If the right table has only one matching row, the results of queries with `ANY` と `ALL` キーワードは同じです。
-   `ASOF` — For joining sequences with a non-exact match. `ASOF JOIN` 以下に使用方法を説明します。

**ASOF結合の使用**

`ASOF JOIN` 完全一致のないレコードを結合する必要がある場合に便利です。

テーブルのため `ASOF JOIN` 順序列の列を持つ必要があります。 この列はテーブル内で単独で使用することはできません。: `UInt32`, `UInt64`, `Float32`, `Float64`, `Date`、と `DateTime`.

構文 `ASOF JOIN ... ON`:

``` sql
SELECT expressions_list
FROM table_1
ASOF LEFT JOIN table_2
ON equi_cond AND closest_match_cond
```

任意の数の等価条件と正確に最も近い一致条件を使用できます。 例えば, `SELECT count() FROM table_1 ASOF LEFT JOIN table_2 ON table_1.a == table_2.b AND table_2.t <= table_1.t`.

最も近い一致でサポートされる条件: `>`, `>=`, `<`, `<=`.

構文 `ASOF JOIN ... USING`:

``` sql
SELECT expressions_list
FROM table_1
ASOF JOIN table_2
USING (equi_column1, ... equi_columnN, asof_column)
```

`ASOF JOIN` 使用 `equi_columnX` 平等に参加するための `asof_column` との最も近い試合に参加するための `table_1.asof_column >= table_2.asof_column` 条件。 その `asof_column` 列は常に最後の列です `USING` 句。

たとえば、次の表を考えてみます:

’テキスト
テーブル1テーブル2

イベント/ev\_time\|user\_idイベント/ev\_time\|user\_id
