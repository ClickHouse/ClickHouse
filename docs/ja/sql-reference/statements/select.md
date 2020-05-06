---
machine_translated: true
machine_translated_rev: 0f7ef7704d018700049223525bad4a63911b6e70
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
それ以外の場合、適切な制限が指定されていない場合、クエリは大量のRAMを消費する可能性があります: `max_memory_usage`, `max_rows_to_group_by`, `max_rows_to_sort`, `max_rows_in_distinct`, `max_bytes_in_distinct`, `max_rows_in_set`, `max_bytes_in_set`, `max_rows_in_join`, `max_bytes_in_join`, `max_bytes_before_external_sort`, `max_bytes_before_external_group_by`. 詳細については、以下を参照してください “Settings”. 外部ソート（一時テーブルをディスクに保存する）と外部集約を使用することが可能です。 `The system does not have "merge join"`.

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

例2:SELECT句の列リストからsum(bytes)式の結果を削除する

``` sql
WITH sum(bytes) as s
SELECT
    formatReadableSize(s),
    table
FROM system.parts
GROUP BY table
ORDER BY s
```

例3:スカラーサブクエリの結果の使用

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
標準SQLとは対照的に、サブクエリの後にシノニムを指定する必要はありません。

クエリを実行するには、クエリにリストされているすべての列を適切なテーブルから抽出します。 外部クエリに必要のない列は、サブクエリからスローされます。
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
-   サンプリン テーブルに単一のサンプリングキーは、サンプルと同じ係数を常に選択と同じサブセットのデータです。 たとえば、ユーザー Idのサンプルでは、異なるテーブルのすべてのユーザー Idのサブセットが同じ行になります。 これは、サブクエリでサンプルを使用できることを意味します [IN](#select-in-operators) 句。 また、以下を使用してサンプルを結合できます [JOIN](#select-join) 句。
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

実行を許可する `JOIN` 配列または入れ子になったデータ構造。 その意図は、 [arrayJoin](../functions/array-join.md#functions_arrayjoin) 機能が、その機能はより広いです。

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

`ARRAY`また、"参加"で動作します [入れ子のデータ構造](../../sql-reference/data-types/nested-data-structures/nested.md). 例えば:

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

クエリを実行すると、ClickHouseはマルチテーブル結合を二つのテーブル結合のシーケンスに書き換えます。 たとえば、clickhouseに参加するための四つのテーブルがある場合は、最初と二番目のテーブルを結合し、その結果を三番目のテーブルに結合し、最後のステップで

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

         table_1                           table_2
      event   | ev_time | user_id       event   | ev_time | user_id
    ----------|---------|----------   ----------|---------|----------
                  ...                               ...
    event_1_1 |  12:00  |  42         event_2_1 |  11:59  |   42
                  ...                 event_2_2 |  12:30  |   42
    event_1_2 |  13:00  |  42         event_2_3 |  13:00  |   42
                  ...                               ...

`ASOF JOIN` ユーザイベントのタイムスタンプを `table_1` そして、イベントを見つける `table_2` タイムスタンプは、イベントのタイムスタンプに最も近い `table_1` 最も近い一致条件に対応します。 等しいタイムスタンプ値に最も近いします。 ここでは、 `user_id` 列は、等価上の結合に使用することができ、 `ev_time` 列は、最も近いマッチでの結合に使用できます。 この例では, `event_1_1` と結合することができます `event_2_1` と `event_1_2` と結合することができます `event_2_3`、しかし `event_2_2` 参加できない

!!! note "メモ"
    `ASOF` 結合は **ない** で支えられる [参加](../../engines/table-engines/special/join.md) テーブルエンジン。

既定の厳密さの値を設定するには、session構成パラメーターを使用します [join\_default\_strictness](../../operations/settings/settings.md#settings-join_default_strictness).

#### GLOBAL JOIN {#global-join}

通常を使用する場合 `JOIN` クエリはリモートサーバーに送信されます。 サブクエリは、右側のテーブルを作成するためにそれぞれに対して実行され、このテーブルで結合が実行されます。 言い換えれば、右のテーブルは各サーバー上に別々に形成される。

使用する場合 `GLOBAL ... JOIN` 最初に、リクエスター-サーバーがサブクエリーを実行して右のテーブルを計算します。 この一時テーブルは各リモートサーバーに渡され、送信された一時データを使用してクエリが実行されます。

を使用する場合は注意 `GLOBAL`. 詳細については、以下を参照してください [分散サブクエリ](#select-distributed-subqueries).

#### 使用の推奨事項 {#usage-recommendations}

実行しているとき `JOIN`、クエリの他の段階に関連して実行順序の最適化はありません。 結合(右側のテーブルでの検索)は、フィルター処理を行う前に実行されます。 `WHERE` そして、集約の前に。 処理順序を明示的に設定するには、以下を実行することを推奨します。 `JOIN` サブクエリを使用したサブクエリ。

例えば:

``` sql
SELECT
    CounterID,
    hits,
    visits
FROM
(
    SELECT
        CounterID,
        count() AS hits
    FROM test.hits
    GROUP BY CounterID
) ANY LEFT JOIN
(
    SELECT
        CounterID,
        sum(Sign) AS visits
    FROM test.visits
    GROUP BY CounterID
) USING CounterID
ORDER BY hits DESC
LIMIT 10
```

``` text
┌─CounterID─┬───hits─┬─visits─┐
│   1143050 │ 523264 │  13665 │
│    731962 │ 475698 │ 102716 │
│    722545 │ 337212 │ 108187 │
│    722889 │ 252197 │  10547 │
│   2237260 │ 196036 │   9522 │
│  23057320 │ 147211 │   7689 │
│    722818 │  90109 │  17847 │
│     48221 │  85379 │   4652 │
│  19762435 │  77807 │   7026 │
│    722884 │  77492 │  11056 │
└───────────┴────────┴────────┘
```

サブクエリでは、特定のサブクエリの列を参照するために名前を設定したり、名前を使用したりすることはできません。
で指定された列 `USING` 両方のサブクエリで同じ名前を持つ必要があり、他の列の名前は異なる必要があります。 別名を使用して、サブクエリの列の名前を変更できます(この例では、別名を使用します `hits` と `visits`).

その `USING` 句は、これらの列の等価性を確立し、結合する一つ以上の列を指定します。 列のリストは、角かっこなしで設定されます。 より複雑な結合条件はサポートされていません。

右側のテーブル（サブクエリ結果）はRAMに存在します。 十分なメモリがない場合は、実行することはできません `JOIN`.

クエリが同じで実行されるたびに `JOIN` 結果がキャッシュされていないため、サブクエリが再度実行されます。 これを回避するには、特別な [参加](../../engines/table-engines/special/join.md) テーブルエンジンは、常にRAMにある結合の準備された配列です。

場合によっては、使用する方が効率的です `IN` 代わりに `JOIN`.
様々なタイプの中で `JOIN` は、最も効率的です `ANY LEFT JOIN`、その後 `ANY INNER JOIN`. の少なくとも効率の高いて `ALL LEFT JOIN` と `ALL INNER JOIN`.

必要な場合は `JOIN` ディメンションテーブ `JOIN` 右のテーブルがすべてのクエリに対して再アクセスされるため、あまり便利ではないかもしれません。 そのような場合には、 “external dictionaries” 代わりに使用する必要がある機能 `JOIN`. 詳細については、以下を参照してください [外部辞書](../dictionaries/external-dictionaries/external-dicts.md).

**メモリの制限**

クリックハウスは [ハッシュ結合](https://en.wikipedia.org/wiki/Hash_join) アルゴリズムだ クリックハウスは、 `<right_subquery>` RAMにハッシュテーブルを作成します。 Join操作のメモリ消費を制限する必要がある場合は、次の設定を使用します:

-   [max\_rows\_in\_join](../../operations/settings/query-complexity.md#settings-max_rows_in_join) — Limits number of rows in the hash table.
-   [max\_bytes\_in\_join](../../operations/settings/query-complexity.md#settings-max_bytes_in_join) — Limits size of the hash table.

これらの制限のいずれかに達すると、ClickHouseは次のように機能します [join\_overflow\_mode](../../operations/settings/query-complexity.md#settings-join_overflow_mode) 設定を指示します。

#### 空またはNULLのセルの処理 {#processing-of-empty-or-null-cells}

を結合を使ったテーブルの結合、空の細胞が表示される場合があります。 を設定 [join\_use\_nulls](../../operations/settings/settings.md#join_use_nulls) する方法を定義するClickHouse填するこれらの細胞。

この `JOIN` キーは [Nullable](../data-types/nullable.md) フィールド、キーの少なくとも一方が値を持つ行 [NULL](../syntax.md#null-literal) 結合されていません。

#### 構文の制限 {#syntax-limitations}

倍数のため `JOIN` 単一の句 `SELECT` クエリ:

-   すべての列を `*` サブクエリではなく、テーブルが結合されている場合にのみ使用可能です。
-   その `PREWHERE` 句は使用できません。

のために `ON`, `WHERE`、と `GROUP BY` 句:

-   任意の式を使用することはできません `ON`, `WHERE`、と `GROUP BY` ただし、aに式を定義することはできます `SELECT` これらの節では、エイリアスを使用して句を使用します。

### WHERE句 {#select-where}

WHERE句がある場合は、UInt8型の式が含まれている必要があります。 これは通常、比較演算子と論理演算子を含む式です。
この表現を使用するフィルタリングデータはすべての小さなものに過ぎません。

ば土地の再評価を行い、土地再評価支援データベースのテーブルエンジンの式が値評価され、使用が可能。

### PREWHERE句 {#prewhere-clause}

この句はWHERE句と同じ意味を持ちます。 違いは、テーブルからデータを読み取ることです。
PRELOWを使用する場合は、まずPRELOWを実行するために必要な列のみが読み込まれます。 次に、クエリを実行するために必要な他の列が読み込まれますが、PREWHERE式がtrueの場合のブロックのみが読み込まれます。

クエリの少数の列で使用されるが、強力なデータのフィルタリングを提供するろ過条件がある場合は、PREWHEREを使用することは理にかなっています。 これにより、読み取るデータの量が減少します。

たとえば、多数の列を抽出するが、少数の列のフィルタリングしか持たないクエリに対してPREWHEREを記述すると便利です。

PREWHEREのようテーブルからの `*MergeTree` 家族

クエリは同時にPREWHEREとWHEREを指定できます。 この場合、PREWHEREはWHEREに先行します。

この ‘optimize\_move\_to\_prewhere’ 設定は1に設定され、PREWHEREは省略され、システムはヒューリスティックを使用して式の一部をどこからPREWHEREに自動的に移動します。

### GROUP BY句 {#select-group-by-clause}

これは、列指向DBMSの最も重要な部分の一つです。

GROUP BY句がある場合は、式のリストが含まれている必要があります。 それぞれの式は、ここではaと呼ばれます。 “key”.
SELECT句、HAVING句、ORDER BY句のすべての式は、キーまたは集計関数から計算する必要があります。 つまり、テーブルから選択された各列は、キーまたは集計関数内で使用する必要があります。

クエリに集計関数内のテーブル列のみが含まれている場合、GROUP BY句を省略することができ、空のキーのセットによる集計が想定されます。

例えば:

``` sql
SELECT
    count(),
    median(FetchTiming > 60 ? 60 : FetchTiming),
    count() - sum(Refresh)
FROM hits
```

ただし、標準SQLとは対照的に、テーブルに行がない場合（まったくない場合、またはwhereフィルタを使用した後に行がない場合）、空の結果が返され、集計関数

MySQL（および標準SQLに準拠）とは対照的に、キーまたは集約関数（定数式を除く）にないカラムの値を取得することはできません。 これを回避するには、次のコマンドを使用します ‘any’ 集約関数（最初に遭遇した値を取得する）または ‘min/max’.

例えば:

``` sql
SELECT
    domainWithoutWWW(URL) AS domain,
    count(),
    any(Title) AS title -- getting the first occurred page header for each domain.
FROM hits
GROUP BY domain
```

異なるキー値が検出されるたびに、GROUP BYは集計関数値のセットを計算します。

GROUP BYは、配列列ではサポートされません。

集計関数の引数として定数を指定することはできません。 例:sum(1)。 これの代わりに、定数を取り除くことができます。 例えば: `count()`.

#### ヌル処理 {#null-processing}

グループ化のために、ClickHouseは [NULL](../syntax.md#null-literal) 値として、 `NULL=NULL`.

これが何を意味するのかを示す例があります。

このテーブルがあるとします:

``` text
┌─x─┬────y─┐
│ 1 │    2 │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │    2 │
│ 3 │    3 │
│ 3 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

クエリ `SELECT sum(x), y FROM t_null_big GROUP BY y` 結果は:

``` text
┌─sum(x)─┬────y─┐
│      4 │    2 │
│      3 │    3 │
│      5 │ ᴺᵁᴸᴸ │
└────────┴──────┘
```

あなたが見ることが `GROUP BY` のために `y = NULL` 総括 `x`、かのように `NULL` この値です。

いくつかのキーを渡す場合 `GROUP BY`、結果はあなたの選択のすべての組み合わせを与えるかのように `NULL` 特定の値でした。

#### 合計モディファイア {#with-totals-modifier}

WITH TOTALS修飾子を指定すると、別の行が計算されます。 この行には、デフォルト値（ゼロまたは空行）を含むキー列と、すべての行にわたって計算された値を持つ集計関数の列があります “total” 値)。

この余分な行は、他の行とは別に、JSON\*、TabSeparated\*、およびPretty\*形式で出力されます。 他の形式では、この行は出力されません。

JSON\*形式では、この行は別の行として出力されます ‘totals’ フィールド。 TabSeparated\*形式では、行はメインの結果の後に来て、空の行（他のデータの後）が先行します。 Pretty\*形式では、行は主な結果の後に別のテーブルとして出力されます。

`WITH TOTALS` HAVINGが存在する場合は、さまざまな方法で実行できます。 この動作は、 ‘totals\_mode’ 設定。
デフォルトでは, `totals_mode = 'before_having'`. この場合, ‘totals’ HAVINGおよびHAVINGを通過しない行を含む、すべての行にわたって計算されます ‘max\_rows\_to\_group\_by’.

他の選択肢には、inを持つ行を通過する行のみが含まれます ‘totals’、および設定とは異なる動作をします `max_rows_to_group_by` と `group_by_overflow_mode = 'any'`.

`after_having_exclusive` – Don't include rows that didn't pass through `max_rows_to_group_by`. つまり, ‘totals’ 以下の行と同じ数の行を持つことになります。 `max_rows_to_group_by` 省略した。

`after_having_inclusive` – Include all the rows that didn't pass through ‘max\_rows\_to\_group\_by’ で ‘totals’. つまり, ‘totals’ これは、次の場合と同じか、同じ数の行を持つことになります `max_rows_to_group_by` 省略した。

`after_having_auto` – Count the number of rows that passed through HAVING. If it is more than a certain amount (by default, 50%), include all the rows that didn't pass through ‘max\_rows\_to\_group\_by’ で ‘totals’. それ以外の場合は、含めないでください。

`totals_auto_threshold` – By default, 0.5. The coefficient for `after_having_auto`.

もし `max_rows_to_group_by` と `group_by_overflow_mode = 'any'` 使用されない、すべての変化の `after_having` 同じであり、それらのいずれかを使用することができます（たとえば, `after_having_auto`).

JOIN句の副照会を含む副照会の合計を使用することができます(この場合、それぞれの合計値が結合されます)。

#### 外部メモリによるグループ化 {#select-group-by-in-external-memory}

一時データのディスクへのダンプを有効にして、使用中のメモリ使用量を制限できます `GROUP BY`.
その [max\_bytes\_before\_external\_group\_by](../../operations/settings/settings.md#settings-max_bytes_before_external_group_by) 設定のしきい値RAM消費のためにダンピング `GROUP BY` ファイルシステムへの一時データ。 0(デフォルト)に設定すると、無効になります。

使用する場合 `max_bytes_before_external_group_by`、私達は置くことを推薦します `max_memory_usage` 約二倍の高さ。 日付の読み取りと中間データの生成(1)と中間データのマージ(2)です。 ダンピングデータのファイルシステムでのみ発生時のステージ1です。 一時データがダンプされなかった場合、ステージ2はステージ1と同じ量のメモリを必要とする可能性があります。

たとえば、次の場合 [max\_memory\_usage](../../operations/settings/settings.md#settings_max_memory_usage) 10000000000に設定されていて、外部集約を使用したい場合は、次のように設定するのが理にかなっています `max_bytes_before_external_group_by` to10000000000,そしてmax\_memory\_usageへ2000000000. 外部集約がトリガーされると(一時データのダンプが少なくともひとつある場合)、RAMの最大消費量はわずかに多くなります `max_bytes_before_external_group_by`.

分散クエリ処理では、リモートサーバーで外部集約が実行されます。 リクエスタサーバが少量のRAMしか使用しないようにするには、以下を設定します `distributed_aggregation_memory_efficient` へ1.

データをディスクにマージするとき、およびリモートサーバーからの結果をマージするとき `distributed_aggregation_memory_efficient` 設定が有効になっている、消費まで `1/256 * the_number_of_threads` RAMの総量から。

外部集約が有効になっているときに、 `max_bytes_before_external_group_by` of data (i.e. data was not flushed), the query runs just as fast as without external aggregation. If any temporary data was flushed, the run time will be several times longer (approximately three times).

あなたが持っている場合 `ORDER BY` と `LIMIT` 後に `GROUP BY` のデータ量に依存します。 `LIMIT`、ないテーブル全体で。 しかし、もし `ORDER BY` 持っていない `LIMIT` を忘れてはならないよ外部ソート (`max_bytes_before_external_sort`).

### 句による制限 {#limit-by-clause}

とのクエリ `LIMIT n BY expressions` 句は最初の句を選択します。 `n` それぞれの個別の値の行 `expressions`. をキー用 `LIMIT BY` 任意の数を含むことができます [式](../syntax.md#syntax-expressions).

ClickHouseは次の構文をサポートします:

-   `LIMIT [offset_value, ]n BY expressions`
-   `LIMIT n OFFSET offset_value BY expressions`

中のクエリ処理、ClickHouseを選択しデータの順に並べ替えることによります。 ソート-キーは、明示的に [ORDER BY](#select-order-by) 句または暗黙的にテーブルエンジンのプロパティとして。 次にClickHouseが適用されます `LIMIT n BY expressions` 最初のものを返します `n` それぞれの個別の組み合わせの行 `expressions`. もし `OFFSET` 指定され、各データブロックに所属する異なる組み合わせ `expressions`、ClickHouseスキップ `offset_value` ブロックの先頭からの行の数との最大値を返します。 `n` 結果としての行。 もし `offset_value` データブロック内の行数より大きい場合、ClickHouseはブロックからゼロ行を返します。

`LIMIT BY` に関連していない `LIMIT`. これらは両方とも同じクエリで使用できます。

**例**

サンプル表:

``` sql
CREATE TABLE limit_by(id Int, val Int) ENGINE = Memory;
INSERT INTO limit_by values(1, 10), (1, 11), (1, 12), (2, 20), (2, 21);
```

クエリ:

``` sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 2 BY id
```

``` text
┌─id─┬─val─┐
│  1 │  10 │
│  1 │  11 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

``` sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 1, 2 BY id
```

``` text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

その `SELECT * FROM limit_by ORDER BY id, val LIMIT 2 OFFSET 1 BY id` queryは同じ結果を返します。

次のクエリは、それぞれの上位5個のリファラーを返します `domain, device_type` 合計で最大100行のペア (`LIMIT n BY + LIMIT`).

``` sql
SELECT
    domainWithoutWWW(URL) AS domain,
    domainWithoutWWW(REFERRER_URL) AS referrer,
    device_type,
    count() cnt
FROM hits
GROUP BY domain, referrer, device_type
ORDER BY cnt DESC
LIMIT 5 BY domain, device_type
LIMIT 100
```

### HAVING Clause {#having-clause}

WHERE句と同様に、GROUP BYの後に受け取った結果をフィルタリングできます。
集約前に実行される箇所と異なる箇所（GROUP BY）を有している間は、その後に実行される。
集計が実行されない場合、HAVINGは使用できません。

### ORDER BY句 {#select-order-by}

ORDER BY句には式のリストが含まれており、それぞれにDESCまたはASC(ソート方向)を割り当てることができます。 方向が指定されていない場合は、ASCが想定されます。 ASCは昇順でソートされ、DESCは降順でソートされます。 並べ替え方向は、リスト全体ではなく、単一の式に適用されます。 例えば: `ORDER BY Visits DESC, SearchPhrase`

文字列値による並べ替えでは、照合(比較)を指定できます。 例えば: `ORDER BY SearchPhrase COLLATE 'tr'` -によるソートキーワードの昇順では、トルコのアルファベット大文字、小文字大文字と小文字を区別しません、ここから文字列はUTF-8エンコードされます。 COLLATEは、各式に対して独立して順番に指定するかどうかを指定できます。 ASCまたはDESCが指定されている場合は、その後にCOLLATEが指定されます。 COLLATEを使用する場合、並べ替えは常に大文字と小文字を区別しません。

COLLATEでソートするのは、通常のバイトでのソートよりも効率的ではないため、少数の行の最終ソートにはCOLLATEを使用することをお勧めします。

並べ替え式のリストに同じ値を持つ行は、任意の順序で出力されます。
ORDER BY句を省略すると、行の順序も未定義になり、非決定的になることもあります。

`NaN` と `NULL` ソート順序:

-   モディファイア `NULLS FIRST` — First `NULL`、その後 `NaN`、その後、他の値。
-   モディファイア `NULLS LAST` — First the values, then `NaN`、その後 `NULL`.
-   Default — The same as with the `NULLS LAST` 修飾子。

例えば:

テーブルのため

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    2 │
│ 1 │  nan │
│ 2 │    2 │
│ 3 │    4 │
│ 5 │    6 │
│ 6 │  nan │
│ 7 │ ᴺᵁᴸᴸ │
│ 6 │    7 │
│ 8 │    9 │
└───┴──────┘
```

クエリの実行 `SELECT * FROM t_null_nan ORDER BY y NULLS FIRST` を取得するには:

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 7 │ ᴺᵁᴸᴸ │
│ 1 │  nan │
│ 6 │  nan │
│ 2 │    2 │
│ 2 │    2 │
│ 3 │    4 │
│ 5 │    6 │
│ 6 │    7 │
│ 8 │    9 │
└───┴──────┘
```

浮動小数点数がソートされると、Nanは他の値とは別になります。 ソート順序にかかわらず、Nanは最後に来ます。 言い換えれば、昇順ソートの場合、それらは他のすべての数字よりも大きいかのように配置され、降順ソートの場合は残りの数字よりも小さいかのよう

ORDER BYに加えて十分に小さい制限が指定されている場合は、より少ないRAMが使用されます。 それ以外の場合、消費されるメモリの量は、ソートのためのデータ量に比例します。 分散クエリ処理では、GROUP BYを省略した場合、リモートサーバー上で部分的にソートが行われ、結果はリクエスタサーバー上でマージされます。 これは、分散ソートの場合、ソートするデータの量は、単一のサーバー上のメモリ量よりも大きくなる可能性があることを意味します。

十分なRAMがない場合は、（ディスク上の一時ファイルを作成する）外部メモリにソートを実行することが可能です。 設定を使用する `max_bytes_before_external_sort` この目的のために。 0(デフォルト)に設定すると、外部ソートは無効になります。 これを有効にすると、ソートするデータのボリュームが指定されたバイト数に達すると、収集されたデータがソートされ、一時ファイルにダンプされます。 すべてのデータの読み込み、すべてのソートファイルとして合併し、その結果を出力します。 ファイルはconfigの/var/lib/clickhouse/tmp/ディレクトリに書き込まれます（デフォルトでは、 ‘tmp\_path’ この設定を変更するパラメータ)。

クエリを実行すると ‘max\_bytes\_before\_external\_sort’. このため、この設定の値は、以下よりも大幅に小さくする必要があります ‘max\_memory\_usage’. たとえば、サーバーに128GBのRAMがあり、単一のクエリを実行する必要がある場合は、次のように設定します ‘max\_memory\_usage’ 100GBに、 ‘max\_bytes\_before\_external\_sort’ 80GBまで。

外部ソートは、RAMのソートよりも効果的ではありません。

### SELECT句 {#select-select}

[式](../syntax.md#syntax-expressions) で指定される `SELECT` 句は、上記の句のすべての操作が終了した後に計算されます。 これらの式は、結果の別々の行に適用されるかのように機能します。 式の場合 `SELECT` 句には集計関数が含まれ、ClickHouseは集計関数とその引数として使用される式を処理します。 [GROUP BY](#select-group-by-clause) 集約。

結果にすべての列を含める場合は、アスタリスクを使用します (`*`)シンボル。 例えば, `SELECT * FROM ...`.

結果のいくつかの列をaと一致させるには [re2unit description in lists](https://en.wikipedia.org/wiki/RE2_(software)) 正規表現を使用することができます `COLUMNS` 式。

``` sql
COLUMNS('regexp')
```

たとえば、次の表を考えてみます:

``` sql
CREATE TABLE default.col_names (aa Int8, ab Int8, bc Int8) ENGINE = TinyLog
```

以下のクエリを選択しデータからすべての列を含む `a` 彼らの名前のシンボル。

``` sql
SELECT COLUMNS('a') FROM col_names
```

``` text
┌─aa─┬─ab─┐
│  1 │  1 │
└────┴────┘
```

選択した列は、アルファベット順ではなく返されます。

複数を使用できます `COLUMNS` クエリ内の式とそれらに関数を適用します。

例えば:

``` sql
SELECT COLUMNS('a'), COLUMNS('c'), toTypeName(COLUMNS('c')) FROM col_names
```

``` text
┌─aa─┬─ab─┬─bc─┬─toTypeName(bc)─┐
│  1 │  1 │  1 │ Int8           │
└────┴────┴────┴────────────────┘
```

によって返される各列 `COLUMNS` 式は、別の引数として関数に渡されます。 また、他の引数を関数に渡すこともできます。 関数を使用する場合は注意してください。 関数が渡された引数の数をサポートしていない場合、ClickHouseは例外をスローします。

例えば:

``` sql
SELECT COLUMNS('a') + COLUMNS('c') FROM col_names
```

``` text
Received exception from server (version 19.14.1):
Code: 42. DB::Exception: Received from localhost:9000. DB::Exception: Number of arguments for function plus doesn't match: passed 3, should be 2.
```

この例では, `COLUMNS('a')` 二つの列を返します: `aa` と `ab`. `COLUMNS('c')` を返します `bc` コラム その `+` 演算子は3つの引数には適用できないため、ClickHouseは関連するメッセージで例外をスローします。

一致した列 `COLUMNS` 式のデータ型は異なる場合があります。 もし `COLUMNS` 列には一致せず、唯一の式です `SELECT`、ClickHouseは例外をスローします。

### DISTINCT句 {#select-distinct}

DISTINCTが指定されている場合、結果に完全に一致する行のすべてのセットから単一の行だけが残ります。
結果は、group BYが集計関数なしでSELECTで指定されたすべてのフィールドに指定された場合と同じになります。 しかし、GROUP BYとの違いはいくつかあります:

-   DISTINCTはGROUP BYと一緒に適用できます。
-   ORDER BYを省略してLIMITを定義すると、必要な数の異なる行が読み込まれた直後にクエリが実行を停止します。
-   データブロックは、クエリ全体の実行が完了するのを待たずに、処理されたときに出力されます。

個別には対応していない場合に選択して少なくとも一つの配列です。

`DISTINCT` で動作 [NULL](../syntax.md#null-literal) まるで `NULL` 特定の値であり、 `NULL=NULL`. つまり、 `DISTINCT` 結果、異なる組み合わせ `NULL` 一度だけ発生する。

ClickHouseは使用を支えます `DISTINCT` と `ORDER BY` あるクエリ内の異なる列の句。 その `DISTINCT` の前に句が実行されます。 `ORDER BY` 句。

表の例:

``` text
┌─a─┬─b─┐
│ 2 │ 1 │
│ 1 │ 2 │
│ 3 │ 3 │
│ 2 │ 4 │
└───┴───┘
```

とデータを選択すると `SELECT DISTINCT a FROM t1 ORDER BY b ASC` クエリは、我々は次の結果を得る:

``` text
┌─a─┐
│ 2 │
│ 1 │
│ 3 │
└───┘
```

ソートの方向を変えれば `SELECT DISTINCT a FROM t1 ORDER BY b DESC`、我々は、次の結果を得る:

``` text
┌─a─┐
│ 3 │
│ 1 │
│ 2 │
└───┘
```

行 `2, 4` ソート前にカットされた。

この実装に特異性を考慮グます。

### LIMIT句 {#limit-clause}

`LIMIT m` 最初のものを選択することができます `m` 結果からの行。

`LIMIT n, m` 最初のものを選択することができます `m` 最初の行をスキップした後の結果の行 `n` 行。 その `LIMIT m OFFSET n` 構文もサポートされています。

`n` と `m` 負でない整数でなければなりません。

がない場合 `ORDER BY` 結果を明示的にソートする句は、結果が任意で非決定的である可能性があります。

### UNION ALL句 {#union-all-clause}

UNION ALLを使用すると、任意の数のクエリを結合できます。 例えば:

``` sql
SELECT CounterID, 1 AS table, toInt64(count()) AS c
    FROM test.hits
    GROUP BY CounterID

UNION ALL

SELECT CounterID, 2 AS table, sum(Sign) AS c
    FROM test.visits
    GROUP BY CounterID
    HAVING c > 0
```

UNION ALLのみがサポートされます。 通常の共用体(UNION DISTINCT)はサポートされていません。 UNION DISTINCTが必要な場合は、UNION ALLを含むサブクエリからSELECT DISTINCTを書くことができます。

UNION ALLの一部であるクエリを同時に実行し、その結果を混在させることができます。

結果の構造(列の数と型)は、クエリに一致する必要があります。 しかし、列名は異なる場合があります。 この場合、最終的な結果の列名は最初のクエリから取得されます。 タイプ鋳造は連合のために行われます。 たとえば、結合されている二つのクエリが非同じフィールドを持つ場合-`Nullable` と `Nullable` 互換性のあるタイプのタイプ `UNION ALL` は `Nullable` タイプフィールド。

UNION ALLの一部であるクエリは、角かっこで囲むことはできません。 ORDER BYとLIMITは、最終結果ではなく、別々のクエリに適用されます。 最終結果に変換を適用する必要がある場合は、FROM句のサブクエリにUNION ALLを含むすべてのクエリを入れることができます。

### INTO OUTFILE句 {#into-outfile-clause}

を追加 `INTO OUTFILE filename` 指定されたファイルにクエリ出力をリダイレクトする句(filenameは文字列リテラル)。
MySQLとは対照的に、ファイルはクライアント側で作成されます。 同じファイル名のファイルが既に存在する場合、クエリは失敗します。
この機能は、コマンドラインクライアントとclickhouse-localで使用できます（HTTPインターフェイス経由で送信されるクエリは失敗します）。

デフォルトの出力形式はTabSeparatedです(コマンドラインクライアントバッチモードと同じです)。

### フォーマット句 {#format-clause}

指定 ‘FORMAT format’ 指定された形式のデータを取得する。
これは、便宜上、またはダンプを作成するために使用できます。
詳細については、以下を参照してください “Formats”.
これは、DBへのアクセスに使用される設定とインターフェイスの両方に依存します。 HTTPインターフェイスとバッチモードのコマンドラインクライアントの場合、デフォルトの形式はTabSeparatedです。 対話モードのコマンドラインクライアントの場合、デフォルトの形式はPrettyCompactです（魅力的でコンパクトな表があります）。

コマンドラインクライアントを使用する場合、データは内部の効率的な形式でクライアントに渡されます。 クライアントは、クエリのFORMAT句を独立して解釈し、データ自体をフォーマットします(したがって、ネットワークとサーバーを負荷から解放します)。

### 演算子の場合 {#select-in-operators}

その `IN`, `NOT IN`, `GLOBAL IN`、と `GLOBAL NOT IN` 事業者は、別途その機能はかなり豊富です。

演算子の左側は、単一の列またはタプルです。

例:

``` sql
SELECT UserID IN (123, 456) FROM ...
SELECT (CounterID, UserID) IN ((34, 123), (101500, 456)) FROM ...
```

左側がインデックス内の単一の列で、右側が定数のセットである場合、システムはクエリを処理するためにインデックスを使用します。

Don't list too many values explicitly (i.e. millions). If a data set is large, put it in a temporary table (for example, see the section “External data for query processing”)、サブクエリを使用します。

演算子の右側には、定数式のセット、定数式を持つタプルのセット(上の例で示します)、データベーステーブルの名前、または括弧で囲んだサブクエリのセッ

演算子の右側がテーブルの名前である場合(たとえば, `UserID IN users`)これはサブクエリと同じです `UserID IN (SELECT * FROM users)`. これは、クエリと共に送信される外部データを操作する場合に使用します。 たとえば、クエリは、ロードされたユーザIdのセットと一緒に送信することができます。 ‘users’ フィルタリングする必要がある一時テーブル。

演算子の右側が、Setエンジン(常にRAMにある準備済みデータ-セット)を持つテーブル名である場合、データ-セットはクエリごとに再作成されません。

サブクエリでは、タプルのフィルター処理に複数の列を指定できます。
例えば:

``` sql
SELECT (CounterID, UserID) IN (SELECT CounterID, UserID FROM ...) FROM ...
```

IN演算子の左と右の列は同じ型にする必要があります。

IN演算子およびサブクエリは、集計関数およびラムダ関数を含む、クエリの任意の部分で発生する可能性があります。
例えば:

``` sql
SELECT
    EventDate,
    avg(UserID IN
    (
        SELECT UserID
        FROM test.hits
        WHERE EventDate = toDate('2014-03-17')
    )) AS ratio
FROM test.hits
GROUP BY EventDate
ORDER BY EventDate ASC
```

``` text
┌──EventDate─┬────ratio─┐
│ 2014-03-17 │        1 │
│ 2014-03-18 │ 0.807696 │
│ 2014-03-19 │ 0.755406 │
│ 2014-03-20 │ 0.723218 │
│ 2014-03-21 │ 0.697021 │
│ 2014-03-22 │ 0.647851 │
│ 2014-03-23 │ 0.648416 │
└────────────┴──────────┘
```

月17th後の各日のために,月17日にサイトを訪問したユーザーによって行われたページビューの割合を数えます.
IN句のサブクエリは、常に単一のサーバーで一度だけ実行されます。 従属サブクエリはありません。

#### ヌル処理 {#null-processing-1}

要求の処理中にIN演算子は、次の条件を満たす操作の結果 [NULL](../syntax.md#null-literal) は常に等しい `0` かどうかにかかわらず `NULL` 演算子の右側または左側にあります。 `NULL` 値はどのデータセットにも含まれず、互いに対応せず、比較することもできません。

ここに例があります `t_null` テーブル:

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

クエリの実行 `SELECT x FROM t_null WHERE y IN (NULL,3)` あなたに次の結果を与えます:

``` text
┌─x─┐
│ 2 │
└───┘
```

あなたはその行を見ることができます `y = NULL` は、クエリの結果からスローされます。 これは、ClickHouseが `NULL` に含まれている `(NULL,3)` セット、戻り値 `0` 操作の結果として、 `SELECT` この行を最終出力から除外します。

``` sql
SELECT y IN (NULL, 3)
FROM t_null
```

``` text
┌─in(y, tuple(NULL, 3))─┐
│                     0 │
│                     1 │
└───────────────────────┘
```

#### 分散サブクエリ {#select-distributed-subqueries}

サブクエリを持つIN-sには二つのオプションがあります。 `IN` / `JOIN` と `GLOBAL IN` / `GLOBAL JOIN`. これらは、分散クエリ処理の実行方法が異なります。

!!! attention "注意"
    以下で説明するアル [設定](../../operations/settings/settings.md) `distributed_product_mode` 設定。

通常のINを使用すると、クエリはリモートサーバーに送信され、それぞれのサブクエリが実行されます。 `IN` または `JOIN` 句。

使用する場合 `GLOBAL IN` / `GLOBAL JOINs`、最初にすべての副照会はのために動きます `GLOBAL IN` / `GLOBAL JOINs` 結果は一時テーブルに収集されます。 次に、各リモートサーバーに一時テーブルが送信され、この一時データを使用してクエリが実行されます。

非分散クエリの場合は、regularを使用します `IN` / `JOIN`.

サブクエリを使用するときは注意してください。 `IN` / `JOIN` 分散クエリ処理のための句。

いくつかの例を見てみましょう。 クラスター内の各サーバーが正常 **local\_table**. 各サーバーには、 **distributed\_table** とテーブル **分散** これは、クラスタ内のすべてのサーバーを参照します。

クエリの場合 **distributed\_table** クエリはすべてのリモートサーバーに送信され、それらのサーバー上で実行されます。 **local\_table**.

たとえば、クエリ

``` sql
SELECT uniq(UserID) FROM distributed_table
```

すべてのリモートサーバーに送信されます。

``` sql
SELECT uniq(UserID) FROM local_table
```

そして、中間結果を組み合わせることができる段階に達するまで、それぞれを並行して実行します。 その後、中間結果が要求元サーバーに返され、その上にマージされ、最終的な結果がクライアントに送信されます。

次にINを使用してクエリを調べてみましょう:

``` sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM local_table WHERE CounterID = 34)
```

-   二つのサイトの観客の交差点の計算。

このクエリを送信すべてのリモートサーバーとして

``` sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM local_table WHERE CounterID = 34)
```

つまり、IN句のデータ-セットは、各サーバー上で独立して収集され、各サーバー上にローカルに格納されるデータ全体でのみ収集されます。

これは、このケースに備えられており、単一のユーザー Idのデータが単一のサーバー上に完全に存在するように、クラスタサーバーにデータを分散している場合に、正し この場合、必要なデータはすべて各サーバーでローカルに利用できます。 それ以外の場合、結果は不正確になります。 私たちは，シミュレーションをクエリとして “local IN”.

正方のクエリにしているときのデータはランダムにクラスタサーバを指定でき **distributed\_table** サブクエリ内。 クエリは次のようになります:

``` sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

このクエリを送信すべてのリモートサーバーとして

``` sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

サブクエリは、各リモートサーバーで実行を開始します。 サブクエリは分散テーブルを使用するため、各リモートサーバー上にあるサブクエリは、すべてのリモートサーバーに再送信されます。

``` sql
SELECT UserID FROM local_table WHERE CounterID = 34
```

たとえば、100台のサーバーのクラスターがある場合、クエリ全体を実行するには10,000個の基本要求が必要になります。

そのような場合は、常にIN代わりにGLOBAL INを使用する必要があります。 クエリでどのように動作するかを見てみましょう

``` sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID GLOBAL IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

リクエスタサーバはサブクエリを実行します

``` sql
SELECT UserID FROM distributed_table WHERE CounterID = 34
```

結果はRAMの一時テーブルに入れられます。 次に、要求は各リモートサーバーに次のように送信されます

``` sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID GLOBAL IN _data1
```

そして、一時テーブル `_data1` クエリを使用してすべてのリモートサーバーに送信されます(一時テーブルの名前は実装定義です)。

これは、通常のINを使用するよりも最適です。 ただし、次の点に留意してください:

1.  一時テーブルを作成する場合、データは一意になりません。 ネットワーク経由で送信されるデータの量を減らすには、サブクエリでDISTINCTを指定します。 （あなたは通常のためにこれを行う必要はありません。)
2.  一時テーブルは、すべてのリモートサーバーに送信されます。 送信はネットワークトポロジを考慮しません。 たとえば、リクエスタサーバに対して非常に離れたデータセンターに10台のリモートサーバーが存在する場合、データはチャネル経由でリモートデータセンターに10回送 をしないようにして大量のデータセット利用の場合グローバルです。
3.  発信する場合にはデータへのリモートサーバー、ネットワークの帯域幅は設定できます。 ネッ
4.  GLOBAL INを定期的に使用する必要がないように、サーバー間でデータを分散してみてください。
5.  グローバルを頻繁に使用する必要がある場合は、クエリを単一のデータセンター内で完全に処理できるように、レプリカの単一のグループが高速ネットワー

また、ローカルテーブルを指定することも意味があります。 `GLOBAL IN` 条項の場合には、この地方のテーブルのみの文章、映像、音声サーバーに使用したいデータからですることができます。

### 極値 {#extreme-values}

結果に加えて、結果列の最小値と最大値を取得することもできます。 これを行うには、 **極端な** 1に設定します。 数値型、日付、および時刻を含む日付について、最小値と最大値が計算されます。 その他の列では、既定値が出力されます。

An extra two rows are calculated – the minimums and maximums, respectively. These extra two rows are output in `JSON*`, `TabSeparated*`、と `Pretty*` [形式](../../interfaces/formats.md)、他の行とは別に。 他の形式では出力されません。

で `JSON*` フォーマットでは、極値は別の形式で出力されます。 ‘extremes’ フィールド。 で `TabSeparated*` 書式、行は主な結果の後に来る、と後 ‘totals’ 存在する場合。 これは、（他のデータの後に）空の行が先行しています。 で `Pretty*` この行は、メインの結果の後に別のテーブルとして出力されます。 `totals` 存在する場合。

極端な値は、前の行について計算されます `LIMIT`,しかし、後に `LIMIT BY`. ただし、使用する場合 `LIMIT offset, size`、前の行 `offset` に含まれています `extremes`. ストリーム要求では、結果には、通過した少数の行が含まれることもあります `LIMIT`.

### 備考 {#notes}

その `GROUP BY` と `ORDER BY` 句は定位置引数をサポートしません。 これはMySQLと矛盾しますが、標準SQLに準拠しています。
例えば, `GROUP BY 1, 2` will be interpreted as grouping by constants (i.e. aggregation of all rows into one).

同義語を使用できます (`AS` クエリの任意の部分でエイリアス)。

式の代わりに、クエリの任意の部分にアスタリスクを付けることができます。 クエリが分析されると、アスタリスクはすべてのテーブルの列のリストに展開されます。 `MATERIALIZED` と `ALIAS` 列）。 アスタリスクの使用が正当化されるケースはほんのわずかです:

-   テーブルダンプを作成するとき。
-   システムテーブルなど、少数の列だけを含むテーブルの場合。
-   テーブル内の列に関する情報を取得するためのものです。 この場合、 `LIMIT 1`. しかし、それを使用する方が良いです `DESC TABLE` クエリ。
-   少数のコラムの強いろ過がを使用してある時 `PREWHERE`.
-   サブクエリでは(外部クエリに必要のない列はサブクエリから除外されるため)。

他のすべてのケースでは、利点の代わりに柱状DBMSの欠点しか与えないので、アスタリスクを使用することはお勧めしません。 つまり、アスタリスクを使用することはお勧めしません。

[元の記事](https://clickhouse.tech/docs/en/query_language/select/) <!--hide-->
