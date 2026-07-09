---
description: 'MergeTree を継承しつつ、マージ処理中に行を折りたたむロジックを追加したものです。'
keywords: ['更新', '折りたたみ']
sidebar_label: 'CollapsingMergeTree'
sidebar_position: 70
slug: /engines/table-engines/mergetree-family/collapsingmergetree
title: 'CollapsingMergeTree テーブルエンジン'
doc_type: 'guide'
---

<div id="description">
  ## 説明
</div>

`CollapsingMergeTree` エンジンは [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md) を継承しており、
マージ処理中に行を折りたたむロジックが追加されています。
`CollapsingMergeTree` テーブルエンジンは、
ソートキー (`ORDER BY`) 内のすべてのフィールドが特別なフィールド `Sign` を除いて同じで、
その `Sign` が `1` または `-1` のいずれかである場合、
その行のペアを非同期に削除 (折りたたみ) します。
反対の値を持つ `Sign` のペアがない行は保持されます。

詳細については、このドキュメントの [Collapsing](#table_engine-collapsingmergetree-collapsing) セクションを参照してください。

:::note
このエンジンにより、ストレージ使用量が大幅に削減され、
その結果、`SELECT` クエリの効率が向上する可能性があります。
:::

<div id="parameters">
  ## パラメーター
</div>

このテーブルエンジンのすべてのパラメーターは、`Sign` パラメーターを除き、
[`MergeTree`](/ja/engines/table-engines/mergetree-family/mergetree) のものと同じ意味です。

* `Sign` — `1` が「状態行」、`-1` が「取消行」を表す行種別のカラム名です。型: [Int8](/ja/sql-reference/data-types/int-uint).

<div id="creating-a-table">
  ## テーブルの作成
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) 
ENGINE = CollapsingMergeTree(Sign)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

<details markdown="1">
  <summary>テーブルを作成するための非推奨の方法</summary>

  :::note
  以下の方法を新しいプロジェクトで使用することは推奨されません。
  可能であれば、古いプロジェクトは新しい方法を使用するよう更新することをお勧めします。
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) 
  ENGINE [=] CollapsingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, Sign)
  ```

  `Sign` — `1` が状態行、`-1` が取消行であることを示す、行の種類を表すカラム名です。[Int8](/ja/sql-reference/data-types/int-uint)。
</details>

* クエリパラメータについては、[クエリの説明](../../../sql-reference/statements/create/table.md)を参照してください。
* `CollapsingMergeTree` テーブルの作成時には、`MergeTree` テーブルの作成時と同じ[クエリ句](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)が必要です。

<div id="table_engine-collapsingmergetree-collapsing">
  ## 折りたたみ
</div>

<div id="data">
  ### データ
</div>

あるオブジェクトについて、継続的に変化するデータを保存する必要がある状況を考えてみましょう。
オブジェクトごとに 1 行を持たせ、何かが変わるたびにそれを更新するのが理にかなっているように思えるかもしれませんが、
更新操作ではストレージ上のデータを書き換える必要があるため、DBMS にとって高コストで低速です。
データを高速に書き込む必要がある場合、大量の更新を行う方法は現実的ではありません。
一方で、オブジェクトの変更は順次書き込んでいくことができます。
そのために、特別なカラム `Sign` を使用します。

* `Sign` = `1` の場合、その行は状態行を意味します。つまり、*現在の有効な状態を表すフィールドを含む行*です。
* `Sign` = `-1` の場合、その行は取消行を意味します。つまり、*同じ属性を持つオブジェクトの状態を取り消すために使用される行*です。

たとえば、ある Web サイトで、ユーザーが閲覧したページ数と各ページの滞在時間を計算したいとします。
ある時点で、ユーザーのアクティビティの状態として次の行を書き込みます。

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

さらに後の時点で、ユーザーアクティビティの変化を記録し、次の2行を書き込みます。

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

最初の行は、オブジェクト (この場合はユーザーを表します) の直前の state を打ち消します。
`Sign` を除き、&quot;canceled&quot; 行のすべてのソートキー フィールドをコピーする必要があります。
上の2 行目には現在の state が含まれています。

必要なのはユーザーアクティビティの最新の state だけなので、元の状態行と、挿入した取消行
行は、以下に示すように削除できます。これにより、オブジェクトの無効な (古い) state が折りたたまれます。

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │ -- old "state" row can be deleted
│ 4324182021466249494 │         5 │      146 │   -1 │ -- "cancel" row can be deleted
│ 4324182021466249494 │         6 │      185 │    1 │ -- new "state" row remains
└─────────────────────┴───────────┴──────────┴──────┘
```

`CollapsingMergeTree` は、データパーツのマージ時に、まさにこの*折りたたみ*動作を実行します。

:::note
変更ごとに 2 行が必要な理由については、
[Algorithm](#table_engine-collapsingmergetree-collapsing-algorithm) の段落でさらに説明しています。
:::

**このようなアプローチの特性**

1. データを書き込むプログラムは、オブジェクトを打ち消せるように、その状態を覚えておく必要があります。取消行には、状態行のソートキー フィールドのコピーと、逆の `Sign` を含める必要があります。これによりストレージの初期サイズは増えますが、データを高速に書き込めます。
2. カラム内の肥大化した長い配列は、書き込み負荷の増加により、エンジンの効率を低下させます。データは単純であるほど効率が高くなります。
3. `SELECT` の結果は、オブジェクトの変更履歴の整合性に大きく左右されます。insert 用のデータを準備する際は、正確に行ってください。データに不整合があると、予測不能な結果になることがあります。たとえば、セッションの深さのような非負のメトリクスに負の値が入ることがあります。

<div id="table_engine-collapsingmergetree-collapsing-algorithm">
  ### アルゴリズム
</div>

ClickHouse がデータ[パーツ](/ja/concepts/glossary#parts)をマージすると、
同じソートキー (`ORDER BY`) を持つ連続した各行グループは、最大でも 2 行までに減らされます。
それは、`Sign` = `1` の「状態行」と、`Sign` = `-1` の「取消行」です。
つまり、ClickHouse ではエントリが折りたたまれます。

結果として得られる各データパーツについて、ClickHouse は次を保存します。

|    |                                                        |
| -- | ------------------------------------------------------ |
| 1. | 「状態行」と「取消行」の数が一致し、かつ最後の行が「状態行」である場合、最初の「取消行」と最後の「状態行」。 |
| 2. | 「状態行」の数が「取消行」の数より多い場合、最後の「状態行」。                        |
| 3. | 「取消行」の数が「状態行」の数より多い場合、最初の「取消行」。                        |
| 4. | その他すべての場合、どの行も保存しません。                                  |

さらに、「状態行」が「取消行」より少なくとも 2 行多い場合、
または「取消行」が「状態行」より少なくとも 2 行多い場合でも、マージは続行されます。
ただし ClickHouse はこの状況を論理エラーとして扱い、サーバーログに記録します。
このエラーは、同じデータが複数回挿入された場合に発生することがあります。
したがって、折りたたみによって統計計算の結果が変わることはありません。
変更は徐々に折りたたまれ、最終的にはほぼすべてのオブジェクトについて最新の状態だけが残ります。

`Sign` カラムが必要なのは、マージアルゴリズムでは
同じソートキーを持つすべての行が、結果として得られる同じデータパーツ内、さらには同じ物理サーバー上に配置されることまで保証されないためです。
ClickHouse は複数のスレッドで `SELECT` クエリを処理するため、結果内の行の順序を予測できません。

`CollapsingMergeTree` テーブルから完全に「折りたたまれた」データを取得する必要がある場合は、集約が必要です。
折りたたみを完了するには、`GROUP BY` 句と、符号を考慮した集約関数を使ったクエリを記述します。
たとえば件数を計算するには、`count()` の代わりに `sum(Sign)` を使います。
何らかの合計を計算するには、以下の[例](#example-of-use)のように、`sum(x)` の代わりに `sum(Sign * x)` を `HAVING sum(Sign) > 0` と組み合わせて使います。

この方法で `count`、`sum`、`avg` の集約を計算できます。
オブジェクトに少なくとも 1 つの未折りたたみの状態があれば、`uniq` の集約も計算できます。
`min` と `max` の集約は計算できません。
これは `CollapsingMergeTree` が折りたたまれた状態の履歴を保存しないためです。

:::note
集約せずにデータを取り出す必要がある場合は
(たとえば、最新の値が特定の条件に一致する行が存在するかどうかを確認する場合) 、
`FROM` 句で [`FINAL`](../../../sql-reference/statements/select/from.md#final-modifier) modifier を使用できます。結果を返す前にデータがマージされます。
CollapsingMergeTree では、各キーについて最新の状態行のみが返されます。
:::

<div id="examples">
  ## 例
</div>

<div id="example-of-use">
  ### 使用例
</div>

次のサンプルデータを使用します。

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

`CollapsingMergeTree` を使って、テーブル `UAct` を作成します。

```sql
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

次に、データをいくつか挿入します。

```sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, 1)
```

```sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, -1),(4324182021466249494, 6, 185, 1)
```

2つの異なるデータパーツを作成するために、`INSERT`クエリを2つ使用します。

:::note
単一のクエリでデータを挿入すると、ClickHouse はデータパーツを1つしか作成せず、その後マージは一切実行されません。
:::

次のようにしてデータを選択できます。

```sql
SELECT * FROM UAct
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

上で返されたデータを見て、折りたたみが発生したかどうかを確認してみましょう...
2 つの `INSERT` クエリによって、2 つのデータパーツが作成されました。
`SELECT` クエリは 2 つのスレッドで実行されたため、行の順序はランダムになりました。
しかし、データパーツのマージがまだ行われていなかったため、折りたたみは **発生しませんでした**。
また、ClickHouse はデータパーツをバックグラウンドで、予測できないタイミングでマージします。

そのため、集約が必要です。
これは [`sum`](/ja/sql-reference/aggregate-functions/reference/sum)
集約関数と [`HAVING`](/ja/sql-reference/statements/select/having) 句を使って行います。

```sql
SELECT
    UserID,
    sum(PageViews * Sign) AS PageViews,
    sum(Duration * Sign) AS Duration
FROM UAct
GROUP BY UserID
HAVING sum(Sign) > 0
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┐
│ 4324182021466249494 │         6 │      185 │
└─────────────────────┴───────────┴──────────┘
```

集約が不要で、折りたたみを強制したい場合は、`FROM` 句で `FINAL` 修飾子を使用することもできます。

```sql
SELECT * FROM UAct FINAL
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

:::note
この方法でデータを選択するのは効率が低く、大量のスキャン対象データ (数百万行) がある場合には推奨されません。
:::

<div id="example-of-another-approach">
  ### 別のアプローチの例
</div>

このアプローチのポイントは、マージで考慮されるのがキーフィールドのみであることです。
そのため、&quot;cancel&quot; の行では負の値を指定でき、
`Sign` カラムを使わずに合計した際に前のバージョンの行を相殺できます。

この例では、以下のサンプルデータを使用します。

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │        -5 │     -146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

この方法では、負の値を格納できるように `PageViews` と `Duration` のデータ型を変更する必要があります。
そのため、`collapsingMergeTree` を使用してテーブル `UAct` を作成する際に、これらのカラムの型を `UInt8` から `Int16` に変更します。

```sql
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

テーブルにデータを挿入して、この方法を試してみましょう。

ただし、サンプルや小規模なテーブルであれば、これでも問題ありません：

```sql
INSERT INTO UAct VALUES(4324182021466249494,  5,  146,  1);
INSERT INTO UAct VALUES(4324182021466249494, -5, -146, -1);
INSERT INTO UAct VALUES(4324182021466249494,  6,  185,  1);

SELECT * FROM UAct FINAL;
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

```sql
SELECT
    UserID,
    sum(PageViews) AS PageViews,
    sum(Duration) AS Duration
FROM UAct
GROUP BY UserID
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┐
│ 4324182021466249494 │         6 │      185 │
└─────────────────────┴───────────┴──────────┘
```

```sql
SELECT COUNT() FROM UAct
```

```text
┌─count()─┐
│       3 │
└─────────┘
```

```sql
OPTIMIZE TABLE UAct FINAL;

SELECT * FROM UAct
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```