---
description: '継続的に変化するオブジェクトの状態をすばやく書き込み、
  古いオブジェクトの状態をバックグラウンドで削除できます。'
sidebar_label: 'VersionedCollapsingMergeTree'
sidebar_position: 80
slug: /engines/table-engines/mergetree-family/versionedcollapsingmergetree
title: 'VersionedCollapsingMergeTree テーブルエンジン'
doc_type: 'reference'
---

このエンジンには、次の特徴があります。

* 継続的に変化するオブジェクトの状態をすばやく書き込めます。
* 古いオブジェクトの状態をバックグラウンドで削除します。これにより、ストレージ使用量を大幅に削減できます。

詳細については、[折りたたみ](#table_engines_versionedcollapsingmergetree) のセクションを参照してください。

このエンジンは [MergeTree](/ja/engines/table-engines/mergetree-family/mergetree) を継承し、データパーツのマージアルゴリズムに行を折りたたむロジックを追加したものです。`VersionedCollapsingMergeTree` は [CollapsingMergeTree](../../../engines/table-engines/mergetree-family/collapsingmergetree.md) と同じ目的で使用されますが、異なる折りたたみアルゴリズムを採用しているため、複数のスレッドで任意の順序でデータを挿入できます。特に、`Version` カラムは、誤った順序で挿入された場合でも、行を適切に折りたためるようにします。一方、`CollapsingMergeTree` では、厳密に連続した順序での挿入しかできません。

<div id="creating-a-table">
  ## テーブルの作成
</div>

```sql
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

<div id="engine-parameters">
  ### エンジンパラメータ
</div>

```sql
VersionedCollapsingMergeTree(sign, version)
```

| パラメータ     | 説明                                                                     | 型                                                                                                                                                                                                                                                                                               |
| --------- | ---------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `sign`    | 行の種類を示すカラム名です。`1` は 状態行、`-1` は 取消行です。 | [`Int8`](/ja/sql-reference/data-types/int-uint)                                                                                                                                                                                                                                                    |
| `version` | オブジェクトの state のバージョンを示すカラム名です。                                         | [`Int*`](/ja/sql-reference/data-types/int-uint), [`UInt*`](/ja/sql-reference/data-types/int-uint), [`Date`](/ja/sql-reference/data-types/date), [`Date32`](/ja/sql-reference/data-types/date32), [`DateTime`](/ja/sql-reference/data-types/datetime), または [`DateTime64`](/ja/sql-reference/data-types/datetime64) |

<div id="query-clauses">
  ### クエリ句
</div>

`VersionedCollapsingMergeTree` テーブルの作成時には、`MergeTree` テーブルの作成時と同じ [句](../../../engines/table-engines/mergetree-family/mergetree.md) を指定する必要があります。

<details markdown="1">
  <summary>非推奨のテーブル作成方法</summary>

  :::note
  新しいプロジェクトではこの方法を使用しないでください。可能であれば、既存のプロジェクトも上記の方法に切り替えてください。
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] VersionedCollapsingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, sign, version)
  ```

  `sign` と `version` を除くすべてのパラメーターは、`MergeTree` の場合と同じ意味です。

  * `sign` — 行の種類を表すカラム名です。`1` は「状態行」、`-1` は「取消行」です。

    カラムのデータ型 — `Int8`。

  * `version` — オブジェクトの状態のバージョンを表すカラム名です。

    カラムのデータ型は `UInt*` である必要があります。
</details>

<div id="table_engines_versionedcollapsingmergetree">
  ## 折りたたみ
</div>

<div id="data">
  ### データ
</div>

あるオブジェクトについて、継続的に変化するデータを保存する必要があるケースを考えてみましょう。オブジェクトごとに 1 行を持ち、変更があるたびにその行を更新するのは自然です。しかし、更新操作ではストレージ上のデータを書き換える必要があるため、DBMS にとっては高コストで低速です。すばやくデータを書き込む必要がある場合、更新は適していません。一方で、オブジェクトへの変更は次のように順次書き込めます。

行を書き込む際には `Sign` カラムを使用します。`Sign = 1` の場合、その行はオブジェクトの状態を表します (これを 状態行と呼ぶことにします) 。`Sign = -1` の場合、同じ属性を持つオブジェクトの状態を打ち消すことを示します (これを 取消行と呼ぶことにします) 。また、`Version` カラムも使用します。これは、オブジェクトの各状態をそれぞれ別の番号で識別するためのものです。

たとえば、あるサイトでユーザーが何ページ訪問し、そこにどれくらい滞在したかを計算したいとします。ある時点で、ユーザーのアクティビティの状態として次の行を書き込みます。

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

その後、ユーザーアクティビティの変更を記録し、次の2行を書き込みます。

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
│ 4324182021466249494 │         6 │      185 │    1 │       2 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

最初の行は、object (user) の前の state を取り消します。`Sign` を除き、取り消される state のすべてのフィールドをコピーする必要があります。

2 行目には現在の state が入ります。

必要なのは user activity の最後の state だけなので、これらの行は

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

は削除でき、オブジェクトの無効な (古い) 状態が折りたたまれます。`VersionedCollapsingMergeTree` は、データパーツのマージ時にこれを行います。

変更ごとに 2 行が必要な理由については、[アルゴリズム](#table_engines-versionedcollapsingmergetree-algorithm)を参照してください。

**使用上の注意**

1. データを書き込むプログラムは、オブジェクトをキャンセルできるよう、その状態を記憶しておく必要があります。&quot;Cancel&quot; 文字列には、主キーフィールドのコピー、&quot;state&quot; 文字列のバージョン、および逆の `Sign` を含める必要があります。これによりストレージの初期サイズは増えますが、データを高速に書き込めます。
2. カラム内で長く増大する配列は、書き込み時の負荷によってエンジンの効率を低下させます。データ構造が単純であるほど、効率は高くなります。
3. `SELECT` の結果は、オブジェクトの変更履歴の整合性に大きく依存します。挿入するデータを準備する際は、正確に行ってください。データに不整合があると、セッション深度のような非負のメトリクスに負の値が現れるなど、予測できない結果になることがあります。

<div id="table_engines-versionedcollapsingmergetree-algorithm">
  ### アルゴリズム
</div>

ClickHouse がデータパーツをマージする際、同じ主キーとバージョンを持ち、`Sign` が異なる行のペアは削除されます。行の順序は関係ありません。

ClickHouse がデータを挿入する際、行は主キー順に並べられます。`Version` カラムが主キーに含まれていない場合、ClickHouse はそれを暗黙的に主キーの最後のフィールドとして追加し、並べ替えに使用します。

<div id="selecting-data">
  ## データの選択
</div>

ClickHouse は、同じ主キーを持つすべての行が、結果として同じデータパーツに含まれることや、同じ物理サーバー上に存在することを保証しません。これは、データの書き込み時にも、その後のデータパーツのマージ時にも当てはまります。さらに、ClickHouse は `SELECT` クエリを複数のスレッドで処理するため、結果内の行の順序を予測できません。つまり、`VersionedCollapsingMergeTree` テーブルから完全に「折りたたみ」されたデータを取得するには、集約が必要です。

折りたたみを確定するには、符号を考慮した `GROUP BY` 句と集約関数を使ったクエリを記述します。たとえば、件数を計算するには `count()` の代わりに `sum(Sign)` を使用します。何らかの合計値を計算するには、`sum(x)` の代わりに `sum(Sign * x)` を使用し、さらに `HAVING sum(Sign) > 0` を追加します。

集約関数 `count`、`sum`、`avg` はこの方法で計算できます。集約関数 `uniq` は、オブジェクトに少なくとも 1 つの未折りたたみの状態がある場合に計算できます。集約関数 `min` と `max` は、`VersionedCollapsingMergeTree` が折りたたまれた状態の値の履歴を保存しないため、計算できません。

集約は行わずに「折りたたみ」されたデータを抽出する必要がある場合 (たとえば、最新の値が特定の条件に一致する行が存在するかどうかを確認する場合) は、`FROM` 句に `FINAL` 修飾子を使用できます。この方法は非効率であり、大きなテーブルには使用すべきではありません。

<div id="example-of-use">
  ## 使用例
</div>

サンプルデータ:

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
│ 4324182021466249494 │         6 │      185 │    1 │       2 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

テーブルの作成:

```sql
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

データを挿入します：

```sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, 1, 1)
```

```sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, -1, 1),(4324182021466249494, 6, 185, 1, 2)
```

2 つの異なるデータパーツを作成するために、`INSERT` クエリを 2 回使用します。データを 1 つのクエリで挿入すると、ClickHouse は 1 つのデータパーツしか作成せず、マージは行われません。

データの取得:

```sql
SELECT * FROM UAct
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │   -1 │       1 │
│ 4324182021466249494 │         6 │      185 │    1 │       2 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

ここで何が起きていて、折りたたまれたパーツはどこにあるのでしょうか。
2 つの `INSERT` クエリによって、2 つのデータパーツが作成されました。`SELECT` クエリは 2 つのスレッドで実行されたため、結果の行順はランダムになります。
データパーツがまだマージされていないため、折りたたみは発生していません。ClickHouse は、予測できない不定のタイミングでデータパーツをマージします。

そのため、集約が必要になります:

```sql
SELECT
    UserID,
    sum(PageViews * Sign) AS PageViews,
    sum(Duration * Sign) AS Duration,
    Version
FROM UAct
GROUP BY UserID, Version
HAVING sum(Sign) > 0
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Version─┐
│ 4324182021466249494 │         6 │      185 │       2 │
└─────────────────────┴───────────┴──────────┴─────────┘
```

集約が不要で、折りたたみを強制したい場合は、`FROM` 句で `FINAL` 修飾子を使用できます。

```sql
SELECT * FROM UAct FINAL
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         6 │      185 │    1 │       2 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

これはデータを取得するうえで非常に非効率的な方法です。大規模なテーブルでは使用しないでください。