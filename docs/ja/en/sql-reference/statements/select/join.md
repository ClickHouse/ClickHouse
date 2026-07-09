---
description: 'JOIN 句のドキュメント'
sidebar_label: 'JOIN'
slug: /sql-reference/statements/select/join
title: 'JOIN 句'
keywords: ['INNER JOIN', 'LEFT JOIN', 'LEFT OUTER JOIN', 'RIGHT JOIN', 'RIGHT OUTER JOIN', 'FULL OUTER JOIN', 'CROSS JOIN', 'LEFT SEMI JOIN', 'RIGHT SEMI JOIN', 'LEFT ANTI JOIN', 'RIGHT ANTI JOIN', 'LEFT ANY JOIN', 'RIGHT ANY JOIN', 'INNER ANY JOIN', 'ASOF JOIN', 'LEFT ASOF JOIN', 'PASTE JOIN', 'NATURAL JOIN']
doc_type: 'reference'
---

`JOIN` 句は、共通する値を使って 1 つまたは複数のテーブルのカラムを結合し、新しいテーブルを生成します。これは SQL をサポートするデータベースで一般的な操作であり、[関係代数](https://en.wikipedia.org/wiki/Relational_algebra#Joins_and_join-like_operators) における join に対応します。1 つのテーブルをそれ自身と結合する特殊なケースは、一般に &quot;self-join&quot; と呼ばれます。

**構文**

```sql
SELECT <expr_list>
FROM <left_table>
[GLOBAL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI|ANY|ALL|ASOF] JOIN <right_table>
(ON <expr_list>)|(USING <column_list>) ...
```

`ON`句の式と`USING`句のカラムは「結合キー」と呼ばれます。特に明記されていない限り、`JOIN`では「結合キー」が一致する行の[デカルト積](https://en.wikipedia.org/wiki/Cartesian_product)が生成されるため、元のテーブルよりもはるかに多くの行を含む結果になることがあります。

<div id="supported-types-of-join">
  ## サポートされている JOIN の種類
</div>

標準的な [SQL JOIN](https://en.wikipedia.org/wiki/Join_\(SQL\)) の種類はすべてサポートされています。

| Type               | Description                                                                                                                                                                  |
| ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `INNER JOIN`       | 一致する行のみが返されます。                                                                                                                                                               |
| `LEFT OUTER JOIN`  | 一致する行に加えて、左テーブルの一致しない行も返されます。                                                                                                                                                |
| `RIGHT OUTER JOIN` | 一致する行に加えて、右テーブルの一致しない行も返されます。                                                                                                                                                |
| `FULL OUTER JOIN`  | 一致する行に加えて、両方のテーブルの一致しない行も返されます。                                                                                                                                              |
| `CROSS JOIN`       | テーブル全体のデカルト積を生成し、「結合キー」は指定しません。                                                                                                                                              |
| `NATURAL JOIN`     | 両方のテーブルで同じ名前を持つすべてのカラムを使って自動的に結合します。各共通カラムは結果に1回だけ現れます。`INNER` (デフォルト) 、`LEFT`、`RIGHT`、`FULL` の各バリアントをサポートします。これは `JOIN ... USING (col1, col2, ...)` と等価で、カラムの一覧は自動的に導出されます。 |

* 種類を指定しない `JOIN` は `INNER` を意味します。
* キーワード `OUTER` は省略できます。
* `CROSS JOIN` の別構文として、[`FROM` clause](../../../sql-reference/statements/select/from.md) で複数のテーブルをカンマ区切りで指定できます。
* `NATURAL JOIN` に一致するカラムがない場合は、`CROSS JOIN` と同様に動作します。

ClickHouse で使用できる追加の JOIN の種類は次のとおりです。

| Type                                                | Description                                                                                         |
| --------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `LEFT SEMI JOIN`, `RIGHT SEMI JOIN`                 | デカルト積を生成せずに、「結合キー」に対する許可リストとして機能します。                                                                |
| `LEFT ANTI JOIN`, `RIGHT ANTI JOIN`                 | デカルト積を生成せずに、「結合キー」に対する拒否リストとして機能します。                                                                |
| `LEFT ANY JOIN`, `RIGHT ANY JOIN`, `INNER ANY JOIN` | 標準の `JOIN` 種類に対して、デカルト積を部分的に (`LEFT` および `RIGHT` の反対側について) または完全に (`INNER` および `FULL` について) 無効化します。 |
| `ASOF JOIN`, `LEFT ASOF JOIN`                       | 完全一致ではない条件で数列を結合します。`ASOF JOIN` の使用方法は以下で説明します。                                                     |
| `PASTE JOIN`                                        | 2つのテーブルを水平方向に連結します。                                                                                 |

:::note
[join&#95;algorithm](../../../operations/settings/settings.md#join_algorithm) が `partial_merge` に設定されている場合、`RIGHT JOIN` と `FULL JOIN` は strictness が `ALL` の場合にのみサポートされます (`SEMI`、`ANTI`、`ANY`、`ASOF` はサポートされません) 。
:::

<div id="settings">
  ## 設定
</div>

デフォルトの JOIN タイプは、[`join_default_strictness`](../../../operations/settings/settings.md#join_default_strictness) 設定で上書きできます。

`ANY JOIN` 操作における ClickHouse server の動作は、[`any_join_distinct_right_table_keys`](../../../operations/settings/settings.md#any_join_distinct_right_table_keys) 設定に依存します。

**関連項目**

* [`join_algorithm`](../../../operations/settings/settings.md#join_algorithm)
* [`join_any_take_last_row`](../../../operations/settings/settings.md#join_any_take_last_row)
* [`join_use_nulls`](../../../operations/settings/settings.md#join_use_nulls)
* [`partial_merge_join_rows_in_right_blocks`](../../../operations/settings/settings.md#partial_merge_join_rows_in_right_blocks)
* [`join_on_disk_max_files_to_merge`](../../../operations/settings/settings.md#join_on_disk_max_files_to_merge)
* [`any_join_distinct_right_table_keys`](../../../operations/settings/settings.md#any_join_distinct_right_table_keys)

ClickHouse が `CROSS JOIN` を `INNER JOIN` に書き換えられなかった場合の動作は、`cross_to_inner_join_rewrite` 設定で指定します。デフォルト値は `1` で、この場合は JOIN を継続しますが、処理は遅くなります。エラーを発生させる場合は `cross_to_inner_join_rewrite` を `0` に設定し、cross join を実行せず、代わりにすべてのカンマ区切り/cross join の書き換えを強制する場合は `2` に設定します。値が `2` のときに書き換えに失敗すると、&quot;Please, try to simplify `WHERE` section&quot; というエラーメッセージが表示されます。

<div id="on-section-conditions">
  ## ON セクション内の条件
</div>

`ON` セクションには、`AND` 演算子や `OR` 演算子で組み合わせた複数の条件を含めることができます。結合キーを指定する条件は、次を満たしている必要があります。

* 左テーブルと右テーブルの両方を参照していること
* 等価演算子を使用していること

それ以外の条件では、ほかの論理演算子を使用できますが、クエリの左テーブルまたは右テーブルのいずれか一方を参照している必要があります。

複合条件全体が満たされると、行が結合されます。条件が満たされない場合でも、`JOIN` の種類によっては行が結果に含まれることがあります。なお、同じ条件を `WHERE` セクションに置いた場合、それらが満たされなければ、行は常に結果から除外されます。

`ON` 句内の `OR` 演算子は、ハッシュ結合アルゴリズムを使って動作します。`JOIN` の結合キーを含む `OR` の各引数ごとに個別のハッシュテーブルが作成されるため、メモリ使用量とクエリ実行時間は、`ON` 句内の `OR` 式の数が増えるのに比例して増加します。

:::note
条件が異なるテーブルのカラムを参照している場合、現時点でサポートされているのは等価演算子 (`=`) のみです。
:::

**例**

`table_1` と `table_2` について考えます。

```response
┌─Id─┬─name─┐     ┌─Id─┬─text───────────┬─scores─┐
│  1 │ A    │     │  1 │ Text A         │     10 │
│  2 │ B    │     │  1 │ Another text A │     12 │
│  3 │ C    │     │  2 │ Text B         │     15 │
└────┴──────┘     └────┴────────────────┴────────┘
```

1 つの結合キー条件と、`table_2` に対する追加条件を含むクエリ:

```sql title="Query"
SELECT name, text FROM table_1 LEFT OUTER JOIN table_2
    ON table_1.Id = table_2.Id AND startsWith(table_2.text, 'Text');
```

結果には、名前が `C` で text カラムが空の行が含まれている点に注意してください。これは、`OUTER` 型の join を使用しているためです。

```response title="Response"
┌─name─┬─text───┐
│ A    │ Text A │
│ B    │ Text B │
│ C    │        │
└──────┴────────┘
```

`INNER`型のJOINと複数の条件を使用したクエリ:

```sql title="Query"
SELECT name, text, scores FROM table_1 INNER JOIN table_2
    ON table_1.Id = table_2.Id AND table_2.scores > 10 AND startsWith(table_2.text, 'Text');
```

```sql title="Response"
┌─name─┬─text───┬─scores─┐
│ B    │ Text B │     15 │
└──────┴────────┴────────┘
```

`INNER` タイプの join と `OR` 条件を使用したクエリ:

```sql title="Query"
CREATE TABLE t1 (`a` Int64, `b` Int64) ENGINE = MergeTree() ORDER BY a;

CREATE TABLE t2 (`key` Int32, `val` Int64) ENGINE = MergeTree() ORDER BY key;

INSERT INTO t1 SELECT number as a, -a as b from numbers(5);

INSERT INTO t2 SELECT if(number % 2 == 0, toInt64(number), -number) as key, number as val from numbers(5);

SELECT a, b, val FROM t1 INNER JOIN t2 ON t1.a = t2.key OR t1.b = t2.key;
```

```response title="Response"
┌─a─┬──b─┬─val─┐
│ 0 │  0 │   0 │
│ 1 │ -1 │   1 │
│ 2 │ -2 │   2 │
│ 3 │ -3 │   3 │
│ 4 │ -4 │   4 │
└───┴────┴─────┘
```

`INNER` 型の JOIN で、条件に `OR` と `AND` を含むクエリ:

:::note

デフォルトでは、非等価条件は、同じテーブルのカラムを使用している場合に限りサポートされます。
たとえば、`t1.a = t2.key AND t1.b > 0 AND t2.b > t2.c` はサポートされます。これは、`t1.b > 0` では `t1` のカラムのみを使用し、`t2.b > t2.c` では `t2` のカラムのみを使用しているためです。
ただし、`t1.a = t2.key AND t1.b > t2.key` のような条件に対する実験的サポートを試すこともできます。詳細は以下のセクションを参照してください。

:::

```sql title="Query"
SELECT a, b, val FROM t1 INNER JOIN t2 ON t1.a = t2.key OR t1.b = t2.key AND t2.val > 3;
```

```response title="Response"
┌─a─┬──b─┬─val─┐
│ 0 │  0 │   0 │
│ 2 │ -2 │   2 │
│ 4 │ -4 │   4 │
└───┴────┴─────┘
```

<div id="join-with-inequality-conditions-for-columns-from-different-tables">
  ## 異なるテーブルのカラムに対する不等条件付き JOIN
</div>

ClickHouse は現在、等価条件に加えて不等条件を含む `ALL/ANY/SEMI/ANTI INNER/LEFT/RIGHT/FULL JOIN` をサポートしています。不等条件に対応しているのは、`hash` および `grace_hash` JOIN アルゴリズムのみです。`join_use_nulls` と併用する場合、不等条件はサポートされません。

**例**

テーブル `t1`:

```response
┌─key──┬─attr─┬─a─┬─b─┬─c─┐
│ key1 │ a    │ 1 │ 1 │ 2 │
│ key1 │ b    │ 2 │ 3 │ 2 │
│ key1 │ c    │ 3 │ 2 │ 1 │
│ key1 │ d    │ 4 │ 7 │ 2 │
│ key1 │ e    │ 5 │ 5 │ 5 │
│ key2 │ a2   │ 1 │ 1 │ 1 │
│ key4 │ f    │ 2 │ 3 │ 4 │
└──────┴──────┴───┴───┴───┘
```

テーブル `t2`

```response
┌─key──┬─attr─┬─a─┬─b─┬─c─┐
│ key1 │ A    │ 1 │ 2 │ 1 │
│ key1 │ B    │ 2 │ 1 │ 2 │
│ key1 │ C    │ 3 │ 4 │ 5 │
│ key1 │ D    │ 4 │ 1 │ 6 │
│ key3 │ a3   │ 1 │ 1 │ 1 │
│ key4 │ F    │ 1 │ 1 │ 1 │
└──────┴──────┴───┴───┴───┘
```

```sql
SELECT t1.*, t2.* FROM t1 LEFT JOIN t2 ON t1.key = t2.key AND (t1.a < t2.a) ORDER BY (t1.key, t1.attr, t2.key, t2.attr);
```

```response
key1    a    1    1    2    key1    B    2    1    2
key1    a    1    1    2    key1    C    3    4    5
key1    a    1    1    2    key1    D    4    1    6
key1    b    2    3    2    key1    C    3    4    5
key1    b    2    3    2    key1    D    4    1    6
key1    c    3    2    1    key1    D    4    1    6
key1    d    4    7    2            0    0    \N
key1    e    5    5    5            0    0    \N
key2    a2    1    1    1            0    0    \N
key4    f    2    3    4            0    0    \N
```

<div id="null-values-in-join-keys">
  ## JOIN キー内の NULL 値
</div>

`NULL` は、自分自身を含め、どの値とも等しくありません。つまり、一方のテーブルの `JOIN` キーが `NULL` 値である場合、もう一方のテーブルの `NULL` 値とは一致しません。

**例**

テーブル `A`:

```response
┌───id─┬─name────┐
│    1 │ Alice   │
│    2 │ Bob     │
│ ᴺᵁᴸᴸ │ Charlie │
└──────┴─────────┘
```

テーブル `B`:

```response
┌───id─┬─score─┐
│    1 │    90 │
│    3 │    85 │
│ ᴺᵁᴸᴸ │    88 │
└──────┴───────┘
```

```sql
SELECT A.name, B.score FROM A LEFT JOIN B ON A.id = B.id
```

```response
┌─name────┬─score─┐
│ Alice   │    90 │
│ Bob     │     0 │
│ Charlie │     0 │
└─────────┴───────┘
```

`JOIN` キーの `NULL` 値が原因で、table `A` の `Charlie` の行と、table `B` のスコア 88 の行は結果に含まれていないことに注意してください。

`NULL` 値同士を一致させたい場合は、`JOIN` キーの比較に `isNotDistinctFrom` 関数を使用します。

```sql
SELECT A.name, B.score FROM A LEFT JOIN B ON isNotDistinctFrom(A.id, B.id)
```

```markdown
┌─name────┬─score─┐
│ Alice   │    90 │
│ Bob     │     0 │
│ Charlie │    88 │
└─────────┴───────┘
```

<div id="asof-join-usage">
  ## ASOF JOIN の使用法
</div>

`ASOF JOIN` は、厳密一致するレコードがない場合にレコードを結合する際に役立ちます。

この JOIN アルゴリズムでは、テーブルに特別なカラムが必要です。このカラムは次の条件を満たす必要があります。

* 順序付けられた数列を含んでいる必要があります。
* 型は次のいずれかである必要があります: [Int, UInt](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md), [Date](../../../sql-reference/data-types/date.md), [DateTime](../../../sql-reference/data-types/datetime.md), [Decimal](../../../sql-reference/data-types/decimal.md)。
* `hash` JOIN アルゴリズムでは、`JOIN` 句内の唯一のカラムにすることはできません。

構文 `ASOF JOIN ... ON`:

```sql
SELECT expressions_list
FROM table_1
ASOF LEFT JOIN table_2
ON equi_cond AND closest_match_cond
```

任意の数の等価条件と、最も近い一致条件を 1 つだけ使用できます。たとえば、`SELECT count() FROM table_1 ASOF LEFT JOIN table_2 ON table_1.a == table_2.b AND table_2.t <= table_1.t` です。

最も近い一致条件で使用できる比較演算子: `>`, `>=`, `<`, `<=`。

構文 `ASOF JOIN ... USING`:

```sql
SELECT expressions_list
FROM table_1
ASOF JOIN table_2
USING (equi_column1, ... equi_columnN, asof_column)
```

`ASOF JOIN` では、等値条件での結合に `equi_columnX` を使用し、`table_1.asof_column >= table_2.asof_column` 条件で最も近い値に一致する結合に `asof_column` を使用します。`asof_column` カラムは、`USING` 句では常に最後に指定されます。

たとえば、次のテーブルを考えます。

```text
         table_1                           table_2
      event   | ev_time | user_id       event   | ev_time | user_id
    ----------|---------|----------   ----------|---------|----------
                  ...                               ...
    event_1_1 |  12:00  |  42         event_2_1 |  11:59  |   42
                  ...                 event_2_2 |  12:30  |   42
    event_1_2 |  13:00  |  42         event_2_3 |  13:00  |   42
                  ...                               ...
```

`ASOF JOIN` では、`table_1` のユーザーイベントの timestamp を基に、最も近い一致条件に対応する `table_2` 内のイベントのうち、timestamp が最も近いものを見つけることができます。timestamp の値が同じものがあれば、それが最も近いものとして扱われます。ここでは、`user_id` カラムを等価条件での JOIN に、`ev_time` カラムを最も近い一致での JOIN に使用できます。この例では、`event_1_1` は `event_2_1` と JOIN でき、`event_1_2` は `event_2_3` と JOIN できますが、`event_2_2` は JOIN できません。

:::note
`ASOF JOIN` は、`hash` および `full_sorting_merge` JOIN アルゴリズムでのみサポートされています。
[Join](../../../engines/table-engines/special/join.md) テーブルエンジンでは**サポートされていません**。
:::”

<div id="paste-join-usage">
  ## PASTE JOIN の使用法
</div>

`PASTE JOIN` の結果は、左側のサブクエリのすべてのカラムに、続けて右側のサブクエリのすべてのカラムを含むテーブルになります。
行は元のテーブル内での位置に基づいて対応付けられます (行の順序が定義されている必要があります) 。
サブクエリが返す行数が異なる場合、余分な行は切り捨てられます。

例:

```sql
SELECT *
FROM
(
    SELECT number AS a
    FROM numbers(2)
) AS t1
PASTE JOIN
(
    SELECT number AS a
    FROM numbers(2)
    ORDER BY a DESC
) AS t2

┌─a─┬─t2.a─┐
│ 0 │    1 │
│ 1 │    0 │
└───┴──────┘
```

注意: この場合、読み取りが並列に行われると、結果は非決定論的になる可能性があります。たとえば、次のようになります。

```sql
SELECT *
FROM
(
    SELECT number AS a
    FROM numbers_mt(5)
) AS t1
PASTE JOIN
(
    SELECT number AS a
    FROM numbers(10)
    ORDER BY a DESC
) AS t2
SETTINGS max_block_size = 2;

┌─a─┬─t2.a─┐
│ 2 │    9 │
│ 3 │    8 │
└───┴──────┘
┌─a─┬─t2.a─┐
│ 0 │    7 │
│ 1 │    6 │
└───┴──────┘
┌─a─┬─t2.a─┐
│ 4 │    5 │
└───┴──────┘
```

<div id="distributed-join">
  ## 分散 JOIN
</div>

分散テーブルを含む JOIN を実行する方法は 2 つあります。

* 通常の `JOIN` を使用する場合、クエリはリモートサーバーに送信されます。各サーバー上で右テーブルを作成するためのサブクエリが実行され、そのテーブルとの JOIN が行われます。つまり、右テーブルは各サーバーごとに個別に作成されます。
* `GLOBAL ... JOIN` を使用する場合、まず要求元のサーバーがサブクエリを実行して JOIN の片側を計算し、その結果を一時テーブルに格納します。次に、この一時テーブルが各リモートサーバーに渡され、転送された一時データを使ってそれらのサーバー上でクエリが実行されます。`LEFT` JOIN と `INNER` JOIN では、右テーブルがサブクエリとして計算されます。`RIGHT` JOIN では、保持されるのは右テーブルであり、分片から読み取る必要があるため、代わりに左テーブルが計算されます。

`GLOBAL` の使用時は注意してください。詳細については、[Distributed subqueries](/ja/sql-reference/operators/in#distributed-subqueries) セクションを参照してください。

<div id="implicit-type-conversion">
  ## 暗黙的な型変換
</div>

`INNER JOIN`、`LEFT JOIN`、`RIGHT JOIN`、`FULL JOIN` のクエリでは、&quot;結合キー&quot; に対する暗黙的な型変換がサポートされています。ただし、左テーブルと右テーブルの結合キーを単一の型に変換できない場合、クエリは実行できません (たとえば、`UInt64` と `Int64` の両方のすべての値、または `String` と `Int32` の両方のすべての値を保持できるデータ型は存在しません) 。

**例**

テーブル `t_1` について考えます。

```response
┌─a─┬─b─┬─toTypeName(a)─┬─toTypeName(b)─┐
│ 1 │ 1 │ UInt16        │ UInt8         │
│ 2 │ 2 │ UInt16        │ UInt8         │
└───┴───┴───────────────┴───────────────┘
```

およびテーブル `t_2`：

```response
┌──a─┬────b─┬─toTypeName(a)─┬─toTypeName(b)───┐
│ -1 │    1 │ Int16         │ Nullable(Int64) │
│  1 │   -1 │ Int16         │ Nullable(Int64) │
│  1 │    1 │ Int16         │ Nullable(Int64) │
└────┴──────┴───────────────┴─────────────────┘
```

クエリ

```sql
SELECT a, b, toTypeName(a), toTypeName(b) FROM t_1 FULL JOIN t_2 USING (a, b);
```

集合を返します:

```response
┌──a─┬────b─┬─toTypeName(a)─┬─toTypeName(b)───┐
│  1 │    1 │ Int32         │ Nullable(Int64) │
│  2 │    2 │ Int32         │ Nullable(Int64) │
│ -1 │    1 │ Int32         │ Nullable(Int64) │
│  1 │   -1 │ Int32         │ Nullable(Int64) │
└────┴──────┴───────────────┴─────────────────┘
```

<div id="usage-recommendations">
  ## 利用上の推奨事項
</div>

<div id="processing-of-empty-or-null-cells">
  ### 空のセルまたは NULL セルの処理
</div>

テーブルを結合すると、空のセルが生じることがあります。[join&#95;use&#95;nulls](../../../operations/settings/settings.md#join_use_nulls) 設定では、ClickHouse がこれらのセルをどのように埋めるかを指定します。

`JOIN` キーが [Nullable](../../../sql-reference/data-types/nullable.md) フィールドである場合、キーの少なくとも 1 つの値が [NULL](/ja/sql-reference/syntax#null) である行は結合されません。

<div id="syntax">
  ### 構文
</div>

`USING` で指定するカラムは、両方のサブクエリで同じ名前である必要があり、その他のカラムは異なる名前でなければなりません。サブクエリ内のカラム名を変更するには、別名を使用できます。

`USING` 句では、結合に使用する1つ以上のカラムを指定します。これにより、それらのカラムが等しいものとして扱われます。カラムのリストは括弧なしで指定します。より複雑な結合条件はサポートされていません。

<div id="syntax-limitations">
  ### 構文上の制限
</div>

1 つの `SELECT` クエリ内で複数の `JOIN` 句を使用する場合:

* `*` によるすべてのカラムの取得は、サブクエリではなくテーブルを結合した場合にのみ使用できます。
* `PREWHERE` 句は使用できません。
* `USING` 句は使用できません。

`ON`、`WHERE`、および `GROUP BY` 句について:

* `ON`、`WHERE`、および `GROUP BY` 句では任意の式は使用できませんが、`SELECT` 句で式を定義し、その後エイリアスを介してこれらの句で使用できます。

<div id="performance">
  ### パフォーマンス
</div>

`JOIN` を実行する際、クエリ内のほかの処理段階との関係で実行順序が最適化されることはありません。join (右テーブルの検索) は、`WHERE` でのフィルタリングや aggregation より前に実行されます。

同じ `JOIN` を含むクエリを実行するたびに、結果が cached されないため、サブクエリは毎回再実行されます。これを避けるには、結合用にあらかじめ用意された配列で、常に RAM 上に保持される特別な [Join](../../../engines/table-engines/special/join.md) テーブルエンジンを使用してください。

場合によっては、`JOIN` の代わりに [IN](../../../sql-reference/operators/in.md) を使用したほうが効率的です。

ディメンションテーブル (広告キャンペーン名のようなディメンションのプロパティを含む比較的小さなテーブル) との結合に `JOIN` が必要な場合、クエリごとに右テーブルへ再度アクセスする必要があるため、`JOIN` はあまり適していないことがあります。そのような場合は、`JOIN` の代わりに &quot;dictionaries&quot; 機能を使用してください。詳細については、[Dictionaries](/ja/sql-reference/statements/create/dictionary/overview.md) セクションを参照してください。

<div id="memory-limitations">
  ### メモリ制限
</div>

デフォルトでは、ClickHouse は [ハッシュ結合](https://en.wikipedia.org/wiki/Hash_join) アルゴリズムを使用します。ClickHouse は right&#95;table を受け取り、それに対するハッシュテーブルを RAM 上に作成します。`join_algorithm = 'auto'` が有効な場合、メモリ使用量が一定のしきい値を超えると、ClickHouse は [merge](https://en.wikipedia.org/wiki/Sort-merge_join) 結合アルゴリズムにフォールバックします。`JOIN` アルゴリズムの説明については、[join&#95;algorithm](../../../operations/settings/settings.md#join_algorithm) 設定を参照してください。

`JOIN` 操作のメモリ使用量を制限する必要がある場合は、次の設定を使用します。

* [max&#95;rows&#95;in&#95;join](/ja/operations/settings/settings#max_rows_in_join) — ハッシュテーブル内の行数を制限します。
* [max&#95;bytes&#95;in&#95;join](/ja/operations/settings/settings#max_bytes_in_join) — ハッシュテーブルのサイズを制限します。

これらの制限のいずれかに達すると、ClickHouse は [join&#95;overflow&#95;mode](/ja/operations/settings/settings#join_overflow_mode)
設定の指示に従って動作します。

<div id="examples">
  ## 例
</div>

例:

```sql
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

```text
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

<div id="related-content">
  ## 関連コンテンツ
</div>

* ブログ: [ClickHouse: 完全な SQL JOIN サポートを備えた超高速 DBMS - 第1部](https://clickhouse.com/blog/clickhouse-fully-supports-joins)
* ブログ: [ClickHouse: 完全な SQL JOIN サポートを備えた超高速 DBMS - その内部構造 - 第2部](https://clickhouse.com/blog/clickhouse-fully-supports-joins-hash-joins-part2)
* ブログ: [ClickHouse: 完全な SQL JOIN サポートを備えた超高速 DBMS - その内部構造 - 第3部](https://clickhouse.com/blog/clickhouse-fully-supports-joins-full-sort-partial-merge-part3)
* ブログ: [ClickHouse: 完全な SQL JOIN サポートを備えた超高速 DBMS - その内部構造 - 第4部](https://clickhouse.com/blog/clickhouse-fully-supports-joins-direct-join-part4)