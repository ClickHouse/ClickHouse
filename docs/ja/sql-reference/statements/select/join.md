---
slug: /ja/sql-reference/statements/select/join
sidebar_label: Join Table
---

# JOIN句

**JOIN**は、共通の値を使用して複数のテーブルの**カラム**を組み合わせることで、新しいテーブルを生成します。これは、SQLをサポートするデータベースで一般的な操作であり、[関係代数](https://en.wikipedia.org/wiki/Relational_algebra#Joins_and_join-like_operators)の結合に対応します。1つのテーブル内での結合の特別なケースは、「自己結合」と呼ばれることがよくあります。

**構文**

``` sql
SELECT <expr_list>
FROM <left_table>
[GLOBAL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI|ANY|ALL|ASOF] JOIN <right_table>
(ON <expr_list>)|(USING <column_list>) ...
```

`ON`句からの式と`USING`句からの**カラム**は「結合キー」と呼ばれます。特に指定がない限り、結合は一致する「結合キー」を持つ**行**からの[直積](https://en.wikipedia.org/wiki/Cartesian_product)を生成し、元のテーブルよりもはるかに多くの**行**を持つ結果を生成する可能性があります。

## 関連コンテンツ

- ブログ: [ClickHouse: A Blazingly Fast DBMS with Full SQL Join Support - Part 1](https://clickhouse.com/blog/clickhouse-fully-supports-joins)
- ブログ: [ClickHouse: A Blazingly Fast DBMS with Full SQL Join Support - Under the Hood - Part 2](https://clickhouse.com/blog/clickhouse-fully-supports-joins-hash-joins-part2)
- ブログ: [ClickHouse: A Blazingly Fast DBMS with Full SQL Join Support - Under the Hood - Part 3](https://clickhouse.com/blog/clickhouse-fully-supports-joins-full-sort-partial-merge-part3)
- ブログ: [ClickHouse: A Blazingly Fast DBMS with Full SQL Join Support - Under the Hood - Part 4](https://clickhouse.com/blog/clickhouse-fully-supports-joins-direct-join-part4)

## サポートされるJOINの種類

全ての標準[SQL JOIN](https://en.wikipedia.org/wiki/Join_(SQL))タイプがサポートされています:

- `INNER JOIN`: 一致する**行**のみが返されます。
- `LEFT OUTER JOIN`: 左テーブルからの非一致行も一致した**行**と共に返されます。
- `RIGHT OUTER JOIN`: 右テーブルからの非一致行も一致した**行**と共に返されます。
- `FULL OUTER JOIN`: 両テーブルからの非一致行も一致した**行**と共に返されます。
- `CROSS JOIN`: テーブル全体の直積を生成し、「結合キー」は**指定しません**。

`JOIN`でタイプが指定されていない場合、`INNER`が暗黙に適用されます。キーワード`OUTER`は省略可能です。`CROSS JOIN`の代替構文として[FROM句](../../../sql-reference/statements/select/from.md)で複数のテーブルをカンマで区切って指定することもできます。

ClickHouseで利用可能な追加の結合タイプ:

- `LEFT SEMI JOIN`と`RIGHT SEMI JOIN`: 「結合キー」のホワイトリストを生成し、直積を生成しません。
- `LEFT ANTI JOIN`と`RIGHT ANTI JOIN`: 「結合キー」のブラックリストを生成し、直積を生成しません。
- `LEFT ANY JOIN`, `RIGHT ANY JOIN`と`INNER ANY JOIN`: 標準的な`JOIN`タイプの直積を部分的（`LEFT`と`RIGHT`の反対側）または完全（`INNER`と`FULL`）に無効にします。
- `ASOF JOIN`と`LEFT ASOF JOIN`: 厳密でない一致でシーケンスを結合します。`ASOF JOIN`の使用法については以下で説明します。
- `PASTE JOIN`: 2つのテーブルを水平方向に結合します。

:::note
`join_algorithm`が`partial_merge`に設定されている場合、`RIGHT JOIN`と`FULL JOIN`は`ALL`厳密性（`SEMI`, `ANTI`, `ANY`, `ASOF`はサポートされていません）のみサポートされます。
:::

## 設定

デフォルトの結合タイプは[join_default_strictness](../../../operations/settings/settings.md#join_default_strictness)設定を使用してオーバーライドすることができます。

`ANY JOIN`操作に対するClickHouseサーバーの動作は[any_join_distinct_right_table_keys](../../../operations/settings/settings.md#any_join_distinct_right_table_keys)設定に依存します。

**参照**

- [join_algorithm](../../../operations/settings/settings.md#join_algorithm)
- [join_any_take_last_row](../../../operations/settings/settings.md#join_any_take_last_row)
- [join_use_nulls](../../../operations/settings/settings.md#join_use_nulls)
- [partial_merge_join_optimizations](../../../operations/settings/settings.md#partial_merge_join_optimizations)
- [partial_merge_join_rows_in_right_blocks](../../../operations/settings/settings.md#partial_merge_join_rows_in_right_blocks)
- [join_on_disk_max_files_to_merge](../../../operations/settings/settings.md#join_on_disk_max_files_to_merge)
- [any_join_distinct_right_table_keys](../../../operations/settings/settings.md#any_join_distinct_right_table_keys)

ClickHouseが`CROSS JOIN`を`INNER JOIN`として書き直すのに失敗した時の動作を定義するために、`cross_to_inner_join_rewrite`設定を使用してください。デフォルト値は`1`であり、これにより結合は継続されますが、遅くなります。エラーを発生させたい場合は`cross_to_inner_join_rewrite`を`0`に設定し、全てのカンマ/クロス結合を書く直すことを強制したい場合は`2`に設定してください。値が`2`のときに書き換えが失敗すると、「`WHERE`セクションを簡略化してみてください」というエラーメッセージが表示されます。

## ONセクションの条件

`ON`セクションは`AND`および`OR`演算子を使用して結合された複数の条件を含むことができます。結合キーを指定する条件は、左テーブルと右テーブルの両方を参照し、等号演算子を使用しなければなりません。他の条件は、その他の論理演算子を使用できますが、**クエリ**の左または右テーブルのいずれかを参照する必要があります。

条件が満たされると**行**が結合されます。条件が満たされない場合でも、`JOIN`タイプによっては**行**が結果に含まれることがあります。注意すべき点は、同じ条件が`WHERE`セクションに配置され、条件が満たされていない場合、**行**は常に結果からフィルタリングされます。

`ON`句内の`OR`演算子はハッシュ結合アルゴリズムを使用して動作します — `JOIN`の結合キーを持つ各`OR`引数に対して、個別のハッシュテーブルが作成されるため、メモリ消費と**クエリ**の実行時間は`ON`句の`OR`の表現の数の増加に伴い線形に増加します。

:::note
異なるテーブルの**カラム**に言及する条件の場合、現時点では等号演算子（`=`）のみがサポートされています。
:::

**例**

`table_1`と`table_2`を考慮してください:

```
┌─Id─┬─name─┐     ┌─Id─┬─text───────────┬─scores─┐
│  1  │ A    │     │  1  │ Text A         │    10  │
│  2  │ B    │     │  1  │ Another text A │    12  │
│  3  │ C    │     │  2  │ Text B         │    15  │
└────┴──────┘     └────┴────────────────┴────────┘
```

1つの結合キー条件と`table_2`に対する追加の条件を持つ**クエリ**：

``` sql
SELECT name, text FROM table_1 LEFT OUTER JOIN table_2
    ON table_1.Id = table_2.Id AND startsWith(table_2.text, 'Text');
```

結果には、nameが`C`でテキストが空の**行**が含まれています。これは、`OUTER`タイプの結合が使用されているために結果に含まれています。

```
┌─name─┬─text───┐
│ A    │ Text A │
│ B    │ Text B │
│ C    │        │
└──────┴────────┘
```

`INNER`タイプの結合と複数の条件を持つ**クエリ**：

``` sql
SELECT name, text, scores FROM table_1 INNER JOIN table_2
    ON table_1.Id = table_2.Id AND table_2.scores > 10 AND startsWith(table_2.text, 'Text');
```

結果：

```
┌─name─┬─text───┬─scores─┐
│ B    │ Text B │    15  │
└──────┴────────┴────────┘
```

`INNER`タイプの結合と`OR`を含む条件を持つ**クエリ**：

``` sql
CREATE TABLE t1 (`a` Int64, `b` Int64) ENGINE = MergeTree() ORDER BY a;

CREATE TABLE t2 (`key` Int32, `val` Int64) ENGINE = MergeTree() ORDER BY key;

INSERT INTO t1 SELECT number as a, -a as b from numbers(5);

INSERT INTO t2 SELECT if(number % 2 == 0, toInt64(number), -number) as key, number as val from numbers(5);

SELECT a, b, val FROM t1 INNER JOIN t2 ON t1.a = t2.key OR t1.b = t2.key;
```

結果：

```
┌─a─┬──b─┬─val─┐
│ 0 │  0 │   0 │
│ 1 │ -1 │   1 │
│ 2 │ -2 │   2 │
│ 3 │ -3 │   3 │
│ 4 │ -4 │   4 │
└───┴────┴─────┘
```

`INNER`タイプの結合と`OR`および`AND`を含む条件を持つ**クエリ**：

:::note
デフォルトでは、異なるテーブルからのカラムを使う条件はサポートされません。例えば`t1.a = t2.key AND t1.b > 0 AND t2.b > t2.c`は`t1.b > 0`が`t1`のカラムのみを使用し、`t2.b > t2.c`が`t2`のカラムのみを使用するため可能です。しかし、`t1.a = t2.key AND t1.b > t2.key`のような条件のエクスペリメンタルサポートを試みることができます。詳細については以下を参照してください。
:::

``` sql
SELECT a, b, val FROM t1 INNER JOIN t2 ON t1.a = t2.key OR t1.b = t2.key AND t2.val > 3;
```

結果：

```
┌─a─┬──b─┬─val─┐
│ 0 │  0 │   0 │
│ 2 │ -2 │   2 │
│ 4 │ -4 │   4 │
└───┴────┴─────┘
```

## [試験的機能] 異なるテーブルのカラムに対する不等式条件を伴う結合

:::note
この機能は試験的です。これを利用するには、設定ファイルや`SET`コマンドを用いて`allow_experimental_join_condition`を1に設定してください：

```sql
SET allow_experimental_join_condition=1
```

そうでなければ、`INVALID_JOIN_ON_EXPRESSION`が返されます。

:::

ClickHouseは現在、等式条件に加えて不等式条件を持つ`ALL/ANY/SEMI/ANTI INNER/LEFT/RIGHT/FULL JOIN`をサポートしています。不等式条件は`hash`および`grace_hash`結合アルゴリズムのみでサポートされています。不等式条件は`join_use_nulls`ではサポートされていません。

**例**

テーブル`t1`:

```
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

テーブル`t2`

```
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
SELECT t1.*, t2.* from t1 LEFT JOIN t2 ON t1.key = t2.key and (t1.a < t2.a) ORDER BY (t1.key, t1.attr, t2.key, t2.attr);
```

```
key1	a	1	1	2	key1	B	2	1	2
key1	a	1	1	2	key1	C	3	4	5
key1	a	1	1	2	key1	D	4	1	6
key1	b	2	3	2	key1	C	3	4	5
key1	b	2	3	2	key1	D	4	1	6
key1	c	3	2	1	key1	D	4	1	6
key1	d	4	7	2			0	0	\N
key1	e	5	5	5			0	0	\N
key2	a2	1	1	1			0	0	\N
key4	f	2	3	4			0	0	\N
```

## JOINキー内のNULL値

NULLはどの値とも、また自身とも等しくありません。つまり、JOINキーにNULL値が一方のテーブルにある場合、もう一方のテーブルのNULL値と一致しません。

**例**

テーブル`A`:

```
┌───id─┬─name────┐
│    1 │ Alice   │
│    2 │ Bob     │
│ ᴺᵁᴸᴸ │ Charlie │
└──────┴─────────┘
```

テーブル`B`:

```
┌───id─┬─score─┐
│    1 │    90 │
│    3 │    85 │
│ ᴺᵁᴸᴸ │    88 │
└──────┴───────┘
```

```sql
SELECT A.name, B.score FROM A LEFT JOIN B ON A.id = B.id
```

```
┌─name────┬─score─┐
│ Alice   │    90 │
│ Bob     │     0 │
│ Charlie │     0 │
└─────────┴───────┘
```

`A`テーブルの`Charlie`行と`B`テーブルのスコア88の行は、JOINキーのNULL値のため結果に含まれていないことに注意してください。

NULL値を一致させたい場合は、`isNotDistinctFrom`関数を使用してJOINキーを比較します。

```sql
SELECT A.name, B.score FROM A LEFT JOIN B ON isNotDistinctFrom(A.id, B.id)
```

```
┌─name────┬─score─┐
│ Alice   │    90 │
│ Bob     │     0 │
│ Charlie │    88 │
└─────────┴───────┘
```

## ASOF JOINの使用方法

`ASOF JOIN`は、正確な一致がないレコードを結合するときに役立ちます。

アルゴリズムには特別な**カラム**がテーブルに必要です。この**カラム**:

- 順序付けられたシーケンスを含まなければならない
- 次のいずれかの型を持つことができる: [Int, UInt](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md), [Date](../../../sql-reference/data-types/date.md), [DateTime](../../../sql-reference/data-types/datetime.md), [Decimal](../../../sql-reference/data-types/decimal.md)
- `hash`結合アルゴリズムでは、`JOIN`の句に唯一の**カラム**にすることはできない

構文 `ASOF JOIN ... ON`:

``` sql
SELECT expressions_list
FROM table_1
ASOF LEFT JOIN table_2
ON equi_cond AND closest_match_cond
```

任意の数の等式条件と厳密に1つの最も近い一致条件を使用できます。例えば、`SELECT count() FROM table_1 ASOF LEFT JOIN table_2 ON table_1.a == table_2.b AND table_2.t <= table_1.t`。

最も近い一致でサポートされる条件: `>`, `>=`, `<`, `<=`。

構文 `ASOF JOIN ... USING`:

``` sql
SELECT expressions_list
FROM table_1
ASOF JOIN table_2
USING (equi_column1, ... equi_columnN, asof_column)
```

`ASOF JOIN`は`equi_columnX`を等価結合に使用し、`asof_column`を最も近い一致での結合に使用します。この`asof_column`は常に`USING`句の最後の**カラム**です。

例えば、以下のテーブルを考慮してください:

         table_1                           table_2
      event   | ev_time | user_id       event   | ev_time | user_id
    ----------|---------|---------- ----------|---------|----------
                  ...                               ...
    event_1_1 |  12:00  |  42         event_2_1 |  11:59  |   42
                  ...                 event_2_2 |  12:30  |   42
    event_1_2 |  13:00  |  42         event_2_3 |  13:00  |   42
                  ...                               ...

`ASOF JOIN`は、`table_1`のユーザーイベントのタイムスタンプを取得し、最も近い一致条件に対応する`table_1`イベントのタイムスタンプに最も近い`table_2`のイベントを見つけます。等しいタイムスタンプの値が利用可能な場合、最も近いと見なされます。この例では、`user_id`列は等価結合に使用され、`ev_time`列は最も近い一致での結合に使用されます。この例では、`event_1_1`は`event_2_1`と結合され、`event_1_2`は`event_2_3`と結合されますが、`event_2_2`は結合されません。

:::note
`ASOF JOIN`は`hash`および`full_sorting_merge`結合アルゴリズムのみでサポートされます。
[Join](../../../engines/table-engines/special/join.md)テーブルエンジンではサポートされていません。
:::

## PASTE JOINの使用方法

`PASTE JOIN`の結果は、左サブクエリのすべての**カラム**の後に右サブクエリのすべての**カラム**が続くテーブルです。
元のテーブルでのポジションに基づいて**行**が一致します（**行**の順序は定義されている必要があります）。
サブクエリが異なる数の**行**を返す場合、余分な**行**はカットされます。

例:
```SQL
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
注意: この場合、結果は並列で読み取ると非決定的になる可能性があります。例:
```SQL
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

## 分散JOIN

分散テーブルを含むJOINを実行する方法は2つあります:

- 通常の`JOIN`を使用する場合、クエリはリモートサーバーに送信されます。サブクエリはそれぞれのサーバーで実行され、右テーブルが作成され、このテーブルとのJOINが実行されます。言い換えれば、右テーブルは各サーバーで個別に形成されます。
- `GLOBAL ... JOIN`を使用する場合、まずリクエスタサーバーが右テーブルを計算するためのサブクエリを実行します。この一時テーブルは各リモートサーバーに渡され、送信された一時データを使用してクエリが実行されます。

`GLOBAL`を使用する際は注意してください。詳細は[分散サブクエリ](../../../sql-reference/operators/in.md#select-distributed-subqueries)セクションを参照してください。

## 暗黙の型変換

`INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, および`FULL JOIN` **クエリ**は、「結合キー」の暗黙的な型変換をサポートしています。ただし、左テーブルと右テーブルからの結合キーが単一の型に変換できない場合（例えば、`UInt64`と`Int64`、あるいは`String`と`Int32`）にクエリは実行されません。

**例**

以下のテーブル `t_1` を考慮してください：
```text
┌─a─┬─b─┬─toTypeName(a)─┬─toTypeName(b)─┐
│ 1 │ 1 │ UInt16        │ UInt8         │
│ 2 │ 2 │ UInt16        │ UInt8         │
└───┴───┴───────────────┴───────────────┘
```
そしてテーブル `t_2`：
```text
┌──a─┬────b─┬─toTypeName(a)─┬─toTypeName(b)───┐
│ -1 │    1 │ Int16         │ Nullable(Int64) │
│  1 │   -1 │ Int16         │ Nullable(Int64) │
│  1 │    1 │ Int16         │ Nullable(Int64) │
└────┴──────┴───────────────┴─────────────────┘
```

以下のクエリ
```sql
SELECT a, b, toTypeName(a), toTypeName(b) FROM t_1 FULL JOIN t_2 USING (a, b);
```
が返すセット：
```text
┌──a─┬────b─┬─toTypeName(a)─┬─toTypeName(b)───┐
│  1 │    1 │ Int32         │ Nullable(Int64) │
│  2 │    2 │ Int32         │ Nullable(Int64) │
│ -1 │    1 │ Int32         │ Nullable(Int64) │
│  1 │   -1 │ Int32         │ Nullable(Int64) │
└────┴──────┴───────────────┴─────────────────┘
```

## 使用の推奨事項

### 空またはNULLセルの処理

テーブルを結合するとき、空のセルが生じる場合があります。[join_use_nulls](../../../operations/settings/settings.md#join_use_nulls)設定はClickHouseがこれらのセルをどのように埋めるかを定義します。

`JOIN`キーが[Nullable](../../../sql-reference/data-types/nullable.md)フィールドである場合、少なくとも1つのキーが[NULL](../../../sql-reference/syntax.md#null-literal)値を持っている**行**は結合されません。

### 構文

`USING`で指定された**カラム**は両方のサブクエリで同じ名前を持たなければならず、他の**カラム**は異なる名前でなければなりません。サブクエリ内で**カラム**の名前を変更するためにエイリアスを使用できます。

`USING`句は1つ以上の**カラム**を指定することで、これらのカラムの等価性を確立します。**カラム**のリストは括弧を用いずに設定されます。より複雑な結合条件はサポートされていません。

### 構文制限

1つの`SELECT` **クエリ**に複数の`JOIN`句がある場合：

- `*`による全てのカラムの取得は、サブクエリの**カラム**でなくテーブルが結合される場合のみ利用可能です。
- `PREWHERE`句は利用できません。
- `USING`句は利用できません。

`ON`、`WHERE`、および`GROUP BY`句の場合：

- `ON`、`WHERE`、および`GROUP BY`句で任意の式を使用することはできませんが、`SELECT`句で式を定義し、エイリアスを介してこれらの句で使用することはできます。

### パフォーマンス

`JOIN`を実行する際、クエリの他のステージに関連する実行順序の最適化はありません。JOIN（右テーブルの検索）は`WHERE`でのフィルタリングの前に、集計の前に実行されます。

同じ`JOIN`を伴うクエリを実行するたびにサブクエリが再度実行されます。結果はキャッシュされません。これを避けるために、特殊な[Join](../../../engines/table-engines/special/join.md)テーブルエンジンを使用します。このエンジンは、常にRAMにある結合のための準備された配列です。

場合によっては、`JOIN`の代わりに[IN](../../../sql-reference/operators/in.md)を使用する方が効率的です。

ディメンションテーブル（広告キャンペーンの名前などのディメンションプロパティを含む比較的小さなテーブル）との結合に`JOIN`が必要な場合、右テーブルが毎回再アクセスされるため`JOIN`はあまり便利ではないかもしれません。そういった場合には「Dictionary」機能を使用することをお勧めします。詳細は[Dictionaries](../../../sql-reference/dictionaries/index.md)セクションを参照してください。

### メモリ制限

デフォルトでClickHouseは[ハッシュ結合](https://en.wikipedia.org/wiki/Hash_join)アルゴリズムを使用します。ClickHouseはright_tableを取り、RAMにハッシュテーブルを作成します。`join_algorithm = 'auto'`が有効な場合、メモリ消費のしきい値を超えるとClickHouseは[マージ](https://en.wikipedia.org/wiki/Sort-merge_join)結合アルゴリズムにフォールバックします。`JOIN`アルゴリズムの説明は[join_algorithm](../../../operations/settings/settings.md#join_algorithm)設定を参照してください。

`JOIN`操作のメモリ消費を制限する必要がある場合、以下の設定を使用します：

- [max_rows_in_join](../../../operations/settings/query-complexity.md#settings-max_rows_in_join) — ハッシュテーブルの行数を制限します。
- [max_bytes_in_join](../../../operations/settings/query-complexity.md#settings-max_bytes_in_join) — ハッシュテーブルのサイズを制限します。

これらの制限のいずれかが到達された場合、ClickHouseは[join_overflow_mode](../../../operations/settings/query-complexity.md#settings-join_overflow_mode)設定に指示された通りに動作します。

## 例

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
