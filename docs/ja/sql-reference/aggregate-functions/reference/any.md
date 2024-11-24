---
slug: /ja/sql-reference/aggregate-functions/reference/any
sidebar_position: 102
---

# any

`NULL` 値を無視し、カラムの最初に出現する値を選択します。

**構文**

```sql
any(column) [RESPECT NULLS]
```

別名: `any_value`, [`first_value`](../reference/first_value.md)。

**パラメータ**
- `column`: カラム名。 

**戻り値**

:::note
この関数名の後に `RESPECT NULLS` 修飾子を使用することができます。この修飾子を使用すると、関数は `NULL` かどうかに関係なく、渡された最初の値を選択します。
:::

:::note
関数の戻り値の型は、入力と同じですが、LowCardinality は無視されます。つまり、入力として行がない場合、その型のデフォルト値（整数の場合は0、Nullable() カラムの場合は Null）を返します。この動作を変更するためには `-OrNull` [コンビネータ](../../../sql-reference/aggregate-functions/combinators.md) を使用することができます。
:::

:::warning
クエリは任意の順序で、さらには毎回異なる順序で実行される可能性があるため、この関数の結果は不確定です。
確定的な結果を得るには、`any` の代わりに [`min`](../reference/min.md) または [`max`](../reference/max.md) 関数を使用することをお勧めします。
:::

**実装の詳細**

いくつかのケースでは、実行順序に依存することができます。これは、`ORDER BY` を使用したサブクエリから `SELECT` が来る場合に適用されます。

`SELECT` クエリが `GROUP BY` 句または少なくとも1つの集約関数を持つ場合、ClickHouse は（MySQL とは対照的に） `SELECT`、`HAVING`、および `ORDER BY` 句のすべての式がキーまたは集約関数から計算されることを要求します。言い換えると、テーブルから選択された各カラムは、キーまたは集約関数内で使用されなければなりません。MySQL のような動作を得るには、他のカラムを `any` 集約関数に入れることができます。

**例**

クエリ：

```sql
CREATE TABLE any_nulls (city Nullable(String)) ENGINE=Log;

INSERT INTO any_nulls (city) VALUES (NULL), ('Amsterdam'), ('New York'), ('Tokyo'), ('Valencia'), (NULL);

SELECT any(city) FROM any_nulls;
```

```response
┌─any(city)─┐
│ Amsterdam │
└───────────┘
```
