---
description: 'ClickHouse の `WHERE` 句に関するドキュメント'
sidebar_label: 'WHERE'
slug: /sql-reference/statements/select/where
title: 'WHERE 句'
doc_type: 'reference'
keywords: ['WHERE']
---

`WHERE` 句を使用すると、`SELECT` の [`FROM`](../../../sql-reference/statements/select/from.md) 句から取得されるデータを絞り込めます。

`WHERE` 句がある場合は、その後に `UInt8` 型の式を続ける必要があります。
この式の評価結果が `0` になる行は、後続の変換処理や結果から除外されます。

`WHERE` 句に続く式では、[comparison](/ja/sql-reference/operators#comparison-operators) や [論理演算子](/ja/sql-reference/operators#operators-for-working-with-data-sets)、あるいは多数の [regular functions](/ja/sql-reference/functions/regular-functions) のいずれかがよく使われます。

基になるテーブル engine が対応している場合、`WHERE` 式は索引や partition pruning を利用できるように評価されます。

:::note PREWHERE
[`PREWHERE`](../../../sql-reference/statements/select/prewhere.md) というフィルタリング最適化もあります。
Prewhere は、フィルタリングをより効率的に適用するための最適化です。
`PREWHERE` 句を明示的に指定しなくても、これはデフォルトで有効です。
:::

<div id="testing-for-null">
  ## `NULL` の判定
</div>

値が [`NULL`](/ja/sql-reference/syntax#null) かどうかを判定する必要がある場合は、以下を使用します。

* [`IS NULL`](/ja/sql-reference/operators#is_null) または [`isNull`](../../../sql-reference/functions/functions-for-nulls.md#isNull)
* [`IS NOT NULL`](/ja/sql-reference/operators#is_not_null)   または [`isNotNull`](../../../sql-reference/functions/functions-for-nulls.md#isNotNull)

これらを使用しない場合、`NULL` を含む式が条件を満たすことはありません。

<div id="filtering-data-with-logical-operators">
  ## 論理演算子を使ったデータの絞り込み
</div>

複数の条件を組み合わせるには、`WHERE` 句とともに以下の[論理関数](/ja/sql-reference/functions/logical-functions#and)を使用できます。

* [`and()`](/ja/sql-reference/functions/logical-functions#and) または `AND`
* [`not()`](/ja/sql-reference/functions/logical-functions#not) または `NOT`
* [`or()`](/ja/sql-reference/functions/logical-functions#or) または `NOT`
* [`xor()`](/ja/sql-reference/functions/logical-functions#xor)

<div id="using-uint8-columns-as-a-condition">
  ## 条件として UInt8 カラムを使用する
</div>

ClickHouse では、`UInt8` カラムをブール条件として直接使用できます。この場合、`0` は `false`、0 以外の値 (通常は `1`) は `true` として扱われます。
この使用例は、[以下](#example-uint8-column-as-condition)のセクションに示されています。

<div id="using-comparison-operators">
  ## 比較演算子の使用
</div>

以下の[比較演算子](/ja/sql-reference/operators#comparison-operators)を使用できます。

| 演算子                     | 関数                      | 説明                         | 例                               |
| ----------------------- | ----------------------- | -------------------------- | ------------------------------- |
| `a = b`                 | `equals(a, b)`          | 等しい                        | `price = 100`                   |
| `a == b`                | `equals(a, b)`          | 等しい (別の構文)                 | `price == 100`                  |
| `a != b`                | `notEquals(a, b)`       | 等しくない                      | `category != 'Electronics'`     |
| `a <> b`                | `notEquals(a, b)`       | 等しくない (別の構文)               | `category <> 'Electronics'`     |
| `a < b`                 | `less(a, b)`            | より小さい                      | `price < 200`                   |
| `a <= b`                | `lessOrEquals(a, b)`    | 以下                         | `price <= 200`                  |
| `a > b`                 | `greater(a, b)`         | より大きい                      | `price > 500`                   |
| `a >= b`                | `greaterOrEquals(a, b)` | 以上                         | `price >= 500`                  |
| `a LIKE s`              | `like(a, b)`            | パターンマッチング (大文字と小文字を区別する)   | `name LIKE '%top%'`             |
| `a NOT LIKE s`          | `notLike(a, b)`         | パターンに一致しない (大文字と小文字を区別する)  | `name NOT LIKE '%top%'`         |
| `a ILIKE s`             | `ilike(a, b)`           | パターンマッチング (大文字と小文字を区別しない)  | `name ILIKE '%LAPTOP%'`         |
| `a BETWEEN b AND c`     | `a >= b AND a <= c`     | 範囲チェック (両端を含む)             | `price BETWEEN 100 AND 500`     |
| `a NOT BETWEEN b AND c` | `a < b OR a > c`        | 範囲外のチェック                   | `price NOT BETWEEN 100 AND 500` |

<div id="pattern-matching-and-conditional-expressions">
  ## パターンマッチングと条件式
</div>

比較演算子に加えて、`WHERE` 句ではパターンマッチングや条件式も使用できます。

| 機能          | 構文                             | 大文字と小文字を区別 | 性能   | 主な用途                  |
| ----------- | ------------------------------ | ---------- | ---- | --------------------- |
| `LIKE`      | `col LIKE '%pattern%'`         | はい         | 高速   | 大文字と小文字を区別するパターンマッチング |
| `ILIKE`     | `col ILIKE '%pattern%'`        | いいえ        | やや低速 | 大文字と小文字を区別しない検索       |
| `if()`      | `if(cond, a, b)`               | 該当なし       | 高速   | 単純な二分岐条件              |
| `multiIf()` | `multiIf(c1, r1, c2, r2, def)` | 該当なし       | 高速   | 複数条件                  |
| `CASE`      | `CASE WHEN ... THEN ... END`   | 該当なし       | 高速   | SQL標準の条件分岐ロジック        |

使用例については、[&quot;パターンマッチングと条件式&quot;](#examples-pattern-matching-and-conditional-expressions)を参照してください。

<div id="expressions-with-literals-columns-subqueries">
  ## リテラル、カラム、またはサブクエリを含む式
</div>

`WHERE` 句に続く式には、[リテラル](/ja/sql-reference/syntax#literals)、カラム、またはサブクエリを含めることもできます。サブクエリは、条件で使用される値を返す、入れ子になった `SELECT` ステートメントです。

| 種類           | 定義          | 評価     | 性能    | 例                          |
| ------------ | ----------- | ------ | ----- | -------------------------- |
| **Literal**  | 固定の定数値      | クエリ記述時 | 最速    | `WHERE price > 100`        |
| **Column**   | テーブルデータへの参照 | 行ごと    | 高速    | `WHERE price > cost`       |
| **Subquery** | 入れ子の SELECT | クエリ実行時 | 状況による | `WHERE id IN (SELECT ...)` |

複雑な条件では、リテラル、カラム、サブクエリを組み合わせることができます:

```sql
-- Literal + Column
WHERE price > 100 AND category = 'Electronics'

-- Column + Subquery
WHERE price > (SELECT AVG(price) FROM products) AND in_stock = true

-- Literal + Column + Subquery
WHERE category = 'Electronics' 
  AND price < 500
  AND id IN (SELECT product_id FROM bestsellers)

-- All three with logical operators
WHERE (price > 100 OR category IN (SELECT category FROM featured))
  AND in_stock = true
  AND name LIKE '%Special%'
```

<div id="examples">
  ## 例
</div>

<div id="examples-testing-for-null">
  ### `NULL`の判定
</div>

`NULL`値を含むクエリ:

```sql
CREATE TABLE t_null(x Int8, y Nullable(Int8)) ENGINE=MergeTree() ORDER BY x;
INSERT INTO t_null VALUES (1, NULL), (2, 3);

SELECT * FROM t_null WHERE y IS NULL;
SELECT * FROM t_null WHERE y != 0;
```

```response
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
└───┴──────┘
┌─x─┬─y─┐
│ 2 │ 3 │
└───┴───┘
```

<div id="example-filtering-with-logical-operators">
  ### 論理演算子を使用したデータのフィルタリング
</div>

次のテーブルとデータがあるとします。

```sql
CREATE TABLE products (
    id UInt32,
    name String,
    price Float32,
    category String,
    in_stock Bool
) ENGINE = MergeTree()
ORDER BY id;

INSERT INTO products VALUES
(1, 'Laptop', 999.99, 'Electronics', true),
(2, 'Mouse', 25.50, 'Electronics', true),
(3, 'Desk', 299.00, 'Furniture', false),
(4, 'Chair', 150.00, 'Furniture', true),
(5, 'Monitor', 350.00, 'Electronics', true),
(6, 'Lamp', 45.00, 'Furniture', false);
```

**1. `AND` - 両方の条件が true でなければなりません:**

```sql
SELECT * FROM products
WHERE category = 'Electronics' AND price < 500;
```

```response
   ┌─id─┬─name────┬─price─┬─category────┬─in_stock─┐
1. │  2 │ Mouse   │  25.5 │ Electronics │ true     │
2. │  5 │ Monitor │   350 │ Electronics │ true     │
   └────┴─────────┴───────┴─────────────┴──────────┘
```

**2. `OR` - 少なくとも1つの条件が true でなければなりません:**

```sql
SELECT * FROM products
WHERE category = 'Furniture' OR price > 500;
```

```response
   ┌─id─┬─name───┬──price─┬─category────┬─in_stock─┐
1. │  1 │ Laptop │ 999.99 │ Electronics │ true     │
2. │  3 │ Desk   │    299 │ Furniture   │ false    │
3. │  4 │ Chair  │    150 │ Furniture   │ true     │
4. │  6 │ Lamp   │     45 │ Furniture   │ false    │
   └────┴────────┴────────┴─────────────┴──────────┘
```

**3. `NOT` - 条件を否定する:**

```sql
SELECT * FROM products
WHERE NOT in_stock;
```

```response
   ┌─id─┬─name─┬─price─┬─category──┬─in_stock─┐
1. │  3 │ Desk │   299 │ Furniture │ false    │
2. │  6 │ Lamp │    45 │ Furniture │ false    │
   └────┴──────┴───────┴───────────┴──────────┘
```

**4. `XOR` - 条件のうち、true になるのはちょうど1つだけです (両方は true になりません) ：**

```sql
SELECT *
FROM products
WHERE xor(price > 200, category = 'Electronics')
```

```response
   ┌─id─┬─name──┬─price─┬─category────┬─in_stock─┐
1. │  2 │ Mouse │  25.5 │ Electronics │ true     │
2. │  3 │ Desk  │   299 │ Furniture   │ false    │
   └────┴───────┴───────┴─────────────┴──────────┘
```

**5. 複数の演算子を組み合わせる：**

```sql
SELECT * FROM products
WHERE (category = 'Electronics' OR category = 'Furniture')
  AND in_stock = true
  AND price < 400;
```

```response
   ┌─id─┬─name────┬─price─┬─category────┬─in_stock─┐
1. │  2 │ Mouse   │  25.5 │ Electronics │ true     │
2. │  4 │ Chair   │   150 │ Furniture   │ true     │
3. │  5 │ Monitor │   350 │ Electronics │ true     │
   └────┴─────────┴───────┴─────────────┴──────────┘
```

**6. 関数構文を使う:**

```sql
SELECT * FROM products
WHERE and(or(category = 'Electronics', price > 100), in_stock);
```

```response
   ┌─id─┬─name────┬──price─┬─category────┬─in_stock─┐
1. │  1 │ Laptop  │ 999.99 │ Electronics │ true     │
2. │  2 │ Mouse   │   25.5 │ Electronics │ true     │
3. │  4 │ Chair   │    150 │ Furniture   │ true     │
4. │  5 │ Monitor │    350 │ Electronics │ true     │
   └────┴─────────┴────────┴─────────────┴──────────┘
```

SQLのキーワード構文 (`AND`、`OR`、`NOT`、`XOR`) のほうが一般的には読みやすいですが、複雑な式や動的なクエリを組み立てる場合には、関数構文が便利なことがあります。

<div id="example-uint8-column-as-condition">
  ### 条件として UInt8 カラムを使用する
</div>

[前の例](#example-filtering-with-logical-operators)で使ったテーブルでは、カラム名をそのまま条件として使用できます。

```sql
SELECT * FROM products
WHERE in_stock
```

```response
   ┌─id─┬─name────┬──price─┬─category────┬─in_stock─┐
1. │  1 │ Laptop  │ 999.99 │ Electronics │ true     │
2. │  2 │ Mouse   │   25.5 │ Electronics │ true     │
3. │  4 │ Chair   │    150 │ Furniture   │ true     │
4. │  5 │ Monitor │    350 │ Electronics │ true     │
   └────┴─────────┴────────┴─────────────┴──────────┘
```

<div id="example-using-comparison-operators">
  ### 比較演算子の使用
</div>

以下の例では、上記の[例](#example-filtering-with-logical-operators)で使用したテーブルとデータを使います。簡潔にするため、結果は省略しています。

**1. `true` との明示的な等価比較 (`= 1` または `= true`) :**

```sql
SELECT * FROM products
WHERE in_stock = true;
-- or
WHERE in_stock = 1;
```

**2. `false` との明示的な等価比較 (`= 0` または `= false`) :**

```sql
SELECT * FROM products
WHERE in_stock = false;
-- or
WHERE in_stock = 0;
```

**3. 不等価 (`!= 0` または `!= false`)：**

```sql
SELECT * FROM products
WHERE in_stock != false;
-- or
WHERE in_stock != 0;
```

**4. 大なり:**

```sql
SELECT * FROM products
WHERE in_stock > 0;
```

**5. 以下:**

```sql
SELECT * FROM products
WHERE in_stock <= 0;
```

**6. 他の条件式との組み合わせ:**

```sql
SELECT * FROM products
WHERE in_stock AND price < 400;
```

**7. `IN` 演算子を使用する:**

以下の例で、`(1, true)` は [タプル](/ja/sql-reference/data-types/tuple) です。

```sql
SELECT * FROM products
WHERE in_stock IN (1, true);
```

これを行うには、[Array](/ja/sql-reference/data-types/array) を使うこともできます：

```sql
SELECT * FROM products
WHERE in_stock IN [1, true];
```

**8. 比較方法を混在させる:**

```sql
SELECT * FROM products
WHERE category = 'Electronics' AND in_stock = true;
```

<div id="examples-pattern-matching-and-conditional-expressions">
  ### パターンマッチングと条件式
</div>

以下の例では、上記の[例](#example-filtering-with-logical-operators)で使用したテーブルとデータを使います。簡潔さのため、結果は省略しています。

<div id="like-examples">
  #### LIKE の使用例
</div>

```sql
-- Find products with 'o' in the name
SELECT * FROM products WHERE name LIKE '%o%';
-- Result: Laptop, Monitor

-- Find products starting with 'L'
SELECT * FROM products WHERE name LIKE 'L%';
-- Result: Laptop, Lamp

-- Find products with exactly 4 characters
SELECT * FROM products WHERE name LIKE '____';
-- Result: Desk, Lamp
```

<div id="ilike-examples">
  #### ILIKEの例
</div>

```sql
-- Case-insensitive search for 'LAPTOP'
SELECT * FROM products WHERE name ILIKE '%laptop%';
-- Result: Laptop

-- Case-insensitive prefix match
SELECT * FROM products WHERE name ILIKE 'l%';
-- Result: Laptop, Lamp
```

<div id="if-examples">
  #### IF の例
</div>

```sql
-- Different price thresholds by category
SELECT * FROM products
WHERE if(category = 'Electronics', price < 500, price < 200);
-- Result: Mouse, Chair, Monitor
-- (Electronics under $500 OR Furniture under $200)

-- Filter based on stock status
SELECT * FROM products
WHERE if(in_stock, price > 100, true);
-- Result: Laptop, Chair, Monitor, Desk, Lamp
-- (In stock items over $100 OR all out-of-stock items)
```

<div id="multiif-examples">
  #### multiIf の例
</div>

```sql
-- Multiple category-based conditions
SELECT * FROM products
WHERE multiIf(
    category = 'Electronics', price < 600,
    category = 'Furniture', in_stock = true,
    false
);
-- Result: Mouse, Monitor, Chair
-- (Electronics < $600 OR in-stock Furniture)

-- Tiered filtering
SELECT * FROM products
WHERE multiIf(
    price > 500, category = 'Electronics',
    price > 100, in_stock = true,
    true
);
-- Result: Laptop, Chair, Monitor, Lamp
```

<div id="case-examples">
  #### CASE の例
</div>

**単純CASE:**

```sql
-- Different rules per category
SELECT * FROM products
WHERE CASE category
    WHEN 'Electronics' THEN price < 400
    WHEN 'Furniture' THEN in_stock = true
    ELSE false
END;
-- Result: Mouse, Monitor, Chair
```

**検索CASE式:**

```sql
-- Price-based tiered logic
SELECT * FROM products
WHERE CASE
    WHEN price > 500 THEN in_stock = true
    WHEN price > 100 THEN category = 'Electronics'
    ELSE true
END;
-- Result: Laptop, Monitor, Mouse, Lamp
```