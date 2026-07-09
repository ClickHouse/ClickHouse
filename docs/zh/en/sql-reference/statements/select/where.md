---
description: 'ClickHouse 中 `WHERE` 子句的文档'
sidebar_label: 'WHERE'
slug: /sql-reference/statements/select/where
title: 'WHERE 子句'
doc_type: 'reference'
keywords: ['WHERE']
---

`WHERE` 子句允许你对 `SELECT` 的 [`FROM`](../../../sql-reference/statements/select/from.md) 子句返回的数据进行过滤。

如果存在 `WHERE` 子句，其后必须跟一个 `UInt8` 类型的表达式。
表达式计算结果为 `0` 的行会从后续转换或结果中排除。

`WHERE` 子句后的表达式通常会与 [comparison](/zh/sql-reference/operators#comparison-operators)、[逻辑运算符](/zh/sql-reference/operators#operators-for-working-with-data-sets) 或众多 [regular functions](/zh/sql-reference/functions/regular-functions) 中的某一个一起使用。

系统会对 `WHERE` 表达式进行评估，以判断是否能够利用索引和分区裁剪，前提是底层表引擎支持这些能力。

:::note PREWHERE
还有一种名为 [`PREWHERE`](../../../sql-reference/statements/select/prewhere.md) 的筛选优化。
Prewhere 是一种能更高效应用筛选条件的优化方式。
即使未显式指定 `PREWHERE` 子句，它默认也是启用的。
:::

<div id="testing-for-null">
  ## 判断 `NULL`
</div>

如果您需要判断某个值是否为 [`NULL`](/zh/sql-reference/syntax#null)，请使用：

* [`IS NULL`](/zh/sql-reference/operators#is_null) 或 [`isNull`](../../../sql-reference/functions/functions-for-nulls.md#isNull)
* [`IS NOT NULL`](/zh/sql-reference/operators#is_not_null)   或 [`isNotNull`](../../../sql-reference/functions/functions-for-nulls.md#isNotNull)

否则，包含 `NULL` 的表达式将永远不会成立。

<div id="filtering-data-with-logical-operators">
  ## 使用逻辑运算符筛选数据
</div>

您可以将以下[逻辑函数](/zh/sql-reference/functions/logical-functions#and)与 `WHERE` 子句配合使用，以组合多个条件：

* [`and()`](/zh/sql-reference/functions/logical-functions#and) 或 `AND`
* [`not()`](/zh/sql-reference/functions/logical-functions#not) 或 `NOT`
* [`or()`](/zh/sql-reference/functions/logical-functions#or) 或 `NOT`
* [`xor()`](/zh/sql-reference/functions/logical-functions#xor)

<div id="using-uint8-columns-as-a-condition">
  ## 将 UInt8 列作为条件使用
</div>

在 ClickHouse 中，`UInt8` 列可直接作为布尔条件使用：`0` 表示 `false`，任何非零值 (通常为 `1`) 都表示 `true`。
示例见[下方](#example-uint8-column-as-condition)章节。

<div id="using-comparison-operators">
  ## 使用比较运算符
</div>

可使用以下[比较运算符](/zh/sql-reference/operators#comparison-operators)：

| 运算符                     | 函数                      | 描述             | 示例                              |
| ----------------------- | ----------------------- | -------------- | ------------------------------- |
| `a = b`                 | `equals(a, b)`          | 等于             | `price = 100`                   |
| `a == b`                | `equals(a, b)`          | 等于 (另一种语法)     | `price == 100`                  |
| `a != b`                | `notEquals(a, b)`       | 不等于            | `category != 'Electronics'`     |
| `a <> b`                | `notEquals(a, b)`       | 不等于 (另一种语法)    | `category <> 'Electronics'`     |
| `a < b`                 | `less(a, b)`            | 小于             | `price < 200`                   |
| `a <= b`                | `lessOrEquals(a, b)`    | 小于或等于          | `price <= 200`                  |
| `a > b`                 | `greater(a, b)`         | 大于             | `price > 500`                   |
| `a >= b`                | `greaterOrEquals(a, b)` | 大于或等于          | `price >= 500`                  |
| `a LIKE s`              | `like(a, b)`            | 模式匹配 (区分大小写)   | `name LIKE '%top%'`             |
| `a NOT LIKE s`          | `notLike(a, b)`         | 模式不匹配 (区分大小写)  | `name NOT LIKE '%top%'`         |
| `a ILIKE s`             | `ilike(a, b)`           | 模式匹配 (不区分大小写)  | `name ILIKE '%LAPTOP%'`         |
| `a BETWEEN b AND c`     | `a >= b AND a <= c`     | 范围检查 (包含边界)    | `price BETWEEN 100 AND 500`     |
| `a NOT BETWEEN b AND c` | `a < b OR a > c`        | 范围外检查          | `price NOT BETWEEN 100 AND 500` |

<div id="pattern-matching-and-conditional-expressions">
  ## 模式匹配和条件表达式
</div>

除了比较运算符外，你还可以在 `WHERE` 子句中使用模式匹配和条件表达式。

| 功能          | 语法                             | 区分大小写 | 性能 | 适用场景           |
| ----------- | ------------------------------ | ----- | -- | -------------- |
| `LIKE`      | `col LIKE '%pattern%'`         | 是     | 快  | 精确区分大小写的模式匹配   |
| `ILIKE`     | `col ILIKE '%pattern%'`        | 否     | 较慢 | 不区分大小写的搜索      |
| `if()`      | `if(cond, a, b)`               | 不适用   | 快  | 简单的二元条件        |
| `multiIf()` | `multiIf(c1, r1, c2, r2, def)` | 不适用   | 快  | 多条件判断          |
| `CASE`      | `CASE WHEN ... THEN ... END`   | 不适用   | 快  | 符合 SQL 标准的条件逻辑 |

用法示例请参见[“模式匹配和条件表达式”](#examples-pattern-matching-and-conditional-expressions)。

<div id="expressions-with-literals-columns-subqueries">
  ## 包含字面量、列或子查询的表达式
</div>

`WHERE` 子句后面的表达式也可以包含 [字面量](/zh/sql-reference/syntax#literals)、列或子查询。子查询是嵌套的 `SELECT` 语句，会返回可用于条件判断的值。

| 类型      | 定义         | 求值时机  | 性能    | 示例                         |
| ------- | ---------- | ----- | ----- | -------------------------- |
| **字面量** | 固定常量值      | 编写查询时 | 最快    | `WHERE price > 100`        |
| **列**   | 表中数据引用     | 逐行    | 快     | `WHERE price > cost`       |
| **子查询** | 嵌套的 SELECT | 查询执行时 | 视情况而定 | `WHERE id IN (SELECT ...)` |

你可以在复杂条件中混合使用字面量、列和子查询：

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
  ## 示例
</div>

<div id="examples-testing-for-null">
  ### 判断 `NULL`
</div>

含有 `NULL` 的查询：

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
  ### 使用逻辑运算符筛选数据
</div>

给定如下表和数据：

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

**1. `AND` - 两个条件必须同时为 true：**

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

**2. `OR` - 至少有一个条件必须为真：**

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

**3. `NOT` - 否定条件：**

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

**4. `XOR` - 必须恰好有一个条件为 `true` (不能同时都为 `true`) ：**

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

**5. 组合多个运算符：**

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

**6. 使用函数语法：**

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

SQL 关键字语法 (`AND`、`OR`、`NOT`、`XOR`) 通常更便于阅读，但在复杂表达式中或构建动态查询时，函数语法也很有用。

<div id="example-uint8-column-as-condition">
  ### 将 UInt8 列作为条件使用
</div>

以前面的[示例](#example-filtering-with-logical-operators)中的表为例，你可以直接将列名作为条件使用：

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
  ### 使用比较运算符
</div>

下面的示例使用了上文 [示例](#example-filtering-with-logical-operators) 中的表和数据。为简洁起见，结果已省略。

**1. 显式与 true 相等比较 (`= 1` 或 `= true`) ：**

```sql
SELECT * FROM products
WHERE in_stock = true;
-- or
WHERE in_stock = 1;
```

**2. 显式与 false 相等 (`= 0` 或 `= false`) ：**

```sql
SELECT * FROM products
WHERE in_stock = false;
-- or
WHERE in_stock = 0;
```

**3. 不等于 (`!= 0` 或 `!= false`) ：**

```sql
SELECT * FROM products
WHERE in_stock != false;
-- or
WHERE in_stock != 0;
```

**4. 大于：**

```sql
SELECT * FROM products
WHERE in_stock > 0;
```

**5. 小于或等于：**

```sql
SELECT * FROM products
WHERE in_stock <= 0;
```

**6. 与其他条件组合使用：**

```sql
SELECT * FROM products
WHERE in_stock AND price < 400;
```

**7. 使用 `IN` 运算符：**

在下面的示例中，`(1, true)` 是一个 [Tuple](/zh/sql-reference/data-types/tuple)。

```sql
SELECT * FROM products
WHERE in_stock IN (1, true);
```

你也可以使用[数组](/zh/sql-reference/data-types/array)来实现这一点：

```sql
SELECT * FROM products
WHERE in_stock IN [1, true];
```

**8. 混用比较写法：**

```sql
SELECT * FROM products
WHERE category = 'Electronics' AND in_stock = true;
```

<div id="examples-pattern-matching-and-conditional-expressions">
  ### 模式匹配与条件表达式
</div>

下面的示例使用了上文[示例](#example-filtering-with-logical-operators)中的表和数据。为简洁起见，结果已省略。

<div id="like-examples">
  #### LIKE 示例
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
  #### ILIKE 示例
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
  #### IF 示例
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
  #### multiIf 示例
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
  #### CASE 示例
</div>

**简单 CASE 表达式：**

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

**搜索 CASE：**

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