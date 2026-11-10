---
description: 'Documentation for the `WHERE` clause in ClickHouse'
sidebar_label: 'WHERE'
slug: /sql-reference/statements/select/where
title: 'WHERE clause'
doc_type: 'reference'
keywords: ['WHERE']
---

# WHERE clause

The `WHERE` clause allows you to filter the data that comes from the[`FROM`](../../../sql-reference/statements/select/from.md) clause of `SELECT`.

If there is a `WHERE` clause, it must be followed by an expression of type `UInt8`.
Rows where this expression evaluates to `0` are excluded from further transformations or the result.

The expression following the `WHERE` clause is often used with [comparison](/sql-reference/operators#comparison-operators) and [logical operators](/sql-reference/operators#operators-for-working-with-data-sets), or one of the many [regular functions](/sql-reference/functions/regular-functions).

The `WHERE` expression is evaluated on the ability to use indexes and partition pruning, if the underlying table engine supports that.

:::note PREWHERE
There is also a filtering optimization called [`PREWHERE`](../../../sql-reference/statements/select/prewhere.md).
Prewhere is an optimization to apply filtering more efficiently.
It is enabled by default even if `PREWHERE` clause is not specified explicitly.
:::

## Testing for `NULL` {#testing-for-null}

If you need to test a value for [`NULL`](/sql-reference/syntax#null), use:
- [`IS NULL`](/sql-reference/operators#is_null) or [`isNull`](../../../sql-reference/functions/functions-for-nulls.md#isNull)
- [`IS NOT NULL`](/sql-reference/operators#is_not_null)   or [`isNotNull`](../../../sql-reference/functions/functions-for-nulls.md#isNotNull)

An expression with `NULL` will otherwise never pass.

## Filtering data with logical operators {#filtering-data-with-logical-operators}

You can use the following [logical functions](/sql-reference/functions/logical-functions#and) together with the `WHERE` clause for combining multiple conditions:

- [`and()`](/sql-reference/functions/logical-functions#and) or `AND`
- [`not()`](/sql-reference/functions/logical-functions#not) or `NOT`
- [`or()`](/sql-reference/functions/logical-functions#or) or `NOT`
- [`xor()`](/sql-reference/functions/logical-functions#xor)

## Using UInt8 columns as a condition {#using-uint8-columns-as-a-condition}

In ClickHouse, `UInt8` columns can be used directly as boolean conditions, where `0` is `false` and any non-zero value (typically `1`) is `true`.
An example of this is given in the section [below](#example-uint8-column-as-condition).

## Using comparison operators {#using-comparison-operators}

The following [comparison operators](/sql-reference/operators#comparison-operators) can be used:

| Operator | Function | Description | Example |
|----------|----------|-------------|---------|
| `a = b` | `equals(a, b)` | Equal to | `price = 100` |
| `a == b` | `equals(a, b)` | Equal to (alternative syntax) | `price == 100` |
| `a != b` | `notEquals(a, b)` | Not equal to | `category != 'Electronics'` |
| `a <> b` | `notEquals(a, b)` | Not equal to (alternative syntax) | `category <> 'Electronics'` |
| `a < b` | `less(a, b)` | Less than | `price < 200` |
| `a <= b` | `lessOrEquals(a, b)` | Less than or equal to | `price <= 200` |
| `a > b` | `greater(a, b)` | Greater than | `price > 500` |
| `a >= b` | `greaterOrEquals(a, b)` | Greater than or equal to | `price >= 500` |
| `a LIKE s` | `like(a, b)` | Pattern matching (case-sensitive) | `name LIKE '%top%'` |
| `a NOT LIKE s` | `notLike(a, b)` | Pattern not matching (case-sensitive) | `name NOT LIKE '%top%'` |
| `a ILIKE s` | `ilike(a, b)` | Pattern matching (case-insensitive) | `name ILIKE '%LAPTOP%'` |
| `a BETWEEN b AND c` | `a >= b AND a <= c` | Range check (inclusive) | `price BETWEEN 100 AND 500` |
| `a NOT BETWEEN b AND c` | `a < b OR a > c` | Outside range check | `price NOT BETWEEN 100 AND 500` |

## Pattern matching and conditional expressions {#pattern-matching-and-conditional-expressions}

Beyond comparison operators, you can use pattern matching and conditional expressions in the `WHERE` clause.

| Feature     | Syntax                         | Case-Sensitive | Performance | Best For                       |
| ----------- | ------------------------------ | -------------- | ----------- | ------------------------------ |
| `LIKE`      | `col LIKE '%pattern%'`         | Yes            | Fast        | Exact case pattern matching    |
| `ILIKE`     | `col ILIKE '%pattern%'`        | No             | Slower      | Case-insensitive searching     |
| `if()`      | `if(cond, a, b)`               | N/A            | Fast        | Simple binary conditions       |
| `multiIf()` | `multiIf(c1, r1, c2, r2, def)` | N/A            | Fast        | Multiple conditions            |
| `CASE`      | `CASE WHEN ... THEN ... END`   | N/A            | Fast        | SQL-standard conditional logic |

See ["Pattern matching and conditional expressions"](#examples-pattern-matching-and-conditional-expressions) for usage examples.

## Expression with literals, columns or subqueries {#expressions-with-literals-columns-subqueries}

The expression following the `WHERE` clause can also include [literals](/sql-reference/syntax#literals), columns or subqueries, which are nested `SELECT` statements that return values used in conditions.

| Type | Definition | Evaluation | Performance | Example |
|------|------------|------------|-------------|---------|
| **Literal** | Fixed constant value | Query write time | Fastest | `WHERE price > 100` |
| **Column** | Table data reference | Per row | Fast | `WHERE price > cost` |
| **Subquery** | Nested SELECT | Query execution time | Varies | `WHERE id IN (SELECT ...)` |

You can mix literals, columns, and subqueries in complex conditions:

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
## Examples {#examples}

### Testing for `NULL` {#examples-testing-for-null}

Queries with `NULL` values:

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

### Filtering data with logical operators {#example-filtering-with-logical-operators}

Given the following table and data:

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

**1. `AND` - both conditions must be true:**

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

**2. `OR` - at least one condition must be true:**

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

**3. `NOT` - Negates a condition:**

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

**4. `XOR` - Exactly one condition must be true (not both):**

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

**5. Combining multiple operators:**

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

**6. Using function syntax:**

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

The SQL keyword syntax (`AND`, `OR`, `NOT`, `XOR`) is generally more readable, but the function syntax can be useful in complex expressions or when building dynamic queries.

### Using UInt8 columns as a condition {#example-uint8-column-as-condition}

Taking the table from a [previous example](#example-filtering-with-logical-operators), you can use a column name directly as a condition:

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

### Using comparison operators {#example-using-comparison-operators}

The examples below use the table and data from the [example](#example-filtering-with-logical-operators) above. Results are omitted for sake of brevity.

**1. Explicit equality with true (`= 1` or `= true`):**

```sql
SELECT * FROM products
WHERE in_stock = true;
-- or
WHERE in_stock = 1;
```

**2. Explicit equality with false (`= 0` or `= false`):**

```sql
SELECT * FROM products
WHERE in_stock = false;
-- or
WHERE in_stock = 0;
```

**3. Inequality (`!= 0` or `!= false`):**

```sql
SELECT * FROM products
WHERE in_stock != false;
-- or
WHERE in_stock != 0;
```

**4. Greater than:**

```sql
SELECT * FROM products
WHERE in_stock > 0;
```

**5. Less than or equal:**

```sql
SELECT * FROM products
WHERE in_stock <= 0;
```

**6. Combining with other conditions:**

```sql
SELECT * FROM products
WHERE in_stock AND price < 400;
```

**7. Using the `IN` operator:**

In the example below `(1, true)` is a [tuple](/sql-reference/data-types/tuple).

```sql
SELECT * FROM products
WHERE in_stock IN (1, true);
```

You can also use an [array](/sql-reference/data-types/array) to do this:

```sql
SELECT * FROM products
WHERE in_stock IN [1, true];
```

**8. Mixing comparison styles:**

```sql
SELECT * FROM products
WHERE category = 'Electronics' AND in_stock = true;
```

### Pattern matching and conditional expressions {#examples-pattern-matching-and-conditional-expressions}

The examples below use the table and data from the [example](#example-filtering-with-logical-operators) above. Results are omitted for sake of brevity.

#### LIKE examples {#like-examples}

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

#### ILIKE examples {#ilike-examples}

```sql
-- Case-insensitive search for 'LAPTOP'
SELECT * FROM products WHERE name ILIKE '%laptop%';
-- Result: Laptop

-- Case-insensitive prefix match
SELECT * FROM products WHERE name ILIKE 'l%';
-- Result: Laptop, Lamp
```

#### IF examples {#if-examples}

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

#### multiIf examples {#multiif-examples}

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

#### CASE examples {#case-examples}

**Simple CASE:**

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

**Searched CASE:**

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
