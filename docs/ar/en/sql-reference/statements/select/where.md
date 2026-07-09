---
description: 'توثيق لعبارة `WHERE` في ClickHouse'
sidebar_label: 'WHERE'
slug: /sql-reference/statements/select/where
title: 'عبارة `WHERE`'
doc_type: 'مرجع'
keywords: ['WHERE']
---

تتيح لك عبارة `WHERE` تصفية البيانات الواردة من عبارة [`FROM`](../../../sql-reference/statements/select/from.md) في `SELECT`.

إذا وُجدت عبارة `WHERE`، فيجب أن يتبعها تعبير من النوع `UInt8`.
وتُستبعَد الصفوف التي يُقيَّم فيها هذا التعبير إلى `0` من التحويلات اللاحقة أو من النتيجة.

غالبًا ما يُستخدم التعبير الذي يلي عبارة `WHERE` مع [عوامل المقارنة](/ar/sql-reference/operators#comparison-operators) و[العوامل المنطقية](/ar/sql-reference/operators#operators-for-working-with-data-sets)، أو مع إحدى [الدوال العادية](/ar/sql-reference/functions/regular-functions) العديدة.

ويُقيَّم تعبير `WHERE` أيضًا من حيث إمكانية استخدام الفهارس وتشذيب الأقسام، إذا كان محرك الجدول الأساسي يدعم ذلك.

:::note PREWHERE
يوجد أيضًا تحسين للتصفية يُسمى [`PREWHERE`](../../../sql-reference/statements/select/prewhere.md).
ويُعد Prewhere تحسينًا يتيح تطبيق التصفية بكفاءة أكبر.
وهو مُمكَّن افتراضيًا حتى إذا لم تُحدَّد عبارة `PREWHERE` صراحةً.
:::

<div id="testing-for-null">
  ## اختبار ما إذا كانت القيمة `NULL`
</div>

إذا كنت بحاجة إلى اختبار ما إذا كانت قيمة ما تساوي [`NULL`](/ar/sql-reference/syntax#null)، فاستخدم:

* [`IS NULL`](/ar/sql-reference/operators#is_null) أو [`isNull`](../../../sql-reference/functions/functions-for-nulls.md#isNull)
* [`IS NOT NULL`](/ar/sql-reference/operators#is_not_null)   أو [`isNotNull`](../../../sql-reference/functions/functions-for-nulls.md#isNotNull)

وإلا فلن ينجح أي تعبير يتضمن `NULL` مطلقًا.

<div id="filtering-data-with-logical-operators">
  ## تصفية البيانات باستخدام العوامل المنطقية
</div>

يمكنك استخدام [الدوال المنطقية](/ar/sql-reference/functions/logical-functions#and) التالية مع عبارة `WHERE` لدمج عدة شروط:

* [`and()`](/ar/sql-reference/functions/logical-functions#and) أو `AND`
* [`not()`](/ar/sql-reference/functions/logical-functions#not) أو `NOT`
* [`or()`](/ar/sql-reference/functions/logical-functions#or) أو `NOT`
* [`xor()`](/ar/sql-reference/functions/logical-functions#xor)

<div id="using-uint8-columns-as-a-condition">
  ## استخدام أعمدة `UInt8` كشرط
</div>

في ClickHouse، يمكن استخدام أعمدة `UInt8` مباشرةً كشروط منطقية، بحيث تمثّل `0` القيمة `false`، وتمثّل أي قيمة غير صفرية (عادةً `1`) القيمة `true`.
ويَرِد مثال على ذلك في القسم [أدناه](#example-uint8-column-as-condition).

<div id="using-comparison-operators">
  ## استخدام عوامل المقارنة
</div>

يمكن استخدام [عوامل المقارنة](/ar/sql-reference/operators#comparison-operators) التالية:

| العامل                  | الدالة                  | الوصف                                  | مثال                            |
| ----------------------- | ----------------------- | -------------------------------------- | ------------------------------- |
| `a = b`                 | `equals(a, b)`          | يساوي                                  | `price = 100`                   |
| `a == b`                | `equals(a, b)`          | يساوي (صيغة بديلة)                     | `price == 100`                  |
| `a != b`                | `notEquals(a, b)`       | لا يساوي                               | `category != 'Electronics'`     |
| `a <> b`                | `notEquals(a, b)`       | لا يساوي (صيغة بديلة)                  | `category <> 'Electronics'`     |
| `a < b`                 | `less(a, b)`            | أقل من                                 | `price < 200`                   |
| `a <= b`                | `lessOrEquals(a, b)`    | أقل من أو يساوي                        | `price <= 200`                  |
| `a > b`                 | `greater(a, b)`         | أكبر من                                | `price > 500`                   |
| `a >= b`                | `greaterOrEquals(a, b)` | أكبر من أو يساوي                       | `price >= 500`                  |
| `a LIKE s`              | `like(a, b)`            | مطابقة النمط (حسّاسة لحالة الأحرف)     | `name LIKE '%top%'`             |
| `a NOT LIKE s`          | `notLike(a, b)`         | عدم مطابقة النمط (حسّاسة لحالة الأحرف) | `name NOT LIKE '%top%'`         |
| `a ILIKE s`             | `ilike(a, b)`           | مطابقة النمط (غير حسّاسة لحالة الأحرف) | `name ILIKE '%LAPTOP%'`         |
| `a BETWEEN b AND c`     | `a >= b AND a <= c`     | التحقق من النطاق (شامل)                | `price BETWEEN 100 AND 500`     |
| `a NOT BETWEEN b AND c` | `a < b OR a > c`        | التحقق من كون القيمة خارج النطاق       | `price NOT BETWEEN 100 AND 500` |

<div id="pattern-matching-and-conditional-expressions">
  ## مطابقة الأنماط والتعبيرات الشرطية
</div>

إلى جانب عوامل المقارنة، يمكنك استخدام مطابقة الأنماط والتعبيرات الشرطية في عبارة `WHERE`.

| الميزة      | الصياغة                        | مراعي لحالة الأحرف | الأداء | الأنسب لـ                                 |
| ----------- | ------------------------------ | ------------------ | ------ | ----------------------------------------- |
| `LIKE`      | `col LIKE '%pattern%'`         | نعم                | سريع   | مطابقة الأنماط بدقة مع مراعاة حالة الأحرف |
| `ILIKE`     | `col ILIKE '%pattern%'`        | لا                 | أبطأ   | البحث غير المراعي لحالة الأحرف            |
| `if()`      | `if(cond, a, b)`               | غير منطبق          | سريع   | الشروط الثنائية البسيطة                   |
| `multiIf()` | `multiIf(c1, r1, c2, r2, def)` | غير منطبق          | سريع   | شروط متعددة                               |
| `CASE`      | `CASE WHEN ... THEN ... END`   | غير منطبق          | سريع   | منطق شرطي وفق معيار SQL                   |

راجع [&quot;مطابقة الأنماط والتعبيرات الشرطية&quot;](#examples-pattern-matching-and-conditional-expressions) للاطلاع على أمثلة الاستخدام.

<div id="expressions-with-literals-columns-subqueries">
  ## تعبير يتضمن قيمًا حرفية أو أعمدة أو استعلامات فرعية
</div>

يمكن أن يتضمن التعبير الذي يلي عبارة `WHERE` أيضًا [قيمًا حرفية](/ar/sql-reference/syntax#literals)، أو أعمدة، أو استعلامات فرعية، وهي عبارات `SELECT` متداخلة تُرجع قيمًا تُستخدم في الشروط.

| النوع            | التعريف                | التقييم             | الأداء | مثال                       |
| ---------------- | ---------------------- | ------------------- | ------ | -------------------------- |
| **قيمة حرفية**   | قيمة ثابتة محددة       | عند كتابة الاستعلام | الأسرع | `WHERE price > 100`        |
| **عمود**         | مرجع إلى بيانات الجدول | لكل صف              | سريع   | `WHERE price > cost`       |
| **استعلام فرعي** | SELECT متداخل          | وقت تنفيذ الاستعلام | يختلف  | `WHERE id IN (SELECT ...)` |

يمكنك المزج بين القيم الحرفية والأعمدة والاستعلامات الفرعية في الشروط المعقدة:

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
  ## أمثلة
</div>

<div id="examples-testing-for-null">
  ### التحقق من `NULL`
</div>

الاستعلامات التي تحتوي على قيم `NULL`:

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
  ### تصفية البيانات باستخدام العوامل المنطقية
</div>

بالنظر إلى الجدول التالي والبيانات الواردة فيه:

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

**1. `AND` - يجب أن يتحقق الشرطان معًا:**

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

**2. `OR` - يجب أن يكون أحد الشروط على الأقل صحيحًا:**

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

**3. `NOT` - ينفي شرطًا:**

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

**4. `XOR` - يجب أن يكون شرط واحد فقط بقيمة `true` (وليس كلاهما):**

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

**5. دمج عدة عوامل تشغيل:**

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

**6. استخدام صيغة الدالة:**

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

تكون صيغة الكلمات المفتاحية في SQL (`AND`, `OR`, `NOT`, `XOR`) عمومًا أوضح قراءةً، لكن صيغة الدوال قد تكون مفيدة في التعبيرات المعقدة أو عند إنشاء استعلامات ديناميكية.

<div id="example-uint8-column-as-condition">
  ### استخدام أعمدة UInt8 كشرط
</div>

استنادًا إلى الجدول في [مثال سابق](#example-filtering-with-logical-operators)، يمكنك استخدام اسم عمود مباشرةً كشرط:

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
  ### استخدام عوامل المقارنة
</div>

تستخدم الأمثلة أدناه الجدول والبيانات الواردة في [المثال](#example-filtering-with-logical-operators) أعلاه. وقد حُذفت النتائج اختصارًا.

**1. المساواة الصريحة مع true (`= 1` or `= true`):**

```sql
SELECT * FROM products
WHERE in_stock = true;
-- or
WHERE in_stock = 1;
```

**2. المساواة الصريحة مع false (`= 0` أو `= false`):**

```sql
SELECT * FROM products
WHERE in_stock = false;
-- or
WHERE in_stock = 0;
```

**3. عدم المساواة (`!= 0` أو `!= false`):**

```sql
SELECT * FROM products
WHERE in_stock != false;
-- or
WHERE in_stock != 0;
```

**4. أكبر من:**

```sql
SELECT * FROM products
WHERE in_stock > 0;
```

**5. أقل من أو يساوي:**

```sql
SELECT * FROM products
WHERE in_stock <= 0;
```

**6. الدمج مع شروط أخرى:**

```sql
SELECT * FROM products
WHERE in_stock AND price < 400;
```

**7. استخدام عامل التشغيل `IN`:**

في المثال أدناه، تُعد `(1, true)` قيمة من النوع [Tuple](/ar/sql-reference/data-types/tuple).

```sql
SELECT * FROM products
WHERE in_stock IN (1, true);
```

يمكنك أيضًا استخدام [مصفوفة](/ar/sql-reference/data-types/array) للقيام بذلك:

```sql
SELECT * FROM products
WHERE in_stock IN [1, true];
```

**8. الخلط بين أساليب المقارنة:**

```sql
SELECT * FROM products
WHERE category = 'Electronics' AND in_stock = true;
```

<div id="examples-pattern-matching-and-conditional-expressions">
  ### مطابقة الأنماط والتعبيرات الشرطية
</div>

تستخدم الأمثلة أدناه الجدول والبيانات الواردين في [المثال](#example-filtering-with-logical-operators) أعلاه. وقد حُذفت النتائج للاختصار.

<div id="like-examples">
  #### أمثلة على LIKE
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
  #### أمثلة على ILIKE
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
  #### أمثلة IF
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
  #### أمثلة على multiIf
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
  #### أمثلة CASE
</div>

**CASE بسيط:**

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

**صيغة CASE المشروطة:**

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