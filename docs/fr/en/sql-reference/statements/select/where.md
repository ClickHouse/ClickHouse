---
description: 'Documentation de la clause `WHERE` dans ClickHouse'
sidebar_label: 'WHERE'
slug: /sql-reference/statements/select/where
title: 'Clause `WHERE`'
doc_type: 'reference'
keywords: ['WHERE']
---

La clause `WHERE` vous permet de filtrer les données provenant de la clause [`FROM`](../../../sql-reference/statements/select/from.md) de `SELECT`.

Si une clause `WHERE` est présente, elle doit être suivie d&#39;une expression de type `UInt8`.
Les lignes pour lesquelles cette expression s&#39;évalue à `0` sont exclues des transformations ultérieures ou du résultat.

L&#39;expression qui suit la clause `WHERE` est souvent utilisée avec des [opérateurs de comparaison](/fr/sql-reference/operators#comparison-operators) et des [opérateurs logiques](/fr/sql-reference/operators#operators-for-working-with-data-sets), ou avec l&#39;une des nombreuses [fonctions régulières](/fr/sql-reference/functions/regular-functions).

L&#39;expression `WHERE` est évaluée de façon à pouvoir utiliser les index et l&#39;élagage des partitions, si le moteur de la table sous-jacente le prend en charge.

:::note PREWHERE
Il existe également une optimisation de filtrage appelée [`PREWHERE`](../../../sql-reference/statements/select/prewhere.md).
Prewhere est une optimisation qui permet d&#39;appliquer le filtrage plus efficacement.
Elle est activée par défaut, même si la clause `PREWHERE` n&#39;est pas explicitement spécifiée.
:::

<div id="testing-for-null">
  ## Tester la présence de `NULL`
</div>

Si vous devez vérifier si une valeur est [`NULL`](/fr/sql-reference/syntax#null), utilisez :

* [`IS NULL`](/fr/sql-reference/operators#is_null) ou [`isNull`](../../../sql-reference/functions/functions-for-nulls.md#isNull)
* [`IS NOT NULL`](/fr/sql-reference/operators#is_not_null)   ou [`isNotNull`](../../../sql-reference/functions/functions-for-nulls.md#isNotNull)

Sinon, une expression contenant `NULL` ne sera jamais évaluée comme vraie.

<div id="filtering-data-with-logical-operators">
  ## Filtrer les données avec des opérateurs logiques
</div>

Vous pouvez utiliser les [fonctions logiques](/fr/sql-reference/functions/logical-functions#and) suivantes avec la clause `WHERE` pour combiner plusieurs conditions :

* [`and()`](/fr/sql-reference/functions/logical-functions#and) ou `AND`
* [`not()`](/fr/sql-reference/functions/logical-functions#not) ou `NOT`
* [`or()`](/fr/sql-reference/functions/logical-functions#or) ou `NOT`
* [`xor()`](/fr/sql-reference/functions/logical-functions#xor)

<div id="using-uint8-columns-as-a-condition">
  ## Utilisation des colonnes UInt8 comme condition
</div>

Dans ClickHouse, les colonnes `UInt8` peuvent être utilisées directement comme conditions booléennes, où `0` vaut `false` et toute valeur non nulle (généralement `1`) vaut `true`.
Vous trouverez un exemple dans la section [ci-dessous](#example-uint8-column-as-condition).

<div id="using-comparison-operators">
  ## Utilisation des opérateurs de comparaison
</div>

Les [opérateurs de comparaison](/fr/sql-reference/operators#comparison-operators) suivants peuvent être utilisés :

| Opérateur               | Fonction                | Description                                              | Exemple                         |
| ----------------------- | ----------------------- | -------------------------------------------------------- | ------------------------------- |
| `a = b`                 | `equals(a, b)`          | Égal à                                                   | `price = 100`                   |
| `a == b`                | `equals(a, b)`          | Égal à (syntaxe alternative)                             | `price == 100`                  |
| `a != b`                | `notEquals(a, b)`       | Différent de                                             | `category != 'Electronics'`     |
| `a <> b`                | `notEquals(a, b)`       | Différent de (syntaxe alternative)                       | `category <> 'Electronics'`     |
| `a < b`                 | `less(a, b)`            | Inférieur à                                              | `price < 200`                   |
| `a <= b`                | `lessOrEquals(a, b)`    | Inférieur ou égal à                                      | `price <= 200`                  |
| `a > b`                 | `greater(a, b)`         | Supérieur à                                              | `price > 500`                   |
| `a >= b`                | `greaterOrEquals(a, b)` | Supérieur ou égal à                                      | `price >= 500`                  |
| `a LIKE s`              | `like(a, b)`            | Correspondance de motif (sensible à la casse)            | `name LIKE '%top%'`             |
| `a NOT LIKE s`          | `notLike(a, b)`         | Absence de correspondance au motif (sensible à la casse) | `name NOT LIKE '%top%'`         |
| `a ILIKE s`             | `ilike(a, b)`           | Correspondance de motif (insensible à la casse)          | `name ILIKE '%LAPTOP%'`         |
| `a BETWEEN b AND c`     | `a >= b AND a <= c`     | Vérification d&#39;intervalle (bornes incluses)          | `price BETWEEN 100 AND 500`     |
| `a NOT BETWEEN b AND c` | `a < b OR a > c`        | Vérification hors intervalle                             | `price NOT BETWEEN 100 AND 500` |

<div id="pattern-matching-and-conditional-expressions">
  ## Correspondance de motifs et expressions conditionnelles
</div>

En plus des opérateurs de comparaison, vous pouvez utiliser la correspondance de motifs et des expressions conditionnelles dans la clause `WHERE`.

| Fonctionnalité | Syntaxe                        | Sensible à la casse | Performances | Idéal pour                                             |
| -------------- | ------------------------------ | ------------------- | ------------ | ------------------------------------------------------ |
| `LIKE`         | `col LIKE '%pattern%'`         | Oui                 | Rapide       | Correspondance exacte de motifs en respectant la casse |
| `ILIKE`        | `col ILIKE '%pattern%'`        | Non                 | Plus lent    | Recherche insensible à la casse                        |
| `if()`         | `if(cond, a, b)`               | N/A                 | Rapide       | Conditions binaires simples                            |
| `multiIf()`    | `multiIf(c1, r1, c2, r2, def)` | N/A                 | Rapide       | Conditions multiples                                   |
| `CASE`         | `CASE WHEN ... THEN ... END`   | N/A                 | Rapide       | Logique conditionnelle SQL standard                    |

Voir [&quot;Correspondance de motifs et expressions conditionnelles&quot;](#examples-pattern-matching-and-conditional-expressions) pour des exemples d&#39;utilisation.

<div id="expressions-with-literals-columns-subqueries">
  ## Expression avec des littéraux, des colonnes ou des sous-requêtes
</div>

L’expression qui suit la clause `WHERE` peut également inclure des [littéraux](/fr/sql-reference/syntax#literals), des colonnes ou des sous-requêtes, c’est-à-dire des instructions `SELECT` imbriquées qui renvoient des valeurs utilisées dans des conditions.

| Type             | Définition                        | Évaluation                              | Performance    | Exemple                    |
| ---------------- | --------------------------------- | --------------------------------------- | -------------- | -------------------------- |
| **Littéral**     | Valeur constante fixe             | Au moment de la rédaction de la requête | Le plus rapide | `WHERE price > 100`        |
| **Colonne**      | Référence aux données de la table | Par ligne                               | Rapide         | `WHERE price > cost`       |
| **Sous-requête** | SELECT imbriqué                   | Temps d’exécution de la requête         | Variable       | `WHERE id IN (SELECT ...)` |

Vous pouvez combiner des littéraux, des colonnes et des sous-requêtes dans des conditions complexes :

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
  ## Exemples
</div>

<div id="examples-testing-for-null">
  ### tester la présence de `NULL`
</div>

Requêtes avec des valeurs `NULL` :

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
  ### Filtrer les données avec des opérateurs logiques
</div>

Étant donné la table et les données suivantes :

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

**1. `AND` - les deux conditions doivent être vraies :**

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

**2. `OR` - au moins une condition doit être vraie :**

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

**3. `NOT` - Nie une condition :**

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

**4. `XOR` - Une seule condition doit être vraie (pas les deux) :**

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

**5. Combiner plusieurs opérateurs :**

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

**6. Utilisation de la syntaxe fonctionnelle :**

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

La syntaxe SQL par mots-clés (`AND`, `OR`, `NOT`, `XOR`) est généralement plus lisible, mais la syntaxe des fonctions peut être utile dans des expressions complexes ou lors de la construction de requêtes dynamiques.

<div id="example-uint8-column-as-condition">
  ### utilisation des colonnes UInt8 comme condition
</div>

En reprenant la table de l’[exemple précédent](#example-filtering-with-logical-operators), vous pouvez utiliser directement un nom de colonne comme condition :

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
  ### Utilisation des opérateurs de comparaison
</div>

Les exemples ci-dessous utilisent la table et les données de l’[exemple](#example-filtering-with-logical-operators) ci-dessus. Les résultats sont omis par souci de concision.

**1. Égalité explicite avec true (`= 1` or `= true`) :**

```sql
SELECT * FROM products
WHERE in_stock = true;
-- or
WHERE in_stock = 1;
```

**2. Égalité explicite avec false (`= 0` ou `= false`) :**

```sql
SELECT * FROM products
WHERE in_stock = false;
-- or
WHERE in_stock = 0;
```

**3. Inégalité (`!= 0` ou `!= false`) :**

```sql
SELECT * FROM products
WHERE in_stock != false;
-- or
WHERE in_stock != 0;
```

**4. Plus grand que :**

```sql
SELECT * FROM products
WHERE in_stock > 0;
```

**5. Inférieur ou égal à :**

```sql
SELECT * FROM products
WHERE in_stock <= 0;
```

**6. Combiner avec d’autres conditions :**

```sql
SELECT * FROM products
WHERE in_stock AND price < 400;
```

**7. Utilisation de l’opérateur `IN` :**

Dans l’exemple ci-dessous, `(1, true)` est un [tuple](/fr/sql-reference/data-types/tuple).

```sql
SELECT * FROM products
WHERE in_stock IN (1, true);
```

Vous pouvez aussi utiliser un [array](/fr/sql-reference/data-types/array) pour cela :

```sql
SELECT * FROM products
WHERE in_stock IN [1, true];
```

**8. Mélange des styles de comparaison :**

```sql
SELECT * FROM products
WHERE category = 'Electronics' AND in_stock = true;
```

<div id="examples-pattern-matching-and-conditional-expressions">
  ### correspondance de motifs et expressions conditionnelles
</div>

Les exemples ci-dessous utilisent la table et les données de l’[exemple](#example-filtering-with-logical-operators) ci-dessus. Les résultats sont omis par souci de concision.

<div id="like-examples">
  #### Exemples avec LIKE
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
  #### Exemples avec ILIKE
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
  #### Exemples d’IF
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
  #### Exemples de multiIf
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
  #### Exemples de l’expression CASE
</div>

**CASE simple :**

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

**CASE avec conditions :**

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