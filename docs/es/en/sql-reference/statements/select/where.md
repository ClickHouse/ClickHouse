---
description: 'Documentación de la cláusula `WHERE` en ClickHouse'
sidebar_label: 'WHERE'
slug: /sql-reference/statements/select/where
title: 'Cláusula WHERE'
doc_type: 'reference'
keywords: ['WHERE']
---

La cláusula `WHERE` le permite filtrar los datos que provienen de la cláusula [`FROM`](../../../sql-reference/statements/select/from.md) de `SELECT`.

Si hay una cláusula `WHERE`, debe ir seguida de una expresión de tipo `UInt8`.
Las filas en las que esta expresión se evalúa como `0` se excluyen de transformaciones posteriores o del resultado.

La expresión que sigue a la cláusula `WHERE` suele usarse con [operadores de comparación](/es/sql-reference/operators#comparison-operators) y [operadores lógicos](/es/sql-reference/operators#operators-for-working-with-data-sets), o con una de las muchas [funciones regulares](/es/sql-reference/functions/regular-functions).

La expresión `WHERE` se evalúa en función de si es posible usar índices y poda de particiones, si el motor de tabla de la tabla subyacente lo admite.

:::note PREWHERE
También existe una optimización de filtrado llamada [`PREWHERE`](../../../sql-reference/statements/select/prewhere.md).
`PREWHERE` es una optimización para aplicar el filtrado de forma más eficiente.
Está habilitada de forma predeterminada incluso si la cláusula `PREWHERE` no se especifica explícitamente.
:::

<div id="testing-for-null">
  ## Comprobación de `NULL`
</div>

Si necesita comprobar si un valor es [`NULL`](/es/sql-reference/syntax#null), use:

* [`IS NULL`](/es/sql-reference/operators#is_null) o [`isNull`](../../../sql-reference/functions/functions-for-nulls.md#isNull)
* [`IS NOT NULL`](/es/sql-reference/operators#is_not_null)   o [`isNotNull`](../../../sql-reference/functions/functions-for-nulls.md#isNotNull)

De lo contrario, una expresión con `NULL` nunca se evaluará como verdadera.

<div id="filtering-data-with-logical-operators">
  ## Filtrado de datos con operadores lógicos
</div>

Puede utilizar las siguientes [funciones lógicas](/es/sql-reference/functions/logical-functions#and) junto con la cláusula `WHERE` para combinar varias condiciones:

* [`and()`](/es/sql-reference/functions/logical-functions#and) o `AND`
* [`not()`](/es/sql-reference/functions/logical-functions#not) o `NOT`
* [`or()`](/es/sql-reference/functions/logical-functions#or) o `NOT`
* [`xor()`](/es/sql-reference/functions/logical-functions#xor)

<div id="using-uint8-columns-as-a-condition">
  ## Uso de columnas `UInt8` como condición
</div>

En ClickHouse, las columnas `UInt8` pueden usarse directamente como condiciones booleanas, donde `0` es `false` y cualquier valor distinto de cero (normalmente `1`) es `true`.
Se muestra un ejemplo de esto en la sección [siguiente](#example-uint8-column-as-condition).

<div id="using-comparison-operators">
  ## Uso de operadores de comparación
</div>

Se pueden utilizar los siguientes [operadores de comparación](/es/sql-reference/operators#comparison-operators):

| Operador                | Función                 | Descripción                                                       | Ejemplo                         |
| ----------------------- | ----------------------- | ----------------------------------------------------------------- | ------------------------------- |
| `a = b`                 | `equals(a, b)`          | Igual a                                                           | `price = 100`                   |
| `a == b`                | `equals(a, b)`          | Igual a (sintaxis alternativa)                                    | `price == 100`                  |
| `a != b`                | `notEquals(a, b)`       | Distinto de                                                       | `category != 'Electronics'`     |
| `a <> b`                | `notEquals(a, b)`       | Distinto de (sintaxis alternativa)                                | `category <> 'Electronics'`     |
| `a < b`                 | `less(a, b)`            | Menor que                                                         | `price < 200`                   |
| `a <= b`                | `lessOrEquals(a, b)`    | Menor o igual que                                                 | `price <= 200`                  |
| `a > b`                 | `greater(a, b)`         | Mayor que                                                         | `price > 500`                   |
| `a >= b`                | `greaterOrEquals(a, b)` | Mayor o igual que                                                 | `price >= 500`                  |
| `a LIKE s`              | `like(a, b)`            | Coincidencia de patrones (sensible a mayúsculas y minúsculas)     | `name LIKE '%top%'`             |
| `a NOT LIKE s`          | `notLike(a, b)`         | Sin coincidencia de patrones (sensible a mayúsculas y minúsculas) | `name NOT LIKE '%top%'`         |
| `a ILIKE s`             | `ilike(a, b)`           | Coincidencia de patrones (sin distinguir mayúsculas y minúsculas) | `name ILIKE '%LAPTOP%'`         |
| `a BETWEEN b AND c`     | `a >= b AND a <= c`     | Comprobación de rango (incluye los límites)                       | `price BETWEEN 100 AND 500`     |
| `a NOT BETWEEN b AND c` | `a < b OR a > c`        | Comprobación fuera del rango                                      | `price NOT BETWEEN 100 AND 500` |

<div id="pattern-matching-and-conditional-expressions">
  ## Coincidencia de patrones y expresiones condicionales
</div>

Además de los operadores de comparación, puede usar la coincidencia de patrones y las expresiones condicionales en la cláusula `WHERE`.

| Funcionalidad | Sintaxis                       | Sensible a mayúsculas y minúsculas | Rendimiento | Mejor para                                                            |
| ------------- | ------------------------------ | ---------------------------------- | ----------- | --------------------------------------------------------------------- |
| `LIKE`        | `col LIKE '%pattern%'`         | Sí                                 | Rápido      | Coincidencia de patrones con distinción entre mayúsculas y minúsculas |
| `ILIKE`       | `col ILIKE '%pattern%'`        | No                                 | Más lento   | Búsquedas sin distinción entre mayúsculas y minúsculas                |
| `if()`        | `if(cond, a, b)`               | N/A                                | Rápido      | Condiciones binarias simples                                          |
| `multiIf()`   | `multiIf(c1, r1, c2, r2, def)` | N/A                                | Rápido      | Múltiples condiciones                                                 |
| `CASE`        | `CASE WHEN ... THEN ... END`   | N/A                                | Rápido      | Lógica condicional estándar de SQL                                    |

Consulte [&quot;Coincidencia de patrones y expresiones condicionales&quot;](#examples-pattern-matching-and-conditional-expressions) para ver ejemplos de uso.

<div id="expressions-with-literals-columns-subqueries">
  ## Expresión con literales, columnas o subconsultas
</div>

La expresión que sigue a la cláusula `WHERE` también puede incluir [literales](/es/sql-reference/syntax#literals), columnas o subconsultas, que son sentencias `SELECT` anidadas que devuelven valores utilizados en las condiciones.

| Tipo            | Definición                     | Evaluación                            | Rendimiento   | Ejemplo                    |
| --------------- | ------------------------------ | ------------------------------------- | ------------- | -------------------------- |
| **Literal**     | Valor constante fijo           | En el momento de escribir la consulta | El más rápido | `WHERE price > 100`        |
| **Columna**     | Referencia a datos de la tabla | Por fila                              | Rápido        | `WHERE price > cost`       |
| **Subconsulta** | `SELECT` anidado               | Tiempo de ejecución de la consulta    | Variable      | `WHERE id IN (SELECT ...)` |

Puedes combinar literales, columnas y subconsultas en condiciones complejas:

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
  ## Ejemplos
</div>

<div id="examples-testing-for-null">
  ### Comprobación de `NULL`
</div>

Consultas con valores `NULL`:

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
  ### Filtrado de datos con operadores lógicos
</div>

Dada la siguiente tabla y los siguientes datos:

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

**1. `AND` - ambas condiciones deben ser verdaderas:**

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

**2. `OR` - al menos una condición debe ser verdadera:**

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

**3. `NOT` - Niega una condición:**

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

**4. `XOR` - Debe cumplirse exactamente una condición (no ambas):**

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

**5. Combinación de varios operadores:**

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

**6. Uso de la sintaxis de funciones:**

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

La sintaxis con palabras clave de SQL (`AND`, `OR`, `NOT`, `XOR`) suele ser más legible, pero la sintaxis de funciones puede resultar útil en expresiones complejas o al crear consultas dinámicas.

<div id="example-uint8-column-as-condition">
  ### Uso de columnas UInt8 como condición
</div>

Tomando la tabla de un [ejemplo anterior](#example-filtering-with-logical-operators), puede usar un nombre de columna directamente como condición:

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
  ### Uso de operadores de comparación
</div>

Los ejemplos siguientes usan la tabla y los datos del [ejemplo](#example-filtering-with-logical-operators) anterior. Se omiten los resultados por motivos de brevedad.

**1. Igualdad explícita con true (`= 1` o `= true`):**

```sql
SELECT * FROM products
WHERE in_stock = true;
-- or
WHERE in_stock = 1;
```

**2. Igualdad explícita con false (`= 0` o `= false`):**

```sql
SELECT * FROM products
WHERE in_stock = false;
-- or
WHERE in_stock = 0;
```

**3. Desigualdad (`!= 0` o `!= false`):**

```sql
SELECT * FROM products
WHERE in_stock != false;
-- or
WHERE in_stock != 0;
```

**4. Mayor que:**

```sql
SELECT * FROM products
WHERE in_stock > 0;
```

**5. Menor o igual a:**

```sql
SELECT * FROM products
WHERE in_stock <= 0;
```

**6. Combinación con otras condiciones:**

```sql
SELECT * FROM products
WHERE in_stock AND price < 400;
```

**7. Uso del operador `IN`:**

En el ejemplo de abajo, `(1, true)` es una [tupla](/es/sql-reference/data-types/tuple).

```sql
SELECT * FROM products
WHERE in_stock IN (1, true);
```

También puedes usar un [Array](/es/sql-reference/data-types/array) para ello:

```sql
SELECT * FROM products
WHERE in_stock IN [1, true];
```

**8. Mezcla de estilos de comparación:**

```sql
SELECT * FROM products
WHERE category = 'Electronics' AND in_stock = true;
```

<div id="examples-pattern-matching-and-conditional-expressions">
  ### Coincidencia de patrones y expresiones condicionales
</div>

Los siguientes ejemplos usan la tabla y los datos del [ejemplo](#example-filtering-with-logical-operators) anterior. Se omiten los resultados por brevedad.

<div id="like-examples">
  #### Ejemplos de LIKE
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
  #### Ejemplos de ILIKE
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
  #### Ejemplos de IF
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
  #### Ejemplos de multiIf
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
  #### Ejemplos de CASE
</div>

**CASE simple:**

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

**CASE de búsqueda:**

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