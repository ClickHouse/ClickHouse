---
description: 'Documentação da cláusula `WHERE` no ClickHouse'
sidebar_label: 'WHERE'
slug: /sql-reference/statements/select/where
title: 'Cláusula WHERE'
doc_type: 'referência'
keywords: ['WHERE']
---

A cláusula `WHERE` permite filtrar os dados que vêm da cláusula [`FROM`](../../../sql-reference/statements/select/from.md) de `SELECT`.

Se houver uma cláusula `WHERE`, ela deve ser seguida por uma expressão do tipo `UInt8`.
As linhas para as quais essa expressão resulta em `0` são excluídas de transformações posteriores ou do resultado.

A expressão que segue a cláusula `WHERE` costuma ser usada com [operadores de comparação](/pt-BR/sql-reference/operators#comparison-operators) e [operadores lógicos](/pt-BR/sql-reference/operators#operators-for-working-with-data-sets), ou com uma das muitas [funções regulares](/pt-BR/sql-reference/functions/regular-functions).

A expressão `WHERE` é analisada quanto à possibilidade de usar índices e poda de partições, se o mecanismo da tabela subjacente oferecer suporte a isso.

:::note PREWHERE
Também existe uma otimização de filtragem chamada [`PREWHERE`](../../../sql-reference/statements/select/prewhere.md).
`PREWHERE` é uma otimização para aplicar a filtragem com mais eficiência.
Ela é habilitada por padrão, mesmo que a cláusula `PREWHERE` não seja especificada explicitamente.
:::

<div id="testing-for-null">
  ## Testando se é `NULL`
</div>

Se você precisar verificar se um valor é [`NULL`](/pt-BR/sql-reference/syntax#null), use:

* [`IS NULL`](/pt-BR/sql-reference/operators#is_null) ou [`isNull`](../../../sql-reference/functions/functions-for-nulls.md#isNull)
* [`IS NOT NULL`](/pt-BR/sql-reference/operators#is_not_null)   ou [`isNotNull`](../../../sql-reference/functions/functions-for-nulls.md#isNotNull)

Caso contrário, uma expression com `NULL` nunca será avaliada como verdadeira.

<div id="filtering-data-with-logical-operators">
  ## Filtragem de dados com operadores lógicos
</div>

Você pode usar as seguintes [funções lógicas](/pt-BR/sql-reference/functions/logical-functions#and) junto com a cláusula `WHERE` para combinar várias condições:

* [`and()`](/pt-BR/sql-reference/functions/logical-functions#and) ou `AND`
* [`not()`](/pt-BR/sql-reference/functions/logical-functions#not) ou `NOT`
* [`or()`](/pt-BR/sql-reference/functions/logical-functions#or) ou `NOT`
* [`xor()`](/pt-BR/sql-reference/functions/logical-functions#xor)

<div id="using-uint8-columns-as-a-condition">
  ## Usando colunas UInt8 como condição
</div>

No ClickHouse, colunas `UInt8` podem ser usadas diretamente como condições booleanas, em que `0` é `false` e qualquer valor diferente de zero (normalmente `1`) é `true`.
Um exemplo disso é mostrado na seção [abaixo](#example-uint8-column-as-condition).

<div id="using-comparison-operators">
  ## Usando operadores de comparação
</div>

Os seguintes [operadores de comparação](/pt-BR/sql-reference/operators#comparison-operators) podem ser usados:

| Operador                | Função                  | Descrição                                                         | Exemplo                         |
| ----------------------- | ----------------------- | ----------------------------------------------------------------- | ------------------------------- |
| `a = b`                 | `equals(a, b)`          | Igual a                                                           | `price = 100`                   |
| `a == b`                | `equals(a, b)`          | Igual a (sintaxe alternativa)                                     | `price == 100`                  |
| `a != b`                | `notEquals(a, b)`       | Diferente de                                                      | `category != 'Electronics'`     |
| `a <> b`                | `notEquals(a, b)`       | Diferente de (sintaxe alternativa)                                | `category <> 'Electronics'`     |
| `a < b`                 | `less(a, b)`            | Menor que                                                         | `price < 200`                   |
| `a <= b`                | `lessOrEquals(a, b)`    | Menor que ou igual a                                              | `price <= 200`                  |
| `a > b`                 | `greater(a, b)`         | Maior que                                                         | `price > 500`                   |
| `a >= b`                | `greaterOrEquals(a, b)` | Maior que ou igual a                                              | `price >= 500`                  |
| `a LIKE s`              | `like(a, b)`            | Correspondência de padrões (sensível a maiúsculas e minúsculas)   | `name LIKE '%top%'`             |
| `a NOT LIKE s`          | `notLike(a, b)`         | Não corresponde ao padrão (sensível a maiúsculas e minúsculas)    | `name NOT LIKE '%top%'`         |
| `a ILIKE s`             | `ilike(a, b)`           | Correspondência de padrões (insensível a maiúsculas e minúsculas) | `name ILIKE '%LAPTOP%'`         |
| `a BETWEEN b AND c`     | `a >= b AND a <= c`     | Verificação de intervalo (inclusiva)                              | `price BETWEEN 100 AND 500`     |
| `a NOT BETWEEN b AND c` | `a < b OR a > c`        | Fora do intervalo                                                 | `price NOT BETWEEN 100 AND 500` |

<div id="pattern-matching-and-conditional-expressions">
  ## Correspondência de padrões e expressões condicionais
</div>

Além dos operadores de comparação, você pode usar correspondência de padrões e expressões condicionais na cláusula `WHERE`.

| Funcionalidade | Sintaxe                        | Sensível a maiúsculas e minúsculas | Desempenho | Melhor para                                                                   |
| -------------- | ------------------------------ | ---------------------------------- | ---------- | ----------------------------------------------------------------------------- |
| `LIKE`         | `col LIKE '%pattern%'`         | Sim                                | Rápido     | Correspondência exata de padrões, com distinção entre maiúsculas e minúsculas |
| `ILIKE`        | `col ILIKE '%pattern%'`        | Não                                | Mais lento | Pesquisa sem diferenciar maiúsculas de minúsculas                             |
| `if()`         | `if(cond, a, b)`               | N/D                                | Rápido     | Condições binárias simples                                                    |
| `multiIf()`    | `multiIf(c1, r1, c2, r2, def)` | N/D                                | Rápido     | Múltiplas condições                                                           |
| `CASE`         | `CASE WHEN ... THEN ... END`   | N/D                                | Rápido     | Lógica condicional padrão de SQL                                              |

Consulte [&quot;Correspondência de padrões e expressões condicionais&quot;](#examples-pattern-matching-and-conditional-expressions) para ver exemplos de uso.

<div id="expressions-with-literals-columns-subqueries">
  ## Expressão com literais, colunas ou subconsultas
</div>

A expressão após a cláusula `WHERE` também pode incluir [literais](/pt-BR/sql-reference/syntax#literals), colunas ou subconsultas, que são instruções `SELECT` aninhadas que retornam valores usados nas condições.

| Tipo            | Definição                    | Avaliação                              | Desempenho  | Exemplo                    |
| --------------- | ---------------------------- | -------------------------------------- | ----------- | -------------------------- |
| **Literal**     | Valor constante fixo         | No momento em que a consulta é escrita | Mais rápido | `WHERE price > 100`        |
| **Coluna**      | Referência a dados da tabela | Por linha                              | Rápido      | `WHERE price > cost`       |
| **Subconsulta** | `SELECT` aninhado            | Tempo de execução da consulta          | Varia       | `WHERE id IN (SELECT ...)` |

Você pode misturar literais, colunas e subconsultas em condições complexas:

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
  ## Exemplos
</div>

<div id="examples-testing-for-null">
  ### Testando valores `NULL`
</div>

Consultas com valores `NULL`:

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
  ### Filtragem de dados com operadores lógicos
</div>

Dada a tabela e os dados a seguir:

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

**1. `AND` - as duas condições devem ser true:**

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

**2. `OR` - pelo menos uma condição deve ser true:**

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

**3. `NOT` - Nega uma condição:**

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

**4. `XOR` - Exatamente uma condição deve ser `true` (não as duas):**

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

**5. Combinando vários operadores:**

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

**6. Usando a sintaxe de função:**

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

A sintaxe com palavras-chave SQL (`AND`, `OR`, `NOT`, `XOR`) geralmente é mais legível, mas a sintaxe de função pode ser útil em expressões complexas ou ao criar consultas dinâmicas.

<div id="example-uint8-column-as-condition">
  ### Usando colunas UInt8 como condição
</div>

Usando a tabela de um [exemplo anterior](#example-filtering-with-logical-operators), você pode usar diretamente o nome de uma coluna como condição:

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
  ### Usando operadores de comparação
</div>

Os exemplos abaixo usam a tabela e os dados do [exemplo](#example-filtering-with-logical-operators) acima. Os resultados foram omitidos por brevidade.

**1. Igualdade explícita com true (`= 1` ou `= true`):**

```sql
SELECT * FROM products
WHERE in_stock = true;
-- or
WHERE in_stock = 1;
```

**2. Igualdade explícita com false (`= 0` ou `= false`):**

```sql
SELECT * FROM products
WHERE in_stock = false;
-- or
WHERE in_stock = 0;
```

**3. Desigualdade (`!= 0` ou `!= false`):**

```sql
SELECT * FROM products
WHERE in_stock != false;
-- or
WHERE in_stock != 0;
```

**4. Maior que:**

```sql
SELECT * FROM products
WHERE in_stock > 0;
```

**5. Menor ou igual a:**

```sql
SELECT * FROM products
WHERE in_stock <= 0;
```

**6. Em combinação com outras condições:**

```sql
SELECT * FROM products
WHERE in_stock AND price < 400;
```

**7. Usando o operador `IN`:**

No exemplo abaixo, `(1, true)` é uma [tupla](/pt-BR/sql-reference/data-types/tuple).

```sql
SELECT * FROM products
WHERE in_stock IN (1, true);
```

Você também pode usar um [array](/pt-BR/sql-reference/data-types/array) para isso:

```sql
SELECT * FROM products
WHERE in_stock IN [1, true];
```

**8. Mistura de estilos de comparação:**

```sql
SELECT * FROM products
WHERE category = 'Electronics' AND in_stock = true;
```

<div id="examples-pattern-matching-and-conditional-expressions">
  ### Correspondência de padrões e expressões condicionais
</div>

Os exemplos abaixo usam a tabela e os dados do [exemplo](#example-filtering-with-logical-operators) acima. Os resultados foram omitidos por brevidade.

<div id="like-examples">
  #### Exemplos de LIKE
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
  #### Exemplos de ILIKE
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
  #### Exemplos de IF
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
  #### Exemplos de multiIf
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
  #### Exemplos de CASE
</div>

**CASE simples:**

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

**CASE pesquisado:**

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