---
description: 'cria um armazenamento temporário que preenche colunas com valores.'
keywords: ['values', 'função de tabela']
sidebar_label: 'values'
sidebar_position: 210
slug: /sql-reference/table-functions/values
title: 'values'
doc_type: 'reference'
---

A função de tabela `Values` permite criar um armazenamento temporário que preenche
colunas com valores. Ela é útil para testes rápidos ou para gerar dados de exemplo.

:::note
Values é uma função que não diferencia maiúsculas de minúsculas. Ou seja, tanto `VALUES` quanto `values` são válidos.
:::

<div id="syntax">
  ## Sintaxe
</div>

A sintaxe básica da função de tabela `VALUES` é:

```sql
VALUES([structure,] values...)
```

Geralmente é usado como:

```sql
VALUES(
    ['column1_name Type1, column2_name Type2, ...'],
    (value1_row1, value2_row1, ...),
    (value1_row2, value2_row2, ...),
    ...
)
```

<div id="arguments">
  ## Argumentos
</div>

* `column1_name Type1, ...` (opcional). [String](/pt-BR/sql-reference/data-types/string)
  que especifica os nomes e os tipos das colunas. Se esse argumento for omitido, as colunas serão
  nomeadas como `c1`, `c2` etc.
* `(value1_row1, value2_row1)`. [Tuples](/pt-BR/sql-reference/data-types/tuple)
  contendo valores de qualquer tipo.

:::note
Tuplas separadas por vírgulas também podem ser substituídas por valores únicos. Nesse caso,
cada valor é considerado uma nova linha. Consulte a seção de [exemplos](#examples) para mais
detalhes.
:::

<div id="returned-value">
  ## Valor retornado
</div>

* Retorna uma tabela temporária com os valores fornecidos.

<div id="examples">
  ## Exemplos
</div>

```sql title="Query"
SELECT *
FROM VALUES(
    'person String, place String',
    ('Noah', 'Paris'),
    ('Emma', 'Tokyo'),
    ('Liam', 'Sydney'),
    ('Olivia', 'Berlin'),
    ('Ilya', 'London'),
    ('Sophia', 'London'),
    ('Jackson', 'Madrid'),
    ('Alexey', 'Amsterdam'),
    ('Mason', 'Venice'),
    ('Isabella', 'Prague')
)
```

```response title="Response"
    ┌─person───┬─place─────┐
 1. │ Noah     │ Paris     │
 2. │ Emma     │ Tokyo     │
 3. │ Liam     │ Sydney    │
 4. │ Olivia   │ Berlin    │
 5. │ Ilya     │ London    │
 6. │ Sophia   │ London    │
 7. │ Jackson  │ Madrid    │
 8. │ Alexey   │ Amsterdam │
 9. │ Mason    │ Venice    │
10. │ Isabella │ Prague    │
    └──────────┴───────────┘
```

`VALUES` também pode ser usado com valores individuais, em vez de tuplas. Por exemplo:

```sql title="Query"
SELECT *
FROM VALUES(
    'person String',
    'Noah',
    'Emma',
    'Liam',
    'Olivia',
    'Ilya',
    'Sophia',
    'Jackson',
    'Alexey',
    'Mason',
    'Isabella'
)
```

```response title="Response"
    ┌─person───┐
 1. │ Noah     │
 2. │ Emma     │
 3. │ Liam     │
 4. │ Olivia   │
 5. │ Ilya     │
 6. │ Sophia   │
 7. │ Jackson  │
 8. │ Alexey   │
 9. │ Mason    │
10. │ Isabella │
    └──────────┘
```

Ou sem fornecer uma especificação de linha (`'column1_name Type1, column2_name Type2, ...'`
na [Sintaxe](#syntax)), caso em que as colunas recebem nomes automaticamente.

Por exemplo:

```sql title="Query"
-- tuples as values
SELECT *
FROM VALUES(
    ('Noah', 'Paris'),
    ('Emma', 'Tokyo'),
    ('Liam', 'Sydney'),
    ('Olivia', 'Berlin'),
    ('Ilya', 'London'),
    ('Sophia', 'London'),
    ('Jackson', 'Madrid'),
    ('Alexey', 'Amsterdam'),
    ('Mason', 'Venice'),
    ('Isabella', 'Prague')
)
```

```response title="Response"
    ┌─c1───────┬─c2────────┐
 1. │ Noah     │ Paris     │
 2. │ Emma     │ Tokyo     │
 3. │ Liam     │ Sydney    │
 4. │ Olivia   │ Berlin    │
 5. │ Ilya     │ London    │
 6. │ Sophia   │ London    │
 7. │ Jackson  │ Madrid    │
 8. │ Alexey   │ Amsterdam │
 9. │ Mason    │ Venice    │
10. │ Isabella │ Prague    │
    └──────────┴───────────┘
```

```sql title="Query"
-- single values
SELECT *
FROM VALUES(
    'Noah',
    'Emma',
    'Liam',
    'Olivia',
    'Ilya',
    'Sophia',
    'Jackson',
    'Alexey',
    'Mason',
    'Isabella'
)
```

```response title="Response"
    ┌─c1───────┐
 1. │ Noah     │
 2. │ Emma     │
 3. │ Liam     │
 4. │ Olivia   │
 5. │ Ilya     │
 6. │ Sophia   │
 7. │ Jackson  │
 8. │ Alexey   │
 9. │ Mason    │
10. │ Isabella │
    └──────────┘
```

<div id="sql-standard-values-clause">
  ## Cláusula VALUES padrão do SQL
</div>

A partir da versão 26.3, o ClickHouse também oferece suporte à cláusula `VALUES` padrão do SQL como uma expressão de tabela
em `FROM`, como no PostgreSQL, MySQL, DuckDB e SQL Server. Essa sintaxe é
reescrita internamente para usar a função de tabela `values` descrita acima.

```sql title="Query"
SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, val);
```

```response title="Response"
┌─id─┬─val─┐
│  1 │ a   │
│  2 │ b   │
│  3 │ c   │
└────┴─────┘
```

Pode ser usado em CTEs:

```sql title="Query"
WITH cte AS (SELECT * FROM (VALUES (1, 'one'), (2, 'two')) AS t(id, name))
SELECT * FROM cte;
```

E nas junções:

```sql title="Query"
SELECT t1.id, t1.val, t2.val2
FROM (VALUES (1, 'a'), (2, 'b')) AS t1(id, val)
JOIN (VALUES (1, 'x'), (2, 'y')) AS t2(id, val2) ON t1.id = t2.id;
```

:::note
Os aliases de coluna após `AS t(col1, col2, ...)` seguem a sintaxe SQL padrão usada para
nomear colunas de tabelas derivadas. Se omitidos, as colunas serão nomeadas como `c1`, `c2` etc.
:::

<div id="see-also">
  ## Veja também
</div>

* [Formato Values](/pt-BR/interfaces/formats/Values)