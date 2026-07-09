---
description: "crée un stockage temporaire pour renseigner les colonnes avec des valeurs."
keywords: ['values', 'fonction de table']
sidebar_label: 'values'
sidebar_position: 210
slug: /sql-reference/table-functions/values
title: 'values'
doc_type: 'reference'
---

La fonction de table `Values` vous permet de créer un stockage temporaire pour renseigner
les colonnes avec des valeurs. Elle est utile pour des tests rapides ou pour générer des données d’exemple.

:::note
Values est une fonction insensible à la casse. Autrement dit, `VALUES` et `values` sont tous deux valides.
:::

<div id="syntax">
  ## Syntaxe
</div>

La syntaxe de base de la fonction de table `VALUES` est :

```sql
VALUES([structure,] values...)
```

Il est généralement utilisé ainsi :

```sql
VALUES(
    ['column1_name Type1, column2_name Type2, ...'],
    (value1_row1, value2_row1, ...),
    (value1_row2, value2_row2, ...),
    ...
)
```

<div id="arguments">
  ## Arguments
</div>

* `column1_name Type1, ...` (facultatif). [String](/fr/sql-reference/data-types/string)
  indiquant les noms et les types des colonnes. Si cet argument est omis, les colonnes seront
  nommées `c1`, `c2`, etc.
* `(value1_row1, value2_row1)`. [Tuples](/fr/sql-reference/data-types/tuple)
  contenant des valeurs de tout type.

:::note
Les tuples séparés par des virgules peuvent également être remplacés par des valeurs individuelles. Dans ce cas,
chaque valeur est considérée comme une nouvelle ligne. Voir la section [exemples](#examples) pour plus de
détails.
:::

<div id="returned-value">
  ## Valeur renvoyée
</div>

* Renvoie une table temporaire contenant les valeurs fournies.

<div id="examples">
  ## Exemples
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

`VALUES` peut également être utilisé avec des valeurs simples plutôt qu’avec des tuples. Par exemple :

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

Ou sans fournir de spécification de ligne (`'column1_name Type1, column2_name Type2, ...'`
dans la [syntaxe](#syntax)), auquel cas les colonnes sont automatiquement nommées.

Par exemple :

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
  ## Clause `VALUES` du standard SQL
</div>

À partir de la version 26.3, ClickHouse prend également en charge la clause `VALUES` du standard SQL en tant qu’expression de table
dans `FROM`, comme dans PostgreSQL, MySQL, DuckDB et SQL Server. Cette syntaxe est
réécrite en interne pour utiliser la fonction de table `values` décrite ci-dessus.

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

Il peut être utilisé dans des CTE :

```sql title="Query"
WITH cte AS (SELECT * FROM (VALUES (1, 'one'), (2, 'two')) AS t(id, name))
SELECT * FROM cte;
```

Et dans les JOIN :

```sql title="Query"
SELECT t1.id, t1.val, t2.val2
FROM (VALUES (1, 'a'), (2, 'b')) AS t1(id, val)
JOIN (VALUES (1, 'x'), (2, 'y')) AS t2(id, val2) ON t1.id = t2.id;
```

:::note
Les alias de colonnes après `AS t(col1, col2, ...)` suivent la syntaxe SQL standard pour
nommer les colonnes des tables dérivées. S’ils sont absents, les colonnes sont nommées `c1`, `c2`, etc.
:::

<div id="see-also">
  ## Voir aussi
</div>

* [Format Values](/fr/interfaces/formats/Values)