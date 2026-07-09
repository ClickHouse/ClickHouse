---
description: 'Documentation relative aux fonctions conditionnelles'
sidebar_label: 'Conditionnel'
slug: /sql-reference/functions/conditional-functions
title: 'Fonctions conditionnelles'
doc_type: 'reference'
---

<div id="overview">
  ## Vue d’ensemble
</div>

<div id="using-conditional-results-directly">
  ### Utiliser directement les résultats des conditions
</div>

Les expressions conditionnelles renvoient toujours `0`, `1` ou `NULL`. Vous pouvez donc utiliser directement ces résultats, comme ceci :

```sql
SELECT left < right AS is_small
FROM LEFT_RIGHT

┌─is_small─┐
│     ᴺᵁᴸᴸ │
│        1 │
│        0 │
│        0 │
│     ᴺᵁᴸᴸ │
└──────────┘
```

<div id="null-values-in-conditionals">
  ### Valeurs NULL dans les expressions conditionnelles
</div>

Lorsqu&#39;une expression conditionnelle implique des valeurs `NULL`, le résultat est également `NULL`.

```sql
SELECT
    NULL < 1,
    2 < NULL,
    NULL < NULL,
    NULL = NULL

┌─less(NULL, 1)─┬─less(2, NULL)─┬─less(NULL, NULL)─┬─equals(NULL, NULL)─┐
│ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ             │ ᴺᵁᴸᴸ               │
└───────────────┴───────────────┴──────────────────┴────────────────────┘
```

Vous devez donc construire vos requêtes avec soin si les types sont `Nullable`.

L&#39;exemple suivant l&#39;illustre en omettant d&#39;ajouter une condition d&#39;égalité à `multiIf`.

```sql
SELECT
    left,
    right,
    multiIf(left < right, 'left is smaller', left > right, 'right is smaller', 'Both equal') AS faulty_result
FROM LEFT_RIGHT

┌─left─┬─right─┬─faulty_result────┐
│ ᴺᵁᴸᴸ │     4 │ Both equal       │
│    1 │     3 │ left is smaller  │
│    2 │     2 │ Both equal       │
│    3 │     1 │ right is smaller │
│    4 │  ᴺᵁᴸᴸ │ Both equal       │
└──────┴───────┴──────────────────┘
```

<div id="case-statement">
  ### Instruction CASE
</div>

L’expression CASE dans ClickHouse fournit une logique conditionnelle semblable à celle de l’opérateur SQL CASE. Elle évalue les conditions et renvoie des valeurs en fonction de la première condition correspondante.

ClickHouse prend en charge deux formes de CASE :

1. `CASE WHEN ... THEN ... ELSE ... END`
   <br />
   Cette forme offre une flexibilité totale et est implémentée en interne à l’aide de la fonction [multiIf](/fr/sql-reference/functions/conditional-functions#multiIf). Chaque condition est évaluée indépendamment, et les expressions peuvent inclure des valeurs non constantes.

```sql
SELECT
    number,
    CASE
        WHEN number % 2 = 0 THEN number + 1
        WHEN number % 2 = 1 THEN number * 10
        ELSE number
    END AS result
FROM system.numbers
WHERE number < 5;

-- is translated to
SELECT
    number,
    multiIf((number % 2) = 0, number + 1, (number % 2) = 1, number * 10, number) AS result
FROM system.numbers
WHERE number < 5

┌─number─┬─result─┐
│      0 │      1 │
│      1 │     10 │
│      2 │      3 │
│      3 │     30 │
│      4 │      5 │
└────────┴────────┘

5 rows in set. Elapsed: 0.002 sec.
```

2. `CASE <expr> WHEN <val1> THEN ... WHEN <val2> THEN ... ELSE ... END`
   <br />
   Cette forme plus compacte est optimisée pour la comparaison avec des valeurs constantes et utilise en interne `caseWithExpression()`.

Par exemple, l’exemple suivant est valide :

```sql
SELECT
    number,
    CASE number
        WHEN 0 THEN 100
        WHEN 1 THEN 200
        ELSE 0
    END AS result
FROM system.numbers
WHERE number < 3;

-- is translated to

SELECT
    number,
    caseWithExpression(number, 0, 100, 1, 200, 0) AS result
FROM system.numbers
WHERE number < 3

┌─number─┬─result─┐
│      0 │    100 │
│      1 │    200 │
│      2 │      0 │
└────────┴────────┘

3 rows in set. Elapsed: 0.002 sec.
```

Cette variante n’exige pas non plus que les expressions retournées soient constantes.

```sql
SELECT
    number,
    CASE number
        WHEN 0 THEN number + 1
        WHEN 1 THEN number * 10
        ELSE number
    END
FROM system.numbers
WHERE number < 3;

-- is translated to

SELECT
    number,
    caseWithExpression(number, 0, number + 1, 1, number * 10, number)
FROM system.numbers
WHERE number < 3

┌─number─┬─caseWithExpr⋯0), number)─┐
│      0 │                        1 │
│      1 │                       10 │
│      2 │                        2 │
└────────┴──────────────────────────┘

3 rows in set. Elapsed: 0.001 sec.
```

<div id="caveats">
  #### Points à noter
</div>

ClickHouse détermine le type de résultat d&#39;une expression CASE (ou de son équivalent interne, comme `multiIf`) avant d&#39;évaluer les conditions. C&#39;est important lorsque les expressions de retour ont des types différents, par exemple des fuseaux horaires ou des types numériques différents.

* Le type de résultat est choisi en fonction du plus grand type compatible parmi toutes les branches.
* Une fois ce type choisi, toutes les autres branches y sont converties implicitement, même si leur logique ne serait jamais exécutée à l&#39;exécution.
* Pour des types comme DateTime64, où le fuseau horaire fait partie de la signature du type, cela peut entraîner un comportement surprenant : le premier fuseau horaire rencontré peut être utilisé pour toutes les branches, même lorsque d&#39;autres branches spécifient des fuseaux horaires différents.

Par exemple, ci-dessous, toutes les lignes renvoient le timestamp dans le fuseau horaire de la première branche correspondante, c.-à-d. `Asia/Kolkata`

```sql
SELECT
    number,
    CASE
        WHEN number = 0 THEN fromUnixTimestamp64Milli(0, 'Asia/Kolkata')
        WHEN number = 1 THEN fromUnixTimestamp64Milli(0, 'America/Los_Angeles')
        ELSE fromUnixTimestamp64Milli(0, 'UTC')
    END AS tz
FROM system.numbers
WHERE number < 3;

-- is translated to

SELECT
    number,
    multiIf(number = 0, fromUnixTimestamp64Milli(0, 'Asia/Kolkata'), number = 1, fromUnixTimestamp64Milli(0, 'America/Los_Angeles'), fromUnixTimestamp64Milli(0, 'UTC')) AS tz
FROM system.numbers
WHERE number < 3

┌─number─┬──────────────────────tz─┐
│      0 │ 1970-01-01 05:30:00.000 │
│      1 │ 1970-01-01 05:30:00.000 │
│      2 │ 1970-01-01 05:30:00.000 │
└────────┴─────────────────────────┘

3 rows in set. Elapsed: 0.011 sec.
```

Ici, ClickHouse voit plusieurs types de retour `DateTime64(3, <timezone>)`. Il déduit que le type commun est `DateTime64(3, 'Asia/Kolkata'` puisqu’il s’agit du premier qu’il rencontre, en convertissant implicitement les autres branches vers ce type.

Pour y remédier, vous pouvez convertir en chaîne de caractères afin de préserver le formatage souhaité du fuseau horaire :

```sql
SELECT
    number,
    multiIf(
        number = 0, formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'Asia/Kolkata'),
        number = 1, formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'America/Los_Angeles'),
        formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'UTC')
    ) AS tz
FROM system.numbers
WHERE number < 3;

-- is translated to

SELECT
    number,
    multiIf(number = 0, formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'Asia/Kolkata'), number = 1, formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'America/Los_Angeles'), formatDateTime(fromUnixTimestamp64Milli(0), '%F %T', 'UTC')) AS tz
FROM system.numbers
WHERE number < 3

┌─number─┬─tz──────────────────┐
│      0 │ 1970-01-01 05:30:00 │
│      1 │ 1969-12-31 16:00:00 │
│      2 │ 1970-01-01 00:00:00 │
└────────┴─────────────────────┘

3 rows in set. Elapsed: 0.002 sec.
```

{/* 
  Le contenu interne des balises ci-dessous est remplacé lors de la build du framework de documentation par 
  la documentation générée à partir de system.functions. Veuillez ne pas modifier ni supprimer ces balises.
  Voir : https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }