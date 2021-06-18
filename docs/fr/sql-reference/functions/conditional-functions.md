---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 43
toc_title: 'Conditionnel '
---

# Fonctions Conditionnelles {#conditional-functions}

## si {#if}

Contrôle la ramification conditionnelle. Contrairement à la plupart des systèmes, ClickHouse évalue toujours les deux expressions `then` et `else`.

**Syntaxe**

``` sql
SELECT if(cond, then, else)
```

Si la condition `cond` renvoie une valeur non nulle, retourne le résultat de l'expression `then` et le résultat de l'expression `else`, si présent, est ignoré. Si l' `cond` est égal à zéro ou `NULL` alors le résultat de la `then` l'expression est ignorée et le résultat de `else` expression, si elle est présente, est renvoyée.

**Paramètre**

-   `cond` – The condition for evaluation that can be zero or not. The type is UInt8, Nullable(UInt8) or NULL.
-   `then` - L'expression à renvoyer si la condition est remplie.
-   `else` - L'expression à renvoyer si la condition n'est pas remplie.

**Valeurs renvoyées**

La fonction s'exécute `then` et `else` expressions et retourne son résultat, selon que la condition `cond` fini par être zéro ou pas.

**Exemple**

Requête:

``` sql
SELECT if(1, plus(2, 2), plus(2, 6))
```

Résultat:

``` text
┌─plus(2, 2)─┐
│          4 │
└────────────┘
```

Requête:

``` sql
SELECT if(0, plus(2, 2), plus(2, 6))
```

Résultat:

``` text
┌─plus(2, 6)─┐
│          8 │
└────────────┘
```

-   `then` et `else` doit avoir le type commun le plus bas.

**Exemple:**

Prendre cette `LEFT_RIGHT` table:

``` sql
SELECT *
FROM LEFT_RIGHT

┌─left─┬─right─┐
│ ᴺᵁᴸᴸ │     4 │
│    1 │     3 │
│    2 │     2 │
│    3 │     1 │
│    4 │  ᴺᵁᴸᴸ │
└──────┴───────┘
```

La requête suivante compare `left` et `right` valeur:

``` sql
SELECT
    left,
    right,
    if(left < right, 'left is smaller than right', 'right is greater or equal than left') AS is_smaller
FROM LEFT_RIGHT
WHERE isNotNull(left) AND isNotNull(right)

┌─left─┬─right─┬─is_smaller──────────────────────────┐
│    1 │     3 │ left is smaller than right          │
│    2 │     2 │ right is greater or equal than left │
│    3 │     1 │ right is greater or equal than left │
└──────┴───────┴─────────────────────────────────────┘
```

Note: `NULL` les valeurs ne sont pas utilisés dans cet exemple, vérifier [Valeurs nulles dans les conditions](#null-values-in-conditionals) section.

## Opérateur Ternaire {#ternary-operator}

Il fonctionne même comme `if` fonction.

Syntaxe: `cond ? then : else`

Retourner `then` si l' `cond` renvoie la valeur vrai (supérieur à zéro), sinon renvoie `else`.

-   `cond` doit être de type de `UInt8`, et `then` et `else` doit avoir le type commun le plus bas.

-   `then` et `else` peut être `NULL`

**Voir aussi**

-   [ifNotFinite](other-functions.md#ifnotfinite).

## multiIf {#multiif}

Permet d'écrire le [CASE](../operators/index.md#operator_case) opérateur plus compacte dans la requête.

Syntaxe: `multiIf(cond_1, then_1, cond_2, then_2, ..., else)`

**Paramètre:**

-   `cond_N` — The condition for the function to return `then_N`.
-   `then_N` — The result of the function when executed.
-   `else` — The result of the function if none of the conditions is met.

La fonction accepte `2N+1` paramètre.

**Valeurs renvoyées**

La fonction renvoie l'une des valeurs `then_N` ou `else` selon les conditions `cond_N`.

**Exemple**

En utilisant à nouveau `LEFT_RIGHT` table.

``` sql
SELECT
    left,
    right,
    multiIf(left < right, 'left is smaller', left > right, 'left is greater', left = right, 'Both equal', 'Null value') AS result
FROM LEFT_RIGHT

┌─left─┬─right─┬─result──────────┐
│ ᴺᵁᴸᴸ │     4 │ Null value      │
│    1 │     3 │ left is smaller │
│    2 │     2 │ Both equal      │
│    3 │     1 │ left is greater │
│    4 │  ᴺᵁᴸᴸ │ Null value      │
└──────┴───────┴─────────────────┘
```

## Utilisation Directe Des Résultats Conditionnels {#using-conditional-results-directly}

Les conditions entraînent toujours `0`, `1` ou `NULL`. Vous pouvez donc utiliser des résultats conditionnels directement comme ceci:

``` sql
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

## Valeurs nulles dans les conditions {#null-values-in-conditionals}

Lorsque `NULL` les valeurs sont impliqués dans des conditions, le résultat sera également `NULL`.

``` sql
SELECT
    NULL < 1,
    2 < NULL,
    NULL < NULL,
    NULL = NULL

┌─less(NULL, 1)─┬─less(2, NULL)─┬─less(NULL, NULL)─┬─equals(NULL, NULL)─┐
│ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ             │ ᴺᵁᴸᴸ               │
└───────────────┴───────────────┴──────────────────┴────────────────────┘
```

Donc, vous devriez construire vos requêtes avec soin si les types sont `Nullable`.

L'exemple suivant le démontre en omettant d'ajouter la condition égale à `multiIf`.

``` sql
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

[Article Original](https://clickhouse.tech/docs/en/query_language/functions/conditional_functions/) <!--hide-->
