---
description: 'Applique des variations aléatoires à la requête fournie.'
sidebar_label: 'fuzzQuery'
sidebar_position: 75
slug: /sql-reference/table-functions/fuzzQuery
title: 'fuzzQuery'
doc_type: 'reference'
---

Applique des variations aléatoires à la requête fournie.

<div id="syntax">
  ## Syntaxe
</div>

```sql
fuzzQuery(query[, max_query_length[, random_seed]])
```

<div id="arguments">
  ## Arguments
</div>

| Argument           | Description                                                                                 |
| ------------------ | ------------------------------------------------------------------------------------------- |
| `query`            | (String) - La requête source sur laquelle appliquer le fuzzing.                             |
| `max_query_length` | (UInt64) - Longueur maximale que la requête peut atteindre pendant le processus de fuzzing. |
| `random_seed`      | (UInt64) - Une graine aléatoire pour obtenir des résultats stables.                         |

<div id="returned_value">
  ## Valeur renvoyée
</div>

Un objet table comportant une seule colonne contenant des chaînes de requête perturbées.

<div id="usage-example">
  ## Exemple d’utilisation
</div>

```sql
SELECT * FROM fuzzQuery('SELECT materialize(\'a\' AS key) GROUP BY key') LIMIT 2;
```

```response
   ┌─query──────────────────────────────────────────────────────────┐
1. │ SELECT 'a' AS key GROUP BY key                                 │
2. │ EXPLAIN PIPELINE compact = true SELECT 'a' AS key GROUP BY key │
   └────────────────────────────────────────────────────────────────┘
```