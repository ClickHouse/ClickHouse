---
description: 'Documentation de la clause QUALIFY'
sidebar_label: 'QUALIFY'
slug: /sql-reference/statements/select/qualify
title: 'Clause QUALIFY'
doc_type: 'reference'
---

Permet de filtrer les résultats des fonctions de fenêtre. Cette clause est similaire à [WHERE](../../../sql-reference/statements/select/where.md), à la différence que `WHERE` s&#39;applique avant l&#39;évaluation des fonctions de fenêtre, tandis que `QUALIFY` s&#39;applique après.

Il est possible de référencer dans la clause `QUALIFY` les résultats de fonctions de fenêtre de la clause `SELECT` à l&#39;aide de leur alias. La clause `QUALIFY` peut également filtrer les résultats de fonctions de fenêtre supplémentaires qui ne sont pas renvoyés dans le résultat de la requête.

<div id="limitations">
  ## Limites
</div>

`QUALIFY` ne peut pas être utilisé s’il n’y a pas de fonctions de fenêtre à évaluer. Utilisez plutôt `WHERE`.

<div id="examples">
  ## Exemples
</div>

Exemple :

```sql
SELECT number, COUNT() OVER (PARTITION BY number % 3) AS partition_count
FROM numbers(10)
QUALIFY partition_count = 4
ORDER BY number;
```

```text
┌─number─┬─partition_count─┐
│      0 │               4 │
│      3 │               4 │
│      6 │               4 │
│      9 │               4 │
└────────┴─────────────────┘
```