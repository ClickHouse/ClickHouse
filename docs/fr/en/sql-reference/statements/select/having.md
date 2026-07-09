---
description: 'Documentation de la clause HAVING'
sidebar_label: 'HAVING'
slug: /sql-reference/statements/select/having
title: 'Clause HAVING'
doc_type: 'reference'
---

Permet de filtrer les résultats d’agrégation obtenus par [GROUP BY](/fr/sql-reference/statements/select/group-by). Elle est similaire à la clause [WHERE](../../../sql-reference/statements/select/where.md), à la différence que `WHERE` est exécutée avant l’agrégation, tandis que `HAVING` l’est après.

Il est possible de faire référence, dans la clause `HAVING`, aux résultats d’agrégation de la clause `SELECT` à l’aide de leur alias. La clause `HAVING` peut également filtrer les résultats d’agrégats supplémentaires qui ne sont pas renvoyés dans le résultat de la requête.

<div id="example">
  ## Exemple
</div>

Si vous avez une table `sales` comme suit :

```sql
CREATE TABLE sales
(
    region String,
    salesperson String,
    amount Float64
)
ORDER BY (region, salesperson);
```

Vous pouvez l’interroger ainsi :

```sql
SELECT
    region,
    salesperson,
    sum(amount) AS total_sales
FROM sales
GROUP BY
    region,
    salesperson
HAVING total_sales > 10000
ORDER BY total_sales DESC;
```

Cela affichera les commerciaux dont le total des ventes dans leur région dépasse 10 000.

<div id="limitations">
  ## Limites
</div>

`HAVING` ne peut pas être utilisé sans agrégation. Utilisez plutôt `WHERE`.