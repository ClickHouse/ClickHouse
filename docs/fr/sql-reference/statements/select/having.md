---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# Clause HAVING {#having-clause}

Permet de filtrer les résultats d'agrégation produits par [GROUP BY](group-by.md). Il est similaire à la [WHERE](where.md) la clause, mais la différence est que `WHERE` est effectuée avant l'agrégation, tandis que `HAVING` est effectué d'après elle.

Il est possible de référencer les résultats d'agrégation à partir de `SELECT` la clause dans `HAVING` clause par leur alias. Alternativement, `HAVING` clause peut filtrer sur les résultats d'agrégats supplémentaires qui ne sont pas retournés dans les résultats de la requête.

## Limitation {#limitations}

`HAVING` ne peut pas être utilisé si le regroupement n'est pas effectuée. Utiliser `WHERE` plutôt.
