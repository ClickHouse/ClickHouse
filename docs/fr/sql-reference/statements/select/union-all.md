---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# Clause UNION ALL {#union-all-clause}

Vous pouvez utiliser `UNION ALL` à combiner `SELECT` requêtes en étendant leurs résultats. Exemple:

``` sql
SELECT CounterID, 1 AS table, toInt64(count()) AS c
    FROM test.hits
    GROUP BY CounterID

UNION ALL

SELECT CounterID, 2 AS table, sum(Sign) AS c
    FROM test.visits
    GROUP BY CounterID
    HAVING c > 0
```

Les colonnes de résultat sont appariées par leur index (ordre intérieur `SELECT`). Si les noms de colonne ne correspondent pas, les noms du résultat final sont tirés de la première requête.

La coulée de Type est effectuée pour les syndicats. Par exemple, si deux requêtes combinées ont le même champ avec non-`Nullable` et `Nullable` types d'un type compatible, la `UNION ALL` a un `Nullable` type de champ.

Requêtes qui font partie de `UNION ALL` ne peut pas être placée entre parenthèses. [ORDER BY](order-by.md) et [LIMIT](limit.md) sont appliqués à des requêtes séparées, pas au résultat final. Si vous devez appliquer une conversion au résultat final, vous pouvez mettre toutes les requêtes avec `UNION ALL` dans une sous-requête dans la [FROM](from.md) clause.

## Limitation {#limitations}

Seulement `UNION ALL` est pris en charge. Régulier `UNION` (`UNION DISTINCT`) n'est pas pris en charge. Si vous avez besoin d' `UNION DISTINCT`, vous pouvez écrire `SELECT DISTINCT` à partir d'une sous-requête contenant `UNION ALL`.

## Détails De Mise En Œuvre {#implementation-details}

Requêtes qui font partie de `UNION ALL` peuvent être exécutées simultanément, et leurs résultats peuvent être mélangés ensemble.
