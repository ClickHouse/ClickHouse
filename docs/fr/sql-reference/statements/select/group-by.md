---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# Clause GROUP BY {#select-group-by-clause}

`GROUP BY` la clause change le `SELECT` requête dans un mode d'agrégation, qui fonctionne comme suit:

-   `GROUP BY` clause contient une liste des expressions (ou une seule expression, qui est considéré comme la liste de longueur). Cette liste agit comme un “grouping key”, tandis que chaque expression individuelle sera appelée “key expressions”.
-   Toutes les expressions dans le [SELECT](index.md), [HAVING](having.md), et [ORDER BY](order-by.md) clause **devoir** être calculé sur la base d'expressions clés **ou** sur [les fonctions d'agrégation](../../../sql-reference/aggregate-functions/index.md) sur les expressions non-clés (y compris les colonnes simples). En d'autres termes, chaque colonne sélectionnée dans la table doit être utilisée soit dans une expression de clé, soit dans une fonction d'agrégat, mais pas les deux.
-   Résultat de l'agrégation de `SELECT` la requête contiendra autant de lignes qu'il y avait des valeurs uniques de “grouping key” dans la table source. Habituellement, cela réduit considérablement le nombre de lignes, souvent par ordre de grandeur, mais pas nécessairement: le nombre de lignes reste le même si tous “grouping key” les valeurs sont distinctes.

!!! note "Note"
    Il existe un moyen supplémentaire d'exécuter l'agrégation sur une table. Si une requête ne contient que des colonnes de table à l'intérieur des fonctions `GROUP BY clause` peut être omis, et l'agrégation par un ensemble vide de touches est supposé. Ces interrogations renvoient toujours exactement une ligne.

## Le Traitement NULL {#null-processing}

Pour le regroupement, ClickHouse interprète [NULL](../../syntax.md#null-literal) comme une valeur, et `NULL==NULL`. Elle diffère de `NULL` traitement dans la plupart des autres contextes.

Voici un exemple pour montrer ce que cela signifie.

Supposons que vous avez cette table:

``` text
┌─x─┬────y─┐
│ 1 │    2 │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │    2 │
│ 3 │    3 │
│ 3 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

Requête `SELECT sum(x), y FROM t_null_big GROUP BY y` résultats dans:

``` text
┌─sum(x)─┬────y─┐
│      4 │    2 │
│      3 │    3 │
│      5 │ ᴺᵁᴸᴸ │
└────────┴──────┘
```

Vous pouvez voir que `GROUP BY` pour `y = NULL` résumer `x` comme si `NULL` a cette valeur.

Si vous passez plusieurs clés `GROUP BY` le résultat vous donnera toutes les combinaisons de la sélection, comme si `NULL` ont une valeur spécifique.

## Avec modificateur de totaux {#with-totals-modifier}

Si l' `WITH TOTALS` modificateur est spécifié, une autre ligne sera calculée. Cette ligne aura des colonnes clés contenant des valeurs par défaut (zéros ou lignes vides), et des colonnes de fonctions d'agrégat avec les valeurs calculées sur toutes les lignes (le “total” valeur).

Cette ligne supplémentaire est uniquement produite en `JSON*`, `TabSeparated*`, et `Pretty*` formats, séparément des autres lignes:

-   Dans `JSON*` formats, cette ligne est sortie en tant que distinct ‘totals’ champ.
-   Dans `TabSeparated*` formats, la ligne vient après le résultat principal, précédé par une ligne vide (après les autres données).
-   Dans `Pretty*` formats, la ligne est sortie comme une table séparée après le résultat principal.
-   Dans les autres formats, il n'est pas disponible.

`WITH TOTALS` peut être exécuté de différentes manières lorsqu'il est présent. Le comportement dépend de l' ‘totals_mode’ paramètre.

### Configuration Du Traitement Des Totaux {#configuring-totals-processing}

Par défaut, `totals_mode = 'before_having'`. Dans ce cas, ‘totals’ est calculé sur toutes les lignes, y compris celles qui ne passent pas par `max_rows_to_group_by`.

Les autres alternatives incluent uniquement les lignes qui passent à travers avoir dans ‘totals’, et se comporter différemment avec le réglage `max_rows_to_group_by` et `group_by_overflow_mode = 'any'`.

`after_having_exclusive` – Don't include rows that didn't pass through `max_rows_to_group_by`. En d'autres termes, ‘totals’ aura moins ou le même nombre de lignes que si `max_rows_to_group_by` ont été omis.

`after_having_inclusive` – Include all the rows that didn't pass through ‘max_rows_to_group_by’ dans ‘totals’. En d'autres termes, ‘totals’ aura plus ou le même nombre de lignes que si `max_rows_to_group_by` ont été omis.

`after_having_auto` – Count the number of rows that passed through HAVING. If it is more than a certain amount (by default, 50%), include all the rows that didn't pass through ‘max_rows_to_group_by’ dans ‘totals’. Sinon, ne pas les inclure.

`totals_auto_threshold` – By default, 0.5. The coefficient for `after_having_auto`.

Si `max_rows_to_group_by` et `group_by_overflow_mode = 'any'` ne sont pas utilisés, toutes les variations de `after_having` sont les mêmes, et vous pouvez utiliser l'un d'eux (par exemple, `after_having_auto`).

Vous pouvez utiliser avec les totaux dans les sous-requêtes, y compris les sous-requêtes dans la clause JOIN (dans ce cas, les valeurs totales respectives sont combinées).

## Exemple {#examples}

Exemple:

``` sql
SELECT
    count(),
    median(FetchTiming > 60 ? 60 : FetchTiming),
    count() - sum(Refresh)
FROM hits
```

Cependant, contrairement au SQL standard, si la table n'a pas de lignes (soit il n'y en a pas du tout, soit il n'y en a pas après avoir utilisé WHERE to filter), un résultat vide est renvoyé, et non le résultat d'une des lignes contenant les valeurs initiales des fonctions d'agrégat.

Contrairement à MySQL (et conforme à SQL standard), vous ne pouvez pas obtenir une valeur d'une colonne qui n'est pas dans une fonction clé ou agrégée (sauf les expressions constantes). Pour contourner ce problème, vous pouvez utiliser le ‘any’ fonction d'agrégation (récupère la première valeur rencontrée) ou ‘min/max’.

Exemple:

``` sql
SELECT
    domainWithoutWWW(URL) AS domain,
    count(),
    any(Title) AS title -- getting the first occurred page header for each domain.
FROM hits
GROUP BY domain
```

Pour chaque valeur de clé différente rencontrée, GROUP BY calcule un ensemble de valeurs de fonction d'agrégation.

GROUP BY n'est pas pris en charge pour les colonnes de tableau.

Une constante ne peut pas être spécifiée comme arguments pour les fonctions d'agrégation. Exemple: somme(1). Au lieu de cela, vous pouvez vous débarrasser de la constante. Exemple: `count()`.

## Détails De Mise En Œuvre {#implementation-details}

L'agrégation est l'une des caractéristiques les plus importantes d'un SGBD orienté colonne, et donc son implémentation est l'une des parties les plus optimisées de ClickHouse. Par défaut, l'agrégation se fait en mémoire à l'aide d'une table de hachage. Il a plus de 40 spécialisations qui sont choisies automatiquement en fonction de “grouping key” types de données.

### Groupe par dans la mémoire externe {#select-group-by-in-external-memory}

Vous pouvez activer le dumping des données temporaires sur le disque pour limiter l'utilisation de la mémoire pendant `GROUP BY`.
Le [max_bytes_before_external_group_by](../../../operations/settings/settings.md#settings-max_bytes_before_external_group_by) réglage détermine le seuil de consommation de RAM pour le dumping `GROUP BY` données temporaires dans le système de fichiers. Si elle est définie sur 0 (valeur par défaut), elle est désactivée.

Lors de l'utilisation de `max_bytes_before_external_group_by`, nous vous recommandons de définir `max_memory_usage` environ deux fois plus élevé. Ceci est nécessaire car il y a deux étapes à l'agrégation: la lecture des données et la formation des données intermédiaires (1) et la fusion des données intermédiaires (2). Le Dumping des données dans le système de fichiers ne peut se produire qu'au cours de l'étape 1. Si les données temporaires n'ont pas été vidées, l'étape 2 peut nécessiter jusqu'à la même quantité de mémoire qu'à l'étape 1.

Par exemple, si [max_memory_usage](../../../operations/settings/settings.md#settings_max_memory_usage) a été défini sur 10000000000 et que vous souhaitez utiliser l'agrégation externe, il est logique de définir `max_bytes_before_external_group_by` à 10000000000, et `max_memory_usage` à 20000000000. Lorsque l'agrégation externe est déclenchée (s'il y a eu au moins un vidage de données temporaires), la consommation maximale de RAM n'est que légèrement supérieure à `max_bytes_before_external_group_by`.

Avec le traitement des requêtes distribuées, l'agrégation externe est effectuée sur des serveurs distants. Pour que le serveur demandeur n'utilise qu'une petite quantité de RAM, définissez `distributed_aggregation_memory_efficient` 1.

Lors de la fusion de données vidées sur le disque, ainsi que lors de la fusion des résultats de serveurs distants lorsque `distributed_aggregation_memory_efficient` paramètre est activé, consomme jusqu'à `1/256 * the_number_of_threads` à partir de la quantité totale de mémoire RAM.

Lorsque l'agrégation externe est activée, s'il y a moins de `max_bytes_before_external_group_by` of data (i.e. data was not flushed), the query runs just as fast as without external aggregation. If any temporary data was flushed, the run time will be several times longer (approximately three times).

Si vous avez un [ORDER BY](order-by.md) avec un [LIMIT](limit.md) après `GROUP BY` puis la quantité de RAM dépend de la quantité de données dans `LIMIT`, pas dans l'ensemble de la table. Mais si l' `ORDER BY` n'a pas `LIMIT`, n'oubliez pas d'activer externe de tri (`max_bytes_before_external_sort`).
