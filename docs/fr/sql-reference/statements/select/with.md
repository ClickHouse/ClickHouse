---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# AVEC la Clause {#with-clause}

Cette section prend en charge les Expressions de Table courantes ([CTE](https://en.wikipedia.org/wiki/Hierarchical_and_recursive_queries_in_SQL)), de sorte que les résultats de `WITH` la clause peut être utilisé à l'intérieur `SELECT` clause.

## Limitation {#limitations}

1.  Les requêtes récursives ne sont pas prises en charge.
2.  Lorsque la sous-requête est utilisée à l'intérieur avec section, son résultat doit être scalaire avec exactement une ligne.
3.  Les résultats d'Expression ne sont pas disponibles dans les sous-requêtes.

## Exemple {#examples}

**Exemple 1:** Utilisation d'une expression constante comme “variable”

``` sql
WITH '2019-08-01 15:23:00' as ts_upper_bound
SELECT *
FROM hits
WHERE
    EventDate = toDate(ts_upper_bound) AND
    EventTime <= ts_upper_bound
```

**Exemple 2:** De les expulser, somme(octets) résultat de l'expression de clause SELECT de la liste de colonnes

``` sql
WITH sum(bytes) as s
SELECT
    formatReadableSize(s),
    table
FROM system.parts
GROUP BY table
ORDER BY s
```

**Exemple 3:** Utilisation des résultats de la sous-requête scalaire

``` sql
/* this example would return TOP 10 of most huge tables */
WITH
    (
        SELECT sum(bytes)
        FROM system.parts
        WHERE active
    ) AS total_disk_usage
SELECT
    (sum(bytes) / total_disk_usage) * 100 AS table_disk_usage,
    table
FROM system.parts
GROUP BY table
ORDER BY table_disk_usage DESC
LIMIT 10
```

**Exemple 4:** Réutilisation de l'expression dans la sous-requête

Comme solution de contournement pour la limitation actuelle de l'utilisation de l'expression dans les sous-requêtes, Vous pouvez la dupliquer.

``` sql
WITH ['hello'] AS hello
SELECT
    hello,
    *
FROM
(
    WITH ['hello'] AS hello
    SELECT hello
)
```

``` text
┌─hello─────┬─hello─────┐
│ ['hello'] │ ['hello'] │
└───────────┴───────────┘
```
