---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# Limite par Clause {#limit-by-clause}

Une requête avec l' `LIMIT n BY expressions` la clause sélectionne le premier `n` lignes pour chaque valeur distincte de `expressions`. La clé pour `LIMIT BY` peut contenir n'importe quel nombre de [expression](../../syntax.md#syntax-expressions).

ClickHouse prend en charge les variantes de syntaxe suivantes:

-   `LIMIT [offset_value, ]n BY expressions`
-   `LIMIT n OFFSET offset_value BY expressions`

Pendant le traitement de la requête, ClickHouse sélectionne les données classées par clé de tri. La clé de tri est définie explicitement à l'aide [ORDER BY](order-by.md) clause ou implicitement en tant que propriété du moteur de table. Puis clickhouse s'applique `LIMIT n BY expressions` et renvoie le premier `n` lignes pour chaque combinaison distincte de `expressions`. Si `OFFSET` est spécifié, puis pour chaque bloc de données qui appartient à une combinaison particulière de `expressions`, Clickhouse saute `offset_value` nombre de lignes depuis le début du bloc et renvoie un maximum de `n` les lignes en conséquence. Si `offset_value` est plus grand que le nombre de lignes dans le bloc de données, ClickHouse renvoie zéro lignes du bloc.

!!! note "Note"
    `LIMIT BY` n'est pas liée à [LIMIT](limit.md). Ils peuvent tous deux être utilisés dans la même requête.

## Exemple {#examples}

Exemple de table:

``` sql
CREATE TABLE limit_by(id Int, val Int) ENGINE = Memory;
INSERT INTO limit_by VALUES (1, 10), (1, 11), (1, 12), (2, 20), (2, 21);
```

Requête:

``` sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 2 BY id
```

``` text
┌─id─┬─val─┐
│  1 │  10 │
│  1 │  11 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

``` sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 1, 2 BY id
```

``` text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

Le `SELECT * FROM limit_by ORDER BY id, val LIMIT 2 OFFSET 1 BY id` requête renvoie le même résultat.

La requête suivante renvoie les 5 principaux référents pour chaque `domain, device_type` paire avec un maximum de 100 lignes au total (`LIMIT n BY + LIMIT`).

``` sql
SELECT
    domainWithoutWWW(URL) AS domain,
    domainWithoutWWW(REFERRER_URL) AS referrer,
    device_type,
    count() cnt
FROM hits
GROUP BY domain, referrer, device_type
ORDER BY cnt DESC
LIMIT 5 BY domain, device_type
LIMIT 100
```
