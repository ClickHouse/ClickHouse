---
description: "Documentation de la clause LIMIT BY"
sidebar_label: 'LIMIT BY'
slug: /sql-reference/statements/select/limit-by
title: "Clause LIMIT BY"
doc_type: 'reference'
---

Une requête avec la clause `LIMIT n BY expressions` sélectionne les `n` premières lignes pour chaque valeur distincte de `expressions`. La clé de `LIMIT BY` peut contenir un nombre quelconque d&#39;[expressions](/fr/sql-reference/syntax#expressions).

ClickHouse prend en charge les variantes de syntaxe suivantes :

* `LIMIT [offset_value, ]n BY expressions`
* `LIMIT n OFFSET offset_value BY expressions`

Lors du traitement de la requête, ClickHouse sélectionne les données ordonnées selon la sorting key. La sorting key est définie explicitement à l&#39;aide d&#39;une clause [ORDER BY](/fr/sql-reference/statements/select/order-by) ou implicitement comme propriété du table engine (l&#39;ordre des lignes n&#39;est garanti qu&#39;avec [ORDER BY](/fr/sql-reference/statements/select/order-by) ; sinon, les blocs de lignes ne seront pas ordonnés en raison du multithreading). ClickHouse applique ensuite `LIMIT n BY expressions` et renvoie les `n` premières lignes pour chaque combinaison distincte de `expressions`. Si `OFFSET` est spécifié, alors pour chaque data block appartenant à une combinaison distincte de `expressions`, ClickHouse ignore `offset_value` lignes à partir du début du bloc et renvoie au maximum `n` lignes dans le résultat. Si `offset_value` est supérieur au nombre de lignes dans le data block, ClickHouse ne renvoie aucune ligne de ce bloc.

:::note
`LIMIT BY` n&#39;est pas lié à [LIMIT](../../../sql-reference/statements/select/limit.md). Ils peuvent tous deux être utilisés dans la même requête.
:::

Si vous souhaitez utiliser des numéros de colonne au lieu de noms de colonnes dans la clause `LIMIT BY`, activez le paramètre [enable&#95;positional&#95;arguments](/fr/operations/settings/settings#enable_positional_arguments).

<div id="examples">
  ## Exemples
</div>

Exemple de table :

```sql
CREATE TABLE limit_by(id Int, val Int) ENGINE = Memory;
INSERT INTO limit_by VALUES (1, 10), (1, 11), (1, 12), (2, 20), (2, 21);
```

Requêtes :

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 2 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  10 │
│  1 │  11 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 1, 2 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

La requête `SELECT * FROM limit_by ORDER BY id, val LIMIT 2 OFFSET 1 BY id` renvoie le même résultat.

La requête suivante renvoie les 5 principaux sites référents pour chaque paire `domain, device_type`, avec un maximum de 100 lignes au total (`LIMIT n BY + LIMIT`).

```sql
SELECT
    domainWithoutWWW(URL) AS domain,
    domainWithoutWWW(REFERRER_URL) AS referrer,
    device_type,
    count() cnt
FROM hits
GROUP BY domain, referrer, device_type
ORDER BY cnt DESC
LIMIT 5 BY domain, device_type
LIMIT 100;
```

`LIMIT BY` fonctionne également avec des limites et des décalages négatifs. Comme pour la [clause LIMIT négative](/fr/sql-reference/statements/select/limit#negative-limits), vous pouvez utiliser des valeurs négatives avec `LIMIT BY` pour sélectionner des lignes depuis la *fin* de chaque groupe.

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT -2 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

Renvoie les 2 dernières lignes pour chaque `id`. Pour `id = 1`, on obtient les lignes `11` et `12` ; pour `id = 2`, les deux lignes sont renvoyées, car le groupe ne contient que 2 lignes.

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT -1 OFFSET -1 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  2 │  20 │
└────┴─────┘
```

Renvoie l’avant-dernière ligne de chaque `id` : le `OFFSET -1` final supprime la dernière ligne de chaque groupe, puis le `-1` initial conserve la dernière ligne de ce qu’il reste.

Il est également possible de combiner des `LIMIT` et des `OFFSET` de signes opposés. Par exemple, pour supprimer la première ligne de chaque groupe puis conserver les 2 dernières de ce qu’il reste :

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT -2 OFFSET 1 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

Pour `id = 1`, la première ligne (`10`) est ignorée ; les 2 dernières, `11` et `12`, sont toutes deux renvoyées. Pour `id = 2`, la première ligne (`20`) est ignorée, ne laissant que `21`.

<div id="limit-by-all">
  ## LIMIT BY ALL
</div>

`LIMIT BY ALL` revient à lister toutes les expressions de `SELECT` qui ne sont pas des fonctions d’agrégation.

Par exemple :

```sql
SELECT col1, col2, col3 FROM table LIMIT 2 BY ALL;
```

est identique à

```sql
SELECT col1, col2, col3 FROM table LIMIT 2 BY col1, col2, col3;
```

Dans le cas particulier où une fonction prend comme arguments à la fois des fonctions d’agrégation et d’autres champs, les clés `LIMIT BY` contiendront le plus grand nombre possible de champs non agrégés que nous pouvons en extraire.

Par exemple :

```sql
SELECT substring(a, 4, 2), substring(substring(a, 1, 2), 1, count(b)) FROM t LIMIT 2 BY ALL;
```

est identique à

```sql
SELECT substring(a, 4, 2), substring(substring(a, 1, 2), 1, count(b)) FROM t LIMIT 2 BY substring(a, 4, 2), substring(a, 1, 2);
```

<div id="examples">
  ## Exemples
</div>

Exemple de table :

```sql
CREATE TABLE limit_by(id Int, val Int) ENGINE = Memory;
INSERT INTO limit_by VALUES (1, 10), (1, 11), (1, 12), (2, 20), (2, 21);
```

Requêtes :

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 2 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  10 │
│  1 │  11 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 1, 2 BY id;
```

```text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

La requête `SELECT * FROM limit_by ORDER BY id, val LIMIT 2 OFFSET 1 BY id` renvoie le même résultat.

Avec `LIMIT BY ALL` :

```sql
SELECT id, val FROM limit_by ORDER BY id, val LIMIT 2 BY ALL;
```

Cela revient à :

```sql
SELECT id, val FROM limit_by ORDER BY id, val LIMIT 2 BY id, val;
```