---
description: 'Documentation de la clause DISTINCT'
sidebar_label: 'DISTINCT'
slug: /sql-reference/statements/select/distinct
title: 'Clause DISTINCT'
doc_type: 'reference'
---

Si `SELECT DISTINCT` est utilisé, seules les lignes distinctes sont conservées dans le résultat de la requête. Ainsi, pour chaque ensemble de lignes entièrement identiques dans le résultat, une seule ligne est conservée.

Vous pouvez spécifier la liste des colonnes dont les valeurs doivent être uniques : `SELECT DISTINCT ON (column1, column2,...)`. Si aucune colonne n&#39;est spécifiée, elles sont toutes prises en compte.

Considérez la table :

```text
┌─a─┬─b─┬─c─┐
│ 1 │ 1 │ 1 │
│ 1 │ 1 │ 1 │
│ 2 │ 2 │ 2 │
│ 2 │ 2 │ 2 │
│ 1 │ 1 │ 2 │
│ 1 │ 2 │ 2 │
└───┴───┴───┘
```

Utilisation de `DISTINCT` sans préciser de colonnes :

```sql
SELECT DISTINCT * FROM t1;
```

```text
┌─a─┬─b─┬─c─┐
│ 1 │ 1 │ 1 │
│ 2 │ 2 │ 2 │
│ 1 │ 1 │ 2 │
│ 1 │ 2 │ 2 │
└───┴───┴───┘
```

Utilisation de `DISTINCT` avec les colonnes spécifiées :

```sql
SELECT DISTINCT ON (a,b) * FROM t1;
```

```text
┌─a─┬─b─┬─c─┐
│ 1 │ 1 │ 1 │
│ 2 │ 2 │ 2 │
│ 1 │ 2 │ 2 │
└───┴───┴───┘
```

<div id="distinct-and-order-by">
  ## DISTINCT et ORDER BY
</div>

ClickHouse permet d’utiliser les clauses `DISTINCT` et `ORDER BY` sur différentes colonnes dans une même requête. La clause `DISTINCT` est exécutée avant la clause `ORDER BY`.

Considérez la table :

```text
┌─a─┬─b─┐
│ 2 │ 1 │
│ 1 │ 2 │
│ 3 │ 3 │
│ 2 │ 4 │
└───┴───┘
```

Sélection des données :

```sql
SELECT DISTINCT a FROM t1 ORDER BY b ASC;
```

```text
┌─a─┐
│ 2 │
│ 1 │
│ 3 │
└───┘
```

Sélection des données selon les différents ordres de tri :

```sql
SELECT DISTINCT a FROM t1 ORDER BY b DESC;
```

```text
┌─a─┐
│ 3 │
│ 1 │
│ 2 │
└───┘
```

La ligne `2, 4` a été tronquée avant le tri.

Tenez compte de cette spécificité d’implémentation lors de l’écriture des requêtes.

<div id="null-processing">
  ## Traitement de NULL
</div>

`DISTINCT` traite [NULL](/fr/sql-reference/syntax#null) comme si `NULL` était une valeur spécifique, et `NULL==NULL`. En d’autres termes, dans les résultats de `DISTINCT`, différentes combinaisons avec `NULL` n’apparaissent qu’une seule fois. Ce comportement diffère de celui de `NULL` dans la plupart des autres contextes.

<div id="alternatives">
  ## Alternatives
</div>

Il est possible d’obtenir le même résultat en appliquant [GROUP BY](/fr/sql-reference/statements/select/group-by) au même ensemble de valeurs que celui spécifié dans la clause `SELECT`, sans utiliser de fonctions d’agrégation. Il existe toutefois quelques différences par rapport à l’approche `GROUP BY` :

* `DISTINCT` peut être utilisé avec `GROUP BY`.
* Lorsque [ORDER BY](../../../sql-reference/statements/select/order-by.md) est omis et que [LIMIT](../../../sql-reference/statements/select/limit.md) est défini, la requête s’arrête immédiatement après avoir lu le nombre requis de lignes distinctes.
* Les blocs de données sont produits au fur et à mesure de leur traitement, sans attendre la fin de l’exécution de la requête.