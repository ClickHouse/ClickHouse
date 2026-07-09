---
description: 'Documentation de la clause PARALLEL WITH'
sidebar_label: 'PARALLEL WITH'
sidebar_position: 53
slug: /sql-reference/statements/parallel_with
title: 'Clause PARALLEL WITH'
doc_type: 'reference'
---

Permet d’exécuter plusieurs instructions en parallèle.

<div id="syntax">
  ## Syntaxe
</div>

```sql
statement1 PARALLEL WITH statement2 [PARALLEL WITH statement3 ...]
```

Exécute les instructions `statement1`, `statement2`, `statement3`, ... en parallèle. Le résultat de ces instructions est ignoré.

Dans de nombreux cas, exécuter des instructions en parallèle peut être plus rapide que de simplement exécuter une succession des mêmes instructions. Par exemple, `statement1 PARALLEL WITH statement2 PARALLEL WITH statement3` sera probablement plus rapide que `statement1; statement2; statement3`.

<div id="examples">
  ## Exemples
</div>

Crée deux tables en parallèle :

```sql
CREATE TABLE table1(x Int32) ENGINE = MergeTree ORDER BY tuple()
PARALLEL WITH
CREATE TABLE table2(y String) ENGINE = MergeTree ORDER BY tuple();
```

Supprime deux tables en parallèle :

```sql
DROP TABLE table1
PARALLEL WITH
DROP TABLE table2;
```

<div id="settings">
  ## Paramètres
</div>

Le paramètre [max&#95;threads](../../operations/settings/settings.md#max_threads) détermine le nombre de threads lancés.

<div id="comparison-with-union">
  ## Comparaison avec UNION
</div>

La clause `PARALLEL WITH` est assez semblable à [UNION](select/union.md), qui exécute lui aussi ses opérandes en parallèle. Il existe toutefois quelques différences :

* `PARALLEL WITH` ne renvoie aucun résultat de l&#39;exécution de ses opérandes ; il peut seulement relancer une exception provenant de l&#39;un d&#39;eux, le cas échéant ;
* `PARALLEL WITH` n&#39;exige pas que ses opérandes aient le même ensemble de colonnes de résultat ;
* `PARALLEL WITH` peut exécuter n&#39;importe quelle instruction SQL (pas seulement `SELECT`).