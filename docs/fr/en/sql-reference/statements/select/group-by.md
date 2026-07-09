---
description: 'Documentation sur la clause GROUP BY'
sidebar_label: 'GROUP BY'
slug: /sql-reference/statements/select/group-by
title: 'Clause GROUP BY'
doc_type: 'reference'
---

La clause `GROUP BY` fait passer la requête `SELECT` en mode agrégation, qui fonctionne comme suit :

* La clause `GROUP BY` contient une liste d&#39;expressions (ou une seule expression, considérée comme une liste d&#39;un seul élément). Cette liste sert de « clé de regroupement », tandis que chaque expression individuelle est appelée « expression de clé ».
* Toutes les expressions des clauses [SELECT](/fr/sql-reference/statements/select/index.md), [HAVING](/fr/sql-reference/statements/select/having.md) et [ORDER BY](/fr/sql-reference/statements/select/order-by.md) **doivent** être calculées à partir d&#39;expressions de clé **ou** de [fonctions d&#39;agrégation](../../../sql-reference/aggregate-functions/index.md) appliquées à des expressions qui ne sont pas des clés (y compris des colonnes simples). En d&#39;autres termes, chaque colonne sélectionnée dans la table doit être utilisée soit dans une expression de clé, soit dans une fonction d&#39;agrégation, mais pas les deux.
* Le résultat de l&#39;agrégation de la requête `SELECT` contiendra autant de lignes qu&#39;il existe de valeurs uniques de la « clé de regroupement » dans la table source. En général, cela réduit considérablement le nombre de lignes, souvent de plusieurs ordres de grandeur, mais pas nécessairement : le nombre de lignes reste identique si toutes les valeurs de la « clé de regroupement » sont distinctes.

Si vous souhaitez regrouper les données de la table par numéro de colonne plutôt que par nom de colonne, activez le paramètre [enable&#95;positional&#95;arguments](/fr/operations/settings/settings#enable_positional_arguments).

:::note
Il existe une autre façon d&#39;effectuer une agrégation sur une table. Si une requête contient des colonnes de table uniquement à l&#39;intérieur de fonctions d&#39;agrégation, la clause `GROUP BY` peut être omise, et une agrégation sur un ensemble vide de clés est alors supposée. Ces requêtes renvoient toujours exactement une ligne.
:::

<div id="null-processing">
  ## Traitement de NULL
</div>

Pour les regroupements, ClickHouse interprète [NULL](/fr/sql-reference/syntax#null) comme une valeur, et `NULL==NULL`. Cela diffère du traitement de `NULL` dans la plupart des autres contextes.

Voici un exemple pour illustrer ce que cela signifie.

Supposons que vous ayez cette table :

```text
┌─x─┬────y─┐
│ 1 │    2 │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │    2 │
│ 3 │    3 │
│ 3 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

La requête `SELECT sum(x), y FROM t_null_big GROUP BY y` donne :

```text
┌─sum(x)─┬────y─┐
│      4 │    2 │
│      3 │    3 │
│      5 │ ᴺᵁᴸᴸ │
└────────┴──────┘
```

Vous pouvez voir que `GROUP BY` pour `y = NULL` a additionné `x`, comme si `NULL` était cette valeur.

Si vous passez plusieurs clés à `GROUP BY`, le résultat vous donnera toutes les combinaisons de la sélection, comme si `NULL` était une valeur particulière.

<div id="rollup-modifier">
  ## Modificateur ROLLUP
</div>

Le modificateur `ROLLUP` permet de calculer des sous-totaux pour les expressions de clé, en fonction de leur ordre dans la liste `GROUP BY`. Les lignes de sous-totaux sont ajoutées après la table de résultats.

Les sous-totaux sont calculés dans l&#39;ordre inverse : d&#39;abord pour la dernière expression de clé de la liste, puis pour la précédente, et ainsi de suite jusqu&#39;à la première expression de clé.

Dans les lignes de sous-totaux, les valeurs des expressions de clé déjà &quot;grouped&quot; sont définies sur `0` ou sur une ligne vide.

:::note
Gardez à l&#39;esprit que la clause [HAVING](/fr/sql-reference/statements/select/having.md) peut affecter les résultats des sous-totaux.
:::

**Exemple**

Considérez la table t :

```text
┌─year─┬─month─┬─day─┐
│ 2019 │     1 │   5 │
│ 2019 │     1 │  15 │
│ 2020 │     1 │   5 │
│ 2020 │     1 │  15 │
│ 2020 │    10 │   5 │
│ 2020 │    10 │  15 │
└──────┴───────┴─────┘
```

```sql title="Query"
SELECT year, month, day, count(*) FROM t GROUP BY ROLLUP(year, month, day);
```

Comme la section `GROUP BY` comporte trois expressions de clé, le résultat contient quatre tables avec des sous-totaux agrégés de droite à gauche :

* `GROUP BY year, month, day` ;
* `GROUP BY year, month` (et la colonne `day` est remplie de zéros) ;
* `GROUP BY year` (désormais, les colonnes `month` et `day` sont toutes deux remplies de zéros) ;
* et les totaux (et les trois colonnes des expressions de clé valent zéro).

```text title="Response"
┌─year─┬─month─┬─day─┬─count()─┐
│ 2020 │    10 │  15 │       1 │
│ 2020 │     1 │   5 │       1 │
│ 2019 │     1 │   5 │       1 │
│ 2020 │     1 │  15 │       1 │
│ 2019 │     1 │  15 │       1 │
│ 2020 │    10 │   5 │       1 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│ 2019 │     1 │   0 │       2 │
│ 2020 │     1 │   0 │       2 │
│ 2020 │    10 │   0 │       2 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│ 2019 │     0 │   0 │       2 │
│ 2020 │     0 │   0 │       4 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│    0 │     0 │   0 │       6 │
└──────┴───────┴─────┴─────────┘
```

La même requête peut aussi s’écrire avec le mot-clé `WITH`.

```sql title="Query"
SELECT year, month, day, count(*) FROM t GROUP BY year, month, day WITH ROLLUP;
```

**Voir aussi**

* Le paramètre [group&#95;by&#95;use&#95;nulls](/fr/operations/settings/settings.md#group_by_use_nulls), pour la compatibilité avec la norme SQL.

<div id="cube-modifier">
  ## Modificateur CUBE
</div>

Le modificateur `CUBE` est utilisé pour calculer des sous-totaux pour chaque combinaison des expressions de clé de la liste `GROUP BY`. Les lignes de sous-totaux sont ajoutées après la table de résultats.

Dans les lignes de sous-totaux, les valeurs de toutes les expressions de clé « regroupées » sont remplacées par `0` ou une chaîne vide.

:::note
Notez que la clause [HAVING](/fr/sql-reference/statements/select/having.md) peut affecter les résultats des sous-totaux.
:::

**Exemple**

Considérez la table t :

```text
┌─year─┬─month─┬─day─┐
│ 2019 │     1 │   5 │
│ 2019 │     1 │  15 │
│ 2020 │     1 │   5 │
│ 2020 │     1 │  15 │
│ 2020 │    10 │   5 │
│ 2020 │    10 │  15 │
└──────┴───────┴─────┘
```

```sql title="Query"
SELECT year, month, day, count(*) FROM t GROUP BY CUBE(year, month, day);
```

Comme la section `GROUP BY` contient trois expressions de clé, le résultat comprend huit tables avec des sous-totaux pour toutes les combinaisons possibles d&#39;expressions de clé :

* `GROUP BY year, month, day`
* `GROUP BY year, month`
* `GROUP BY year, day`
* `GROUP BY year`
* `GROUP BY month, day`
* `GROUP BY month`
* `GROUP BY day`
* et les totaux.

Les colonnes qui ne figurent pas dans `GROUP BY` sont remplies de zéros.

```text title="Response"
┌─year─┬─month─┬─day─┬─count()─┐
│ 2020 │    10 │  15 │       1 │
│ 2020 │     1 │   5 │       1 │
│ 2019 │     1 │   5 │       1 │
│ 2020 │     1 │  15 │       1 │
│ 2019 │     1 │  15 │       1 │
│ 2020 │    10 │   5 │       1 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│ 2019 │     1 │   0 │       2 │
│ 2020 │     1 │   0 │       2 │
│ 2020 │    10 │   0 │       2 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│ 2020 │     0 │   5 │       2 │
│ 2019 │     0 │   5 │       1 │
│ 2020 │     0 │  15 │       2 │
│ 2019 │     0 │  15 │       1 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│ 2019 │     0 │   0 │       2 │
│ 2020 │     0 │   0 │       4 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│    0 │     1 │   5 │       2 │
│    0 │    10 │  15 │       1 │
│    0 │    10 │   5 │       1 │
│    0 │     1 │  15 │       2 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│    0 │     1 │   0 │       4 │
│    0 │    10 │   0 │       2 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│    0 │     0 │   5 │       3 │
│    0 │     0 │  15 │       3 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│    0 │     0 │   0 │       6 │
└──────┴───────┴─────┴─────────┘
```

La même requête peut également être écrite à l’aide du mot-clé `WITH`.

```sql title="Query"
SELECT year, month, day, count(*) FROM t GROUP BY year, month, day WITH CUBE;
```

**Voir aussi**

* le paramètre [group&#95;by&#95;use&#95;nulls](/fr/operations/settings/settings.md#group_by_use_nulls) pour assurer la compatibilité avec la norme SQL.

<div id="with-totals-modifier">
  ## Modificateur WITH TOTALS
</div>

Si le modificateur `WITH TOTALS` est spécifié, une ligne supplémentaire est calculée. Cette ligne contient des colonnes clés avec des valeurs par défaut (zéros ou chaînes vides), ainsi que des colonnes de fonctions d’agrégation avec les valeurs calculées sur l’ensemble des lignes (les valeurs « totales »).

Cette ligne supplémentaire n’est produite que dans les formats `JSON*`, `TabSeparated*` et `Pretty*`, séparément des autres lignes :

* Dans les formats `XML` et `JSON*`, cette ligne est renvoyée dans un champ `totals` distinct.
* Dans les formats `TabSeparated*`, `CSV*` et `Vertical`, la ligne apparaît après le résultat principal, précédée d’une ligne vide (après les autres données).
* Dans les formats `Pretty*`, la ligne est affichée sous la forme d’une table distincte après le résultat principal.
* Dans le format `Template`, la ligne est affichée selon le modèle spécifié.
* Dans les autres formats, elle n’est pas disponible.

:::note
`totals` est renvoyé dans les résultats des requêtes `SELECT`, mais pas dans `INSERT INTO ... SELECT`.
:::

`WITH TOTALS` peut se comporter de différentes manières lorsque [HAVING](/fr/sql-reference/statements/select/having.md) est présent. Le comportement dépend du paramètre `totals_mode`.

<div id="configuring-totals-processing">
  ### Configuration du traitement des totaux
</div>

Par défaut, `totals_mode = 'before_having'`. Dans ce cas, « totals » est calculé sur l’ensemble des lignes, y compris celles qui ne satisfont pas la clause HAVING ni `max_rows_to_group_by`.

Les autres options n’incluent dans « totals » que les lignes qui satisfont HAVING, et se comportent différemment avec le paramètre `max_rows_to_group_by` et `group_by_overflow_mode = 'any'`.

`after_having_exclusive` – N’inclut pas les lignes qui ne passent pas `max_rows_to_group_by`. En d’autres termes, « totals » contiendra un nombre de lignes inférieur ou égal à celui qu’il aurait si `max_rows_to_group_by` était omis.

`after_having_inclusive` – Inclut dans « totals » toutes les lignes qui ne passent pas `max_rows_to_group_by`. En d’autres termes, « totals » contiendra un nombre de lignes supérieur ou égal à celui qu’il aurait si `max_rows_to_group_by` était omis.

`after_having_auto` – Compte le nombre de lignes qui satisfont HAVING. S’il dépasse un certain seuil (50 % par défaut), inclut dans « totals » toutes les lignes qui ne passent pas `max_rows_to_group_by`. Sinon, ne les inclut pas.

`totals_auto_threshold` – Par défaut, 0.5. Coefficient utilisé pour `after_having_auto`.

Si `max_rows_to_group_by` et `group_by_overflow_mode = 'any'` ne sont pas utilisés, toutes les variantes de `after_having` sont identiques, et vous pouvez utiliser n’importe laquelle d’entre elles (par exemple, `after_having_auto`).

Vous pouvez utiliser `WITH TOTALS` dans des sous-requêtes, y compris des sous-requêtes dans la clause [JOIN](/fr/sql-reference/statements/select/join.md) (dans ce cas, les valeurs totales correspondantes sont combinées).

<div id="group-by-all">
  ## GROUP BY ALL
</div>

`GROUP BY ALL` équivaut à lister toutes les expressions de `SELECT` qui ne sont pas des fonctions d’agrégation.

Par exemple :

```sql
SELECT
    a * 2,
    b,
    count(c),
FROM t
GROUP BY ALL
```

est identique à

```sql
SELECT
    a * 2,
    b,
    count(c),
FROM t
GROUP BY a * 2, b
```

Dans le cas particulier où une fonction prend à la fois des fonctions d’agrégation et d’autres champs comme arguments, les clés de `GROUP BY` contiendront le plus grand nombre possible de champs non agrégés que nous pouvons en extraire.

Par exemple :

```sql
SELECT
    substring(a, 4, 2),
    substring(substring(a, 1, 2), 1, count(b))
FROM t
GROUP BY ALL
```

est identique à

```sql
SELECT
    substring(a, 4, 2),
    substring(substring(a, 1, 2), 1, count(b))
FROM t
GROUP BY substring(a, 4, 2), substring(a, 1, 2)
```

<div id="examples">
  ## Exemples
</div>

Exemple :

```sql
SELECT
    count(),
    median(FetchTiming > 60 ? 60 : FetchTiming),
    count() - sum(Refresh)
FROM hits
```

Contrairement à MySQL (et conformément à la norme SQL), vous ne pouvez pas récupérer la valeur d’une colonne qui n’apparaît ni dans une clé ni dans une fonction d’agrégation (sauf pour les expressions constantes). Pour contourner cette limitation, vous pouvez utiliser la fonction d’agrégation &#39;any&#39; (qui renvoie la première valeur rencontrée) ou &#39;min/max&#39;.

Exemple :

```sql
SELECT
    domainWithoutWWW(URL) AS domain,
    count(),
    any(Title) AS title -- getting the first occurred page header for each domain.
FROM hits
GROUP BY domain
```

Pour chaque valeur de clé distincte rencontrée, `GROUP BY` calcule un ensemble de valeurs de fonctions d’agrégation.

<div id="grouping-sets-modifier">
  ## Modificateur GROUPING SETS
</div>

C’est le modificateur le plus général.
Ce modificateur permet de spécifier manuellement plusieurs ensembles de clés d’agrégation (`grouping sets`).
L’agrégation est effectuée séparément pour chaque grouping set, puis tous les résultats sont combinés.
Si une colonne n’est pas présente dans un grouping set, elle reçoit une valeur par défaut.

Autrement dit, les modificateurs décrits ci-dessus peuvent être exprimés à l’aide de `GROUPING SETS`.
Bien que les requêtes avec les modificateurs `ROLLUP`, `CUBE` et `GROUPING SETS` soient syntaxiquement équivalentes, elles peuvent s’exécuter différemment.
Alors que `GROUPING SETS` tente d’exécuter l’ensemble en parallèle, `ROLLUP` et `CUBE` effectuent la fusion finale des agrégats dans un seul thread.

Lorsque les colonnes d’origine contiennent des valeurs par défaut, il peut être difficile de déterminer si une ligne fait partie d’une agrégation qui utilise ces colonnes comme clés ou non.
Pour résoudre ce problème, la fonction `GROUPING` doit être utilisée.

**Exemple**

Les deux requêtes suivantes sont équivalentes.

```sql
-- Query 1
SELECT year, month, day, count(*) FROM t GROUP BY year, month, day WITH ROLLUP;

-- Query 2
SELECT year, month, day, count(*) FROM t GROUP BY
GROUPING SETS
(
    (year, month, day),
    (year, month),
    (year),
    ()
);
```

**Voir aussi**

* Le paramètre [group&#95;by&#95;use&#95;nulls](/fr/operations/settings/settings.md#group_by_use_nulls) pour assurer la compatibilité avec la norme SQL.

<div id="implementation-details">
  ## Détails d’implémentation
</div>

L’agrégation est l’une des fonctionnalités les plus importantes d’un SGBD orienté colonnes, et son implémentation fait donc partie des composants les plus optimisés de ClickHouse. Par défaut, l’agrégation s’effectue en mémoire à l’aide d’une table de hachage. Il existe plus de 40 spécialisations, choisies automatiquement en fonction des types de données de la « clé de regroupement ».

<div id="group-by-optimization-depending-on-table-sorting-key">
  ### Optimisation de `GROUP BY` en fonction de la clé de tri de la table
</div>

L&#39;agrégation peut être effectuée plus efficacement si une table est triée selon une clé donnée et que l&#39;expression `GROUP BY` contient au moins un préfixe de la clé de tri ou des fonctions injectives. Dans ce cas, lorsqu&#39;une nouvelle clé est lue dans la table, le résultat intermédiaire de l&#39;agrégation peut être finalisé puis envoyé au client. Ce comportement est activé par le paramètre [optimize&#95;aggregation&#95;in&#95;order](../../../operations/settings/settings.md#optimize_aggregation_in_order). Une telle optimisation réduit la consommation mémoire pendant l&#39;agrégation, mais peut, dans certains cas, ralentir l&#39;exécution de la requête.

<div id="group-by-in-external-memory">
  ### GROUP BY en mémoire externe
</div>

Vous pouvez activer l’écriture des données temporaires sur le disque afin de limiter l’utilisation de la mémoire pendant `GROUP BY`.
Le paramètre [max&#95;bytes&#95;before&#95;external&#95;group&#95;by](/fr/operations/settings/settings#max_bytes_before_external_group_by) détermine le seuil de consommation de RAM à partir duquel les données temporaires de `GROUP BY` sont écrites dans le système de fichiers. S’il est défini sur 0 (valeur par défaut), il est désactivé.
Vous pouvez également définir [max&#95;bytes&#95;ratio&#95;before&#95;external&#95;group&#95;by](/fr/operations/settings/settings#max_bytes_ratio_before_external_group_by), ce qui permet d’utiliser `GROUP BY` en mémoire externe uniquement lorsque la requête atteint un certain seuil de mémoire utilisée.

Lors de l’utilisation de `max_bytes_before_external_group_by`, nous recommandons de définir `max_memory_usage` à une valeur environ deux fois plus élevée (ou `max_bytes_ratio_before_external_group_by=0.5`). Cela est nécessaire, car l’agrégation comporte deux étapes : la lecture des données et la formation des données intermédiaires (1), puis la fusion des données intermédiaires (2). L’écriture des données dans le système de fichiers ne peut se produire que pendant l’étape 1. Si les données temporaires n’ont pas été écrites, l’étape 2 peut alors nécessiter jusqu’à la même quantité de mémoire que l’étape 1.

Par exemple, si [max&#95;memory&#95;usage](/fr/operations/settings/settings#max_memory_usage) est défini sur 10000000000 et que vous souhaitez utiliser l’agrégation externe, il est judicieux de définir `max_bytes_before_external_group_by` sur 10000000000 et `max_memory_usage` sur 20000000000. Lorsque l’agrégation externe est déclenchée (s’il y a eu au moins une écriture des données temporaires), la consommation maximale de RAM n’est que légèrement supérieure à `max_bytes_before_external_group_by`.

Avec le traitement distribué des requêtes, l’agrégation externe est effectuée sur des serveurs distants. Pour que le serveur demandeur n’utilise qu’une faible quantité de RAM, définissez `distributed_aggregation_memory_efficient` sur 1.

Lors de la fusion des données écrites sur le disque, ainsi que lors de la fusion des résultats provenant de serveurs distants lorsque le paramètre `distributed_aggregation_memory_efficient` est activé, la consommation peut atteindre `1/256 * the_number_of_threads` de la quantité totale de RAM.

Lorsque l’agrégation externe est activée, s’il y avait moins de `max_bytes_before_external_group_by` de données (c’est-à-dire que les données n’ont pas été écrites), la requête s’exécute aussi rapidement que sans agrégation externe. Si des données temporaires ont été écrites, le temps d’exécution sera plusieurs fois plus long (environ trois fois plus long).

Si vous avez un [ORDER BY](/fr/sql-reference/statements/select/order-by.md) avec un [LIMIT](/fr/sql-reference/statements/select/limit.md) après `GROUP BY`, alors la quantité de RAM utilisée dépend de la quantité de données dans `LIMIT`, et non dans la table entière. Mais si `ORDER BY` n’a pas de `LIMIT`, n’oubliez pas d’activer le tri externe (`max_bytes_before_external_sort`).