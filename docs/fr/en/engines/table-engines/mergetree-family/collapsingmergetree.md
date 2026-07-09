---
description: 'Hérite de MergeTree, mais ajoute une logique de collapsing des lignes
  lors du processus de fusion.'
keywords: ['updates', 'collapsing']
sidebar_label: 'CollapsingMergeTree'
sidebar_position: 70
slug: /engines/table-engines/mergetree-family/collapsingmergetree
title: 'Moteur de table CollapsingMergeTree'
doc_type: 'guide'
---

<div id="description">
  ## Description
</div>

Le moteur `CollapsingMergeTree` hérite de [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md)
et ajoute une logique de collapsing des lignes pendant le processus de fusion.
Le moteur de table `CollapsingMergeTree` supprime (collapsing) de manière asynchrone
des paires de lignes si tous les champs d’une clé de tri (`ORDER BY`) sont équivalents, à l’exception du champ spécial `Sign`,
qui peut prendre les valeurs `1` ou `-1`.
Les lignes sans paire ayant une valeur `Sign` opposée sont conservées.

Pour plus de détails, consultez la section [Collapsing](#table_engine-collapsingmergetree-collapsing) de ce document.

:::note
Ce moteur peut réduire considérablement le volume de stockage,
et ainsi améliorer l’efficacité des requêtes `SELECT`.
:::

<div id="parameters">
  ## Paramètres
</div>

Tous les paramètres de ce moteur de table, à l&#39;exception du paramètre `Sign`,
ont la même signification que dans [`MergeTree`](/fr/engines/table-engines/mergetree-family/mergetree).

* `Sign` — Le nom donné à une colonne indiquant le type de ligne, où `1` correspond à une ligne d&#39;« état » et `-1` à une ligne d&#39;« annulation ». Type : [Int8](/fr/sql-reference/data-types/int-uint).

<div id="creating-a-table">
  ## Créer une table
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) 
ENGINE = CollapsingMergeTree(Sign)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

<details markdown="1">
  <summary>Méthode obsolète de création d&#39;une table</summary>

  :::note
  La méthode ci-dessous n&#39;est pas recommandée pour les nouveaux projets.
  Nous vous conseillons, si possible, de mettre à jour les anciens projets pour utiliser la nouvelle méthode.
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) 
  ENGINE [=] CollapsingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, Sign)
  ```

  `Sign` — Le nom donné à une colonne indiquant le type de ligne, où `1` est une ligne « état » et `-1` une ligne « annulation ». [Int8](/fr/sql-reference/data-types/int-uint).
</details>

* Pour une description des paramètres de requête, consultez la [description de la requête](../../../sql-reference/statements/create/table.md).
* Lors de la création d&#39;une table `CollapsingMergeTree`, les mêmes [clauses de requête](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) sont requises que pour la création d&#39;une table `MergeTree`.

<div id="table_engine-collapsingmergetree-collapsing">
  ## Collapsing
</div>

<div id="data">
  ### Données
</div>

Prenons le cas où vous devez enregistrer des données qui changent en permanence pour un objet donné.
Il peut sembler logique d’avoir une ligne par objet et de la mettre à jour à chaque modification.
Cependant, les opérations de mise à jour sont coûteuses et lentes pour le SGBD, car elles nécessitent de réécrire les données dans le stockage.
Si nous devons écrire des données rapidement, effectuer un grand nombre de mises à jour n’est pas une approche acceptable,
mais nous pouvons toujours écrire les modifications d’un objet de manière séquentielle.
Pour ce faire, nous utilisons la colonne spéciale `Sign`.

* Si `Sign` = `1`, cela signifie que la ligne est une ligne d’&quot;état&quot; : *une ligne contenant des champs qui représentent l’état valide actuel*.
* Si `Sign` = `-1`, cela signifie que la ligne est une ligne d’&quot;annulation&quot; : *une ligne utilisée pour annuler l’état d’un objet ayant les mêmes attributs*.

Par exemple, nous voulons calculer combien de pages les utilisateurs ont consultées sur un site web donné et combien de temps ils les ont visitées.
À un instant donné, nous écrivons la ligne suivante avec l’état de l’activité de l’utilisateur :

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

À un moment ultérieur, nous enregistrons le changement d’activité de l’utilisateur et l’écrivons sous la forme des deux lignes suivantes :

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

La première ligne annule l’état précédent de l’objet (qui représente ici un utilisateur).
Elle doit recopier tous les champs de la clé de tri de la ligne &quot;annulée&quot;, à l’exception de `Sign`.
La deuxième ligne ci-dessus contient l’état actuel.

Comme nous n’avons besoin que du dernier état de l’activité de l’utilisateur, la ligne &quot;état&quot; d’origine et la ligne &quot;annulation&quot;
que nous avons insérée peuvent être supprimées comme indiqué ci-dessous, en éliminant l’état invalide (ancien) d’un objet par collapsing :

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │ -- old "state" row can be deleted
│ 4324182021466249494 │         5 │      146 │   -1 │ -- "cancel" row can be deleted
│ 4324182021466249494 │         6 │      185 │    1 │ -- new "state" row remains
└─────────────────────┴───────────┴──────────┴──────┘
```

`CollapsingMergeTree` applique précisément ce comportement de *collapsing* lors de la fusion des parties de données.

:::note
La raison pour laquelle deux lignes sont nécessaires pour chaque modification
est expliquée plus en détail dans le paragraphe [Algorithme](#table_engine-collapsingmergetree-collapsing-algorithm).
:::

**Les particularités de cette approche**

1. Le programme qui écrit les données doit conserver l&#39;état d&#39;un objet afin de pouvoir l&#39;annuler. La ligne « annulation » doit contenir une copie des champs de la clé de tri de « état » ainsi que le `Sign` opposé. Cela augmente la taille initiale du stockage, mais permet d&#39;écrire les données rapidement.
2. La présence de tableaux longs et en croissance dans les colonnes réduit l&#39;efficacité du moteur en raison de la charge d&#39;écriture accrue. Plus les données sont simples, plus l&#39;efficacité est élevée.
3. Les résultats de `SELECT` dépendent fortement de la cohérence de l&#39;historique des modifications de l&#39;objet. Soyez vigilant lors de la préparation des données à insérer. Des données incohérentes peuvent produire des résultats imprévisibles. Par exemple, des valeurs négatives pour des métriques non négatives, comme la profondeur de session.

<div id="table_engine-collapsingmergetree-collapsing-algorithm">
  ### Algorithme
</div>

Lorsque ClickHouse fusionne des [parties](/fr/concepts/glossary#parts) de données,
chaque groupe de lignes consécutives ayant la même clé de tri (`ORDER BY`) est réduit à un maximum de deux lignes :
la ligne d&#39;« état » avec `Sign` = `1` et la ligne d&#39;« annulation » avec `Sign` = `-1`.
Autrement dit, dans ClickHouse, les entrées sont collapsées.

Pour chaque partie de données résultante, ClickHouse enregistre :

|    |                                                                                                                                                                                                                       |
| -- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1. | La première ligne d&#39;« annulation » et la dernière ligne d&#39;« état », si le nombre de lignes d&#39;« état » et de lignes d&#39;« annulation » est le même et si la dernière ligne est une ligne d&#39;« état ». |
| 2. | La dernière ligne d&#39;« état », s&#39;il y a plus de lignes d&#39;« état » que de lignes d&#39;« annulation ».                                                                                                      |
| 3. | La première ligne d&#39;« annulation », s&#39;il y a plus de lignes d&#39;« annulation » que de lignes d&#39;« état ».                                                                                                |
| 4. | Aucune ligne, dans tous les autres cas.                                                                                                                                                                               |

En outre, lorsqu&#39;il y a au moins deux lignes d&#39;« état » de plus que de lignes d&#39;« annulation »,
ou au moins deux lignes d&#39;« annulation » de plus que de lignes d&#39;« état », la fusion continue.
ClickHouse traite toutefois cette situation comme une erreur logique et l&#39;enregistre dans le journal du serveur.
Cette erreur peut se produire si les mêmes données sont insérées plus d&#39;une fois.
Ainsi, le collapsing ne devrait pas modifier les résultats du calcul des statistiques.
Les modifications sont progressivement collapsées, de sorte qu&#39;à la fin il ne reste que le dernier état de presque chaque objet.

La colonne `Sign` est requise, car l&#39;algorithme de fusion ne garantit pas
que toutes les lignes ayant la même clé de tri se retrouveront dans la même partie de données résultante, ni même sur le même serveur physique.
ClickHouse traite les requêtes `SELECT` avec plusieurs threads, et il ne peut pas prédire l&#39;ordre des lignes dans le résultat.

Une agrégation est nécessaire s&#39;il faut obtenir des données entièrement « collapsées » à partir de la table `CollapsingMergeTree`.
Pour finaliser le collapsing, écrivez une requête avec la clause `GROUP BY` et des fonctions d&#39;agrégation qui tiennent compte du signe.
Par exemple, pour calculer le nombre, utilisez `sum(Sign)` au lieu de `count()`.
Pour calculer une somme, utilisez `sum(Sign * x)` avec `HAVING sum(Sign) > 0` au lieu de `sum(x)`
comme dans l&#39;[exemple](#example-of-use) ci-dessous.

Les agrégats `count`, `sum` et `avg` peuvent être calculés de cette manière.
L&#39;agrégat `uniq` peut être calculé si un objet a au moins un état non collapsé.
Les agrégats `min` et `max` ne peuvent pas être calculés
parce que `CollapsingMergeTree` n&#39;enregistre pas l&#39;historique des états collapsés.

:::note
Si vous devez extraire des données sans agrégation
(par exemple, pour vérifier si des lignes dont les valeurs les plus récentes correspondent à certaines conditions sont présentes),
vous pouvez utiliser le modificateur [`FINAL`](../../../sql-reference/statements/select/from.md#final-modifier) pour la clause `FROM`. Il fusionnera les données avant de renvoyer le résultat.
Pour CollapsingMergeTree, seule la ligne d&#39;état la plus récente pour chaque clé est renvoyée.
:::

<div id="examples">
  ## Exemples
</div>

<div id="example-of-use">
  ### Exemple d&#39;utilisation
</div>

Considérons les données d’exemple suivantes :

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

Créons une table `UAct` avec `CollapsingMergeTree` :

```sql
CREATE TABLE UAct
(
    UserID UInt64,
    PageViews UInt8,
    Duration UInt8,
    Sign Int8
)
ENGINE = CollapsingMergeTree(Sign)
ORDER BY UserID
```

Ensuite, nous allons insérer des données :

```sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, 1)
```

```sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, -1),(4324182021466249494, 6, 185, 1)
```

Nous utilisons deux requêtes `INSERT` pour créer deux `partie de données` différents.

:::note
Si nous insérons les données avec une seule requête, ClickHouse ne crée qu&#39;un seul `partie de données` et n&#39;effectuera ensuite jamais de fusion.
:::

Nous pouvons interroger les données avec :

```sql
SELECT * FROM UAct
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

Examinons les données renvoyées ci-dessus pour voir si le collapsing a eu lieu...
Avec deux requêtes `INSERT`, nous avons créé deux partie de données.
La requête `SELECT` a été exécutée dans deux threads, et nous avons obtenu un ordre aléatoire des lignes.
Cependant, le collapsing **n&#39;a pas eu lieu** parce qu&#39;il n&#39;y a pas encore eu de fusion des partie de données,
et ClickHouse fusionne les partie de données en arrière-plan à un moment indéterminé que nous ne pouvons pas prévoir.

Nous avons donc besoin d&#39;une agrégation,
que nous effectuons avec la fonction d&#39;agrégation [`sum`](/fr/sql-reference/aggregate-functions/reference/sum)
et la clause [`HAVING`](/fr/sql-reference/statements/select/having) :

```sql
SELECT
    UserID,
    sum(PageViews * Sign) AS PageViews,
    sum(Duration * Sign) AS Duration
FROM UAct
GROUP BY UserID
HAVING sum(Sign) > 0
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┐
│ 4324182021466249494 │         6 │      185 │
└─────────────────────┴───────────┴──────────┘
```

Si nous n’avons pas besoin d’agrégation et que nous voulons forcer le collapsing, nous pouvons également utiliser le modificateur `FINAL` dans la clause `FROM`.

```sql
SELECT * FROM UAct FINAL
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

:::note
Cette méthode de sélection des données est moins efficace et n’est pas recommandée pour de grands volumes de données parcourues (des millions de lignes).
:::

<div id="example-of-another-approach">
  ### Exemple d’une autre approche
</div>

L’idée de cette approche est que les fusions ne prennent en compte que les champs clés.
Dans la ligne &quot;cancel&quot;, nous pouvons donc spécifier des valeurs négatives
qui annulent la version précédente de la ligne lors de l’addition, sans utiliser la colonne `Sign`.

Pour cet exemple, nous utiliserons les données d’exemple ci-dessous :

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │        -5 │     -146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

Pour cette approche, il est nécessaire de modifier les types de données de `PageViews` et `Duration` afin de pouvoir stocker des valeurs négatives.
Nous faisons donc passer le type de ces colonnes de `UInt8` à `Int16` lorsque nous créons notre table `UAct` à l’aide de
`collapsingMergeTree` :

```sql
CREATE TABLE UAct
(
    UserID UInt64,
    PageViews Int16,
    Duration Int16,
    Sign Int8
)
ENGINE = CollapsingMergeTree(Sign)
ORDER BY UserID
```

Testons cette approche en insérant des données dans notre table.

Pour des exemples ou de petites tables, cela reste toutefois acceptable :

```sql
INSERT INTO UAct VALUES(4324182021466249494,  5,  146,  1);
INSERT INTO UAct VALUES(4324182021466249494, -5, -146, -1);
INSERT INTO UAct VALUES(4324182021466249494,  6,  185,  1);

SELECT * FROM UAct FINAL;
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

```sql
SELECT
    UserID,
    sum(PageViews) AS PageViews,
    sum(Duration) AS Duration
FROM UAct
GROUP BY UserID
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┐
│ 4324182021466249494 │         6 │      185 │
└─────────────────────┴───────────┴──────────┘
```

```sql
SELECT COUNT() FROM UAct
```

```text
┌─count()─┐
│       3 │
└─────────┘
```

```sql
OPTIMIZE TABLE UAct FINAL;

SELECT * FROM UAct
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```