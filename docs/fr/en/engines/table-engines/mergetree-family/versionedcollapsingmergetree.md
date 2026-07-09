---
description: 'Permet l''écriture rapide d''états d''objet en évolution constante,
  ainsi que la suppression en arrière-plan des anciens états d''objet.'
sidebar_label: 'VersionedCollapsingMergeTree'
sidebar_position: 80
slug: /engines/table-engines/mergetree-family/versionedcollapsingmergetree
title: 'Moteur de table VersionedCollapsingMergeTree'
doc_type: 'reference'
---

Ce moteur :

* Permet l’écriture rapide d’états d’objet en évolution constante.
* Supprime les anciens états d’objet en arrière-plan. Cela réduit considérablement le volume de stockage.

Voir la section [Collapsing](#table_engines_versionedcollapsingmergetree) pour plus de détails.

Le moteur hérite de [MergeTree](/fr/engines/table-engines/mergetree-family/mergetree) et ajoute une logique de collapsing des lignes à l’algorithme de fusion des parties de données. `VersionedCollapsingMergeTree` remplit le même rôle que [CollapsingMergeTree](../../../engines/table-engines/mergetree-family/collapsingmergetree.md), mais utilise un algorithme de collapsing différent qui permet d’insérer les données dans n’importe quel ordre avec plusieurs threads. En particulier, la colonne `Version` permet d’appliquer correctement le collapsing aux lignes, même si elles sont insérées dans le mauvais ordre. À l’inverse, `CollapsingMergeTree` n’autorise qu’une insertion strictement consécutive.

<div id="creating-a-table">
  ## Créer une table
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = VersionedCollapsingMergeTree(sign, version)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

Pour une description des paramètres de requête, consultez la [description de requête](../../../sql-reference/statements/create/table.md).

<div id="engine-parameters">
  ### Paramètres du moteur
</div>

```sql
VersionedCollapsingMergeTree(sign, version)
```

| Paramètre | Description                                                                                                                       | Type                                                                                                                                                                                                                                                                                          |
| --------- | --------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `sign`    | Nom de la colonne indiquant le type de ligne : `1` correspond à une ligne &quot;state&quot;, `-1` à une ligne &quot;cancel&quot;. | [`Int8`](/fr/sql-reference/data-types/int-uint)                                                                                                                                                                                                                                                  |
| `version` | Nom de la colonne contenant la version de l&#39;état de l&#39;objet.                                                              | [`Int*`](/fr/sql-reference/data-types/int-uint), [`UInt*`](/fr/sql-reference/data-types/int-uint), [`Date`](/fr/sql-reference/data-types/date), [`Date32`](/fr/sql-reference/data-types/date32), [`DateTime`](/fr/sql-reference/data-types/datetime) ou [`DateTime64`](/fr/sql-reference/data-types/datetime64) |

<div id="query-clauses">
  ### Clauses de requête
</div>

Lors de la création d&#39;une table `VersionedCollapsingMergeTree`, les mêmes [clauses](../../../engines/table-engines/mergetree-family/mergetree.md) sont requises que pour la création d&#39;une table `MergeTree`.

<details markdown="1">
  <summary>Méthode obsolète pour créer une table</summary>

  :::note
  N&#39;utilisez pas cette méthode dans les nouveaux projets. Si possible, migrez les anciens projets vers la méthode décrite ci-dessus.
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] VersionedCollapsingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, sign, version)
  ```

  Tous les paramètres, à l&#39;exception de `sign` et `version`, ont la même signification que dans `MergeTree`.

  * `sign` — Nom de la colonne contenant le type de ligne : `1` correspond à une ligne « state », `-1` à une ligne « cancel ».

    Type de données de la colonne — `Int8`.

  * `version` — Nom de la colonne contenant la version de l&#39;état de l&#39;objet.

    Le type de données de la colonne doit être `UInt*`.
</details>

<div id="table_engines_versionedcollapsingmergetree">
  ## Collapsing
</div>

<div id="data">
  ### Données
</div>

Supposons que vous deviez enregistrer des données qui évoluent en permanence pour un objet donné. Il semble raisonnable d’avoir une ligne par objet et de mettre à jour cette ligne à chaque changement. Cependant, l’opération de mise à jour est coûteuse et lente pour un SGBD, car elle implique de réécrire les données dans le stockage. Les mises à jour ne conviennent pas si vous devez écrire les données rapidement, mais vous pouvez consigner séquentiellement les modifications d’un objet comme suit.

Utilisez la colonne `Sign` lors de l’écriture de la ligne. Si `Sign = 1`, cela signifie que la ligne correspond à un state de l’objet (appelons-la la ligne d’&quot;state&quot;). Si `Sign = -1`, cela indique l’annulation du state d’un objet ayant les mêmes attributs (appelons-la la ligne d’&quot;cancel&quot;). Utilisez également la colonne `Version`, qui doit identifier chaque state d’un objet par un numéro distinct.

Par exemple, nous voulons calculer combien de pages les utilisateurs ont visitées sur un site donné et combien de temps ils y sont restés. À un moment donné, nous écrivons la ligne suivante pour représenter le state de l’activité de l’utilisateur :

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

Plus tard, nous enregistrons ce changement d’activité de l’utilisateur et l’écrivons dans les deux lignes suivantes.

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
│ 4324182021466249494 │         6 │      185 │    1 │       2 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

La première ligne annule le state précédent de l’objet (utilisateur). Elle doit recopier tous les champs du state annulé, à l’exception de `Sign`.

La deuxième ligne contient le state actuel.

Comme nous n’avons besoin que du dernier state de l’activité de l’utilisateur, les lignes

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

peut être supprimé, ce qui supprime le state invalide (ancien) de l’objet. `VersionedCollapsingMergeTree` le fait lors de la fusion des parties de données.

Pour savoir pourquoi nous avons besoin de deux lignes pour chaque modification, voir [Algorithm](#table_engines-versionedcollapsingmergetree-algorithm).

**Remarques sur l’utilisation**

1. Le programme qui écrit les données doit mémoriser le state d’un objet afin de pouvoir l’annuler. La chaîne &quot;Cancel&quot; doit contenir des copies des champs de la clé primaire, ainsi que la version de la chaîne &quot;state&quot; et le `Sign` opposé. Cela augmente la taille initiale du stockage, mais permet d’écrire rapidement les données.
2. De longs tableaux en croissance dans les colonnes réduisent l’efficacité du moteur en raison de la charge d’écriture. Plus les données sont simples, meilleure est l’efficacité.
3. Les résultats de `SELECT` dépendent fortement de la cohérence de l’historique des modifications de l’objet. Soyez précis lors de la préparation des données à insérer. Avec des données incohérentes, vous pouvez obtenir des résultats imprévisibles, comme des valeurs négatives pour des métriques non négatives telles que la profondeur de session.

<div id="table_engines-versionedcollapsingmergetree-algorithm">
  ### Algorithme
</div>

Lorsque ClickHouse fusionne des parties de données, il supprime chaque paire de lignes ayant la même clé primaire, la même version et un `Sign` différent. L’ordre des lignes n’a pas d’importance.

Lorsque ClickHouse insère des données, il ordonne les lignes selon la clé primaire. Si la colonne `Version` ne fait pas partie de la clé primaire, ClickHouse l’y ajoute implicitement comme dernier champ et l’utilise pour le tri.

<div id="selecting-data">
  ## Sélection des données
</div>

ClickHouse ne garantit pas que toutes les lignes ayant la même clé primaire se retrouvent dans la même partie de données résultante, ni même sur le même serveur physique. Cela vaut aussi bien lors de l’écriture des données que lors du merging ultérieur des parties de données. De plus, ClickHouse traite les requêtes `SELECT` avec plusieurs threads et ne peut pas prédire l’ordre des lignes dans le résultat. Cela signifie qu’une agrégation est nécessaire si vous devez obtenir des données entièrement « collapsed » à partir d’une table `VersionedCollapsingMergeTree`.

Pour finaliser le collapsing, écrivez une requête avec une clause `GROUP BY` et des fonctions d’agrégation qui tiennent compte du signe. Par exemple, pour calculer la quantité, utilisez `sum(Sign)` au lieu de `count()`. Pour calculer la somme d’une valeur, utilisez `sum(Sign * x)` au lieu de `sum(x)`, et ajoutez `HAVING sum(Sign) > 0`.

Les fonctions d’agrégation `count`, `sum` et `avg` peuvent être calculées de cette façon. La fonction d’agrégation `uniq` peut être calculée si un objet possède au moins un état non collapsed. Les fonctions d’agrégation `min` et `max` ne peuvent pas être calculées, car `VersionedCollapsingMergeTree` n’enregistre pas l’historique des valeurs des états collapsed.

Si vous devez extraire les données avec « collapsing » mais sans agrégation (par exemple, pour vérifier si des lignes sont présentes dont les valeurs les plus récentes correspondent à certaines conditions), vous pouvez utiliser le modificateur `FINAL` dans la clause `FROM`. Cette approche est inefficace et ne doit pas être utilisée avec de grandes tables.

<div id="example-of-use">
  ## Exemple d’utilisation
</div>

Données d’exemple :

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
│ 4324182021466249494 │         6 │      185 │    1 │       2 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

Création de la table :

```sql
CREATE TABLE UAct
(
    UserID UInt64,
    PageViews UInt8,
    Duration UInt8,
    Sign Int8,
    Version UInt8
)
ENGINE = VersionedCollapsingMergeTree(Sign, Version)
ORDER BY UserID
```

Insertion des données :

```sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, 1, 1)
```

```sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, -1, 1),(4324182021466249494, 6, 185, 1, 2)
```

Nous utilisons deux requêtes `INSERT` pour créer deux parties de données distinctes. Si nous insérons les données avec une seule requête, ClickHouse crée une seule partie de données et n’effectuera jamais de fusion.

Récupération des données :

```sql
SELECT * FROM UAct
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │   -1 │       1 │
│ 4324182021466249494 │         6 │      185 │    1 │       2 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

Que voyons-nous ici et où sont les parties collapsées ?
Nous avons créé deux parties de données à l’aide de deux requêtes `INSERT`. La requête `SELECT` a été exécutée dans deux threads, et le résultat est un ordre aléatoire des lignes.
Le collapsing ne s’est pas produit, car les parties de données n’ont pas encore été fusionnées. ClickHouse fusionne les parties de données à un moment indéterminé que nous ne pouvons pas prévoir.

C’est pourquoi nous avons besoin d’agrégation :

```sql
SELECT
    UserID,
    sum(PageViews * Sign) AS PageViews,
    sum(Duration * Sign) AS Duration,
    Version
FROM UAct
GROUP BY UserID, Version
HAVING sum(Sign) > 0
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Version─┐
│ 4324182021466249494 │         6 │      185 │       2 │
└─────────────────────┴───────────┴──────────┴─────────┘
```

Si nous n&#39;avons pas besoin d&#39;agrégation et que nous voulons forcer le collapsing, nous pouvons utiliser le modificateur `FINAL` dans la clause `FROM`.

```sql
SELECT * FROM UAct FINAL
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         6 │      185 │    1 │       2 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

C&#39;est une manière très inefficace de sélectionner des données. Ne l&#39;utilisez pas pour les tables volumineuses.