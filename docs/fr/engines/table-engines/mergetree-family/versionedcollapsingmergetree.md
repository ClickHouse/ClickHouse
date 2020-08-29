---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 37
toc_title: VersionedCollapsingMergeTree
---

# VersionedCollapsingMergeTree {#versionedcollapsingmergetree}

Ce moteur:

-   Permet l'écriture rapide des États d'objet qui changent continuellement.
-   Supprime les anciens États d'objets en arrière-plan. Cela réduit considérablement le volume de stockage.

Voir la section [Effondrer](#table_engines_versionedcollapsingmergetree) pour plus de détails.

Le moteur hérite de [MergeTree](mergetree.md#table_engines-mergetree) et ajoute la logique de réduction des lignes à l'algorithme de fusion des parties de données. `VersionedCollapsingMergeTree` sert le même but que [CollapsingMergeTree](collapsingmergetree.md) mais utilise un autre effondrement algorithme qui permet d'insérer les données dans n'importe quel ordre avec plusieurs threads. En particulier, l' `Version` la colonne aide à réduire correctement les lignes même si elles sont insérées dans le mauvais ordre. Contrairement, `CollapsingMergeTree` permet uniquement une insertion strictement consécutive.

## Création d'une Table {#creating-a-table}

``` sql
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

Pour une description des paramètres de requête, voir les [description de la requête](../../../sql-reference/statements/create.md).

**Les Paramètres Du Moteur**

``` sql
VersionedCollapsingMergeTree(sign, version)
```

-   `sign` — Name of the column with the type of row: `1` est un “state” rangée, `-1` est un “cancel” rangée.

    Le type de données de colonne doit être `Int8`.

-   `version` — Name of the column with the version of the object state.

    Le type de données de colonne doit être `UInt*`.

**Les Clauses De Requête**

Lors de la création d'un `VersionedCollapsingMergeTree` de table, de la même [clause](mergetree.md) sont requis lors de la création d'un `MergeTree` table.

<details markdown="1">

<summary>Méthode obsolète pour créer une Table</summary>

!!! attention "Attention"
    N'utilisez pas cette méthode dans les nouveaux projets. Si possible, passer les anciens projets à la méthode décrite ci-dessus.

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] VersionedCollapsingMergeTree(date-column [, samp#table_engines_versionedcollapsingmergetreeling_expression], (primary, key), index_granularity, sign, version)
```

Tous les paramètres, à l'exception `sign` et `version` ont la même signification que dans `MergeTree`.

-   `sign` — Name of the column with the type of row: `1` est un “state” rangée, `-1` est un “cancel” rangée.

    Column Data Type — `Int8`.

-   `version` — Name of the column with the version of the object state.

    Le type de données de colonne doit être `UInt*`.

</details>

## Effondrer {#table_engines_versionedcollapsingmergetree}

### Données {#data}

Considérez une situation où vous devez enregistrer des données en constante évolution pour un objet. Il est raisonnable d'avoir une ligne pour un objet et de mettre à jour la ligne chaque fois qu'il y a des modifications. Cependant, l'opération de mise à jour est coûteuse et lente pour un SGBD car elle nécessite la réécriture des données dans le stockage. La mise à jour n'est pas acceptable si vous devez écrire des données rapidement, mais vous pouvez écrire les modifications sur un objet de manière séquentielle comme suit.

L'utilisation de la `Sign` colonne lors de l'écriture de la ligne. Si `Sign = 1` cela signifie que la ligne est un état d'un objet (appelons-la “state” rangée). Si `Sign = -1` il indique l'annulation de l'état d'un objet avec les mêmes attributs (appelons-la “cancel” rangée). Également utiliser l' `Version` colonne, qui doit identifier chaque état d'un objet avec un numéro distinct.

Par exemple, nous voulons calculer le nombre de pages visitées sur le site et combien de temps ils étaient là. À un moment donné nous écrivons la ligne suivante avec l'état de l'activité de l'utilisateur:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

À un moment donné, nous enregistrons le changement d'activité de l'utilisateur et l'écrivons avec les deux lignes suivantes.

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
│ 4324182021466249494 │         6 │      185 │    1 │       2 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

La première ligne annule le précédent état de l'objet (utilisateur). Il doit copier tous les champs de l'état annulé sauf `Sign`.

La deuxième ligne contient l'état actuel.

Parce que nous avons besoin seulement le dernier état de l'activité de l'utilisateur, les lignes

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

peut être supprimé, réduisant l'état invalide (ancien) de l'objet. `VersionedCollapsingMergeTree` fait cela lors de la fusion des parties de données.

Pour savoir pourquoi nous avons besoin de deux lignes pour chaque changement, voir [Algorithme](#table_engines-versionedcollapsingmergetree-algorithm).

**Notes sur l'Utilisation de la**

1.  Le programme qui écrit les données devraient se souvenir de l'état d'un objet afin de l'annuler. Le “cancel” chaîne doit être une copie de la “state” chaîne avec le contraire `Sign`. Cela augmente la taille initiale de stockage, mais permet d'écrire les données rapidement.
2.  Les tableaux de plus en plus longs dans les colonnes réduisent l'efficacité du moteur en raison de la charge d'écriture. Plus les données sont simples, meilleure est l'efficacité.
3.  `SELECT` les résultats dépendent fortement de la cohérence de l'histoire de l'objet change. Être précis lors de la préparation des données pour l'insertion. Vous pouvez obtenir des résultats imprévisibles avec des données incohérentes, telles que des valeurs négatives pour des métriques non négatives telles que la profondeur de session.

### Algorithme {#table_engines-versionedcollapsingmergetree-algorithm}

Lorsque ClickHouse fusionne des parties de données, il supprime chaque paire de lignes ayant la même clé primaire et la même version et différentes `Sign`. L'ordre des lignes n'a pas d'importance.

Lorsque ClickHouse insère des données, il ordonne les lignes par la clé primaire. Si l' `Version` la colonne n'est pas dans la clé primaire, ClickHouse ajoute à la clé primaire implicitement que le dernier champ et l'utilise pour la commande.

## La Sélection De Données {#selecting-data}

ClickHouse ne garantit pas que toutes les lignes avec la même clé primaire sera dans la même partie des données ou même sur le même serveur physique. Cela est vrai à la fois pour l'écriture des données et pour la fusion ultérieure des parties de données. En outre, les processus ClickHouse `SELECT` requêtes avec plusieurs threads, et il ne peut pas prédire l'ordre des lignes dans le résultat. Cela signifie que le regroupement est nécessaire s'il est nécessaire pour obtenir complètement “collapsed” données à partir d'un `VersionedCollapsingMergeTree` table.

Pour finaliser la réduction, écrivez une requête avec un `GROUP BY` fonctions de clause et d'agrégation qui tiennent compte du signe. Par exemple, pour calculer la quantité, l'utilisation `sum(Sign)` plutôt `count()`. Pour calculer la somme de quelque chose, utilisez `sum(Sign * x)` plutôt `sum(x)` et d'ajouter `HAVING sum(Sign) > 0`.

Aggregate `count`, `sum` et `avg` peut être calculée de cette manière. Aggregate `uniq` peut être calculé si un objet a au moins un non-état effondré. Aggregate `min` et `max` ne peut pas être calculé, car `VersionedCollapsingMergeTree` ne sauvegarde pas l'historique des valeurs des États réduits.

Si vous avez besoin d'extraire les données avec “collapsing” mais sans agrégation (par exemple, pour vérifier si des lignes sont présentes dont les valeurs les plus récentes correspondent à certaines conditions), vous pouvez utiliser `FINAL` le modificateur du `FROM` clause. Cette approche est inefficace et ne devrait pas être utilisée avec de grandes tables.

## Exemple D'utilisation {#example-of-use}

Les données de l'exemple:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
│ 4324182021466249494 │         6 │      185 │    1 │       2 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

Création de la table:

``` sql
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

Insérer les données:

``` sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, 1, 1)
```

``` sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, -1, 1),(4324182021466249494, 6, 185, 1, 2)
```

Nous utilisons deux `INSERT` requêtes pour créer deux parties de données différentes. Si nous insérons les données avec une seule requête, ClickHouse crée une partie de données et n'effectuera jamais de fusion.

L'obtention de données:

``` sql
SELECT * FROM UAct
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │   -1 │       1 │
│ 4324182021466249494 │         6 │      185 │    1 │       2 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

Que voyons-nous ici et où sont les parties effondrées?
Nous avons créé deux parties de données en utilisant deux `INSERT` requête. Le `SELECT` la requête a été effectuée dans deux threads, et le résultat est un ordre aléatoire de lignes.
L'effondrement n'a pas eu lieu car les parties de données n'ont pas encore été fusionnées. ClickHouse fusionne des parties de données à un moment inconnu que nous ne pouvons pas prédire.

C'est pourquoi nous avons besoin de l'agrégation:

``` sql
SELECT
    UserID,
    sum(PageViews * Sign) AS PageViews,
    sum(Duration * Sign) AS Duration,
    Version
FROM UAct
GROUP BY UserID, Version
HAVING sum(Sign) > 0
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Version─┐
│ 4324182021466249494 │         6 │      185 │       2 │
└─────────────────────┴───────────┴──────────┴─────────┘
```

Si nous n'avons pas besoin d'agrégation et que nous voulons forcer l'effondrement, nous pouvons utiliser le `FINAL` le modificateur du `FROM` clause.

``` sql
SELECT * FROM UAct FINAL
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         6 │      185 │    1 │       2 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

C'est un moyen très inefficace de sélectionner des données. Ne l'utilisez pas pour les grandes tables.

[Article Original](https://clickhouse.tech/docs/en/operations/table_engines/versionedcollapsingmergetree/) <!--hide-->
