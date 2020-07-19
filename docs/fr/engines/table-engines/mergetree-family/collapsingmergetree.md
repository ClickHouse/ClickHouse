---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 36
toc_title: CollapsingMergeTree
---

# CollapsingMergeTree {#table_engine-collapsingmergetree}

Le moteur hérite de [MergeTree](mergetree.md) et ajoute la logique de l'effondrement des lignes de données de pièces algorithme de fusion.

`CollapsingMergeTree` supprime de manière asynchrone (réduit) les paires de lignes si tous les champs d'une clé de tri (`ORDER BY`) sont équivalents à l'exception du champ particulier `Sign` ce qui peut avoir `1` et `-1` valeur. Les lignes sans paire sont conservées. Pour plus de détails, voir le [Effondrer](#table_engine-collapsingmergetree-collapsing) la section du document.

Le moteur peut réduire considérablement le volume de stockage et augmenter l'efficacité de `SELECT` requête en conséquence.

## Création d'une Table {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = CollapsingMergeTree(sign)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

Pour une description des paramètres de requête, voir [description de la requête](../../../sql-reference/statements/create.md).

**Paramètres CollapsingMergeTree**

-   `sign` — Name of the column with the type of row: `1` est un “state” rangée, `-1` est un “cancel” rangée.

    Column data type — `Int8`.

**Les clauses de requête**

Lors de la création d'un `CollapsingMergeTree` de table, de la même [les clauses de requête](mergetree.md#table_engine-mergetree-creating-a-table) sont nécessaires, comme lors de la création d'un `MergeTree` table.

<details markdown="1">

<summary>Méthode obsolète pour créer une Table</summary>

!!! attention "Attention"
    N'utilisez pas cette méthode dans les nouveaux projets et, si possible, remplacez les anciens projets par la méthode décrite ci-dessus.

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] CollapsingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, sign)
```

Tous les paramètres excepté `sign` ont la même signification que dans `MergeTree`.

-   `sign` — Name of the column with the type of row: `1` — “state” rangée, `-1` — “cancel” rangée.

    Column Data Type — `Int8`.

</details>

## Effondrer {#table_engine-collapsingmergetree-collapsing}

### Données {#data}

Considérez la situation où vous devez enregistrer des données en constante évolution pour un objet. Il semble logique d'avoir une ligne pour un objet et de la mettre à jour à tout changement, mais l'opération de mise à jour est coûteuse et lente pour le SGBD car elle nécessite une réécriture des données dans le stockage. Si vous avez besoin d'écrire des données rapidement, la mise à jour n'est pas acceptable, mais vous pouvez écrire les modifications d'un objet de manière séquentielle comme suit.

Utilisez la colonne particulière `Sign`. Si `Sign = 1` cela signifie que la ligne est un état d'un objet, appelons-la “state” rangée. Si `Sign = -1` il signifie l'annulation de l'état d'un objet avec les mêmes attributs, nous allons l'appeler “cancel” rangée.

Par exemple, nous voulons calculer combien de pages les utilisateurs ont vérifié sur un site et combien de temps ils étaient là. À un certain moment nous écrire la ligne suivante avec l'état de l'activité de l'utilisateur:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

À un moment donné, nous enregistrons le changement d'activité de l'utilisateur et l'écrivons avec les deux lignes suivantes.

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

La première ligne annule le précédent état de l'objet (utilisateur). Il doit copier les champs de clé de tri de l'état annulé sauf `Sign`.

La deuxième ligne contient l'état actuel.

Comme nous avons besoin seulement le dernier état de l'activité de l'utilisateur, les lignes

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │         5 │      146 │   -1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

peut être supprimé en réduisant l'état invalide (ancien) d'un objet. `CollapsingMergeTree` fait cela lors de la fusion des parties de données.

Pourquoi nous avons besoin de 2 lignes pour chaque changement lu dans le [Algorithme](#table_engine-collapsingmergetree-collapsing-algorithm) paragraphe.

**Propriétés particulières d'une telle approche**

1.  Le programme qui écrit les données doit se souvenir de l'état d'un objet pour pouvoir l'annuler. “Cancel” string doit contenir des copies des champs de clé de tri du “state” chaîne et le contraire `Sign`. Il augmente la taille initiale de stockage, mais permet d'écrire les données rapidement.
2.  Les tableaux de plus en plus longs dans les colonnes réduisent l'efficacité du moteur en raison de la charge pour l'écriture. Plus les données sont simples, plus l'efficacité est élevée.
3.  Le `SELECT` les résultats dépendent fortement de la cohérence de l'historique des modifications d'objet. Être précis lors de la préparation des données pour l'insertion. Vous pouvez obtenir des résultats imprévisibles dans des données incohérentes, par exemple des valeurs négatives pour des mesures non négatives telles que la profondeur de session.

### Algorithme {#table_engine-collapsingmergetree-collapsing-algorithm}

Lorsque ClickHouse fusionne des parties de données, chaque groupe de lignes consécutives avec la même clé de tri (`ORDER BY`) est réduit à pas plus de deux rangées, une avec `Sign = 1` (“state” ligne) et l'autre avec `Sign = -1` (“cancel” rangée). En d'autres termes, les entrées de l'effondrement.

Pour chaque partie de données résultante clickhouse enregistre:

1.  Première “cancel” et la dernière “state” lignes, si le nombre de “state” et “cancel” lignes correspond et la dernière ligne est un “state” rangée.
2.  La dernière “state” ligne, si il y a plus de “state” les lignes de “cancel” rangée.
3.  Première “cancel” ligne, si il y a plus de “cancel” les lignes de “state” rangée.
4.  Aucune des lignes, dans tous les autres cas.

Aussi quand il y a au moins 2 plus “state” les lignes de “cancel” les lignes, ou au moins 2 de plus “cancel” rangs puis “state” la fusion continue, mais ClickHouse traite cette situation comme une erreur logique et l'enregistre dans le journal du serveur. Cette erreur peut se produire si les mêmes données ont été insérées plus d'une fois.

Ainsi, l'effondrement ne devrait pas changer les résultats du calcul des statistiques.
Les changements se sont progressivement effondrés de sorte qu'à la fin seul le dernier état de presque tous les objets à gauche.

Le `Sign` est nécessaire car l'algorithme de fusion ne garantit pas que toutes les lignes avec la même clé de tri seront dans la même partie de données résultante et même sur le même serveur physique. Processus de ClickHouse `SELECT` les requêtes avec plusieurs threads, et il ne peut pas prédire l'ordre des lignes dans le résultat. L'agrégation est nécessaire s'il y a un besoin d'obtenir complètement “collapsed” les données de `CollapsingMergeTree` table.

Pour finaliser la réduction, écrivez une requête avec `GROUP BY` fonctions de clause et d'agrégation qui tiennent compte du signe. Par exemple, pour calculer la quantité, l'utilisation `sum(Sign)` plutôt `count()`. Pour calculer la somme de quelque chose, utilisez `sum(Sign * x)` plutôt `sum(x)` et ainsi de suite , et également ajouter `HAVING sum(Sign) > 0`.

Aggregate `count`, `sum` et `avg` pourrait être calculée de cette manière. Aggregate `uniq` peut être calculé si un objet a au moins un état non réduit. Aggregate `min` et `max` impossible de calculer parce que `CollapsingMergeTree` n'enregistre pas l'historique des valeurs des États réduits.

Si vous avez besoin d'extraire des données sans agrégation (par exemple, pour vérifier si des lignes sont présentes dont les valeurs les plus récentes correspondent à certaines conditions), vous pouvez utiliser `FINAL` le modificateur du `FROM` clause. Cette approche est nettement moins efficace.

## Exemple D'utilisation {#example-of-use}

Les données de l'exemple:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

Création de la table:

``` sql
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

Insertion des données:

``` sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, 1)
```

``` sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, -1),(4324182021466249494, 6, 185, 1)
```

Nous utilisons deux `INSERT` requêtes pour créer deux parties de données différentes. Si nous insérons les données avec une requête, ClickHouse crée une partie de données et n'effectuera aucune fusion.

L'obtention de données:

``` sql
SELECT * FROM UAct
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

Que voyons-nous et où s'effondre?

Avec deux `INSERT` requêtes, nous avons créé 2 parties de données. Le `SELECT` la requête a été effectuée dans 2 threads, et nous avons obtenu un ordre aléatoire de lignes. L'effondrement n'a pas eu lieu car il n'y avait pas encore de fusion des parties de données. ClickHouse fusionne une partie des données dans un moment inconnu que nous ne pouvons pas prédire.

Nous avons donc besoin d'agrégation:

``` sql
SELECT
    UserID,
    sum(PageViews * Sign) AS PageViews,
    sum(Duration * Sign) AS Duration
FROM UAct
GROUP BY UserID
HAVING sum(Sign) > 0
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┐
│ 4324182021466249494 │         6 │      185 │
└─────────────────────┴───────────┴──────────┘
```

Si nous n'avons pas besoin d'agrégation et de vouloir forcer l'effondrement, nous pouvons utiliser `FINAL` le modificateur `FROM` clause.

``` sql
SELECT * FROM UAct FINAL
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

Cette façon de sélectionner les données est très inefficace. Ne l'utilisez pas pour les grandes tables.

## Exemple D'une autre approche {#example-of-another-approach}

Les données de l'exemple:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │        -5 │     -146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

L'idée est que les fusions ne prennent en compte que les champs clés. Et dans le “Cancel” ligne nous pouvons spécifier des valeurs négatives qui égalisent la version précédente de la ligne lors de la sommation sans utiliser la colonne de signe. Pour cette approche, il est nécessaire de changer le type de données `PageViews`,`Duration` pour stocker les valeurs négatives de UInt8 - \> Int16.

``` sql
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

Nous allons tester l'approche:

``` sql
insert into UAct values(4324182021466249494,  5,  146,  1);
insert into UAct values(4324182021466249494, -5, -146, -1);
insert into UAct values(4324182021466249494,  6,  185,  1);

select * from UAct final; // avoid using final in production (just for a test or small tables)
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

``` sql
SELECT
    UserID,
    sum(PageViews) AS PageViews,
    sum(Duration) AS Duration
FROM UAct
GROUP BY UserID
```text
┌──────────────UserID─┬─PageViews─┬─Duration─┐
│ 4324182021466249494 │         6 │      185 │
└─────────────────────┴───────────┴──────────┘
```

``` sqk
select count() FROM UAct
```

``` text
┌─count()─┐
│       3 │
└─────────┘
```

``` sql
optimize table UAct final;

select * FROM UAct
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

[Article Original](https://clickhouse.tech/docs/en/operations/table_engines/collapsingmergetree/) <!--hide-->
