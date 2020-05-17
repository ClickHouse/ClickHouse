---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 33
toc_title: ReplacingMergeTree
---

# ReplacingMergeTree {#replacingmergetree}

Le moteur diffère de [MergeTree](mergetree.md#table_engines-mergetree) en ce qu'il supprime les doublons avec la même valeur de clé primaire (ou, plus précisément, avec la même [clé de tri](mergetree.md) valeur).

La déduplication des données se produit uniquement lors d'une fusion. La fusion se produit en arrière-plan à un moment inconnu, vous ne pouvez donc pas le planifier. Certaines des données peuvent rester non traitées. Bien que vous puissiez exécuter une fusion imprévue en utilisant le `OPTIMIZE` requête, ne comptez pas l'utiliser, parce que la `OPTIMIZE` requête va lire et écrire une grande quantité de données.

Ainsi, `ReplacingMergeTree` convient pour effacer les données en double en arrière-plan afin d'économiser de l'espace, mais cela ne garantit pas l'absence de doublons.

## Création d'une Table {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = ReplacingMergeTree([ver])
[PARTITION BY expr]
[ORDER BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

Pour une description des paramètres de requête, voir [demande de description](../../../sql-reference/statements/create.md).

**ReplacingMergeTree Paramètres**

-   `ver` — column with version. Type `UInt*`, `Date` ou `DateTime`. Paramètre facultatif.

    Lors de la fusion, `ReplacingMergeTree` de toutes les lignes avec la même clé primaire ne laisse qu'un:

    -   Dernier dans la sélection, si `ver` pas ensemble.
    -   Avec la version maximale, si `ver` défini.

**Les clauses de requête**

Lors de la création d'un `ReplacingMergeTree` la table de la même [clause](mergetree.md) sont nécessaires, comme lors de la création d'un `MergeTree` table.

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
) ENGINE [=] ReplacingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [ver])
```

Tous les paramètres excepté `ver` ont la même signification que dans `MergeTree`.

-   `ver` - colonne avec la version. Paramètre facultatif. Pour une description, voir le texte ci-dessus.

</details>

[Article Original](https://clickhouse.tech/docs/en/operations/table_engines/replacingmergetree/) <!--hide-->
