---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 38
toc_title: GraphiteMergeTree
---

# GraphiteMergeTree {#graphitemergetree}

Ce moteur est conçu pour l'amincissement et l'agrégation / moyenne (cumul) [Graphite](http://graphite.readthedocs.io/en/latest/index.html) données. Il peut être utile aux développeurs qui veulent utiliser ClickHouse comme un magasin de données pour Graphite.

Vous pouvez utiliser N'importe quel moteur de table ClickHouse pour stocker les données Graphite si vous n'avez pas besoin de cumul, mais si vous avez besoin d'un cumul, utilisez `GraphiteMergeTree`. Le moteur réduit le volume de stockage et augmente l'efficacité des requêtes de Graphite.

Le moteur hérite des propriétés de [MergeTree](mergetree.md).

## Création d'une Table {#creating-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    Path String,
    Time DateTime,
    Value <Numeric_type>,
    Version <Numeric_type>
    ...
) ENGINE = GraphiteMergeTree(config_section)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

Voir une description détaillée de la [CREATE TABLE](../../../sql-reference/statements/create.md#create-table-query) requête.

Un tableau pour les données de Graphite devrait avoir les colonnes suivantes pour les données suivantes:

-   Nom métrique (Capteur De Graphite). Type de données: `String`.

-   Temps de mesure de la métrique. Type de données: `DateTime`.

-   La valeur de la métrique. Type de données: tout numérique.

-   La Version de la métrique. Type de données: tout numérique.

    ClickHouse enregistre les lignes avec la version la plus élevée ou la dernière écrite si les versions sont les mêmes. Les autres lignes sont supprimées lors de la fusion des parties de données.

Les noms de ces colonnes doivent être définis dans la configuration de cumul.

**GraphiteMergeTree paramètres**

-   `config_section` — Name of the section in the configuration file, where are the rules of rollup set.

**Les clauses de requête**

Lors de la création d'un `GraphiteMergeTree` de table, de la même [clause](mergetree.md#table_engine-mergetree-creating-a-table) sont nécessaires, comme lors de la création d'un `MergeTree` table.

<details markdown="1">

<summary>Méthode obsolète pour créer une Table</summary>

!!! attention "Attention"
    N'utilisez pas cette méthode dans les nouveaux projets et, si possible, remplacez les anciens projets par la méthode décrite ci-dessus.

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    EventDate Date,
    Path String,
    Time DateTime,
    Value <Numeric_type>,
    Version <Numeric_type>
    ...
) ENGINE [=] GraphiteMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, config_section)
```

Tous les paramètres excepté `config_section` ont la même signification que dans `MergeTree`.

-   `config_section` — Name of the section in the configuration file, where are the rules of rollup set.

</details>

## Configuration De Cumul {#rollup-configuration}

Les paramètres de cumul sont définis par [graphite\_rollup](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-graphite) paramètre dans la configuration du serveur. Le nom du paramètre pourrait être tout. Vous pouvez créer plusieurs configurations et les utiliser pour différentes tables.

Structure de configuration de cumul:

      required-columns
      patterns

### Les Colonnes Requises {#required-columns}

-   `path_column_name` — The name of the column storing the metric name (Graphite sensor). Default value: `Path`.
-   `time_column_name` — The name of the column storing the time of measuring the metric. Default value: `Time`.
-   `value_column_name` — The name of the column storing the value of the metric at the time set in `time_column_name`. Valeur par défaut: `Value`.
-   `version_column_name` — The name of the column storing the version of the metric. Default value: `Timestamp`.

### Modèle {#patterns}

La Structure de la `patterns` section:

``` text
pattern
    regexp
    function
pattern
    regexp
    age + precision
    ...
pattern
    regexp
    function
    age + precision
    ...
pattern
    ...
default
    function
    age + precision
    ...
```

!!! warning "Attention"
    Les motifs doivent être strictement commandés:

      1. Patterns without `function` or `retention`.
      1. Patterns with both `function` and `retention`.
      1. Pattern `default`.

Lors du traitement d'une ligne, ClickHouse vérifie les règles `pattern` section. Chacun `pattern` (comprendre `default`) les articles peuvent contenir des `function` paramètre d'agrégation, `retention` les paramètres ou les deux à la fois. Si le nom de la métrique correspond `regexp` les règles de la `pattern` section (ou sections) sont appliquées; sinon, les règles de la `default` section sont utilisés.

Champs pour `pattern` et `default` section:

-   `regexp`– A pattern for the metric name.
-   `age` – The minimum age of the data in seconds.
-   `precision`– How precisely to define the age of the data in seconds. Should be a divisor for 86400 (seconds in a day).
-   `function` – The name of the aggregating function to apply to data whose age falls within the range `[age, age + precision]`.

### Exemple De Configuration {#configuration-example}

``` xml
<graphite_rollup>
    <version_column_name>Version</version_column_name>
    <pattern>
        <regexp>click_cost</regexp>
        <function>any</function>
        <retention>
            <age>0</age>
            <precision>5</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>60</precision>
        </retention>
    </pattern>
    <default>
        <function>max</function>
        <retention>
            <age>0</age>
            <precision>60</precision>
        </retention>
        <retention>
            <age>3600</age>
            <precision>300</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>3600</precision>
        </retention>
    </default>
</graphite_rollup>
```

[Article Original](https://clickhouse.tech/docs/en/operations/table_engines/graphitemergetree/) <!--hide-->
