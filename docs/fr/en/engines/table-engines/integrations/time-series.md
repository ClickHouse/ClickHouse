---
description: 'Un moteur de table qui stocke des séries temporelles, c.-à-d. un ensemble de valeurs associé
  à des horodatages et à des tags (ou labels).'
sidebar_label: 'TimeSeries'
sidebar_position: 60
slug: /engines/table-engines/special/time_series
title: 'Moteur de table TimeSeries'
doc_type: 'référence'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="timeseries-table-engine">
  # Moteur de table TimeSeries
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

Un moteur de table stockant des séries temporelles, c’est-à-dire un ensemble de valeurs associées à des horodatages et à des tags (ou labels) :

```sql
metric_name1[tag1=value1, tag2=value2, ...] = {timestamp1: value1, timestamp2: value2, ...}
metric_name2[...] = ...
```

:::info
Il s’agit d’une fonctionnalité expérimentale susceptible d’évoluer ultérieurement de manière non rétrocompatible.
Activez l’utilisation du moteur de table TimeSeries
à l’aide du paramètre [allow&#95;experimental&#95;time&#95;series&#95;table](/fr/operations/settings/settings#allow_experimental_time_series_table).
Exécutez la commande `set allow_experimental_time_series_table = 1`.
:::

<div id="syntax">
  ## Syntaxe
</div>

```sql
CREATE TABLE name [(columns)] ENGINE=TimeSeries
[SETTINGS var1=value1, ...]
[SAMPLES db.samples_table_name | [SAMPLES INNER COLUMNS (...)] [SAMPLES INNER ENGINE engine(arguments)]]
[TAGS db.tags_table_name | [TAGS INNER COLUMNS (...)] [TAGS INNER ENGINE engine(arguments)]]
[METRICS db.metrics_table_name | [METRICS INNER COLUMNS (...)] [METRICS INNER ENGINE engine(arguments)]]
```

:::note
Le mot-clé `SAMPLES` possède l’alias `DATA`, conservé pour assurer la compatibilité descendante.
:::

<div id="usage">
  ## Utilisation
</div>

Il est plus simple de commencer avec tous les paramètres par défaut (il est possible de créer une table `TimeSeries` sans spécifier de liste de colonnes) :

```sql
CREATE TABLE my_table ENGINE=TimeSeries
```

Cette table peut ensuite être utilisée avec les protocoles suivants (un port doit être défini dans la configuration du serveur) :

* [prometheus remote-write](/fr/interfaces/prometheus#remote-write)
* [prometheus remote-read](/fr/interfaces/prometheus#remote-read)

<div id="outer-columns">
  ### Colonnes externes
</div>

Les colonnes d’une table TimeSeries sont générées automatiquement. Ce sont des colonnes externes : elles ne stockent aucune donnée et fournissent uniquement une interface pour SELECT/INSERT. Les données réelles sont stockées dans les [tables cibles](#target-tables). Voici la liste des colonnes externes :

| Nom             | Type                                              | Description                                                                                                                                                                                                                                                                     |
| --------------- | ------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `metric_name`   | `String`                                          | Le nom de la métrique                                                                                                                                                                                                                                                           |
| `tags`          | `Map(String, String)`                             | Map des tags (labels) de la série temporelle                                                                                                                                                                                                                                    |
| `time_series`   | `Array(Tuple(DateTime64(3), Float64))` par défaut | Tableau de paires (timestamp, valeur) d’une série temporelle. Les types d’élément du timestamp et du scalaire du tuple peuvent être déduits à partir de la déclaration `INNER COLUMNS` des samples (voir [Spécification des colonnes externes](#specifying-outer-columns)) |
| `metric_family` | `String`                                          | Le nom de la famille de métriques (pour les métadonnées des métriques)                                                                                                                                                                                                          |
| `type`          | `String`                                          | Le type de la métrique (par ex. &quot;counter&quot;, &quot;gauge&quot;)                                                                                                                                                                                                         |
| `unit`          | `String`                                          | L’unité de la métrique                                                                                                                                                                                                                                                          |
| `help`          | `String`                                          | La description de la métrique                                                                                                                                                                                                                                                   |

Exemple :

```sql
INSERT INTO my_table (metric_name, tags, time_series) VALUES
    ('cpu_usage', {'job': 'node_exporter', 'instance': 'host1:9100'},
     [(toDateTime64('2024-01-01 00:00:00', 3), 0.5), (toDateTime64('2024-01-01 00:01:00', 3), 0.7)])
```

`metric_name` peut être laissé vide lors de l’insertion, ce qui signifie que le nom de la métrique est indiqué dans `tags` sous `__name__`, par exemple :

```sql
INSERT INTO my_table (tags, time_series) VALUES
    ({'__name__': 'cpu_usage', 'job': 'test'},
     [(toDateTime64('2024-01-01 00:00:00', 3), 0.5)])
```

Pour insérer les métadonnées des métriques, insérez des valeurs dans les colonnes `metric_family`, `type`, `unit` et `help` :

```sql
INSERT INTO my_table (metric_name, tags, time_series, metric_family, type, unit, help) VALUES
    ('http_requests_total', {'method': 'GET'}, [(now64(), 100.0)],
     'http_requests_total', 'counter', 'requests', 'Total HTTP requests')
```

<div id="specifying-outer-columns">
  ### Spécification des colonnes externes
</div>

La colonne externe `time_series` peut être indiquée explicitement dans une instruction `CREATE TABLE` pour remplacer son type par défaut `Array(Tuple(DateTime64(3), Float64))`. ClickHouse extrait l’horodatage et les types scalaires du tuple, puis les propage à la table samples interne :

```sql
CREATE TABLE my_table (time_series Array(Tuple(UInt32, Float32))) ENGINE=TimeSeries
```

Cela revient à déclarer directement, dans la clause `INNER COLUMNS` de samples, les types des colonnes timestamp et value :

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES INNER COLUMNS (timestamp UInt32, value Float32)
```

Si les deux formes sont utilisées dans la même instruction `CREATE TABLE`, les types déclarés doivent être identiques.

<div id="target-tables">
  ## Tables cibles
</div>

Une table `TimeSeries` ne contient pas ses propres données : tout est stocké dans ses tables cibles.
Son fonctionnement est similaire à celui d&#39;une [vue matérialisée](../../../sql-reference/statements/create/view#materialized-view),
à ceci près qu&#39;une vue matérialisée possède une seule table cible,
tandis qu&#39;une table `TimeSeries` en possède trois, appelées [samples](#samples-table), [tags](#tags-table) et [metrics](#metrics-table).

Les tables cibles peuvent être spécifiées explicitement dans la requête `CREATE TABLE`,
ou le moteur de table `TimeSeries` peut générer automatiquement des tables cibles internes.

Les lignes insérées dans une table `TimeSeries` sont transformées, découpées en blocs, puis insérées dans ces trois tables cibles.

Les tables cibles sont les suivantes :

<div id="samples-table">
  ### Table *samples*
</div>

La table *samples* contient des séries temporelles associées à un identifiant.

La table *samples* doit comporter les colonnes suivantes :

| Nom         | Obligatoire ? | Type par défaut | Types possibles        | Description                                               |
| ----------- | ------------- | --------------- | ---------------------- | --------------------------------------------------------- |
| `id`        | [x]           | `UUID`          | n’importe lequel       | Identifie une combinaison de noms de métriques et de tags |
| `timestamp` | [x]           | `DateTime64(3)` | `DateTime64(X)`        | Un instant dans le temps                                  |
| `value`     | [x]           | `Float64`       | `Float32` ou `Float64` | Une valeur associée au `timestamp`                        |

<div id="tags-table">
  ### Table des tags
</div>

La table *tags* contient les identifiants calculés pour chaque combinaison d’un nom de métrique et de tags.

La table *tags* doit comporter les colonnes suivantes :

| Name                 | Mandatory? | Default type                          | Possible types                                                                                                          | Description                                                                                                                                                                             |
| -------------------- | ---------- | ------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `id`                 | [x]        | `UUID`                                | any (must match the type of `id` in the [samples](#samples-table) table)                                                | Un `id` identifie une combinaison d’un nom de métrique et de tags. L’expression DEFAULT indique comment calculer un tel identifiant                                                     |
| `metric_name`        | [x]        | `LowCardinality(String)`              | `String` or `LowCardinality(String)`                                                                                    | Le nom d’une métrique                                                                                                                                                                   |
| `<tag_value_column>` | [ ]        | `String`                              | `String` or `LowCardinality(String)` or `LowCardinality(Nullable(String))`                                              | La valeur d’un tag donné ; le nom du tag et celui de la colonne correspondante sont spécifiés dans le paramètre [tags&#95;to&#95;columns](#settings)                                    |
| `tags`               | [x]        | `Map(LowCardinality(String), String)` | `Map(String, String)` or `Map(LowCardinality(String), String)` or `Map(LowCardinality(String), LowCardinality(String))` | Map des tags, à l’exclusion du tag `__name__` qui contient le nom de la métrique, ainsi que des tags dont les noms sont énumérés dans le paramètre [tags&#95;to&#95;columns](#settings) |
| `all_tags`           | [ ]        | `Map(String, String)`                 | `Map(String, String)` or `Map(LowCardinality(String), String)` or `Map(LowCardinality(String), LowCardinality(String))` | Colonne éphémère : chaque ligne est une map de tous les tags, à l’exclusion du seul tag `__name__` qui contient le nom de la métrique. Cette colonne sert uniquement au calcul de `id`  |
| `min_time`           | [ ]        | `Nullable(DateTime64(3))`             | `DateTime64(X)` or `Nullable(DateTime64(X))`                                                                            | Horodatage minimal de la série temporelle correspondant à cet `id`. La colonne est créée si [store&#95;min&#95;time&#95;and&#95;max&#95;time](#settings) vaut `true`                    |
| `max_time`           | [ ]        | `Nullable(DateTime64(3))`             | `DateTime64(X)` or `Nullable(DateTime64(X))`                                                                            | Horodatage maximal de la série temporelle correspondant à cet `id`. La colonne est créée si [store&#95;min&#95;time&#95;and&#95;max&#95;time](#settings) vaut `true`                    |

<div id="metrics-table">
  ### Table metrics
</div>

La table *metrics* contient des informations sur les métriques collectées, leur type et leur description.

La table *metrics* doit comporter les colonnes suivantes :

| Nom                  | Obligatoire ? | Type par défaut          | Types possibles                      | Description                                                                                                                    |
| -------------------- | ------------- | ------------------------ | ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------ |
| `metric_family_name` | [x]           | `String`                 | `String` or `LowCardinality(String)` | Le nom d’une famille de métriques                                                                                              |
| `type`               | [x]           | `LowCardinality(String)` | `String` or `LowCardinality(String)` | Le type d’une famille de métriques, parmi « counter », « gauge », « summary », « stateset », « histogram », « gaugehistogram » |
| `unit`               | [x]           | `LowCardinality(String)` | `String` or `LowCardinality(String)` | L’unité utilisée par une métrique                                                                                              |
| `help`               | [x]           | `String`                 | `String` or `LowCardinality(String)` | La description d’une métrique                                                                                                  |

<div id="creation">
  ## Création
</div>

Il existe plusieurs façons de créer une table à l’aide du moteur de table `TimeSeries`.
L’instruction la plus simple

```sql
CREATE TABLE my_table ENGINE=TimeSeries
```

créera en fait la table suivante (vous pouvez le vérifier en exécutant `SHOW CREATE TABLE my_table`) :

```sql
CREATE TABLE my_table
(
    `metric_name` String,
    `tags` Map(String, String),
    `time_series` Array(Tuple(DateTime64(3), Float64)),
    `metric_family` String,
    `type` String,
    `unit` String,
    `help` String
)
ENGINE = TimeSeries
SAMPLES INNER COLUMNS
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
SAMPLES INNER ENGINE = MergeTree ORDER BY (id, timestamp)
TAGS INNER COLUMNS
(
    `id` UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),
    `metric_name` LowCardinality(String),
    `tags` Map(LowCardinality(String), String),
    `all_tags` Map(String, String) EPHEMERAL,
    `min_time` SimpleAggregateFunction(min, Nullable(DateTime64(3))),
    `max_time` SimpleAggregateFunction(max, Nullable(DateTime64(3)))
)
TAGS INNER ENGINE = AggregatingMergeTree PRIMARY KEY metric_name ORDER BY (metric_name, id) SETTINGS allow_dimensions_outside_sorting_key = 1
METRICS INNER COLUMNS
(
    `metric_family_name` String,
    `type` LowCardinality(String),
    `unit` LowCardinality(String),
    `help` String
)
METRICS INNER ENGINE = ReplacingMergeTree ORDER BY metric_family_name
```

Les colonnes ont donc été générées automatiquement, et il existe également trois tables cibles internes avec leurs propres définitions de colonnes
stockées dans les clauses `INNER COLUMNS`.

Les tables cibles internes portent des noms comme `.inner_id.samples.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`,
`.inner_id.tags.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`, `.inner_id.metrics.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
et chaque table cible possède son propre ensemble de colonnes :

```sql
CREATE TABLE default.`.inner_id.samples.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
ENGINE = MergeTree
ORDER BY (id, timestamp)
```

```sql
CREATE TABLE default.`.inner_id.tags.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `id` UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),
    `metric_name` LowCardinality(String),
    `tags` Map(LowCardinality(String), String),
    `all_tags` Map(String, String) EPHEMERAL,
    `min_time` SimpleAggregateFunction(min, Nullable(DateTime64(3))),
    `max_time` SimpleAggregateFunction(max, Nullable(DateTime64(3)))
)
ENGINE = AggregatingMergeTree
PRIMARY KEY metric_name
ORDER BY (metric_name, id)
SETTINGS allow_dimensions_outside_sorting_key = 1
```

```sql
CREATE TABLE default.`.inner_id.metrics.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `metric_family_name` String,
    `type` LowCardinality(String),
    `unit` LowCardinality(String),
    `help` String
)
ENGINE = ReplacingMergeTree
ORDER BY metric_family_name
```

<div id="create-as">
  ## Création d’une table AS à partir d’une table existante
</div>

L’instruction `CREATE TABLE new_table AS existing_table` copie à partir de `existing_table` :

* `SETTINGS`
* `INNER COLUMNS` pour chaque type
* `INNER ENGINE` pour chaque type

L’instruction n’est pas autorisée si `existing_table` possède des cibles externes.
La liste externe des colonnes est régénérée et non copiée.

<div id="adjusting-column-types">
  ## Ajustement des types des colonnes
</div>

Vous pouvez ajuster les types des colonnes dans les tables cibles internes à l’aide de la clause `INNER COLUMNS`. Par exemple, pour stocker les horodatages en microsecondes et les valeurs en `Float32` :

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES INNER COLUMNS (timestamp DateTime64(6), value Float32)
```

La même clause peut être utilisée pour spécifier des codecs et d’autres attributs de colonne :

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES INNER COLUMNS (timestamp DateTime64(3) CODEC(DoubleDelta))
```

<div id="id-column">
  ## La colonne `id`
</div>

La colonne `id` contient des identifiants ; chaque identifiant est calculé à partir d’une combinaison d’un nom de métrique et de tags.
Le type et l’expression `DEFAULT` utilisée pour générer les identifiants peuvent être personnalisés via la clause `TAGS INNER COLUMNS` :

```sql
CREATE TABLE my_table ENGINE=TimeSeries
TAGS INNER COLUMNS (id UInt64 DEFAULT sipHash64(metric_name, all_tags))
```

Le type de la colonne `id` doit être `UUID`, `UInt64`, `UInt128` ou `FixedString(16)`. Si aucune expression `DEFAULT` n’est fournie, ClickHouse en choisira une automatiquement en fonction du type de `id`. Les types de `id` déclarés dans les tables internes samples et tags doivent correspondre.

Le paramètre `id_generator` offre la même possibilité de personnalisation sans utiliser la clause `INNER COLUMNS` :

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SETTINGS id_generator = 'sipHash64(metric_name, all_tags)'
```

Si ce paramètre est défini, il est utilisé pour générer `id`, même si le `DEFAULT` de la colonne contient une expression différente.

<div id="tags-and-all-tags">
  ## Les colonnes `tags` et `all_tags`
</div>

Il existe deux colonnes qui contiennent des maps de tags : `tags` et `all_tags`. Dans cet exemple, elles ont la même signification, mais elles peuvent être différentes
si le paramètre `tags_to_columns` est utilisé. Ce paramètre permet de spécifier qu’un tag particulier doit être stocké dans une colonne distincte plutôt que dans
une map au sein de la colonne `tags` :

```sql
CREATE TABLE my_table
ENGINE = TimeSeries 
SETTINGS tags_to_columns = {'instance': 'instance', 'job': 'job'}
```

Cette instruction ajoutera les colonnes `instance` et `job` à la table cible interne [tags](#tags-table).
Dans ce cas, la colonne `tags` ne contiendra pas les tags `instance` et `job`,
mais la colonne `all_tags` les contiendra. La colonne `all_tags` est éphémère et son seul rôle est d’être utilisée dans l’expression DEFAULT
de la colonne `id`.

<div id="inner-table-engines">
  ## Moteurs de table pour les tables cibles internes
</div>

Par défaut, les tables cibles internes utilisent les moteurs de table suivants :

* la table [samples](#samples-table) utilise [MergeTree](../mergetree-family/mergetree) ;
* la table [tags](#tags-table) utilise [AggregatingMergeTree](../mergetree-family/aggregatingmergetree), car les mêmes données y sont souvent insérées plusieurs fois ; il faut donc un moyen
  de supprimer les doublons, et ce moteur est également nécessaire pour effectuer l’agrégation des colonnes `min_time` et `max_time` ;
* la table [metrics](#metrics-table) utilise [ReplacingMergeTree](../mergetree-family/replacingmergetree), car les mêmes données y sont souvent insérées plusieurs fois ; il faut donc un moyen
  de supprimer les doublons.

D’autres moteurs de table peuvent également être utilisés pour les tables cibles internes si cela est explicitement spécifié :

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES ENGINE=ReplicatedMergeTree
TAGS ENGINE=ReplicatedAggregatingMergeTree
METRICS ENGINE=ReplicatedReplacingMergeTree
```

La table [tags](#tags-table) conserve les colonnes de tag (ainsi que les Maps `tags`/`all_tags`) en dehors de sa clé de tri,
ce que `AggregatingMergeTree` refuse par défaut (voir [`allow_dimensions_outside_sorting_key`](../mergetree-family/aggregatingmergetree)).
C&#39;est sans risque ici, car ces colonnes dépendent fonctionnellement de `id`, qui fait partie de la clé de tri : toutes les
lignes qu&#39;une fusion en arrière-plan regroupe ont donc les mêmes valeurs. Lorsque la table interne de tags est générée ou que son
engine est spécifié de manière intégrée, comme ci-dessus, `TimeSeries` y définit automatiquement `allow_dimensions_outside_sorting_key = 1` ;
pour une table de tags d&#39;agrégation [externe](#external-target-tables) créée manuellement, vous devez le définir vous-même.

<div id="external-target-tables">
  ## Tables cibles externes
</div>

Il est possible de faire en sorte qu’une table `TimeSeries` utilise une table créée manuellement :

```sql
CREATE TABLE samples_for_my_table
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
ENGINE = MergeTree
ORDER BY (id, timestamp);

CREATE TABLE tags_for_my_table ...

CREATE TABLE metrics_for_my_table ...

CREATE TABLE my_table ENGINE=TimeSeries SAMPLES samples_for_my_table TAGS tags_for_my_table METRICS metrics_for_my_table;
```

Les types de colonne des tables externes (`id`, `timestamp`, `value` et les `<tag_value_column>` listées dans [`tags_to_columns`](#settings)) doivent correspondre à ceux que la table `TimeSeries` générerait autrement en interne (voir [la table Samples](#samples-table), [la table Tags](#tags-table) et [la table Metrics](#metrics-table) pour les contraintes de type). Les incompatibilités de type sont signalées au moment de `CREATE`.

L’expression du générateur d’identifiants pour une cible de tags externe est résolue au moment de `INSERT`, dans l’ordre suivant : d’abord le paramètre [`id_generator`](#settings) (s’il est défini), puis le `DEFAULT` déclaré sur la colonne `id` de la table externe (le cas échéant), puis le générateur canonique dérivé du type de `id`. Le paramètre surcharge donc tout `DEFAULT` déclaré sur la table externe — voir [la colonne `id`](#id-column) pour plus de détails.

<div id="altering-settings">
  ## Modification des paramètres
</div>

Deux paramètres peuvent être modifiés après `CREATE` :

* `id_generator`
* `filter_by_min_time_and_max_time`

```sql
ALTER TABLE my_table MODIFY SETTING id_generator = 'sipHash64(metric_name, all_tags)';
ALTER TABLE my_table MODIFY SETTING filter_by_min_time_and_max_time = 0;
```

Notez que modifier `id_generator` alors que des données sont déjà présentes dans la table des tags peut produire des ID différents pour une même combinaison métrique+tag — les anciennes lignes conservent leurs anciens ID, les nouvelles utilisent le nouveau générateur.

Les autres paramètres ne peuvent pas être modifiés avec `ALTER ... MODIFY SETTING`, car ils sont intégrés au schéma des tables internes lors du `CREATE`.

<div id="settings">
  ## Paramètres
</div>

Voici la liste des paramètres pouvant être spécifiés lors de la définition d&#39;une table `TimeSeries` :

| Nom                                  | Type       | Par défaut             | Description                                                                                                                                                                                                                                                                                                                      |
| ------------------------------------ | ---------- | ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `id_generator`                       | Expression | dépend du type de `id` | Expression qui calcule l&#39;identifiant (empreinte) d&#39;une série temporelle à partir de ses tags. Si elle n&#39;est pas définie, l&#39;expression par défaut de la colonne `id` est utilisée. Si l&#39;expression par défaut de la colonne `id` n&#39;est pas non plus définie, l&#39;expression est choisie automatiquement |
| `tags_to_columns`                    | Map        | {}                     | Map indiquant quels tags doivent être placés dans des colonnes distinctes de la table [tags](#tags-table). Syntaxe : `{'tag1': 'column1', 'tag2' : column2, ...}`                                                                                                                                                                |
| `use_all_tags_column_to_generate_id` | Bool       | true                   | Lors de la génération d&#39;une expression pour calculer l&#39;identifiant d&#39;une série temporelle, cet indicateur permet d&#39;utiliser la colonne `all_tags` dans ce calcul                                                                                                                                                 |
| `store_min_time_and_max_time`        | Bool       | true                   | Si défini sur true, la table stocke `min_time` et `max_time` pour chaque série temporelle                                                                                                                                                                                                                                        |
| `aggregate_min_time_and_max_time`    | Bool       | true                   | Lors de la création d&#39;une table `tags` interne cible, cet indicateur permet d&#39;utiliser `SimpleAggregateFunction(min, Nullable(DateTime64(3)))` au lieu de `Nullable(DateTime64(3))` comme type de la colonne `min_time`, et de même pour la colonne `max_time`                                                           |
| `filter_by_min_time_and_max_time`    | Bool       | true                   | Si défini sur true, la table utilise les colonnes `min_time` et `max_time` pour filtrer les séries temporelles                                                                                                                                                                                                                   |

<div id="functions">
  # Fonctions
</div>

Voici une liste de fonctions acceptant une table `TimeSeries` comme argument :

* [timeSeriesSamples](../../../sql-reference/table-functions/timeSeriesSamples.md)
* [timeSeriesTags](../../../sql-reference/table-functions/timeSeriesTags.md)
* [timeSeriesMetrics](../../../sql-reference/table-functions/timeSeriesMetrics.md)