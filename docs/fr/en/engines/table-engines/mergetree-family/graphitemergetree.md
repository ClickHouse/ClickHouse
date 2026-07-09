---
description: 'Conçu pour l’éclaircissement et l’agrégation/la mise en moyenne (rollup) des données Graphite.'
sidebar_label: 'GraphiteMergeTree'
sidebar_position: 90
slug: /engines/table-engines/mergetree-family/graphitemergetree
title: 'Moteur de table GraphiteMergeTree'
doc_type: 'guide'
---

Ce moteur est conçu pour l’éclaircissement et l’agrégation/la mise en moyenne (rollup) des données [Graphite](http://graphite.readthedocs.io/en/latest/index.html). Il peut être utile aux développeurs qui souhaitent utiliser ClickHouse comme stockage de données pour Graphite.

Vous pouvez utiliser n’importe quel moteur de table ClickHouse pour stocker les données Graphite si vous n’avez pas besoin de rollup, mais si vous avez besoin d’un rollup, utilisez `GraphiteMergeTree`. Ce moteur réduit le volume de stockage et améliore l’efficacité des requêtes provenant de Graphite.

Ce moteur hérite des propriétés de [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md).

<div id="creating-table">
  ## Créer une table
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    Path String,
    Time DateTime,
    Value Float64,
    Version <Numeric_type>
    ...
) ENGINE = GraphiteMergeTree(config_section)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

Voir une description détaillée de la requête [CREATE TABLE](/fr/sql-reference/statements/create/table).

Une table destinée aux données Graphite doit comporter les colonnes suivantes :

* Nom de la métrique (Graphite sensor). Type de données : `String`.

* Horodatage de la mesure de la métrique. Type de données : `DateTime`.

* Valeur de la métrique. Type de données : `Float64`.

* Version de la métrique. Type de données : n&#39;importe quel type numérique (ClickHouse conserve les lignes avec la version la plus élevée ou, si les versions sont identiques, la dernière ligne écrite. Les autres lignes sont supprimées lors de la fusion des data parts).

Les noms de ces colonnes doivent être définis dans la configuration du rollup.

**Paramètres de GraphiteMergeTree**

* `config_section` — Nom de la section du fichier de configuration dans laquelle sont définies les règles de rollup.

**Clauses de requête**

Lors de la création d&#39;une table `GraphiteMergeTree`, les mêmes [clauses](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) sont requises que pour la création d&#39;une table `MergeTree`.

<details markdown="1">
  <summary>Méthode obsolète pour créer une table</summary>

  :::note
  N&#39;utilisez pas cette méthode dans de nouveaux projets et, si possible, migrez les anciens projets vers la méthode décrite ci-dessus.
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      EventDate Date,
      Path String,
      Time DateTime,
      Value Float64,
      Version <Numeric_type>
      ...
  ) ENGINE [=] GraphiteMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, config_section)
  ```

  Tous les paramètres, à l&#39;exception de `config_section`, ont la même signification que dans `MergeTree`.

  * `config_section` — Nom de la section du fichier de configuration dans laquelle sont définies les règles de rollup.
</details>

<div id="rollup-configuration">
  ## Configuration du rollup
</div>

Les paramètres du rollup sont définis par le paramètre [graphite&#95;rollup](../../../operations/server-configuration-parameters/settings.md#graphite) dans la configuration du serveur. Le paramètre peut porter n’importe quel nom. Vous pouvez créer plusieurs configurations et les utiliser pour différentes tables.

Structure de la configuration du rollup :

required-columns
patterns

<div id="required-columns">
  ### Colonnes requises
</div>

<div id="path_column_name">
  #### `path_column_name`
</div>

`path_column_name` — Nom de la colonne qui stocke le nom de la métrique (Graphite sensor). Valeur par défaut : `Path`.

<div id="time_column_name">
  #### `time_column_name`
</div>

`time_column_name` — Le nom de la colonne qui stocke la date et l’heure de mesure de la métrique. Valeur par défaut : `Time`.

<div id="value_column_name">
  #### `value_column_name`
</div>

`value_column_name` — Le nom de la colonne contenant la valeur de la métrique à l’instant défini dans `time_column_name`. Valeur par défaut : `Value`.

<div id="version_column_name">
  #### `version_column_name`
</div>

`version_column_name` — Nom de la colonne qui stocke la version de la métrique. Valeur par défaut : `Timestamp`.

<div id="patterns">
  ### Patterns
</div>

Structure de la section `patterns` :

```text
pattern
    rule_type
    regexp
    function
pattern
    rule_type
    regexp
    age + precision
    ...
pattern
    rule_type
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

:::important
Les motifs doivent être strictement ordonnés :

1. Motifs sans `function` ni `retention`.
2. Motifs avec `function` et `retention`.
3. Motif `default`.
   :::

Lors du traitement d’une row, ClickHouse vérifie les règles dans les sections `pattern`. Chacune des sections `pattern` (y compris `default`) peut contenir le paramètre `function` pour l’agrégation, les paramètres `retention`, ou les deux. Si le nom de la métrique correspond à `regexp`, les règles de la section `pattern` (ou des sections) sont appliquées ; sinon, les règles de la section `default` sont utilisées.

Champs des sections `pattern` et `default` :

* `rule_type` - le type d’une règle. Il s’applique uniquement à certaines métriques. Le moteur l’utilise pour séparer les métriques simples et les métriques avec tag. Paramètre facultatif. Valeur par défaut : `all`.
  Il n’est pas nécessaire lorsque les performances ne sont pas critiques ou lorsqu’un seul type de métriques est utilisé, par exemple des métriques simples. Par défaut, un seul ensemble de règles est créé. Sinon, si l’un des types spéciaux est défini, deux ensembles distincts sont créés. Un pour les métriques simples (root.branch.leaf) et un pour les métriques avec tag (root.branch.leaf;tag1=value1).
  Les règles par défaut se retrouvent dans les deux ensembles.
  Valeurs valides :
  * `all` (par défaut) - une règle universelle, utilisée lorsque `rule_type` est omis.
  * `plain` - une règle pour les métriques simples. Le champ `regexp` est traité comme une expression régulière.
  * `tagged` - une règle pour les métriques avec tag (les métriques sont stockées dans la DB au format `someName?tag1=value1&tag2=value2&tag3=value3`). L’expression régulière doit être triée selon les noms des tags ; le premier tag doit être `__name__` s’il existe. Le champ `regexp` est traité comme une expression régulière.
  * `tag_list` - une règle pour les métriques avec tag, une DSL simple pour décrire plus facilement les métriques au format graphite `someName;tag1=value1;tag2=value2`, `someName` ou `tag1=value1;tag2=value2`. Le champ `regexp` est converti en règle `tagged`. Le tri selon les noms des tags n’est pas nécessaire, il sera effectué automatiquement. La valeur d’un tag (mais pas son nom) peut être définie comme une expression régulière, par exemple `env=(dev|staging)`.
* `regexp` – Un motif pour le nom de la métrique (expression régulière ou DSL).
* `age` – L’âge minimal des données en secondes.
* `precision` – Le niveau de précision utilisé pour définir l’âge des données en secondes. Doit être un divisor de 86400 (seconds dans une journée).
* `function` – Le nom de la fonction d’agrégation à appliquer aux données dont l’âge se situe dans l’intervalle `[age, age + precision]`. Fonctions acceptées : min / max / any / avg. La moyenne est calculée de manière imprécise, comme la moyenne des moyennes.

<div id="configuration-example">
  ### Exemple de configuration sans types de règles
</div>

```xml
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

<div id="configuration-typed-example">
  ### Exemple de configuration avec les types de règles
</div>

```xml
<graphite_rollup>
    <version_column_name>Version</version_column_name>
    <pattern>
        <rule_type>plain</rule_type>
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
    <pattern>
        <rule_type>tagged</rule_type>
        <regexp>^((.*)|.)min\?</regexp>
        <function>min</function>
        <retention>
            <age>0</age>
            <precision>5</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>60</precision>
        </retention>
    </pattern>
    <pattern>
        <rule_type>tagged</rule_type>
        <regexp><![CDATA[^someName\?(.*&)*tag1=value1(&|$)]]></regexp>
        <function>min</function>
        <retention>
            <age>0</age>
            <precision>5</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>60</precision>
        </retention>
    </pattern>
    <pattern>
        <rule_type>tag_list</rule_type>
        <regexp>someName;tag2=value2</regexp>
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

:::note
Le rollup des données s’effectue pendant les fusions. En général, pour les anciennes partitions, les fusions ne se lancent pas ; pour le rollup, il faut donc déclencher une fusion non planifiée à l’aide de [optimize](../../../sql-reference/statements/optimize.md). Vous pouvez également utiliser des outils supplémentaires, par exemple [graphite-ch-optimizer](https://github.com/innogames/graphite-ch-optimizer).
:::