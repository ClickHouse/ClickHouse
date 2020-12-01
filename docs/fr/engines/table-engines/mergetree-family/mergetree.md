---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 30
toc_title: MergeTree
---

# MergeTree {#table_engines-mergetree}

Le `MergeTree` moteur et autres moteurs de cette famille (`*MergeTree`) sont les moteurs de table ClickHouse les plus robustes.

Les moteurs de la `MergeTree` famille sont conçus pour l'insertion d'une très grande quantité de données dans une table. Les données sont rapidement écrites dans la table partie par partie, puis des règles sont appliquées pour fusionner les parties en arrière-plan. Cette méthode est beaucoup plus efficace que de réécrire continuellement les données dans le stockage pendant l'insertion.

Principales caractéristiques:

-   Stocke les données triées par clé primaire.

    Cela vous permet de créer un petit index clairsemé qui aide à trouver les données plus rapidement.

-   Les Partitions peuvent être utilisées si [clé de partitionnement](custom-partitioning-key.md) est spécifié.

    ClickHouse prend en charge certaines opérations avec des partitions plus efficaces que les opérations générales sur les mêmes données avec le même résultat. ClickHouse Coupe également automatiquement les données de partition où la clé de partitionnement est spécifiée dans la requête. Cela améliore également les performances de la requête.

-   Prise en charge de la réplication des données.

    La famille de `ReplicatedMergeTree` tables fournit la réplication des données. Pour plus d'informations, voir [Réplication des données](replication.md).

-   Appui d'échantillonnage de données.

    Si nécessaire, vous pouvez définir la méthode d'échantillonnage des données dans le tableau.

!!! info "Info"
    Le [Fusionner](../special/merge.md#merge) le moteur n'appartient pas à la `*MergeTree` famille.

## Création d'une Table {#table_engine-mergetree-creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
    INDEX index_name1 expr1 TYPE type1(...) GRANULARITY value1,
    INDEX index_name2 expr2 TYPE type2(...) GRANULARITY value2
) ENGINE = MergeTree()
[PARTITION BY expr]
[ORDER BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[TTL expr [DELETE|TO DISK 'xxx'|TO VOLUME 'xxx'], ...]
[SETTINGS name=value, ...]
```

Pour une description des paramètres, voir [Créer une description de requête](../../../sql-reference/statements/create.md).

!!! note "Note"
    `INDEX` est une fonctionnalité expérimentale, voir [Index De Saut De Données](#table_engine-mergetree-data_skipping-indexes).

### Les Clauses De Requête {#mergetree-query-clauses}

-   `ENGINE` — Name and parameters of the engine. `ENGINE = MergeTree()`. Le `MergeTree` le moteur n'a pas de paramètres.

-   `PARTITION BY` — The [clé de partitionnement](custom-partitioning-key.md).

    Pour le partitionnement par mois, utilisez les `toYYYYMM(date_column)` l'expression, où `date_column` est une colonne avec une date du type [Date](../../../sql-reference/data-types/date.md). Les noms de partition ici ont le `"YYYYMM"` format.

-   `ORDER BY` — The sorting key.

    Un tuple de colonnes ou d'expressions arbitraires. Exemple: `ORDER BY (CounterID, EventDate)`.

-   `PRIMARY KEY` — The primary key if it [diffère de la clé de tri](#choosing-a-primary-key-that-differs-from-the-sorting-key).

    Par défaut, la clé primaire est la même que la clé de tri (qui est spécifiée par `ORDER BY` clause). Ainsi dans la plupart des cas il n'est pas nécessaire de spécifier un `PRIMARY KEY` clause.

-   `SAMPLE BY` — An expression for sampling.

    Si un échantillonnage expression est utilisée, la clé primaire doit contenir. Exemple: `SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID))`.

-   `TTL` — A list of rules specifying storage duration of rows and defining logic of automatic parts movement [entre disques et volumes](#table_engine-mergetree-multiple-volumes).

    L'Expression doit en avoir une `Date` ou `DateTime` colonne comme un résultat. Exemple:
    `TTL date + INTERVAL 1 DAY`

    Le Type de la règle `DELETE|TO DISK 'xxx'|TO VOLUME 'xxx'` spécifie une action à effectuer avec la partie si l'expression est satisfaite (atteint l'heure actuelle): suppression des Lignes expirées, déplacement d'une partie (si l'expression est satisfaite pour toutes les lignes d'une partie) sur le disque spécifié (`TO DISK 'xxx'`) ou de volume (`TO VOLUME 'xxx'`). Le type par défaut de la règle est suppression (`DELETE`). Liste de règles multiples peut spécifié, mais il ne devrait pas y avoir plus d'un `DELETE` règle.

    Pour plus de détails, voir [TTL pour les colonnes et les tableaux](#table_engine-mergetree-ttl)

-   `SETTINGS` — Additional parameters that control the behavior of the `MergeTree`:

    -   `index_granularity` — Maximum number of data rows between the marks of an index. Default value: 8192. See [Le Stockage De Données](#mergetree-data-storage).
    -   `index_granularity_bytes` — Maximum size of data granules in bytes. Default value: 10Mb. To restrict the granule size only by number of rows, set to 0 (not recommended). See [Le Stockage De Données](#mergetree-data-storage).
    -   `enable_mixed_granularity_parts` — Enables or disables transitioning to control the granule size with the `index_granularity_bytes` paramètre. Avant la version 19.11, il n'y avait que le `index_granularity` réglage pour restreindre la taille des granules. Le `index_granularity_bytes` le paramètre améliore les performances de ClickHouse lors de la sélection de données à partir de tables avec de grandes lignes (des dizaines et des centaines de mégaoctets). Si vous avez des tables avec de grandes lignes, vous pouvez activer ce paramètre pour les tables d'améliorer l'efficacité de `SELECT` requête.
    -   `use_minimalistic_part_header_in_zookeeper` — Storage method of the data parts headers in ZooKeeper. If `use_minimalistic_part_header_in_zookeeper=1`, puis Zookeeper stocke moins de données. Pour plus d'informations, voir le [Description du réglage](../../../operations/server-configuration-parameters/settings.md#server-settings-use_minimalistic_part_header_in_zookeeper) dans “Server configuration parameters”.
    -   `min_merge_bytes_to_use_direct_io` — The minimum data volume for merge operation that is required for using direct I/O access to the storage disk. When merging data parts, ClickHouse calculates the total storage volume of all the data to be merged. If the volume exceeds `min_merge_bytes_to_use_direct_io` octets, ClickHouse lit et écrit les données sur le disque de stockage en utilisant l'interface d'E/S directe (`O_DIRECT` option). Si `min_merge_bytes_to_use_direct_io = 0`, puis les e/s directes sont désactivées. Valeur par défaut: `10 * 1024 * 1024 * 1024` octet.
        <a name="mergetree_setting-merge_with_ttl_timeout"></a>
    -   `merge_with_ttl_timeout` — Minimum delay in seconds before repeating a merge with TTL. Default value: 86400 (1 day).
    -   `write_final_mark` — Enables or disables writing the final index mark at the end of data part (after the last byte). Default value: 1. Don't turn it off.
    -   `merge_max_block_size` — Maximum number of rows in block for merge operations. Default value: 8192.
    -   `storage_policy` — Storage policy. See [Utilisation de plusieurs périphériques de bloc pour le stockage de données](#table_engine-mergetree-multiple-volumes).

**Exemple de réglage des Sections**

``` sql
ENGINE MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity=8192
```

Dans l'exemple, nous définissons le partitionnement par mois.

Nous définissons également une expression pour l'échantillonnage en tant que hachage par l'ID utilisateur. Cela vous permet de pseudorandomiser les données dans la table pour chaque `CounterID` et `EventDate`. Si vous définissez un [SAMPLE](../../../sql-reference/statements/select/sample.md#select-sample-clause) clause lors de la sélection des données, ClickHouse retournera un échantillon de données uniformément pseudo-aléatoire pour un sous-ensemble d'utilisateurs.

Le `index_granularity` paramètre peut être omis, car 8192 est la valeur par défaut.

<details markdown="1">

<summary>Méthode obsolète pour créer une Table</summary>

!!! attention "Attention"
    N'utilisez pas cette méthode dans les nouveaux projets. Si possible, optez anciens projets à la méthode décrite ci-dessus.

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] MergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
```

**Paramètres MergeTree ()**

-   `date-column` — The name of a column of the [Date](../../../sql-reference/data-types/date.md) type. ClickHouse crée automatiquement des partitions par mois en fonction de cette colonne. Les noms de partition sont dans le `"YYYYMM"` format.
-   `sampling_expression` — An expression for sampling.
-   `(primary, key)` — Primary key. Type: [Tuple()](../../../sql-reference/data-types/tuple.md)
-   `index_granularity` — The granularity of an index. The number of data rows between the “marks” d'un index. La valeur 8192 est appropriée pour la plupart des tâches.

**Exemple**

``` sql
MergeTree(EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID)), 8192)
```

Le `MergeTree` le moteur est configuré de la même manière que dans l'exemple ci-dessus pour la méthode de configuration du moteur principal.
</details>

## Le Stockage De Données {#mergetree-data-storage}

Une table se compose de parties de données triées par clé primaire.

Lorsque des données sont insérées dans une table, des parties de données distinctes sont créées et chacune d'elles est lexicographiquement triée par clé primaire. Par exemple, si la clé primaire est `(CounterID, Date)`, les données de la pièce sont triées par `CounterID` et au sein de chaque `CounterID` il est commandé par `Date`.

Les données appartenant à différentes partitions sont séparés en différentes parties. En arrière-plan, ClickHouse fusionne des parties de données pour un stockage plus efficace. Les parties appartenant à des partitions différentes ne sont pas fusionnées. La fusion mécanisme ne garantit pas que toutes les lignes avec la même clé primaire sera dans la même partie des données.

Chaque partie de données est logiquement divisée en granules. Un granule est le plus petit ensemble de données indivisible que ClickHouse lit lors de la sélection des données. ClickHouse ne divise pas les lignes ou les valeurs, de sorte que chaque granule contient toujours un nombre entier de lignes. La première rangée de granules est marqué avec la valeur de la clé primaire de la ligne. Pour chaque partie de données, ClickHouse crée un fichier d'index qui stocke les marques. Pour chaque colonne, que ce soit dans la clé primaire ou non, ClickHouse stocke également les mêmes marques. Ces marques vous permettent de trouver des données directement dans les fichiers de colonnes.

La taille de granule est limitée par `index_granularity` et `index_granularity_bytes` paramètres du moteur de table. Le nombre de lignes dans un granule jette dans la `[1, index_granularity]` gamme, en fonction de la taille des lignes. La taille des granulés peut dépasser `index_granularity_bytes` si la taille d'une seule ligne est supérieure à la valeur du paramètre. Dans ce cas, la taille du granule est égale à la taille de la ligne.

## Clés primaires et Index dans les requêtes {#primary-keys-and-indexes-in-queries}

Prendre la `(CounterID, Date)` clé primaire comme un exemple. Dans ce cas, le tri et l'index peuvent être illustrés comme suit:

      Whole data:     [---------------------------------------------]
      CounterID:      [aaaaaaaaaaaaaaaaaabbbbcdeeeeeeeeeeeeefgggggggghhhhhhhhhiiiiiiiiikllllllll]
      Date:           [1111111222222233331233211111222222333211111112122222223111112223311122333]
      Marks:           |      |      |      |      |      |      |      |      |      |      |
                      a,1    a,2    a,3    b,3    e,2    e,3    g,1    h,2    i,1    i,3    l,3
      Marks numbers:   0      1      2      3      4      5      6      7      8      9      10

Si la requête de données spécifie:

-   `CounterID in ('a', 'h')` le serveur lit les données dans les gammes des marques `[0, 3)` et `[6, 8)`.
-   `CounterID IN ('a', 'h') AND Date = 3` le serveur lit les données dans les gammes des marques `[1, 3)` et `[7, 8)`.
-   `Date = 3`, le serveur lit les données de la plage de marque `[1, 10]`.

Les exemples ci-dessus montrent qu'il est toujours plus efficace d'utiliser un indice qu'une analyse complète.

Un index clairsemé permet de lire des données supplémentaires. Lors de la lecture d'une plage unique de la clé primaire, jusqu'à `index_granularity * 2` lignes supplémentaires dans chaque bloc de données peut être lu.

Les index clairsemés vous permettent de travailler avec un très grand nombre de lignes de table, car dans la plupart des cas, ces index tiennent dans la RAM de l'ordinateur.

ClickHouse ne nécessite pas de clé primaire unique. Vous pouvez insérer plusieurs lignes avec la même clé primaire.

### Sélection de la clé primaire {#selecting-the-primary-key}

Le nombre de colonnes de la clé primaire n'est pas explicitement limitée. Selon la structure de données, vous pouvez inclure plus ou moins de colonnes dans la clé primaire. Cela peut:

-   Améliorer la performance d'un indice.

    Si la clé primaire est `(a, b)`, puis ajouter une autre colonne `c` pour améliorer les performances si les conditions suivantes sont réunies:

    -   Il y a des requêtes avec une condition sur la colonne `c`.
    -   Longues plages de données (plusieurs fois plus longues que `index_granularity`) avec des valeurs identiques pour `(a, b)` sont communs. En d'autres termes, lors de l'ajout d'une autre colonne vous permet de passer très longues plages de données.

-   Améliorer la compression des données.

    ClickHouse trie les données par clé primaire, donc plus la cohérence est élevée, meilleure est la compression.

-   Fournir une logique supplémentaire lors de la fusion de parties de [CollapsingMergeTree](collapsingmergetree.md#table_engine-collapsingmergetree) et [SummingMergeTree](summingmergetree.md) moteur.

    Dans ce cas, on peut spécifier l' *clé de tri* qui est différente de la clé primaire.

Une clé primaire longue affectera négativement les performances d'insertion et la consommation de mémoire, mais des colonnes supplémentaires dans la clé primaire n'affecteront pas les performances de ClickHouse pendant `SELECT` requête.

### Choisir une clé primaire qui diffère de la clé de tri {#choosing-a-primary-key-that-differs-from-the-sorting-key}

Il est possible de spécifier une clé primaire (une expression avec des valeurs qui sont écrites dans le fichier d'index pour chaque marque) qui est différente de la clé de tri (une expression pour trier les lignes dans les parties de données). Dans ce cas, le tuple d'expression de clé primaire doit être un préfixe du tuple d'expression de clé de tri.

Cette fonctionnalité est utile lorsque vous utilisez le [SummingMergeTree](summingmergetree.md) et
[AggregatingMergeTree](aggregatingmergetree.md) table des moteurs. Dans un cas courant lors de l'utilisation de ces moteurs, la table a deux types de colonnes: *dimension* et *mesure*. Les requêtes typiques agrégent les valeurs des colonnes de mesure avec arbitraire `GROUP BY` et filtrage par dimensions. Comme SummingMergeTree et AggregatingMergeTree regroupent des lignes avec la même valeur de la clé de tri, il est naturel d'y ajouter toutes les dimensions. En conséquence, l'expression se compose d'une longue liste de colonnes, et cette liste doit être mise à jour fréquemment avec nouvellement ajoutée.

Dans ce cas, il est logique de ne laisser que quelques colonnes dans la clé primaire qui fourniront des analyses de plage efficaces et ajouteront les colonnes de dimension restantes au tuple de clé de tri.

[ALTER](../../../sql-reference/statements/alter.md) la clé de tri est une opération légère car lorsqu'une nouvelle colonne est ajoutée simultanément à la table et à la clé de tri, les parties de données existantes n'ont pas besoin d'être modifiées. Comme l'ancienne clé de tri est un préfixe de la nouvelle clé de tri et qu'il n'y a pas de données dans la colonne nouvellement ajoutée, les données sont triées à la fois par l'ancienne et la nouvelle clé de tri au moment de la modification de la table.

### Utilisation D'Index et de Partitions dans les requêtes {#use-of-indexes-and-partitions-in-queries}

Pour `SELECT` requêtes, clickhouse analyse si un index peut être utilisé. Un index peut être utilisé si le `WHERE/PREWHERE` clause a une expression (comme l'un des éléments de conjonction, ou entièrement) qui représente une opération de comparaison d'égalité ou d'inégalité, ou si elle a `IN` ou `LIKE` avec un préfixe fixe sur les colonnes ou les expressions qui sont dans la clé primaire ou la clé de partitionnement, ou sur certaines fonctions partiellement répétitives de ces colonnes, ou les relations logiques de ces expressions.

Ainsi, il est possible d'exécuter des requêtes sur une ou plusieurs plages de la clé primaire. Dans cet exemple, les requêtes seront rapides lorsqu'elles sont exécutées pour une balise de suivi spécifique, pour une balise et une plage de dates spécifiques, pour une balise et une date spécifiques, pour plusieurs balises avec une plage de dates, etc.

Regardons le moteur configuré comme suit:

      ENGINE MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate) SETTINGS index_granularity=8192

Dans ce cas, dans les requêtes:

``` sql
SELECT count() FROM table WHERE EventDate = toDate(now()) AND CounterID = 34
SELECT count() FROM table WHERE EventDate = toDate(now()) AND (CounterID = 34 OR CounterID = 42)
SELECT count() FROM table WHERE ((EventDate >= toDate('2014-01-01') AND EventDate <= toDate('2014-01-31')) OR EventDate = toDate('2014-05-01')) AND CounterID IN (101500, 731962, 160656) AND (CounterID = 101500 OR EventDate != toDate('2014-05-01'))
```

ClickHouse utilisera l'index de clé primaire pour supprimer les données incorrectes et la clé de partitionnement mensuel pour supprimer les partitions qui se trouvent dans des plages de dates incorrectes.

Les requêtes ci-dessus montrent que l'index est utilisé même pour les expressions complexes. La lecture de la table est organisée de sorte que l'utilisation de l'index ne peut pas être plus lente qu'une analyse complète.

Dans l'exemple ci-dessous, l'index ne peut pas être utilisé.

``` sql
SELECT count() FROM table WHERE CounterID = 34 OR URL LIKE '%upyachka%'
```

Pour vérifier si ClickHouse pouvez utiliser l'index lors de l'exécution d'une requête, utilisez les paramètres [force\_index\_by\_date](../../../operations/settings/settings.md#settings-force_index_by_date) et [force\_primary\_key](../../../operations/settings/settings.md).

La clé de partitionnement par mois permet de lire uniquement les blocs de données qui contiennent des dates de la plage appropriée. Dans ce cas, le bloc de données peut contenir des données pour plusieurs dates (jusqu'à un mois entier). Dans un bloc, les données sont triées par clé primaire, qui peut ne pas contenir la date comme première colonne. Pour cette raison, l'utilisation d'une requête avec seulement une condition de date qui ne spécifie pas le préfixe de clé primaire entraînera la lecture de plus de données que pour une seule date.

### Utilisation de L'Index pour les clés primaires partiellement monotones {#use-of-index-for-partially-monotonic-primary-keys}

Considérons, par exemple, les jours du mois. Ils forment un [monotone de la séquence](https://en.wikipedia.org/wiki/Monotonic_function) pendant un mois, mais pas monotone pendant des périodes plus longues. C'est une séquence partiellement monotone. Si un utilisateur crée la table avec une clé primaire partiellement monotone, ClickHouse crée un index clairsemé comme d'habitude. Lorsqu'un utilisateur sélectionne des données à partir de ce type de table, ClickHouse analyse les conditions de requête. Si L'utilisateur veut obtenir des données entre deux marques de l'index et que ces deux marques tombent dans un mois, ClickHouse peut utiliser l'index dans ce cas particulier car il peut calculer la distance entre les paramètres d'une requête et les marques d'index.

ClickHouse ne peut pas utiliser un index si les valeurs de la clé primaire dans la plage de paramètres de requête ne représentent pas une séquence monotone. Dans ce cas, ClickHouse utilise la méthode full scan.

ClickHouse utilise cette logique non seulement pour les séquences de jours du mois, mais pour toute clé primaire qui représente une séquence partiellement monotone.

### Index de saut de données (expérimental) {#table_engine-mergetree-data_skipping-indexes}

La déclaration d'index se trouve dans la section colonnes du `CREATE` requête.

``` sql
INDEX index_name expr TYPE type(...) GRANULARITY granularity_value
```

Pour les tables de la `*MergeTree` famille, les indices de saut de données peuvent être spécifiés.

Ces indices agrégent certaines informations sur l'expression spécifiée sur les blocs, qui consistent en `granularity_value` granules (la taille du granule est spécifiée en utilisant `index_granularity` réglage dans le moteur de table). Ensuite, ces agrégats sont utilisés dans `SELECT` requêtes pour réduire la quantité de données à lire à partir du disque en ignorant de gros blocs de données `where` la requête ne peut pas être satisfait.

**Exemple**

``` sql
CREATE TABLE table_name
(
    u64 UInt64,
    i32 Int32,
    s String,
    ...
    INDEX a (u64 * i32, s) TYPE minmax GRANULARITY 3,
    INDEX b (u64 * length(s)) TYPE set(1000) GRANULARITY 4
) ENGINE = MergeTree()
...
```

Les Indices de L'exemple peuvent être utilisés par ClickHouse pour réduire la quantité de données à lire à partir du disque dans les requêtes suivantes:

``` sql
SELECT count() FROM table WHERE s < 'z'
SELECT count() FROM table WHERE u64 * i32 == 10 AND u64 * length(s) >= 1234
```

#### Types d'Indices disponibles {#available-types-of-indices}

-   `minmax`

    Magasins extrêmes de l'expression spécifiée (si l'expression est `tuple` puis il stocke les extrêmes pour chaque élément de `tuple`), utilise les informations stockées pour sauter des blocs de données comme la clé primaire.

-   `set(max_rows)`

    Stocke les valeurs uniques de l'expression spécifiée (pas plus de `max_rows` rangée, `max_rows=0` moyen “no limits”). Utilise les valeurs pour vérifier si le `WHERE` l'expression n'est pas satisfiable sur un bloc de données.

-   `ngrambf_v1(n, size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)`

    Magasins un [Filtre de Bloom](https://en.wikipedia.org/wiki/Bloom_filter) qui contient tous les ngrams d'un bloc de données. Fonctionne uniquement avec des chaînes. Peut être utilisé pour l'optimisation de `equals`, `like` et `in` expression.

    -   `n` — ngram size,
    -   `size_of_bloom_filter_in_bytes` — Bloom filter size in bytes (you can use large values here, for example, 256 or 512, because it can be compressed well).
    -   `number_of_hash_functions` — The number of hash functions used in the Bloom filter.
    -   `random_seed` — The seed for Bloom filter hash functions.

-   `tokenbf_v1(size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)`

    Le même que `ngrambf_v1`, mais stocke des jetons au lieu de ngrams. Les jetons sont des séquences séparées par des caractères non alphanumériques.

-   `bloom_filter([false_positive])` — Stores a [Filtre de Bloom](https://en.wikipedia.org/wiki/Bloom_filter) pour les colonnes spécifiées.

    Facultatif `false_positive` le paramètre est la probabilité de recevoir une réponse faussement positive du filtre. Valeurs possibles: (0, 1). Valeur par défaut: 0.025.

    Types de données pris en charge: `Int*`, `UInt*`, `Float*`, `Enum`, `Date`, `DateTime`, `String`, `FixedString`, `Array`, `LowCardinality`, `Nullable`.

    Les fonctions suivantes peuvent l'utiliser: [égal](../../../sql-reference/functions/comparison-functions.md), [notEquals](../../../sql-reference/functions/comparison-functions.md), [dans](../../../sql-reference/functions/in-functions.md), [notIn](../../../sql-reference/functions/in-functions.md), [avoir](../../../sql-reference/functions/array-functions.md).

<!-- -->

``` sql
INDEX sample_index (u64 * length(s)) TYPE minmax GRANULARITY 4
INDEX sample_index2 (u64 * length(str), i32 + f64 * 100, date, str) TYPE set(100) GRANULARITY 4
INDEX sample_index3 (lower(str), str) TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 4
```

#### Les Fonctions De Soutien {#functions-support}

Les Conditions dans le `WHERE` la clause contient des appels des fonctions qui fonctionnent avec des colonnes. Si la colonne fait partie d'un index, ClickHouse essaie d'utiliser cet index lors de l'exécution des fonctions. ClickHouse prend en charge différents sous-ensembles de fonctions pour l'utilisation d'index.

Le `set` l'indice peut être utilisé avec toutes les fonctions. Les sous-ensembles de fonctions pour les autres index sont présentés dans le tableau ci-dessous.

| Fonction (opérateur) / Indice de                                                                             | clé primaire | minmax | ngrambf\_v1 | tokenbf\_v1 | bloom\_filter |
|--------------------------------------------------------------------------------------------------------------|--------------|--------|-------------|-------------|---------------|
| [égal (=, ==)](../../../sql-reference/functions/comparison-functions.md#function-equals)                     | ✔            | ✔      | ✔           | ✔           | ✔             |
| [notEquals (!=, \<\>)](../../../sql-reference/functions/comparison-functions.md#function-notequals)          | ✔            | ✔      | ✔           | ✔           | ✔             |
| [comme](../../../sql-reference/functions/string-search-functions.md#function-like)                           | ✔            | ✔      | ✔           | ✗           | ✗             |
| [notLike](../../../sql-reference/functions/string-search-functions.md#function-notlike)                      | ✔            | ✔      | ✔           | ✗           | ✗             |
| [startsWith](../../../sql-reference/functions/string-functions.md#startswith)                                | ✔            | ✔      | ✔           | ✔           | ✗             |
| [endsWith](../../../sql-reference/functions/string-functions.md#endswith)                                    | ✗            | ✗      | ✔           | ✔           | ✗             |
| [multiSearchAny](../../../sql-reference/functions/string-search-functions.md#function-multisearchany)        | ✗            | ✗      | ✔           | ✗           | ✗             |
| [dans](../../../sql-reference/functions/in-functions.md#in-functions)                                        | ✔            | ✔      | ✔           | ✔           | ✔             |
| [notIn](../../../sql-reference/functions/in-functions.md#in-functions)                                       | ✔            | ✔      | ✔           | ✔           | ✔             |
| [peu (\<)](../../../sql-reference/functions/comparison-functions.md#function-less)                           | ✔            | ✔      | ✗           | ✗           | ✗             |
| [grand (\>)](../../../sql-reference/functions/comparison-functions.md#function-greater)                      | ✔            | ✔      | ✗           | ✗           | ✗             |
| [lessOrEquals (\<=)](../../../sql-reference/functions/comparison-functions.md#function-lessorequals)         | ✔            | ✔      | ✗           | ✗           | ✗             |
| [greaterOrEquals ( \> =)](../../../sql-reference/functions/comparison-functions.md#function-greaterorequals) | ✔            | ✔      | ✗           | ✗           | ✗             |
| [vide](../../../sql-reference/functions/array-functions.md#function-empty)                                   | ✔            | ✔      | ✗           | ✗           | ✗             |
| [notEmpty](../../../sql-reference/functions/array-functions.md#function-notempty)                            | ✔            | ✔      | ✗           | ✗           | ✗             |
| hasToken                                                                                                     | ✗            | ✗      | ✗           | ✔           | ✗             |

Les fonctions avec un argument constant inférieur à la taille ngram ne peuvent pas être utilisées par `ngrambf_v1` pour l'optimisation de la requête.

Les filtres Bloom peuvent avoir des correspondances faussement positives, de sorte que le `ngrambf_v1`, `tokenbf_v1`, et `bloom_filter` les index ne peuvent pas être utilisés pour optimiser les requêtes où le résultat d'une fonction est censé être faux, par exemple:

-   Peut être optimisé:
    -   `s LIKE '%test%'`
    -   `NOT s NOT LIKE '%test%'`
    -   `s = 1`
    -   `NOT s != 1`
    -   `startsWith(s, 'test')`
-   Ne peut pas être optimisé:
    -   `NOT s LIKE '%test%'`
    -   `s NOT LIKE '%test%'`
    -   `NOT s = 1`
    -   `s != 1`
    -   `NOT startsWith(s, 'test')`

## Accès Simultané Aux Données {#concurrent-data-access}

Pour l'accès aux tables simultanées, nous utilisons le multi-versioning. En d'autres termes, lorsqu'une table est lue et mise à jour simultanément, les données sont lues à partir d'un ensemble de parties en cours au moment de la requête. Il n'y a pas de longues mèches. Les Inserts ne gênent pas les opérations de lecture.

Lecture à partir d'un tableau est automatiquement parallélisée.

## TTL pour les colonnes et les tableaux {#table_engine-mergetree-ttl}

Détermine la durée de vie de des valeurs.

Le `TTL` clause peut être définie pour la table entière et pour chaque colonne individuelle. Ttl de niveau Table peut également spécifier la logique de déplacement automatique des données entre les disques et les volumes.

Les Expressions doivent évaluer pour [Date](../../../sql-reference/data-types/date.md) ou [DateTime](../../../sql-reference/data-types/datetime.md) type de données.

Exemple:

``` sql
TTL time_column
TTL time_column + interval
```

Définir `interval`, utiliser [intervalle](../../../sql-reference/operators/index.md#operators-datetime) opérateur.

``` sql
TTL date_time + INTERVAL 1 MONTH
TTL date_time + INTERVAL 15 HOUR
```

### Colonne TTL {#mergetree-column-ttl}

Lorsque les valeurs de la colonne expirent, ClickHouse les remplace par les valeurs par défaut du type de données de la colonne. Si toutes les valeurs de colonne de la partie données expirent, ClickHouse supprime cette colonne de la partie données d'un système de fichiers.

Le `TTL` la clause ne peut pas être utilisée pour les colonnes clés.

Exemple:

Création d'une table avec TTL

``` sql
CREATE TABLE example_table
(
    d DateTime,
    a Int TTL d + INTERVAL 1 MONTH,
    b Int TTL d + INTERVAL 1 MONTH,
    c String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d;
```

Ajout de TTL à une colonne d'une table existante

``` sql
ALTER TABLE example_table
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 DAY;
```

Modification de TTL de la colonne

``` sql
ALTER TABLE example_table
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 MONTH;
```

### Tableau TTL {#mergetree-table-ttl}

Table peut avoir une expression pour la suppression de Lignes expirées, et plusieurs expressions pour le déplacement automatique de pièces entre [disques ou volumes](#table_engine-mergetree-multiple-volumes). Lorsque les lignes de la table expirent, ClickHouse supprime toutes les lignes correspondantes. Pour les pièces en mouvement, toutes les lignes d'une pièce doivent satisfaire aux critères d'expression de mouvement.

``` sql
TTL expr [DELETE|TO DISK 'aaa'|TO VOLUME 'bbb'], ...
```

Type de règle TTL peut suivre chaque expression TTL. Il affecte une action qui doit être faite une fois que l'expression est satisfaite (atteint l'heure actuelle):

-   `DELETE` - supprimer les Lignes expirées (action par défaut);
-   `TO DISK 'aaa'` - déplacer la partie sur le disque `aaa`;
-   `TO VOLUME 'bbb'` - déplacer la partie sur le disque `bbb`.

Exemple:

Création d'une table avec TTL

``` sql
CREATE TABLE example_table
(
    d DateTime,
    a Int
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d
TTL d + INTERVAL 1 MONTH [DELETE],
    d + INTERVAL 1 WEEK TO VOLUME 'aaa',
    d + INTERVAL 2 WEEK TO DISK 'bbb';
```

Modification de TTL de la table

``` sql
ALTER TABLE example_table
    MODIFY TTL d + INTERVAL 1 DAY;
```

**Suppression De Données**

Les données avec un TTL expiré sont supprimées lorsque ClickHouse fusionne des parties de données.

Lorsque ClickHouse voit que les données sont expirées, il effectue une fusion hors calendrier. Pour contrôler la fréquence de ces fusions, vous pouvez définir `merge_with_ttl_timeout`. Si la valeur est trop faible, il effectuera de nombreuses fusions hors calendrier qui peuvent consommer beaucoup de ressources.

Si vous effectuez la `SELECT` requête entre les fusionne, vous pouvez obtenir des données expirées. Pour éviter cela, utilisez la [OPTIMIZE](../../../sql-reference/statements/misc.md#misc_operations-optimize) requête avant de l' `SELECT`.

## Utilisation de plusieurs périphériques de bloc pour le stockage de données {#table_engine-mergetree-multiple-volumes}

### Introduction {#introduction}

`MergeTree` les moteurs de table de famille peuvent stocker des données sur plusieurs périphériques de bloc. Par exemple, il peut être utile lorsque les données d'un tableau sont implicitement divisé en “hot” et “cold”. Les données les plus récentes sont régulièrement demandées mais ne nécessitent qu'une petite quantité d'espace. Au contraire, les données historiques à queue grasse sont rarement demandées. Si plusieurs disques sont disponibles, la “hot” les données peuvent être situées sur des disques rapides (par exemple, SSD NVMe ou en mémoire), tandis que le “cold” des données relativement lente (par exemple, disque dur).

La partie de données est l'unité mobile minimum pour `MergeTree`-tables de moteur. Les données appartenant à une partie sont stockées sur un disque. Les parties de données peuvent être déplacées entre les disques en arrière-plan (selon les paramètres de l'utilisateur) ainsi qu'au moyen du [ALTER](../../../sql-reference/statements/alter.md#alter_move-partition) requête.

### Terme {#terms}

-   Disk — Block device mounted to the filesystem.
-   Default disk — Disk that stores the path specified in the [chemin](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-path) paramètre de serveur.
-   Volume — Ordered set of equal disks (similar to [JBOD](https://en.wikipedia.org/wiki/Non-RAID_drive_architectures)).
-   Storage policy — Set of volumes and the rules for moving data between them.

Les noms donnés aux entités décrites peuvent être trouvés dans les tables système, [système.storage\_policies](../../../operations/system-tables.md#system_tables-storage_policies) et [système.disque](../../../operations/system-tables.md#system_tables-disks). Pour appliquer l'une des stratégies de stockage configurées pour une table, utilisez `storage_policy` réglage de `MergeTree`-moteur de table de famille.

### Configuration {#table_engine-mergetree-multiple-volumes_configure}

Les disques, les volumes et les stratégies de stockage doivent être déclarés `<storage_configuration>` étiquette, soit dans le fichier principal `config.xml` ou dans un fichier distinct dans le `config.d` répertoire.

Structure de Configuration:

``` xml
<storage_configuration>
    <disks>
        <disk_name_1> <!-- disk name -->
            <path>/mnt/fast_ssd/clickhouse/</path>
        </disk_name_1>
        <disk_name_2>
            <path>/mnt/hdd1/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_2>
        <disk_name_3>
            <path>/mnt/hdd2/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_3>

        ...
    </disks>

    ...
</storage_configuration>
```

Balise:

-   `<disk_name_N>` — Disk name. Names must be different for all disks.
-   `path` — path under which a server will store data (`data` et `shadow` des dossiers), doit être terminé par ‘/’.
-   `keep_free_space_bytes` — the amount of free disk space to be reserved.

L'ordre du disque définition n'est pas important.

Stratégies de stockage balisage de configuration:

``` xml
<storage_configuration>
    ...
    <policies>
        <policy_name_1>
            <volumes>
                <volume_name_1>
                    <disk>disk_name_from_disks_configuration</disk>
                    <max_data_part_size_bytes>1073741824</max_data_part_size_bytes>
                </volume_name_1>
                <volume_name_2>
                    <!-- configuration -->
                </volume_name_2>
                <!-- more volumes -->
            </volumes>
            <move_factor>0.2</move_factor>
        </policy_name_1>
        <policy_name_2>
            <!-- configuration -->
        </policy_name_2>

        <!-- more policies -->
    </policies>
    ...
</storage_configuration>
```

Balise:

-   `policy_name_N` — Policy name. Policy names must be unique.
-   `volume_name_N` — Volume name. Volume names must be unique.
-   `disk` — a disk within a volume.
-   `max_data_part_size_bytes` — the maximum size of a part that can be stored on any of the volume's disks.
-   `move_factor` — when the amount of available space gets lower than this factor, data automatically start to move on the next volume if any (by default, 0.1).

Exemples de Cofiguration:

``` xml
<storage_configuration>
    ...
    <policies>
        <hdd_in_order> <!-- policy name -->
            <volumes>
                <single> <!-- volume name -->
                    <disk>disk1</disk>
                    <disk>disk2</disk>
                </single>
            </volumes>
        </hdd_in_order>

        <moving_from_ssd_to_hdd>
            <volumes>
                <hot>
                    <disk>fast_ssd</disk>
                    <max_data_part_size_bytes>1073741824</max_data_part_size_bytes>
                </hot>
                <cold>
                    <disk>disk1</disk>
                </cold>
            </volumes>
            <move_factor>0.2</move_factor>
        </moving_from_ssd_to_hdd>
    </policies>
    ...
</storage_configuration>
```

Dans l'exemple donné, la `hdd_in_order` politique met en œuvre les [round-robin](https://en.wikipedia.org/wiki/Round-robin_scheduling) approche. Ainsi cette politique ne définit qu'un seul volume (`single`), les parties des données sont stockées sur tous ses disques dans l'ordre circulaire. Une telle politique peut être très utile s'il y a plusieurs disques similaires sont montés sur le système, mais RAID N'est pas configuré. Gardez à l'esprit que chaque lecteur de disque n'est pas fiable et vous pouvez compenser avec facteur de réplication de 3 ou plus.

S'il existe différents types de disques disponibles dans le système, `moving_from_ssd_to_hdd` la stratégie peut être utilisée à la place. Volume `hot` se compose d'un disque SSD (`fast_ssd`), et la taille maximale d'une pièce qui peut être stocké sur ce volume est de 1 go. Toutes les pièces avec la taille plus grande que 1 GB sera stocké directement sur le `cold` le volume, qui contient un disque dur de disque `disk1`.
Aussi, une fois le disque `fast_ssd` est complété par plus de 80%, les données seront transférées à la `disk1` par un processus d'arrière-plan.

L'ordre d'énumération des volumes dans une stratégie de stockage est important. Une fois qu'un volume est surchargé, les données sont déplacées vers le suivant. L'ordre d'énumération des disques est important parce que les données sont stockées dans les virages.

Lors de la création d'une table, on peut lui appliquer l'une des stratégies de stockage configurées:

``` sql
CREATE TABLE table_with_non_default_policy (
    EventDate Date,
    OrderID UInt64,
    BannerID UInt64,
    SearchPhrase String
) ENGINE = MergeTree
ORDER BY (OrderID, BannerID)
PARTITION BY toYYYYMM(EventDate)
SETTINGS storage_policy = 'moving_from_ssd_to_hdd'
```

Le `default` la Politique de stockage implique d'utiliser un seul volume, qui se compose d'un seul disque donné dans `<path>`. Une fois qu'une table est créée, sa stratégie de stockage ne peut pas être modifiée.

### Détail {#details}

Dans le cas de `MergeTree` les tableaux, les données sont sur le disque de différentes façons:

-   En tant que résultat d'un insert (`INSERT` requête).
-   En arrière-plan fusionne et [mutation](../../../sql-reference/statements/alter.md#alter-mutations).
-   Lors du téléchargement à partir d'une autre réplique.
-   À la suite du gel de la partition [ALTER TABLE … FREEZE PARTITION](../../../sql-reference/statements/alter.md#alter_freeze-partition).

Dans tous ces cas, à l'exception des mutations et du gel de partition, une pièce est stockée sur un volume et un disque selon la Politique de stockage donnée:

1.  Le premier volume (dans l'ordre de définition) qui a suffisamment d'espace disque pour stocker une pièce (`unreserved_space > current_part_size`) et permet de stocker des pièces d'une taille donnée (`max_data_part_size_bytes > current_part_size`) est choisi.
2.  Dans ce volume, ce disque est choisi qui suit celui, qui a été utilisé pour stocker le bloc de données précédent, et qui a de l'espace libre plus que la taille de la pièce (`unreserved_space - keep_free_space_bytes > current_part_size`).

Sous le capot, les mutations et la congélation des cloisons utilisent [des liens en dur](https://en.wikipedia.org/wiki/Hard_link). Les liens durs entre différents disques ne sont pas pris en charge, donc dans de tels cas, les pièces résultantes sont stockées sur les mêmes disques que les disques initiaux.

En arrière - plan, les pièces sont déplacées entre les volumes en fonction de la quantité d'espace libre (`move_factor` paramètre) selon l'ordre les volumes sont déclarées dans le fichier de configuration.
Les données ne sont jamais transférées du dernier et dans le premier. On peut utiliser des tables système [système.part\_log](../../../operations/system-tables.md#system_tables-part-log) (champ `type = MOVE_PART`) et [système.partie](../../../operations/system-tables.md#system_tables-parts) (Fields `path` et `disk`) pour surveiller l'arrière-plan se déplace. Aussi, les informations détaillées peuvent être trouvées dans les journaux du serveur.

L'utilisateur peut forcer le déplacement d'une partie ou d'une partition d'un volume à l'autre à l'aide de la requête [ALTER TABLE … MOVE PART\|PARTITION … TO VOLUME\|DISK …](../../../sql-reference/statements/alter.md#alter_move-partition), toutes les restrictions pour les opérations de fond sont prises en compte. La requête initie un mouvement seul et n'attend pas que les opérations d'arrière-plan soient terminées. L'utilisateur recevra un message d'erreur si pas assez d'espace libre est disponible ou si l'une des conditions requises ne sont pas remplies.

Le déplacement des données n'interfère pas avec la réplication des données. Par conséquent, différentes stratégies de stockage peuvent être spécifiées pour la même table sur différents réplicas.

Après l'achèvement des fusions d'arrière-plan et des mutations, les anciennes parties ne sont supprimées qu'après un certain temps (`old_parts_lifetime`).
Pendant ce temps, ils ne sont pas déplacés vers d'autres volumes ou des disques. Par conséquent, jusqu'à ce que les pièces soient finalement supprimées, elles sont toujours prises en compte pour l'évaluation de l'espace disque occupé.

[Article Original](https://clickhouse.tech/docs/ru/operations/table_engines/mergetree/) <!--hide-->
