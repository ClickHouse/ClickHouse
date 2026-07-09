---
description: "Les moteurs de table de la famille `MergeTree` sont conçus pour des taux d’ingestion élevés
  et de très grands volumes de données."
sidebar_label: 'MergeTree'
sidebar_position: 11
slug: /engines/table-engines/mergetree-family/mergetree
title: "Moteur de table MergeTree"
doc_type: 'référence'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="mergetree-table-engine">
  # Moteur de table MergeTree
</div>

Le moteur `MergeTree` et les autres moteurs de la famille `MergeTree` (par ex. `ReplacingMergeTree`, `AggregatingMergeTree` ) sont les moteurs de table les plus utilisés et les plus robustes de ClickHouse.

Les moteurs de table de la famille `MergeTree` sont conçus pour des taux d’ingestion élevés et de très gros volumes de données.
Les opérations d’insertion créent des parts de table, qui sont fusionnées en arrière-plan avec d’autres parts de table.

Principales fonctionnalités des moteurs de table de la famille `MergeTree`.

* La clé primaire de la table détermine l’ordre de tri au sein de chaque part de table (index clusterisé). Elle ne référence pas non plus des lignes individuelles, mais des blocs de 8192 lignes appelés granules. Cela permet aux clés primaires de très grands jeux de données de rester suffisamment petites pour être conservées en mémoire vive, tout en assurant un accès rapide aux données sur disque.

* Les tables peuvent être partitionnées à l’aide d’une expression de partition arbitraire. L’élagage des partitions garantit que les partitions inutiles ne sont pas lues lorsque la requête le permet.

* Les données peuvent être répliquées sur plusieurs nœuds du cluster pour assurer une haute disponibilité, le basculement et des mises à niveau sans interruption. Voir [Réplication des données](/fr/engines/table-engines/mergetree-family/replication.md).

* Les moteurs de table `MergeTree` prennent en charge différents types de statistiques et méthodes d’échantillonnage pour faciliter l’optimisation des requêtes.

:::note
Malgré la similarité des noms, le moteur [Merge](/fr/engines/table-engines/special/merge) est différent des moteurs `*MergeTree`.
:::

<div id="table_engine-mergetree-creating-a-table">
  ## Créer des tables
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [[NOT] NULL] [DEFAULT|MATERIALIZED|ALIAS|EPHEMERAL expr1] [COMMENT ...] [CODEC(codec1)] [STATISTICS(stat1)] [TTL expr1] [PRIMARY KEY] [SETTINGS (name = value, ...)],
    name2 [type2] [[NOT] NULL] [DEFAULT|MATERIALIZED|ALIAS|EPHEMERAL expr2] [COMMENT ...] [CODEC(codec2)] [STATISTICS(stat2)] [TTL expr2] [PRIMARY KEY] [SETTINGS (name = value, ...)],
    ...
    INDEX index_name1 expr1 TYPE type1(...) [GRANULARITY value1],
    INDEX index_name2 expr2 TYPE type2(...) [GRANULARITY value2],
    ...
    PROJECTION projection_name_1 (SELECT <COLUMN LIST EXPR> [GROUP BY] [ORDER BY]),
    PROJECTION projection_name_2 (SELECT <COLUMN LIST EXPR> [GROUP BY] [ORDER BY])
) ENGINE = MergeTree()
ORDER BY expr
[PARTITION BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[TTL expr
    [DELETE|TO DISK 'xxx'|TO VOLUME 'xxx' [, ...] ]
    [WHERE conditions]
    [GROUP BY key_expr [SET v1 = aggr_func(v1) [, v2 = aggr_func(v2) ...]] ] ]
[SETTINGS name = value, ...]
```

Pour une description détaillée des paramètres, consultez l’instruction [CREATE TABLE](/fr/sql-reference/statements/create/table.md)

<div id="mergetree-query-clauses">
  ### Clauses de la requête
</div>

<div id="engine">
  #### ENGINE
</div>

`ENGINE` — Nom et paramètres du moteur. `ENGINE = MergeTree()`. Le moteur `MergeTree` n’a pas de paramètres.

<div id="order_by">
  #### ORDER BY
</div>

`ORDER BY` — La clé de tri.

Un tuple de noms de colonnes ou d&#39;expressions arbitraires. Exemple : `ORDER BY (CounterID + 1, EventDate)`.

Si aucune clé primaire n&#39;est définie (c.-à-d. si `PRIMARY KEY` n&#39;a pas été spécifié), ClickHouse utilise la clé de tri comme clé primaire.

Si aucun tri n&#39;est nécessaire, vous pouvez utiliser la syntaxe `ORDER BY tuple()`.
Par ailleurs, si le paramètre `create_table_empty_primary_key_by_default` est activé, `ORDER BY ()` est ajouté implicitement aux instructions `CREATE TABLE`. Voir [Sélection d&#39;une clé primaire](#selecting-a-primary-key).

<div id="partition-by">
  #### PARTITION BY
</div>

`PARTITION BY` — la [clé de partitionnement](/fr/engines/table-engines/mergetree-family/custom-partitioning-key.md). Facultatif. Dans la plupart des cas, vous n’avez pas besoin de clé de partitionnement et, si vous devez partitionner, vous n’avez généralement pas besoin d’une clé de partitionnement plus fine qu’un partitionnement mensuel. Le partitionnement n’accélère pas les requêtes (contrairement à l’expression ORDER BY). Vous ne devez jamais utiliser un partitionnement trop fin. Ne partitionnez pas vos données par identifiant ou nom de client (faites plutôt de l’identifiant ou du nom du client la première colonne de l’expression ORDER BY).

Pour partitionner par mois, utilisez l’expression `toYYYYMM(date_column)`, où `date_column` est une colonne contenant une date de type [Date](/fr/sql-reference/data-types/date.md). Les noms de partition ont ici le format `"YYYYMM"`.

<div id="primary-key">
  #### PRIMARY KEY
</div>

`PRIMARY KEY` — La clé primaire si elle [diffère de la clé de tri](#choosing-a-primary-key-that-differs-from-the-sorting-key). Facultative.

La définition d&#39;une clé de tri (à l&#39;aide de la clause `ORDER BY`) définit implicitement une clé primaire.
Il n&#39;est généralement pas nécessaire de définir la clé primaire en plus de la clé de tri.

<div id="sample-by">
  #### SAMPLE BY
</div>

`SAMPLE BY` — Une expression d’échantillonnage. Optionnelle.

Si elle est spécifiée, elle doit figurer dans la clé primaire.
L’expression d’échantillonnage doit produire un entier non signé.

Exemple : `SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID))`.

<div id="ttl">
  #### TTL
</div>

`TTL` — Une liste de règles qui spécifient la durée de conservation des lignes ainsi que la logique de déplacement automatique des parts [entre les disques et les volumes](#table_engine-mergetree-multiple-volumes). Facultatif.

L&#39;expression doit renvoyer un `Date` ou un `DateTime`, par exemple `TTL date + INTERVAL 1 DAY`.

Le type de règle `DELETE|TO DISK 'xxx'|TO VOLUME 'xxx'|GROUP BY` spécifie l&#39;action à effectuer sur la part si l&#39;expression est satisfaite (atteint l&#39;instant présent) : suppression des lignes expirées, déplacement d&#39;une part (si l&#39;expression est satisfaite pour toutes les lignes de la part) vers le disque spécifié (`TO DISK 'xxx'`) ou vers le volume (`TO VOLUME 'xxx'`), ou agrégation des valeurs des lignes expirées. Le type de règle par défaut est la suppression (`DELETE`). Il est possible de spécifier plusieurs règles, mais il ne doit pas y avoir plus d&#39;une règle `DELETE`.

Pour plus de détails, voir [TTL des colonnes et des tables](#table_engine-mergetree-ttl)

<div id="settings">
  #### PARAMÈTRES
</div>

Voir [les paramètres de MergeTree](../../../operations/settings/merge-tree-settings.md).

**Exemple du paramètre Sections**

```sql
ENGINE MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity=8192
```

Dans l’exemple, nous définissons un partitionnement par mois.

Nous définissons également une expression d’échantillonnage sous forme de hachage de l’ID utilisateur. Cela permet de pseudo-aléatoiriser les données de la table pour chaque `CounterID` et `EventDate`. Si vous définissez une clause [SAMPLE](/fr/sql-reference/statements/select/sample) lors de la sélection des données, ClickHouse renverra un échantillon de données pseudo-aléatoire et uniforme pour un sous-ensemble d’utilisateurs.

Le paramètre `index_granularity` peut être omis, car 8192 est la valeur par défaut.

<details markdown="1">
  <summary>Méthode obsolète pour créer une table</summary>

  :::note
  N’utilisez pas cette méthode dans de nouveaux projets. Si possible, faites migrer les anciens projets vers la méthode décrite ci-dessus.
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] MergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
  ```

  **Paramètres de MergeTree()**

  * `date-column` — Nom d’une colonne de type [Date](/fr/sql-reference/data-types/date.md). ClickHouse crée automatiquement des partitions mensuelles à partir de cette colonne. Les noms des partitions sont au format `"YYYYMM"`.
  * `sampling_expression` — Une expression d’échantillonnage.
  * `(primary, key)` — Clé primaire. Type : [Tuple()](/fr/sql-reference/data-types/tuple.md)
  * `index_granularity` — La granularité d’un index. Le nombre de lignes de données entre les &quot;marks&quot; d’un index. La valeur 8192 convient à la plupart des cas.

  **Exemple**

  ```sql
  MergeTree(EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID)), 8192)
  ```

  Le moteur `MergeTree` se configure de la même manière que dans l’exemple ci-dessus pour la méthode principale de configuration du moteur.
</details>

<div id="mergetree-data-storage">
  ## Stockage des données
</div>

Une table se compose de parties de données triées par clé primaire.

Lorsque des données sont insérées dans une table, des parties de données distinctes sont créées, et chacune d’elles est triée lexicographiquement par clé primaire. Par exemple, si la clé primaire est `(CounterID, Date)`, les données de la partie sont triées par `CounterID` et, au sein de chaque `CounterID`, elles sont ordonnées par `Date`.

Les données appartenant à des partitions différentes sont séparées dans des parties distinctes. En arrière-plan, ClickHouse fusionne les parties de données pour optimiser le stockage. Les parties appartenant à des partitions différentes ne sont pas fusionnées. Le mécanisme de fusion ne garantit pas que toutes les lignes ayant la même clé primaire se trouveront dans la même partie de données.

Les parties de données peuvent être stockées au format `Wide` ou `Compact`. Au format `Wide`, chaque colonne est stockée dans un fichier distinct sur le système de fichiers ; au format `Compact`, toutes les colonnes sont stockées dans un seul fichier. Le format `Compact` peut être utilisé pour améliorer les performances des insertions petites et fréquentes.

Le format de stockage des données est contrôlé par les paramètres `min_bytes_for_wide_part` et `min_rows_for_wide_part` du moteur de table. Si le nombre d’octets ou de lignes dans une partie de données est inférieur à la valeur du paramètre correspondant, la partie est stockée au format `Compact`. Sinon, elle est stockée au format `Wide`. Si aucun de ces paramètres n’est défini, les parties de données sont stockées au format `Wide`.

Chaque partie de données est logiquement divisée en granules. Une granule est le plus petit ensemble de données indivisible que ClickHouse lit lors de la sélection des données. ClickHouse ne divise ni les lignes ni les valeurs ; chaque granule contient donc toujours un nombre entier de lignes. La première ligne d’une granule est marquée par la valeur de la clé primaire de cette ligne. Pour chaque partie de données, ClickHouse crée un fichier d’index qui stocke les marques. Pour chaque colonne, qu’elle fasse partie ou non de la clé primaire, ClickHouse stocke également les mêmes marques. Ces marques permettent de localiser directement les données dans les fichiers de colonnes.

La taille des granules est limitée par les paramètres `index_granularity` et `index_granularity_bytes` du moteur de table. Le nombre de lignes dans une granule se situe dans l’intervalle `[1, index_granularity]`, en fonction de la taille des lignes. La taille d’une granule peut dépasser `index_granularity_bytes` si la taille d’une seule ligne est supérieure à la valeur du paramètre. Dans ce cas, la taille de la granule est égale à la taille de la ligne.

<div id="primary-keys-and-indexes-in-queries">
  ## Clés primaires et index dans les requêtes
</div>

Prenons la clé primaire `(CounterID, Date)` comme exemple. Dans ce cas, l’ordre de tri et l’index peuvent être illustrés comme suit :

```text
Whole data:     [---------------------------------------------]
CounterID:      [aaaaaaaaaaaaaaaaaabbbbcdeeeeeeeeeeeeefgggggggghhhhhhhhhiiiiiiiiikllllllll]
Date:           [1111111222222233331233211111222222333211111112122222223111112223311122333]
Marks:           |      |      |      |      |      |      |      |      |      |      |
                a,1    a,2    a,3    b,3    e,2    e,3    g,1    h,2    i,1    i,3    l,3
Marks numbers:   0      1      2      3      4      5      6      7      8      9      10
```

Si la requête sur les données spécifie :

* `CounterID in ('a', 'h')`, le serveur lit les données dans les plages de marques `[0, 3)` et `[6, 8)`.
* `CounterID IN ('a', 'h') AND Date = 3`, le serveur lit les données dans les plages de marques `[1, 3)` et `[7, 8)`.
* `Date = 3`, le serveur lit les données dans la plage de marques `[1, 10]`.

Les exemples ci-dessus montrent qu&#39;il est toujours plus efficace d&#39;utiliser un index qu&#39;un parcours complet.

Un index sparse permet de lire des données supplémentaires. Lors de la lecture d&#39;une seule plage de la clé primaire, jusqu&#39;à `index_granularity * 2` lignes supplémentaires dans chaque bloc de données peuvent être lues.

Les index sparse permettent de travailler avec un très grand nombre de lignes de table, car dans la plupart des cas, ces index tiennent dans la RAM de l&#39;ordinateur.

ClickHouse n&#39;exige pas de clé primaire unique. Vous pouvez insérer plusieurs lignes avec la même clé primaire.

Vous pouvez utiliser des expressions de type `Nullable` dans les clauses `PRIMARY KEY` et `ORDER BY`, mais cela est fortement déconseillé. Pour autoriser cette fonctionnalité, activez le paramètre [allow&#95;nullable&#95;key](/fr/operations/settings/merge-tree-settings/#allow_nullable_key). Le principe [NULLS&#95;LAST](/fr/sql-reference/statements/select/order-by.md/#sorting-of-special-values) s&#39;applique aux valeurs `NULL` dans la clause `ORDER BY`.

<div id="selecting-a-primary-key">
  ### Sélection d’une clé primaire
</div>

Le nombre de colonnes dans la clé primaire n’est pas explicitement limité. Selon la structure des données, vous pouvez inclure plus ou moins de colonnes dans la clé primaire. Cela peut :

* Améliorer les performances d’un index.

  Si la clé primaire est `(a, b)`, l’ajout d’une autre colonne `c` améliorera les performances si les conditions suivantes sont remplies :

  * Il existe des requêtes avec une condition sur la colonne `c`.
  * De longues plages de données (plusieurs fois plus longues que `index_granularity`) avec des valeurs identiques pour `(a, b)` sont fréquentes. Autrement dit, l’ajout d’une colonne supplémentaire permet d’ignorer des plages de données assez longues.

* Améliorer la compression des données.

  ClickHouse trie les données selon la clé primaire ; ainsi, plus la cohérence est élevée, meilleure est la compression.

* Fournir une logique supplémentaire lors de la fusion des parties de données dans les moteurs [CollapsingMergeTree](/fr/engines/table-engines/mergetree-family/collapsingmergetree) et [SummingMergeTree](/fr/engines/table-engines/mergetree-family/summingmergetree.md).

  Dans ce cas, il est judicieux de spécifier une *clé de tri* différente de la clé primaire.

Une clé primaire longue aura un impact négatif sur les performances d’insertion et la consommation de mémoire, mais les colonnes supplémentaires dans la clé primaire n’affectent pas les performances de ClickHouse lors des requêtes `SELECT`.

Vous pouvez créer une table sans clé primaire en utilisant la syntaxe `ORDER BY tuple()`. Dans ce cas, ClickHouse stocke les données dans l’ordre d’insertion. Si vous souhaitez conserver l’ordre des données lors de l’insertion via des requêtes `INSERT ... SELECT`, définissez [max&#95;insert&#95;threads = 1](/fr/operations/settings/settings#max_insert_threads).

Pour sélectionner les données dans leur ordre initial, utilisez des requêtes `SELECT` [monothread](/fr/operations/settings/settings.md/#max_threads).

<div id="choosing-a-primary-key-that-differs-from-the-sorting-key">
  ### Choisir une clé primaire différente de la clé de tri
</div>

Il est possible de spécifier une clé primaire (une expression dont les valeurs sont écrites dans le fichier d&#39;index pour chaque repère) différente de la clé de tri (une expression utilisée pour trier les lignes dans les parties de données). Dans ce cas, le tuple d&#39;expressions de la clé primaire doit être un préfixe du tuple d&#39;expressions de la clé de tri.

Cette fonctionnalité est utile avec les moteurs de table [SummingMergeTree](/fr/engines/table-engines/mergetree-family/summingmergetree.md) et
[AggregatingMergeTree](/fr/engines/table-engines/mergetree-family/aggregatingmergetree.md). Dans le cas d&#39;usage le plus courant de ces moteurs, la table comporte deux types de colonnes : les *dimensions* et les *mesures*. Les requêtes typiques agrègent les valeurs des colonnes de mesure avec un `GROUP BY` arbitraire et un filtrage sur les dimensions. Comme SummingMergeTree et AggregatingMergeTree agrègent les lignes ayant la même valeur de clé de tri, il est naturel d&#39;y ajouter toutes les dimensions. Par conséquent, l&#39;expression de clé se compose d&#39;une longue liste de colonnes, et cette liste doit être mise à jour fréquemment à mesure que de nouvelles dimensions sont ajoutées.

Dans ce cas, il est judicieux de ne conserver dans la clé primaire que quelques colonnes qui permettront des scans de plage efficaces, et d&#39;ajouter les autres colonnes de dimension au tuple de la clé de tri.

L&#39;[ALTER](/fr/sql-reference/statements/alter/index.md) de la clé de tri est une opération légère, car lorsqu&#39;une nouvelle colonne est ajoutée simultanément à la table et à la clé de tri, les parties de données existantes n&#39;ont pas besoin d&#39;être modifiées. Puisque l&#39;ancienne clé de tri est un préfixe de la nouvelle et qu&#39;il n&#39;y a pas de données dans la colonne nouvellement ajoutée, les données sont triées à la fois selon l&#39;ancienne et la nouvelle clé de tri au moment de la modification de la table.

<div id="use-of-indexes-and-partitions-in-queries">
  ### Utilisation des index et des partitions dans les requêtes
</div>

Pour les requêtes `SELECT`, ClickHouse détermine si un index peut être utilisé. Un index peut être utilisé si la clause `WHERE/PREWHERE` contient une expression (soit comme l’un des éléments de la conjonction, soit dans son intégralité) correspondant à une opération de comparaison d’égalité ou d’inégalité, ou si elle contient `IN` ou `LIKE` avec un préfixe fixe sur des colonnes ou des expressions faisant partie de la clé primaire ou de la clé de partitionnement, ou sur certaines fonctions partiellement répétitives de ces colonnes, ou encore sur des relations logiques entre ces expressions.

Il est donc possible d’exécuter rapidement des requêtes sur une ou plusieurs plages de la clé primaire. Dans cet exemple, les requêtes seront rapides si elles portent sur une balise de suivi spécifique, sur une balise spécifique et une plage de dates, sur une balise spécifique et une date, sur plusieurs balises avec une plage de dates, etc.

Examinons le moteur configuré comme suit :

```sql
ENGINE MergeTree()
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate)
SETTINGS index_granularity=8192
```

Dans ce cas, pour les requêtes :

```sql
SELECT count() FROM table
WHERE EventDate = toDate(now())
AND CounterID = 34

SELECT count() FROM table
WHERE EventDate = toDate(now())
AND (CounterID = 34 OR CounterID = 42)

SELECT count() FROM table
WHERE ((EventDate >= toDate('2014-01-01')
AND EventDate <= toDate('2014-01-31')) OR EventDate = toDate('2014-05-01'))
AND CounterID IN (101500, 731962, 160656)
AND (CounterID = 101500 OR EventDate != toDate('2014-05-01'))
```

ClickHouse utilisera l’index de clé primaire pour écarter les données non pertinentes et la clé de partitionnement mensuel pour exclure les partitions situées hors des plages de dates pertinentes.

Les requêtes ci-dessus montrent que l’index est utilisé même pour des expressions complexes. La lecture de la table est organisée de sorte que l’utilisation de l’index ne peut pas être plus lente qu’un parcours complet.

Dans l’exemple ci-dessous, l’index ne peut pas être utilisé.

```sql
SELECT count() FROM table WHERE CounterID = 34 OR URL LIKE '%upyachka%'
```

Pour vérifier si ClickHouse peut utiliser l’index lors de l’exécution d’une requête, utilisez les paramètres [force&#95;index&#95;by&#95;date](/fr/operations/settings/settings.md/#force_index_by_date) et [force&#95;primary&#95;key](/fr/operations/settings/settings#force_primary_key).

La clé de partitionnement par mois permet de ne lire que les blocs de données qui contiennent des dates dans l’intervalle voulu. Dans ce cas, le bloc de données peut contenir des données correspondant à de nombreuses dates (jusqu’à un mois entier). À l’intérieur d’un bloc, les données sont triées selon la clé primaire, qui peut ne pas contenir la date comme première colonne. De ce fait, l’utilisation d’une requête comportant uniquement une condition sur la date, sans préciser le préfixe de la clé primaire, entraînera la lecture d’un volume de données plus important que dans le cas d’une seule date.

<div id="use-of-index-for-deterministic-expressions-in-primary-keys">
  ### Utilisation de l’index pour les expressions déterministes dans la clé primaire
</div>

La clé primaire peut contenir des expressions, et pas seulement des noms de colonnes. Ces expressions ne se limitent pas à de simples chaînes de fonctions : elles peuvent prendre la forme d’arbres d’expressions arbitraires (par exemple, des fonctions imbriquées et des expressions composées), à condition d’être déterministes.

Une expression est **déterministe** si elle renvoie toujours le même résultat pour les mêmes valeurs d’entrée (par exemple : `length()`, `toDate()`, `lower()`, `left()`, `cityHash64()`, `toUUID()` ; contrairement à `now()` ou `rand()`). Si la clé primaire contient des expressions déterministes, ClickHouse peut les appliquer aux valeurs constantes de la requête et utiliser le résultat pour construire des conditions sur l’index de la clé primaire. Cela permet d’ignorer des données pour des prédicats tels que `=`, `IN` et `has`.

Un cas d’utilisation courant consiste à garder une clé primaire compacte (par ex. stocker un hash au lieu d’une longue `String`), tout en permettant aux prédicats sur la colonne d’origine d’utiliser l’index.

Exemple de clé primaire déterministe (mais non injective) :

```sql
ENGINE = MergeTree()
ORDER BY length(user_id)
```

Exemples de prédicats pouvant utiliser l’index :

```sql
SELECT * FROM table WHERE user_id = 'alice';
SELECT * FROM table WHERE user_id IN ('alice', 'bob');
SELECT * FROM table WHERE has(['alice', 'bob'], user_id);
```

Dans ces cas, ClickHouse calcule `length('alice')` (ainsi que les autres constantes) une seule fois et utilise les valeurs de longueur pour restreindre les intervalles dans l’index de la clé primaire. Comme la longueur d’une chaîne **n’est pas injective**, différentes chaînes `user_id` peuvent avoir la même longueur, de sorte que l’index peut lire des granules supplémentaires (faux positifs). Le résultat reste correct, car le prédicat d’origine (`user_id = ...`, `IN`, etc.) est toujours appliqué après la lecture.

Si l’expression déterministe est également **injective** (des entrées différentes ne peuvent pas produire la même sortie pour les types d’argument utilisés), ClickHouse peut en outre utiliser efficacement l’index pour les formes avec négation : `!=`, `NOT IN` et `NOT has(...)`. Par exemple, `reverse(p)` et `hex(p)` sont injectives pour `String`.

Exemple de clé primaire injective :

```sql
ENGINE = MergeTree()
ORDER BY hex(p)
```

Des expressions injectives plus complexes sont également prises en charge, par exemple :

```sql
ENGINE = MergeTree()
ORDER BY reverse(tuple(reverse(p), hex(p)))
```

Exemples de prédicats pouvant utiliser l’index :

```sql
SELECT * FROM table WHERE p != 'abc';
SELECT * FROM table WHERE p NOT IN ('abc', '12345');
SELECT * FROM table WHERE NOT has(['abc', '12345'], p);
```

<div id="use-of-index-for-partially-monotonic-primary-keys">
  ### Utilisation de l&#39;index pour les clés primaires partiellement monotones
</div>

Prenons par exemple les jours du mois. Ils forment une [séquence monotone](https://en.wikipedia.org/wiki/Monotonic_function) sur un mois, mais ne le sont plus sur des périodes plus longues. Il s&#39;agit d&#39;une séquence partiellement monotone. Si un utilisateur crée une table avec une clé primaire partiellement monotone, ClickHouse crée un index sparse comme d&#39;habitude. Lorsqu&#39;un utilisateur lit des données dans ce type de table, ClickHouse analyse les conditions de la requête. Si l&#39;utilisateur souhaite obtenir des données entre deux marques de l&#39;index et que ces deux marques se trouvent dans le même mois, ClickHouse peut utiliser l&#39;index dans ce cas précis, car il peut calculer la distance entre les paramètres de la requête et les marques de l&#39;index.

ClickHouse ne peut pas utiliser l&#39;index si les valeurs de la clé primaire dans la plage des paramètres de la requête ne forment pas une séquence monotone. Dans ce cas, ClickHouse utilise la méthode du parcours complet.

ClickHouse applique cette logique non seulement aux séquences de jours du mois, mais aussi à toute clé primaire représentant une séquence partiellement monotone.

<div id="table_engine-mergetree-data_skipping-indexes">
  ### Index de saut de données
</div>

La déclaration de l’index figure dans la section des colonnes de la requête `CREATE`.

```sql
INDEX index_name expr TYPE type(...) [GRANULARITY granularity_value]
```

Pour les tables de la famille `*MergeTree`, il est possible de spécifier des indices de saut de données.

Ces indices agrègent certaines informations sur l&#39;expression spécifiée pour des blocks composés de `granularity_value` granules (la taille d&#39;une granule est définie à l&#39;aide du paramètre `index_granularity` dans le moteur de table). Ces agrégats sont ensuite utilisés dans les requêtes `SELECT` pour réduire la quantité de données à lire depuis le disque en ignorant les gros blocks de données pour lesquels la condition de la requête `where` ne peut pas être satisfaite.

La clause `GRANULARITY` peut être omise ; la valeur par défaut de `granularity_value` est 1.

**Exemple**

```sql
CREATE TABLE table_name
(
    u64 UInt64,
    i32 Int32,
    s String,
    ...
    INDEX idx1 u64 TYPE bloom_filter GRANULARITY 3,
    INDEX idx2 u64 * i32 TYPE minmax GRANULARITY 3,
    INDEX idx3 u64 * length(s) TYPE set(1000) GRANULARITY 4
) ENGINE = MergeTree()
...
```

Les index de l’exemple peuvent être utilisés par ClickHouse pour réduire la quantité de données à lire depuis le disque dans les requêtes suivantes :

```sql
SELECT count() FROM table WHERE u64 == 10;
SELECT count() FROM table WHERE u64 * i32 >= 1234
SELECT count() FROM table WHERE u64 * length(s) == 1234
```

Les index de saut de données peuvent également être créés sur des colonnes composées :

```sql
-- on columns of type Map:
INDEX map_key_index mapKeys(map_column) TYPE bloom_filter
INDEX map_value_index mapValues(map_column) TYPE bloom_filter

-- on columns of type JSON:
INDEX json_paths_index JSONAllPaths(json_column) TYPE bloom_filter

-- on columns of type Tuple:
INDEX tuple_1_index tuple_column.1 TYPE bloom_filter
INDEX tuple_2_index tuple_column.2 TYPE bloom_filter

-- on columns of type Nested:
INDEX nested_1_index col.nested_col1 TYPE bloom_filter
INDEX nested_2_index col.nested_col2 TYPE bloom_filter
```

<div id="skip-index-types">
  ### Types d’index de saut
</div>

Le moteur de table `MergeTree` prend en charge les types d’index de saut suivants.
Pour en savoir plus sur l’utilisation des index de saut pour optimiser les performances,
consultez [« Comprendre les index de saut de données dans ClickHouse »](/fr/optimize/skipping-indexes).

* index [`MinMax`](#minmax)
* index [`Set`](#set)
* index [`bloom_filter`](#bloom-filter)
* index [`ngrambf_v1`](#n-gram-bloom-filter) *(Obsolète)*
* index [`tokenbf_v1`](#token-bloom-filter) *(Obsolète)*
* index [`text`](#text)
* index [`vector_similarity`](#vector-similarity)

<div id="minmax">
  #### Index de saut MinMax
</div>

Pour chaque granule d’index, les valeurs minimale et maximale d’une expression sont stockées.
(Si l’expression est de type `tuple`, les valeurs minimale et maximale sont stockées pour chaque élément du tuple.)

```text title="Syntax"
minmax
```

<div id="set">
  #### Set
</div>

Pour chaque granule d’index, jusqu’à `max_rows` valeurs uniques de l’expression spécifiée sont stockées.
`max_rows = 0` signifie &quot;stocker toutes les valeurs uniques&quot;.

```text title="Syntax"
set(max_rows)
```

<div id="bloom-filter">
  #### Filtre de Bloom
</div>

Stocke un [filtre de Bloom](https://en.wikipedia.org/wiki/Bloom_filter) pour les colonnes spécifiées dans chaque granule d’index.

```text title="Syntax"
bloom_filter([false_positive_rate])
```

Le paramètre `false_positive_rate` peut prendre une valeur comprise entre 0 et 1 (par défaut : `0.025`) et spécifie la probabilité de générer un résultat positif (ce qui augmente la quantité de données à lire).

Les types de données suivants sont pris en charge :

* `(U)Int*`
* `Float*`
* `Enum`
* `Date`
* `DateTime`
* `String`
* `FixedString`
* `Array`
* `LowCardinality`
* `Nullable`
* `UUID`
* `Map`

:::note Type de données Map : création d&#39;index sur les clés ou les valeurs
Pour le type de données `Map`, le client peut indiquer si l&#39;index doit être créé sur les clés ou sur les valeurs à l&#39;aide des fonctions [`mapKeys`](/fr/sql-reference/functions/tuple-map-functions.md/#mapKeys) ou [`mapValues`](/fr/sql-reference/functions/tuple-map-functions.md/#mapValues).
:::

:::note Type de données JSON : indexation des chemins JSON
Pour le type de données [`JSON`](/fr/sql-reference/data-types/newjson), un index bloom filter peut être créé sur l&#39;ensemble des chemins à l&#39;aide de la fonction [`JSONAllPaths`](/fr/sql-reference/functions/json-functions#JSONAllPaths). Cela permet d&#39;ignorer les granules dans lesquelles le chemin JSON interrogé est absent. Voir [Data skipping indexes for JSON](/fr/sql-reference/data-types/newjson#data-skipping-indexes-for-json) pour plus de détails.
:::

<div id="n-gram-bloom-filter">
  #### Filtre de Bloom N-gram *(Obsolète)*
</div>

:::note
Avec la disponibilité générale (GA) de l’index `text` à partir de ClickHouse 26.2, l’index `ngrambf_v1` n’est plus recommandé pour la recherche en texte intégral.

Consultez la page [&quot;Recherche en texte intégral avec des index textuels&quot;](./textindexes.md) pour plus de détails.
:::

Chaque granule d’index stocke un [filtre de Bloom](https://en.wikipedia.org/wiki/Bloom_filter) pour les [n-grammes](https://en.wikipedia.org/wiki/N-gram) des colonnes spécifiées.

```text title="Syntax"
ngrambf_v1(n, size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)
```

| Paramètre                       | Description                                                                                                                               |
| ------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `n`                             | taille du ngram                                                                                                                           |
| `size_of_bloom_filter_in_bytes` | Taille du filtre de Bloom en octets. Vous pouvez utiliser ici une valeur élevée, par exemple `256` ou `512`, car elle se compresse bien). |
| `number_of_hash_functions`      | Nombre de fonctions de hachage utilisées dans le filtre de Bloom.                                                                         |
| `random_seed`                   | Graine des fonctions de hachage du filtre de Bloom.                                                                                       |

Cet index fonctionne uniquement avec les types de données suivants :

* [`String`](/fr/sql-reference/data-types/string.md)
* [`FixedString`](/fr/sql-reference/data-types/fixedstring.md)
* [`Map`](/fr/sql-reference/data-types/map.md)

Pour estimer les paramètres de `ngrambf_v1`, vous pouvez utiliser les [fonctions définies par l&#39;utilisateur (UDFs)](/fr/sql-reference/statements/create/function.md).

```sql title="UDFs for ngrambf_v1"
CREATE FUNCTION bfEstimateFunctions [ON CLUSTER cluster]
AS
(total_number_of_all_grams, size_of_bloom_filter_in_bits) -> round((size_of_bloom_filter_in_bits / total_number_of_all_grams) * log(2));

CREATE FUNCTION bfEstimateBmSize [ON CLUSTER cluster]
AS
(total_number_of_all_grams,  probability_of_false_positives) -> ceil((total_number_of_all_grams * log(probability_of_false_positives)) / log(1 / pow(2, log(2))));

CREATE FUNCTION bfEstimateFalsePositive [ON CLUSTER cluster]
AS
(total_number_of_all_grams, number_of_hash_functions, size_of_bloom_filter_in_bytes) -> pow(1 - exp(-number_of_hash_functions/ (size_of_bloom_filter_in_bytes / total_number_of_all_grams)), number_of_hash_functions);

CREATE FUNCTION bfEstimateGramNumber [ON CLUSTER cluster]
AS
(number_of_hash_functions, probability_of_false_positives, size_of_bloom_filter_in_bytes) -> ceil(size_of_bloom_filter_in_bytes / (-number_of_hash_functions / log(1 - exp(log(probability_of_false_positives) / number_of_hash_functions))))
```

Pour utiliser ces fonctions, vous devez spécifier au moins deux paramètres :

* `total_number_of_all_grams`
* `probability_of_false_positives`

Par exemple, s’il y a `4300` ngrams dans le granule et que vous souhaitez que les faux positifs soient inférieurs à `0.0001`,
les autres paramètres peuvent alors être estimés en exécutant les requêtes suivantes :

```sql
--- estimate number of bits in the filter
SELECT bfEstimateBmSize(4300, 0.0001) / 8 AS size_of_bloom_filter_in_bytes;

┌─size_of_bloom_filter_in_bytes─┐
│                         10304 │
└───────────────────────────────┘

--- estimate number of hash functions
SELECT bfEstimateFunctions(4300, bfEstimateBmSize(4300, 0.0001)) as number_of_hash_functions

┌─number_of_hash_functions─┐
│                       13 │
└──────────────────────────┘
```

Bien sûr, vous pouvez également utiliser ces fonctions pour estimer des paramètres pour d’autres conditions.
Les fonctions ci-dessus se réfèrent au calculateur de bloom filter disponible [ici](https://hur.st/bloomfilter).

<div id="token-bloom-filter">
  #### Filtre de Bloom de tokens
</div>

:::note
Avec la disponibilité générale (GA) de l’index `text` à partir de ClickHouse 26.2, l’index `tokenbf_v1` n’est plus recommandé pour la recherche en texte intégral.

Voir la page [&quot;Recherche en texte intégral avec des index textuels&quot;](./textindexes.md) pour plus de détails.
:::

```text title="Syntax"
tokenbf_v1(size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)
```

<div id="sparse-grams-bloom-filter">
  #### Filtre de Bloom sparse grams
</div>

Le filtre de Bloom sparse grams est similaire à `ngrambf_v1`, mais utilise des [tokens sparse grams](/fr/sql-reference/functions/string-functions.md/#sparseGrams) au lieu de ngrams.

```text title="Syntax"
sparse_grams(min_ngram_length, max_ngram_length, min_cutoff_length, size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)
```

<div id="text">
  ### Index `text`
</div>

Crée un index inversé sur des données textuelles tokenisées, permettant une recherche en texte intégral efficace et déterministe. Voir [ici](textindexes.md) pour plus de détails.

<div id="vector-similarity">
  #### Similarité vectorielle
</div>

Prend en charge la recherche approximative des plus proches voisins ; voir [ici](annindexes.md) pour plus de détails.

<div id="functions-support">
  ### Prise en charge des fonctions
</div>

Les conditions de la clause `WHERE` contiennent des appels à des fonctions qui opèrent sur des colonnes. Si la colonne fait partie d’un index, ClickHouse essaie d’utiliser cet index lors de l’exécution de ces fonctions. ClickHouse prend en charge différents sous-ensembles de fonctions pour l’utilisation des index.

Les index de type `set` peuvent être utilisés par toutes les fonctions. Les autres types d’index sont pris en charge comme suit :

| Fonction (opérateur) / index                                                                                              | clé primaire | minmax | ngrambf&#95;v1 | tokenbf&#95;v1 | bloom&#95;filter | sparse&#95;grams | texte |
| ------------------------------------------------------------------------------------------------------------------------- | ------------ | ------ | -------------- | -------------- | ---------------- | ---------------- | ----- |
| [equals (=, ==)](/fr/sql-reference/functions/comparison-functions.md/#equals)                                                | ✔            | ✔      | ✔              | ✔              | ✔                | ✔                | ✔     |
| [notEquals(!=, &lt;&gt;)](/fr/sql-reference/functions/comparison-functions.md/#notEquals)                                    | ✔            | ✔      | ✔              | ✔              | ✔                | ✔                | ✗     |
| [like](/fr/sql-reference/functions/string-search-functions.md/#like)                                                         | ✔            | ✔      | ✔              | ✔              | ✗                | ✔                | ✔     |
| [notLike](/fr/sql-reference/functions/string-search-functions.md/#notLike)                                                   | ✔            | ✔      | ✔              | ✔              | ✗                | ✔                | ✗     |
| [match](/fr/sql-reference/functions/string-search-functions.md/#match)                                                       | ✗            | ✗      | ✔              | ✔              | ✗                | ✔                | ✔     |
| [startsWith](/fr/sql-reference/functions/string-functions.md/#startsWith)                                                    | ✔            | ✔      | ✔              | ✔              | ✗                | ✔                | ✔     |
| [endsWith](/fr/sql-reference/functions/string-functions.md/#endsWith)                                                        | ✗            | ✗      | ✔              | ✔              | ✗                | ✔                | ✔     |
| [multiSearchAny](/fr/sql-reference/functions/string-search-functions.md/#multiSearchAny)                                     | ✗            | ✗      | ✔              | ✗              | ✗                | ✗                | ✔     |
| [multiSearchAnyUTF8](/fr/sql-reference/functions/string-search-functions.md/#multiSearchAnyUTF8)                             | ✗            | ✗      | ✗              | ✗              | ✗                | ✗                | ✔     |
| [multiMatchAny](/fr/sql-reference/functions/string-search-functions.md/#multiMatchAny)                                       | ✗            | ✗      | ✗              | ✗              | ✗                | ✗                | ✔     |
| [in](/fr/sql-reference/functions/in-functions)                                                                               | ✔            | ✔      | ✔              | ✔              | ✔                | ✔                | ✔     |
| [notIn](/fr/sql-reference/functions/in-functions)                                                                            | ✔            | ✔      | ✔              | ✔              | ✔                | ✔                | ✗     |
| [less (`<`)](/fr/sql-reference/functions/comparison-functions.md/#less)                                                      | ✔            | ✔      | ✗              | ✗              | ✗                | ✗                | ✗     |
| [greater (`>`)](/fr/sql-reference/functions/comparison-functions.md/#greater)                                                | ✔            | ✔      | ✗              | ✗              | ✗                | ✗                | ✗     |
| [lessOrEquals (`<=`)](/fr/sql-reference/functions/comparison-functions.md/#lessOrEquals)                                     | ✔            | ✔      | ✗              | ✗              | ✗                | ✗                | ✗     |
| [greaterOrEquals (`>=`)](/fr/sql-reference/functions/comparison-functions.md/#greaterOrEquals)                               | ✔            | ✔      | ✗              | ✗              | ✗                | ✗                | ✗     |
| [empty](/fr/sql-reference/functions/array-functions/#empty)                                                                  | ✔            | ✔      | ✗              | ✗              | ✗                | ✗                | ✗     |
| [notEmpty](/fr/sql-reference/functions/array-functions/#notEmpty)                                                            | ✗            | ✔      | ✗              | ✗              | ✗                | ✔                | ✗     |
| [has](/fr/sql-reference/functions/array-functions#has)                                                                       | ✔            | ✔      | ✔              | ✔              | ✔                | ✔                | ✔     |
| [hasAny](/fr/sql-reference/functions/array-functions#hasAny)                                                                 | ✗            | ✗      | ✔              | ✔              | ✔                | ✔                | ✗     |
| [hasAll](/fr/sql-reference/functions/array-functions#hasAll)                                                                 | ✗            | ✗      | ✔              | ✔              | ✔                | ✔                | ✗     |
| [hasToken](/fr/sql-reference/functions/string-search-functions.md/#hasToken)                                                 | ✗            | ✗      | ✗              | ✔              | ✗                | ✗                | ✔     |
| [hasTokenOrNull](/fr/sql-reference/functions/string-search-functions.md/#hasTokenOrNull)                                     | ✗            | ✗      | ✗              | ✔              | ✗                | ✗                | ✔     |
| [hasTokenCaseInsensitive (`*`)](/fr/sql-reference/functions/string-search-functions.md/#hasTokenCaseInsensitive)             | ✗            | ✗      | ✗              | ✔              | ✗                | ✗                | ✗     |
| [hasTokenCaseInsensitiveOrNull (`*`)](/fr/sql-reference/functions/string-search-functions.md/#hasTokenCaseInsensitiveOrNull) | ✗            | ✗      | ✗              | ✔              | ✗                | ✗                | ✗     |
| [hasAnyTokens](/fr/sql-reference/functions/string-search-functions.md/#hasAnyTokens)                                         | ✗            | ✗      | ✗              | ✗              | ✗                | ✗                | ✔     |
| [hasAllTokens](/fr/sql-reference/functions/string-search-functions.md/#hasAllTokens)                                         | ✗            | ✗      | ✗              | ✗              | ✗                | ✗                | ✔     |
| [pointInPolygon](/fr/sql-reference/functions/geo/coordinates.md#pointinpolygon)                                              | ✔            | ✔      | ✗              | ✗              | ✗                | ✗                | ✗     |
| [mapContains (mapContainsKey)](/fr/sql-reference/functions/tuple-map-functions#mapContainsKey)                               | ✗            | ✗      | ✗              | ✗              | ✗                | ✗                | ✔     |
| [mapContainsKeyLike](/fr/sql-reference/functions/tuple-map-functions#mapContainsKeyLike)                                     | ✗            | ✗      | ✗              | ✗              | ✗                | ✗                | ✔     |
| [mapContainsValue](/fr/sql-reference/functions/tuple-map-functions#mapContainsValue)                                         | ✗            | ✗      | ✗              | ✗              | ✗                | ✗                | ✔     |
| [mapContainsValueLike](/fr/sql-reference/functions/tuple-map-functions#mapContainsValueLike)                                 | ✗            | ✗      | ✗              | ✗              | ✗                | ✗                | ✔     |

Les fonctions avec un argument constant inférieur à la taille du ngram ne peuvent pas être utilisées par `ngrambf_v1` pour l’optimisation des requêtes.

(*) Pour que `hasTokenCaseInsensitive` et `hasTokenCaseInsensitiveOrNull` soient efficaces, l’index `tokenbf_v1` doit être créé sur des données en minuscules, par exemple `INDEX idx (lower(str_col)) TYPE tokenbf_v1(512, 3, 0)`.

:::note
Les filtres de Bloom peuvent produire des faux positifs. Les index `ngrambf_v1`, `tokenbf_v1`, `sparse_grams` et `bloom_filter` ne peuvent donc pas être utilisés pour optimiser les requêtes pour lesquelles le résultat d’une fonction est censé être false.

Par exemple :

* Peuvent être optimisées :
  * `s LIKE '%test%'`
  * `NOT s NOT LIKE '%test%'`
  * `s = 1`
  * `NOT s != 1`
  * `startsWith(s, 'test')`
* Ne peuvent pas être optimisées :
  * `NOT s LIKE '%test%'`
  * `s NOT LIKE '%test%'`
  * `NOT s = 1`
  * `s != 1`
  * `NOT startsWith(s, 'test')`
    :::

<div id="projections">
  ## Projections
</div>

Les projections sont comparables aux [vues matérialisées](/fr/sql-reference/statements/create/view), mais elles sont définies au niveau des parts. Elles offrent des garanties de cohérence et sont utilisées automatiquement dans les requêtes.

:::note
Lorsque vous implémentez des projections, vous devez également prendre en compte le paramètre [force&#95;optimize&#95;projection](/fr/operations/settings/settings#force_optimize_projection).
:::

Les projections ne sont pas prises en charge dans les instructions `SELECT` utilisant le modificateur [FINAL](/fr/sql-reference/statements/select/from#final-modifier).

<div id="projection-query">
  ### Requête de projection
</div>

Une requête de projection définit une projection. Elle sélectionne implicitement des données dans la table parente.
**Syntaxe**

```sql
SELECT <column list expr> [GROUP BY] <group keys expr> [ORDER BY] <expr>
```

Les projections peuvent être modifiées ou supprimées avec l’instruction [ALTER](/fr/sql-reference/statements/alter/projection.md).

<div id="projection-index">
  ### Index de projection
</div>

Les index de projection étendent le sous-système de projection en fournissant un moyen léger et explicite de définir des index au niveau de la projection.
Vu de l&#39;extérieur, un index de projection reste une projection, mais avec une syntaxe simplifiée et une intention plus claire : il définit une expression dédiée au filtrage, plutôt qu&#39;au service de données matérialisées.
En interne, un index de projection ne matérialise pas la table d&#39;origine dans un ordre de lignes permuté comme le fait une projection classique.
À la place, la permutation est stockée sous la forme d&#39;une colonne de permutation numérique `_part_offset`, c.-à-d. `SELECT _part_offset ORDER BY <index_expr>`.

<div id="projection-index-syntax">
  #### Syntaxe
</div>

```sql
PROJECTION <name> INDEX <index_expr> TYPE <index_type>
```

Exemple :

```sql
CREATE TABLE example
(
    id UInt64,
    region String,
    user_id UInt32,
    PROJECTION region_proj INDEX region TYPE basic,
    PROJECTION uid_proj INDEX user_id TYPE basic
)
ENGINE = MergeTree
ORDER BY id;
```

<div id="projection-index-types">
  #### Types d’index
</div>

Actuellement pris en charge :

* **basic** : équivalent à un index MergeTree classique sur l’expression.

Le système permet d’ajouter d’autres types d’index à l’avenir.

<div id="projection-storage">
  ### Stockage des projections
</div>

Les projections sont stockées dans le répertoire de la part. C&#39;est similaire à un index, mais avec un sous-répertoire qui stocke la part d&#39;une table `MergeTree` anonyme. Cette table est déduite de la requête de définition de la projection. S&#39;il existe une clause `GROUP BY`, le moteur de stockage sous-jacent devient [AggregatingMergeTree](aggregatingmergetree.md), et toutes les fonctions d&#39;agrégation sont converties en `AggregateFunction`. S&#39;il existe une clause `ORDER BY`, la table `MergeTree` l&#39;utilise comme expression de clé primaire. Pendant le merge process, la projection part est fusionnée via la routine de fusion de son stockage. Le checksum de la part de la table parente est combiné avec celui de la projection part. Les autres opérations de maintenance sont similaires à celles des skip indices.

<div id="projection-query-analysis">
  ### Analyse de la requête
</div>

1. Vérifiez si la projection peut être utilisée pour répondre à la requête donnée, c’est-à-dire si elle produit le même résultat qu’une requête sur la table de base.
2. Sélectionnez la meilleure correspondance possible, celle qui nécessite la lecture du plus petit nombre de granules.
3. Le pipeline de la requête qui utilise des projections sera différent de celui qui utilise les parts d’origine. Si la projection est absente dans certaines parts, nous pouvons ajouter au pipeline une étape pour la « projeter » à la volée.

<div id="concurrent-data-access">
  ## Accès concurrent aux données
</div>

Pour l&#39;accès concurrent aux tables, nous utilisons le multiversionnage. En d&#39;autres termes, lorsqu&#39;une table est lue et mise à jour simultanément, les données sont lues à partir d&#39;un ensemble de parties à jour au moment de la requête. Il n&#39;y a pas de verrous de longue durée. Les insertions ne bloquent pas les opérations de lecture.

La lecture d&#39;une table est automatiquement parallélisée.

<div id="table_engine-mergetree-ttl">
  ## TTL pour les colonnes et les tables
</div>

Détermine la durée de vie des valeurs.

La clause `TTL` peut être définie pour l’ensemble de la table ainsi que pour chaque colonne individuellement. Le `TTL` au niveau de la table peut également définir la logique de déplacement automatique des données entre les disques et les volumes, ou de recompression des parties dont toutes les données ont expiré.

Les expressions doivent renvoyer un type de données [Date](/fr/sql-reference/data-types/date.md), [Date32](/fr/sql-reference/data-types/date32.md), [DateTime](/fr/sql-reference/data-types/datetime.md) ou [DateTime64](/fr/sql-reference/data-types/datetime64.md).

:::tip[Évitez les fonctions non déterministes dans les expressions TTL]
Le TTL est évalué lors des fusions en arrière-plan, et non au moment de l’insertion.
Les fonctions comme `rand()`, `now()`, ou `now64()` sont réévaluées à chaque fusion, ce qui entraîne un comportement de suppression imprévisible.
ClickHouse bloque les expressions sans aucune dépendance à une colonne, mais ne rejette pas actuellement les fonctions non déterministes combinées à une référence de colonne (par exemple `ts + rand()`). Pour obtenir des résultats prévisibles, les expressions TTL doivent être basées uniquement sur des valeurs déterministes dérivées des colonnes.
:::

**Syntaxe**

Définition de la durée de vie d’une colonne :

```sql
TTL time_column
TTL time_column + interval
```

Pour définir `interval`, utilisez les opérateurs d’[intervalle temporel](/fr/sql-reference/operators#operators-for-working-with-dates-and-times), par exemple :

```sql
TTL date_time + INTERVAL 1 MONTH
TTL date_time + INTERVAL 15 HOUR
```

<div id="mergetree-column-ttl">
  ### TTL de colonne
</div>

Lorsque les valeurs de la colonne expirent, ClickHouse les remplace par les valeurs par défaut du type de données de la colonne. Si toutes les valeurs de la colonne d&#39;une part de données expirent, ClickHouse supprime cette colonne de la part de données dans le système de fichiers.

La clause `TTL` ne peut pas être utilisée pour les colonnes de clé.

**Exemples**

<div id="creating-a-table-with-ttl">
  #### Création d’une table avec `TTL` :
</div>

```sql
CREATE TABLE tab
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

<div id="adding-ttl-to-a-column-of-an-existing-table">
  #### Ajouter un TTL à une colonne d’une table existante
</div>

```sql
ALTER TABLE tab
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 DAY;
```

<div id="altering-ttl-of-the-column">
  #### Modifier le TTL d’une colonne
</div>

```sql
ALTER TABLE tab
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 MONTH;
```

<div id="mergetree-table-ttl">
  ### TTL de la table
</div>

Une table peut comporter une expression de suppression des lignes expirées, ainsi que plusieurs expressions de déplacement automatique des parties entre les [disques ou volumes](#table_engine-mergetree-multiple-volumes). Lorsque des lignes de la table expirent, ClickHouse supprime toutes les lignes correspondantes. Pour le déplacement ou la recompression des parties, toutes les lignes d’une partie doivent répondre aux critères de l’expression `TTL`.

```sql
TTL expr
    [DELETE|RECOMPRESS codec_name1|TO DISK 'xxx'|TO VOLUME 'xxx'][, DELETE|RECOMPRESS codec_name2|TO DISK 'aaa'|TO VOLUME 'bbb'] ...
    [WHERE conditions]
    [GROUP BY key_expr [SET v1 = aggr_func(v1) [, v2 = aggr_func(v2) ...]] ]
```

Un type de règle `TTL` peut être indiqué après chaque expression TTL. Il détermine l&#39;action à effectuer une fois que l&#39;expression est satisfaite (atteint l&#39;heure actuelle) :

* `DELETE` - supprimer les lignes expirées (action par défaut) ;
* `RECOMPRESS codec_name` - recompresser la partie de données avec `codec_name` ;
* `TO DISK 'aaa'` - déplacer la partie vers le disk `aaa` ;
* `TO VOLUME 'bbb'` - déplacer la partie vers le volume `bbb` ;
* `GROUP BY` - agréger les lignes expirées.

L&#39;action `DELETE` peut être utilisée avec la clause `WHERE` pour supprimer uniquement certaines lignes expirées, selon une condition de filtrage :

```sql
TTL time_column + INTERVAL 1 MONTH DELETE WHERE column = 'value'
```

L&#39;expression `GROUP BY` doit être un préfixe de la clé primaire de la table.

Si une colonne ne fait pas partie de l&#39;expression `GROUP BY` et n&#39;est pas définie explicitement dans la clause `SET`, alors, dans la ligne de résultat, elle contient une valeur arbitraire provenant des lignes groupées (comme si la fonction d&#39;agrégation `any` lui était appliquée).

**Exemples**

<div id="creating-a-table-with-ttl">
  #### Création d’une table avec `TTL` :
</div>

```sql
CREATE TABLE tab
(
    d DateTime,
    a Int
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d
TTL d + INTERVAL 1 MONTH DELETE,
    d + INTERVAL 1 WEEK TO VOLUME 'aaa',
    d + INTERVAL 2 WEEK TO DISK 'bbb';
```

<div id="altering-ttl-of-the-table">
  #### Modification du `TTL` de la table :
</div>

```sql
ALTER TABLE tab
    MODIFY TTL d + INTERVAL 1 DAY;
```

Création d’une table dont les lignes expirent au bout d’un mois. Les lignes expirées dont les dates tombent un lundi sont supprimées :

```sql
CREATE TABLE table_with_where
(
    d DateTime,
    a Int
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d
TTL d + INTERVAL 1 MONTH DELETE WHERE toDayOfWeek(d) = 1;
```

<div id="creating-a-table-where-expired-rows-are-recompressed">
  #### Création d’une table dans laquelle les lignes expirées sont recompressées :
</div>

```sql
CREATE TABLE table_for_recompression
(
    d DateTime,
    key UInt64,
    value String
) ENGINE MergeTree()
ORDER BY tuple()
PARTITION BY key
TTL d + INTERVAL 1 MONTH RECOMPRESS CODEC(ZSTD(17)), d + INTERVAL 1 YEAR RECOMPRESS CODEC(LZ4HC(10))
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
```

Création d’une table où les lignes expirées sont agrégées. Dans les lignes de résultat, `x` contient la valeur maximale des lignes regroupées, `y` — la valeur minimale, et `d` — une valeur quelconque issue des lignes regroupées.

```sql
CREATE TABLE table_for_aggregation
(
    d DateTime,
    k1 Int,
    k2 Int,
    x Int,
    y Int
)
ENGINE = MergeTree
ORDER BY (k1, k2)
TTL d + INTERVAL 1 MONTH GROUP BY k1, k2 SET x = max(x), y = min(y);
```

<div id="mergetree-removing-expired-data">
  ### Suppression des données expirées
</div>

Les données dont le `TTL` a expiré sont supprimées lorsque ClickHouse fusionne des parties de données.

Lorsque ClickHouse détecte que des données ont expiré, il effectue une fusion non planifiée. Pour contrôler la fréquence de ces fusions, vous pouvez définir `merge_with_ttl_timeout`. Si la valeur est trop faible, cela entraînera de nombreuses fusions non planifiées, susceptibles de consommer beaucoup de ressources.

Si vous exécutez la requête `SELECT` entre deux fusions, vous pouvez obtenir des données expirées. Pour l’éviter, utilisez la requête [OPTIMIZE](/fr/sql-reference/statements/optimize.md) avant `SELECT`.

**Voir aussi**

* paramètre [ttl&#95;only&#95;drop&#95;parts](/fr/operations/settings/merge-tree-settings#ttl_only_drop_parts)

<div id="disk-types">
  ## Types de disques
</div>

En plus des périphériques bloc locaux, ClickHouse prend en charge les types de stockage suivants :

* [`s3` pour S3 et MinIO](#table_engine-mergetree-s3)
* [`gcs` pour GCS](/fr/integrations/data-ingestion/gcs/index.md/#creating-a-disk)
* [`blob_storage_disk` pour Azure Blob Storage](/fr/operations/storing-data#azure-blob-storage)
* [`hdfs` pour HDFS](/fr/engines/table-engines/integrations/hdfs)
* [`web` pour un accès en lecture seule depuis le Web](/fr/operations/storing-data#web-storage)
* [`cache` pour la mise en cache locale](/fr/operations/storing-data#using-local-cache)
* [`s3_plain` pour les sauvegardes vers S3](/fr/operations/backup/disk)
* [`s3_plain_rewritable` pour les tables immuables non répliquées sur S3](/fr/operations/storing-data.md#s3-plain-rewritable-storage)

<div id="table_engine-mergetree-multiple-volumes">
  ## Utiliser plusieurs périphériques bloc pour stocker les données
</div>

<div id="introduction">
  ### Introduction
</div>

Les moteurs de table de la famille `MergeTree` peuvent stocker des données sur plusieurs périphériques bloc. Cela peut par exemple être utile lorsque les données d&#39;une table sont implicitement réparties entre données « chaudes » et « froides ». Les données les plus récentes sont régulièrement consultées, mais n&#39;occupent qu&#39;un faible espace. À l&#39;inverse, les données historiques à longue traîne sont rarement consultées. Si plusieurs disques sont disponibles, les données « chaudes » peuvent être placées sur des disques rapides (par exemple, des SSD NVMe ou en mémoire), tandis que les données « froides » peuvent être placées sur des disques relativement lents (par exemple, des HDD).

Cela s&#39;applique à tous les types de disques, y compris les disques S3 et les autres disques de stockage objet. Par exemple, vous pouvez répartir les données sur plusieurs buckets S3 au sein d&#39;un même volume, ou créer des politiques par niveaux qui déplacent les données des disques locaux vers S3. Voir [Utilisation des disques S3 avec plusieurs volumes](#s3-multiple-volumes) pour plus de détails.

Une part de données est la plus petite unité déplaçable pour les tables utilisant un moteur `MergeTree`. Les données appartenant à une même part sont stockées sur un seul disque. Les parts de données peuvent être déplacées entre les disques en arrière-plan (conformément aux paramètres utilisateur) ainsi qu&#39;au moyen des requêtes [ALTER](/fr/sql-reference/statements/alter/partition).

<div id="terms">
  ### Termes
</div>

* Disque — Périphérique bloc monté sur le système de fichiers.
* Disque par défaut — Disque sur lequel est stocké le chemin spécifié dans le paramètre serveur [path](/fr/operations/server-configuration-parameters/settings.md/#path).
* Volume — Ensemble ordonné de disques équivalents (similaire à [JBOD](https://en.wikipedia.org/wiki/Non-RAID_drive_architectures)).
* Politique de stockage — Ensemble de volumes et de règles de déplacement des données entre eux.

Les noms attribués aux entités décrites se trouvent dans les tables système [system.storage&#95;policies](/fr/operations/system-tables/storage_policies) et [system.disks](/fr/operations/system-tables/disks). Pour appliquer à une table l&#39;une des politiques de stockage configurées, utilisez le paramètre `storage_policy` des tables de la famille de moteurs `MergeTree`.

<div id="table_engine-mergetree-multiple-volumes_configure">
  ### Configuration
</div>

Les disques, les volumes et les politiques de stockage doivent être déclarés dans la balise `<storage_configuration>`, dans un fichier du répertoire `config.d`.

:::tip
Les disques peuvent également être déclarés dans la section `SETTINGS` d’une requête. Cela est utile
pour une analyse ponctuelle, afin d’attacher temporairement un disque hébergé, par exemple, à une URL.
Voir le [stockage dynamique](/fr/operations/storing-data#dynamic-configuration) pour plus de détails.
:::

Structure de la configuration :

```xml
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

Balises :

* `<disk_name_N>` — Nom du disque. Les noms doivent être différents pour tous les disques.
* `path` — chemin dans lequel le serveur stockera les données (dossiers `data` et `shadow`) ; il doit se terminer par &#39;/&#39;.
* `keep_free_space_bytes` — quantité d&#39;espace disque libre à réserver.

L&#39;ordre de définition des disques n&#39;a pas d&#39;importance.

Exemple de configuration des politiques de stockage :

```xml
<storage_configuration>
    ...
    <policies>
        <policy_name_1>
            <volumes>
                <volume_name_1>
                    <disk>disk_name_from_disks_configuration</disk>
                    <max_data_part_size_bytes>1073741824</max_data_part_size_bytes>
                    <load_balancing>round_robin</load_balancing>
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

Tags :

* `policy_name_N` — Nom de la politique de stockage. Les noms de politiques de stockage doivent être uniques.
* `volume_name_N` — Nom du volume. Les noms de volume doivent être uniques.
* `disk` — un disque au sein d’un volume.
* `max_data_part_size_bytes` — la taille maximale d’une part pouvant être stockée sur l’un des disques du volume. Si la taille estimée d’une part fusionnée dépasse `max_data_part_size_bytes`, cette part sera écrite sur le volume suivant. En pratique, cette fonctionnalité permet de conserver les parts nouvelles ou de petite taille sur un volume rapide (SSD), puis de les déplacer vers un volume lent (HDD) lorsqu’elles deviennent volumineuses. N’utilisez pas ce paramètre si votre politique de stockage ne comporte qu’un seul volume.
* `move_factor` — lorsque l’espace disponible passe en dessous de ce facteur, les données commencent automatiquement à être déplacées vers le volume suivant, s’il existe (par défaut, 0.1). ClickHouse trie les parts existantes par taille, de la plus grande à la plus petite (ordre décroissant), puis sélectionne des parts dont la taille cumulée est suffisante pour satisfaire la condition `move_factor`. Si la taille totale de toutes les parts est insuffisante, toutes les parts seront déplacées.
* `perform_ttl_move_on_insert` — Désactive le TTL move lors de l’INSERT d’une part de données. Par défaut (lorsque ce paramètre est activé), si l’on insère une part de données qui a déjà expiré selon la règle de TTL move, elle est immédiatement envoyée vers le volume/disque déclaré dans cette règle. Cela peut considérablement ralentir l’insert si le volume/disque de destination est lent (par exemple, S3). Si ce paramètre est désactivé, la part de données déjà expirée est écrite sur le volume par défaut, puis déplacée juste après vers le volume TTL.
* `load_balancing` - Stratégie d’équilibrage des disques : `round_robin` ou `least_used`.
* `least_used_ttl_ms` - Configure le délai d’expiration (en millisecondes) de mise à jour de l’espace disponible sur tous les disques (`0` - toujours mettre à jour, `-1` - ne jamais mettre à jour, valeur par défaut : `60000`). Notez que si le disque ne peut être utilisé que par ClickHouse et n’est pas sujet à un redimensionnement/rétrécissement à chaud du système de fichiers, vous pouvez utiliser `-1` ; dans tous les autres cas, cela n’est pas recommandé, car cela finira par entraîner une répartition incorrecte de l’espace.
* `prefer_not_to_merge` — Vous ne devez pas utiliser ce paramètre. Il désactive la fusion des parts de données sur ce volume (ce qui est nuisible et dégrade les performances). Lorsque ce paramètre est activé (ne le faites pas), la fusion des données sur ce volume n’est pas autorisée (et c’est une mauvaise idée). Cela permet (mais vous n’en avez pas besoin) de contrôler (si vous voulez contrôler cela, vous vous trompez) la manière dont ClickHouse fonctionne avec des disques lents (mais ClickHouse sait mieux faire, alors n’utilisez pas ce paramètre).
* `volume_priority` — Définit la priorité (l’ordre) dans laquelle les volumes sont remplis. Une valeur plus faible signifie une priorité plus élevée. Les valeurs du paramètre doivent être des nombres naturels et couvrir collectivement la plage de 1 à N (la priorité la plus faible étant attribuée à N), sans en sauter aucune.
  * Si *tous* les volumes sont balisés, ils sont priorisés dans l’ordre indiqué.
  * Si seulement *certains* volumes sont balisés, ceux qui ne le sont pas ont la priorité la plus faible et sont priorisés dans l’ordre dans lequel ils sont définis dans la configuration.
  * Si *aucun* volume n’est balisé, leur priorité correspond à l’ordre dans lequel ils sont déclarés dans la configuration.
  * Deux volumes ne peuvent pas avoir la même valeur de priorité.

Exemples de configuration :

```xml
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

        <small_jbod_with_external_no_merges>
            <volumes>
                <main>
                    <disk>jbod1</disk>
                </main>
                <external>
                    <disk>external</disk>
                </external>
            </volumes>
        </small_jbod_with_external_no_merges>
    </policies>
    ...
</storage_configuration>
```

Dans cet exemple, la politique `hdd_in_order` met en œuvre une approche [round-robin](https://en.wikipedia.org/wiki/Round-robin_scheduling). Cette politique ne définit donc qu&#39;un seul volume (`single`) et les parties de données sont stockées sur tous ses disques en rotation. Une telle politique peut être très utile si plusieurs disques similaires sont montés sur le système, mais qu&#39;aucune configuration RAID n&#39;est en place. Gardez à l&#39;esprit que chaque disque pris individuellement n&#39;est pas fiable, et qu&#39;il peut être judicieux de compenser cela avec un facteur de réplication de 3 ou plus.

Si différents types de disques sont disponibles dans le système, la politique `moving_from_ssd_to_hdd` peut être utilisée à la place. Le volume `hot` se compose d&#39;un disque SSD (`fast_ssd`), et la taille maximale d&#39;une partie pouvant être stockée sur ce volume est de 1GB. Toutes les parties dont la taille dépasse 1GB seront stockées directement sur le volume `cold`, qui contient un disque HDD `disk1`.
De plus, une fois que le disque `fast_ssd` est rempli à plus de 80 %, les données seront transférées vers `disk1` par un processus en arrière-plan.

L&#39;ordre d&#39;énumération des volumes au sein d&#39;une politique de stockage est important si au moins l&#39;un des volumes répertoriés n&#39;a pas de paramètre `volume_priority` explicite.
Une fois qu&#39;un volume est trop rempli, les données sont déplacées vers le suivant. L&#39;ordre d&#39;énumération des disques est également important, car les données y sont stockées à tour de rôle.

Lors de la création d&#39;une table, il est possible de lui appliquer l&#39;une des politiques de stockage configurées :

```sql
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

La politique de stockage `default` implique l&#39;utilisation d&#39;un seul volume, lui-même constitué d&#39;un seul disque indiqué dans `<path>`.
Vous pouvez modifier la politique de stockage après la création de la table avec la requête [ALTER TABLE ... MODIFY SETTING] ; la nouvelle politique doit inclure tous les anciens disques et volumes en conservant les mêmes noms.

Le nombre de threads effectuant les déplacements en arrière-plan des parties de données peut être modifié via le paramètre [background&#95;move&#95;pool&#95;size](/fr/operations/server-configuration-parameters/settings.md/#background_move_pool_size).

<div id="details">
  ### Détails
</div>

Dans le cas des tables `MergeTree`, les données sont écrites sur le disque de différentes manières :

* À la suite d’un insert (requête `INSERT`).
* Lors des fusions en arrière-plan et des [mutations](/fr/sql-reference/statements/alter#mutations).
* Lors du téléchargement depuis une autre réplique.
* À la suite du gel d’une partition [ALTER TABLE ... FREEZE PARTITION](/fr/sql-reference/statements/alter/partition#freeze-partition).

Dans tous ces cas, à l’exception des mutations et du gel de partition, une part est stockée sur un volume et un disque conformément à la politique de stockage définie :

1. Le premier volume (dans l’ordre de définition) qui dispose de suffisamment d’espace disque pour stocker une part (`unreserved_space > current_part_size`) et autorise le stockage de parts de cette taille (`max_data_part_size_bytes > current_part_size`) est choisi.
2. Au sein de ce volume, on choisit le disque qui suit celui utilisé pour stocker le bloc de données précédent et qui dispose d’un espace libre supérieur à la taille de la part (`unreserved_space - keep_free_space_bytes > current_part_size`).

En interne, les mutations et le gel de partition utilisent des [liens physiques](https://en.wikipedia.org/wiki/Hard_link). Les liens physiques entre différents disques ne sont pas pris en charge ; par conséquent, dans ces cas, les parts résultantes sont stockées sur les mêmes disques que les parts initiales.

En arrière-plan, les parts sont déplacées entre les volumes en fonction de la quantité d’espace libre (paramètre `move_factor`), selon l’ordre dans lequel les volumes sont déclarés dans le fichier de configuration.
Les données ne sont jamais transférées du dernier vers le premier. Vous pouvez utiliser les tables système [system.part&#95;log](/fr/operations/system-tables/part_log) (champ `type = MOVE_PART`) et [system.parts](/fr/operations/system-tables/parts.md) (champs `path` et `disk`) pour surveiller les déplacements en arrière-plan. Des informations détaillées sont également disponibles dans les journaux du serveur.

L’utilisateur peut forcer le déplacement d’une part ou d’une partition d’un volume à un autre à l’aide de la requête [ALTER TABLE ... MOVE PART|PARTITION ... TO VOLUME|DISK ...](/fr/sql-reference/statements/alter/partition) ; toutes les restrictions applicables aux opérations en arrière-plan sont prises en compte. La requête lance elle-même le déplacement et n’attend pas la fin des opérations en arrière-plan. L’utilisateur recevra un message d’erreur s’il n’y a pas assez d’espace libre ou si l’une des conditions requises n’est pas remplie.

Le déplacement des données n’interfère pas avec la réplication des données. Par conséquent, différentes politiques de stockage peuvent être spécifiées pour la même table sur différentes répliques.

Une fois les fusions en arrière-plan et les mutations terminées, les anciennes parts ne sont supprimées qu’après un certain délai (`old_parts_lifetime`).
Pendant cette période, elles ne sont pas déplacées vers d’autres volumes ou disques. Par conséquent, tant que les parts ne sont pas définitivement supprimées, elles sont toujours prises en compte dans l’évaluation de l’espace disque occupé.

L’utilisateur peut répartir de manière équilibrée de nouvelles grandes parts sur différents disques d’un volume [JBOD](https://en.wikipedia.org/wiki/Non-RAID_drive_architectures) en utilisant le paramètre [min&#95;bytes&#95;to&#95;rebalance&#95;partition&#95;over&#95;jbod](/fr/operations/settings/merge-tree-settings.md/#min_bytes_to_rebalance_partition_over_jbod).

<div id="table_engine-mergetree-s3">
  ## Utiliser un stockage externe pour les données
</div>

Les moteurs de table de la famille [MergeTree](/fr/engines/table-engines/mergetree-family/mergetree.md) peuvent stocker les données sur `S3`, `AzureBlobStorage` et `HDFS` à l’aide d’un disque de type `s3`, `azure_blob_storage` ou `hdfs`, respectivement. Consultez [la configuration des options de stockage externe](/fr/operations/storing-data.md/#configuring-external-storage) pour plus de détails.

Exemple d’utilisation de [S3](https://aws.amazon.com/s3/) comme stockage externe avec un disque de type `s3`.

Configuration :

```xml
<storage_configuration>
    ...
    <disks>
        <s3>
            <type>s3</type>
            <support_batch_delete>true</support_batch_delete>
            <endpoint>https://clickhouse-public-datasets.s3.amazonaws.com/my-bucket/root-path/</endpoint>
            <access_key_id>your_access_key_id</access_key_id>
            <secret_access_key>your_secret_access_key</secret_access_key>
            <region></region>
            <header>Authorization: Bearer SOME-TOKEN</header>
            <server_side_encryption_customer_key_base64>your_base64_encoded_customer_key</server_side_encryption_customer_key_base64>
            <server_side_encryption_kms_key_id>your_kms_key_id</server_side_encryption_kms_key_id>
            <server_side_encryption_kms_encryption_context>your_kms_encryption_context</server_side_encryption_kms_encryption_context>
            <server_side_encryption_kms_bucket_key_enabled>true</server_side_encryption_kms_bucket_key_enabled>
            <proxy>
                <uri>http://proxy1</uri>
                <uri>http://proxy2</uri>
            </proxy>
            <connect_timeout_ms>10000</connect_timeout_ms>
            <request_timeout_ms>5000</request_timeout_ms>
            <retry_attempts>10</retry_attempts>
            <single_read_retries>4</single_read_retries>
            <min_bytes_for_seek>1000</min_bytes_for_seek>
            <metadata_path>/var/lib/clickhouse/disks/s3/</metadata_path>
            <skip_access_check>false</skip_access_check>
        </s3>
        <s3_cache>
            <type>cache</type>
            <disk>s3</disk>
            <path>/var/lib/clickhouse/disks/s3_cache/</path>
            <max_size>10Gi</max_size>
        </s3_cache>
    </disks>
    ...
</storage_configuration>
```

Voir aussi [comment configurer les options de stockage externe](/fr/operations/storing-data.md/#configuring-external-storage).

<div id="s3-multiple-volumes">
  ### Utilisation des disques S3 avec plusieurs volumes
</div>

Les disques S3 (et les autres disques de stockage objet) peuvent être utilisés dans des politiques de stockage sur plusieurs disques et plusieurs volumes, de la même manière que les disques locaux. Cela permet de répartir les données entre plusieurs buckets S3 au sein d’un même volume (de type JBOD), ou de configurer des politiques de stockage hiérarchisé avec des volumes S3.

Par exemple, pour répartir les données entre deux buckets S3 selon un mécanisme de round-robin :

```xml
<storage_configuration>
    <disks>
        <s3_bucket1>
            <type>s3</type>
            <endpoint>https://s3.amazonaws.com/bucket-1/data/</endpoint>
            <access_key_id>your_access_key_id</access_key_id>
            <secret_access_key>your_secret_access_key</secret_access_key>
        </s3_bucket1>
        <s3_bucket2>
            <type>s3</type>
            <endpoint>https://s3.amazonaws.com/bucket-2/data/</endpoint>
            <access_key_id>your_access_key_id</access_key_id>
            <secret_access_key>your_secret_access_key</secret_access_key>
        </s3_bucket2>
    </disks>
    <policies>
        <s3_multi_bucket>
            <volumes>
                <main>
                    <disk>s3_bucket1</disk>
                    <disk>s3_bucket2</disk>
                </main>
            </volumes>
        </s3_multi_bucket>
    </policies>
</storage_configuration>
```

Vous pouvez également combiner des volumes locaux et S3 dans une politique par niveaux, par exemple en déplaçant les données d’un SSD local vers S3 au fil du temps :

```xml
<storage_configuration>
    <disks>
        <local_ssd>
            <path>/mnt/fast_ssd/clickhouse/</path>
        </local_ssd>
        <s3_cold>
            <type>s3</type>
            <endpoint>https://s3.amazonaws.com/cold-storage/data/</endpoint>
            <access_key_id>your_access_key_id</access_key_id>
            <secret_access_key>your_secret_access_key</secret_access_key>
        </s3_cold>
    </disks>
    <policies>
        <local_to_s3>
            <volumes>
                <hot>
                    <disk>local_ssd</disk>
                    <max_data_part_size_bytes>1073741824</max_data_part_size_bytes>
                </hot>
                <cold>
                    <disk>s3_cold</disk>
                </cold>
            </volumes>
            <move_factor>0.2</move_factor>
        </local_to_s3>
    </policies>
</storage_configuration>
```

:::note
Lors de l&#39;utilisation de `use_environment_credentials` pour l&#39;authentification S3, les identifiants d&#39;environnement (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`) sont partagés entre tous les disques S3. Il n&#39;est pas possible d&#39;utiliser des identifiants d&#39;environnement différents pour des disques différents. Si vous avez besoin d&#39;identifiants distincts pour chaque disque S3, utilisez plutôt des paramètres `access_key_id` et `secret_access_key` explicites pour chaque disque.
:::

Il est possible de configurer des tables MergeTree non répliquées dans un scénario avec un seul nœud en écriture et plusieurs nœuds en lecture sur un stockage partagé. Cela est rendu possible par le rafraîchissement automatique de la liste des parts, qui peut être configuré sur les nœuds en lecture. Notez que cela nécessite des métadonnées de système de fichiers partagées entre les répliques (ou `table_disk = true` avec un disque local à la table). Voir [refresh&#95;parts&#95;interval and table&#95;disk](/fr/operations/storing-data.md/#refresh-parts-interval-and-table-disk).

:::note configuration du cache
Les versions 22.3 à 22.7 de ClickHouse utilisent une configuration de cache différente ; voir [using local cache](/fr/operations/storing-data.md/#using-local-cache) si vous utilisez l&#39;une de ces versions.
:::

<div id="virtual-columns">
  ## Colonnes virtuelles
</div>

* `_part` — Nom d’une part.
* `_part_index` — Index séquentiel de la part dans le résultat de la requête.
* `_part_starting_offset` — Ligne de départ cumulée de la part dans le résultat de la requête.
* `_part_offset` — Nombre de lignes dans la part.
* `_part_granule_offset` — Nombre de granules dans la part.
* `_partition_id` — Nom d’une partition.
* `_part_uuid` — Identifiant unique de la part (si le paramètre MergeTree `assign_part_uuids` est activé).
* `_part_data_version` — Version des données de la part (soit le numéro de block minimal, soit la version de mutation).
* `_partition_value` — Valeurs (un tuple) d’une expression `partition by`.
* `_sample_factor` — Facteur d’échantillonnage (issu de la requête).
* `_block_number` — Numéro de block d’origine de la ligne, attribué lors de l’insertion et conservé lors des merges lorsque le paramètre `enable_block_number_column` est activé.
* `_block_offset` — Numéro d’origine de la ligne dans le block, attribué lors de l’insertion et conservé lors des merges lorsque le paramètre `enable_block_offset_column` est activé.
* `_disk_name` — Nom du disque utilisé pour le stockage.

<div id="column-statistics">
  ## Statistiques de colonnes
</div>

<CloudNotSupportedBadge />

La déclaration des statistiques figure dans la section des colonnes de la requête `CREATE` pour les tables de la famille `*MergeTree*` :

```sql
CREATE TABLE tab
(
    a Int64 STATISTICS(TDigest, Uniq),
    b Float64
)
ENGINE = MergeTree
ORDER BY a
```

On peut également modifier les statistiques à l’aide d’instructions `ALTER` :

```sql
ALTER TABLE tab ADD STATISTICS b TYPE TDigest, Uniq;
ALTER TABLE tab DROP STATISTICS a;
```

Ces statistiques légères fournissent des informations agrégées sur la distribution des valeurs dans les colonnes. Les statistiques sont stockées dans chaque part et mises à jour à chaque insertion.
Elles ne peuvent être utilisées pour l’optimisation PREWHERE que si l’on active `set use_statistics = 1`.

<div id="part-pruning-with-statistics">
  #### Élagage des parties à l’aide des statistiques
</div>

Lorsque `use_statistics_for_part_pruning` est activé, les statistiques peuvent être utilisées pour l’élagage des parties.
À l’heure actuelle, seules les statistiques `MinMax` et `Basic` prennent en charge l’élagage des parties. Lorsque de telles statistiques sont définies sur une colonne, ClickHouse suit les valeurs minimale et maximale de cette colonne dans chaque partie.
L’élagage des parties permet d’éviter la lecture de parties de données entières lorsque la condition de filtre de la requête ne peut correspondre à aucune ligne de cette partie.

**Exemple :**

```sql
-- Create a table with MinMax statistics on the 'value' column
CREATE TABLE test_stats
(
    id UInt64,
    value Int64 STATISTICS(MinMax)
)
ENGINE = MergeTree
ORDER BY id;

SYSTEM STOP MERGES test_stats;

-- Insert data in separate inserts to create multiple parts
INSERT INTO test_stats SELECT number, number FROM numbers(1000); -- Part 1: value range [0, 999]
INSERT INTO test_stats SELECT number, number + 10000 FROM numbers(1000); -- Part 2: value range [10000, 10999]

SET use_statistics_for_part_pruning = 1;

-- This query will skip Part 1 entirely because its max value (999) < 5000
SELECT count() FROM test_stats WHERE value > 5000;

-- Use EXPLAIN to see the pruning effect
EXPLAIN indexes = 1 SELECT count() FROM test_stats WHERE value > 5000;
-- The output will show "Parts: 1/2" indicating one part was pruned
```

<div id="available-types-of-column-statistics">
  ### Types disponibles de statistiques de colonnes
</div>

* `Basic`

  Un ensemble compact de résumés à valeur unique dérivés d&#39;une colonne. Selon le type de colonne, les éléments suivants sont renseignés :

  * pour toute colonne dont les valeurs sont représentées par un nombre (entiers, flottants, `Decimal*`, `Date*`, `DateTime*`, `Enum*`, `IPv4`, ...) : les valeurs minimale et maximale, qui permettent d&#39;estimer la sélectivité des filtres par plage et d&#39;effectuer l&#39;élagage des parties ;
  * pour les colonnes `String` et `FixedString` : la longueur totale en octets des valeurs non `NULL` (à partir de laquelle on peut déduire la longueur moyenne des chaînes) ;
  * pour les colonnes `Nullable` et `LowCardinality(Nullable)` : le nombre de valeurs `NULL`, que l&#39;optimiseur utilise pour exclure les lignes `NULL` des estimations de sélectivité.

    Une même statistique `Basic` peut renseigner plusieurs de ces éléments à la fois — par exemple, sur une colonne `Nullable(UInt32)`, elle suit à la fois le min/max numérique et le nombre de valeurs nulles. Par rapport à `MinMax`, `Basic` fonctionne aussi sur les colonnes `String` / `FixedString` et peut être déclaré sur des enveloppes `Nullable` de types comme `UUID` ou `IPv6` uniquement pour suivre le nombre de valeurs nulles.

    Syntaxe : `basic`

* `MinMax`

  Les valeurs minimale et maximale de la colonne, ce qui permet d&#39;estimer la sélectivité des filtres par plage sur les colonnes numériques.

  Syntaxe : `minmax`

* `TDigest`

:::warning
Les statistiques de type `tdigest` ont un coût de création élevé et peuvent potentiellement ralentir l&#39;ingestion des données.
:::

Les sketches [TDigest](https://github.com/tdunning/t-digest) permettent de calculer des percentiles approximatifs (par ex. le 90e percentile) pour les colonnes numériques.

Syntaxe : `tdigest`

* `Uniq`

  Les sketches [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) fournissent une estimation du nombre de valeurs distinctes contenues dans une colonne.

  Syntaxe : `uniq`

* `CountMin`

:::warning
Les statistiques de type `countmin` ont un coût de création élevé et peuvent potentiellement ralentir l&#39;ingestion des données.
:::

Les sketches [CountMin](https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch) fournissent un comptage approximatif de la fréquence de chaque valeur dans une colonne.

Syntaxe : `countmin`

<div id="supported-data-types">
  ### Types de données pris en charge
</div>

|          | (U)Int*, Float*, Decimal(*), Date*, Boolean, Enum* | IPv4 | String or FixedString |
| -------- | -------------------------------------------------- | ---- | --------------------- |
| Basic    | ✔                                                  | ✔    | ✔                     |
| CountMin | ✔                                                  | ✔    | ✔                     |
| MinMax   | ✔                                                  | ✔    | ✗                     |
| TDigest  | ✔                                                  | ✗    | ✗                     |
| Uniq     | ✔                                                  | ✔    | ✔                     |

Tous les éléments ci-dessus acceptent également les wrappers `Nullable` et `LowCardinality(Nullable)` des types indiqués. `Basic` peut aussi être déclaré sur des wrappers `Nullable` de types comme `UUID` ou `IPv6`, uniquement pour comptabiliser le nombre de valeurs nulles.

<div id="supported-operations">
  ### Opérations prises en charge
</div>

|          | Filtres d&#39;égalité (==) | Filtres de plage (`>, >=, <, <=`)  |
| -------- | -------------------------- | ---------------------------------- |
| Basic    | ✗                          | ✔ (colonnes numériques uniquement) |
| CountMin | ✔                          | ✗                                  |
| MinMax   | ✗                          | ✔ (colonnes numériques uniquement) |
| TDigest  | ✗                          | ✔ (colonnes numériques uniquement) |
| Uniq     | ✔                          | ✗                                  |

Pour `Basic` sur des colonnes `String` / `FixedString`, la statistique enregistre uniquement la longueur totale en octets des valeurs non NULL
(utilisée pour estimer la longueur moyenne des chaînes) ainsi que le nombre de valeurs NULL ;
elle n&#39;est pas utilisée pour les filtres de plage ni pour l&#39;élagage des parts.

<div id="column-level-settings">
  ## Paramètres au niveau des colonnes
</div>

Certains paramètres de MergeTree peuvent être redéfinis au niveau des colonnes :

* `max_compress_block_size` — Taille maximale des blocs de données non compressées avant leur compression lors de l’écriture dans une table.
* `min_compress_block_size` — Taille minimale des blocs de données non compressées requise pour la compression lors de l’écriture du mark suivant.

Exemple :

```sql
CREATE TABLE tab
(
    id Int64,
    document String SETTINGS (min_compress_block_size = 16777216, max_compress_block_size = 16777216)
)
ENGINE = MergeTree
ORDER BY id
```

Les paramètres de colonne peuvent être modifiés ou supprimés à l’aide de [ALTER MODIFY COLUMN](/fr/sql-reference/statements/alter/column.md), par exemple :

* Supprimer `SETTINGS` de la déclaration de colonne :

```sql
ALTER TABLE tab MODIFY COLUMN document REMOVE SETTINGS;
```

* Modifier un paramètre :

```sql
ALTER TABLE tab MODIFY COLUMN document MODIFY SETTING min_compress_block_size = 8192;
```

* Réinitialisez un ou plusieurs paramètres ; cela supprime également la déclaration du paramètre dans l’expression de colonne de la requête CREATE de la table.

```sql
ALTER TABLE tab MODIFY COLUMN document RESET SETTING min_compress_block_size;
```