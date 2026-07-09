---
description: 'Documentation pour la table'
keywords: ['compression', 'codec', 'schéma', 'DDL']
sidebar_label: 'TABLE'
sidebar_position: 36
slug: /sql-reference/statements/create/table
title: 'CREATE TABLE'
doc_type: 'référence'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Crée une nouvelle table. Cette requête peut prendre différentes formes de syntaxe selon le cas d’usage.

Par défaut, les tables sont créées uniquement sur le serveur actuel. Les requêtes DDL distribuées utilisent la clause `ON CLUSTER`, qui est [décrite séparément](../../../sql-reference/distributed-ddl.md).

<div id="syntax-forms">
  ## Formes de syntaxe
</div>

<div id="with-explicit-schema">
  ### Avec un schéma explicite
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [NULL|NOT NULL] [DEFAULT|MATERIALIZED|EPHEMERAL|ALIAS expr1] [COMMENT 'comment for column'] [compression_codec] [TTL expr1],
    name2 [type2] [NULL|NOT NULL] [DEFAULT|MATERIALIZED|EPHEMERAL|ALIAS expr2] [COMMENT 'comment for column'] [compression_codec] [TTL expr2],
    ...
) ENGINE = engine
  [COMMENT 'comment for table']
```

Crée une table nommée `table_name` dans la base de données `db` ou dans la base de données courante si `db` n’est pas défini, avec la structure spécifiée entre parenthèses et le moteur `engine`.
La structure de la table est une liste de descriptions de colonnes, d’index secondaires, de projections et de contraintes. Si la [clé primaire](#primary-key) est prise en charge par le moteur, elle sera indiquée comme paramètre du moteur de table.

Dans le cas le plus simple, une description de colonne est de la forme `name type`. Exemple : `RegionID UInt32`.

Des expressions peuvent également être définies pour les valeurs par défaut (voir ci-dessous).

Si nécessaire, la clé primaire peut être spécifiée, avec une ou plusieurs expressions de clé.

Des commentaires peuvent être ajoutés aux colonnes et à la table.

<div id="with-a-schema-similar-to-other-table">
  ### Avec le schéma d’une table existante
</div>

ClickHouse permet de copier le schéma et les données d’une table existante.

Pour reproduire le schéma d’une table existante :

```sql
CREATE TABLE [IF NOT EXISTS] [db2.]table_clone AS [db.]table [ENGINE = engine]
```

Cela crée une table ayant la même structure qu’une autre table.

<div id="with-a-schema-and-data-cloned-from-another-table">
  ### Avec le schéma et les données d’une table existante
</div>

Pour répliquer le schéma et les données d’une table existante :

```sql
CREATE TABLE [IF NOT EXISTS] [db2.]table_clone CLONE AS [db.]table [ENGINE = engine]
```

Cela crée une table avec le même schéma et les mêmes données qu’une table existante.  Une fois la nouvelle table créée, toutes les partitions de `db.table` lui sont attachées. En d’autres termes, les données de `db.table` sont clonées dans `db2.table_clone` au moment de la création. Cette requête est équivalente à la suivante :

```sql
CREATE TABLE [IF NOT EXISTS] [db2.]table_clone AS [db.]table [ENGINE = engine];
ALTER TABLE [db2.]table_clone ATTACH PARTITION ALL FROM [db.]table;
```

Pour ces deux fonctionnalités, vous pouvez spécifier un moteur différent pour la table. Si le moteur n&#39;est pas spécifié, c&#39;est le même moteur que celui de la table d&#39;origine qui sera utilisé (`db.table`).

<div id="from-a-table-function">
  ### À partir d’une table function
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name AS table_function()
```

Crée une table produisant le même résultat que la [table function](/fr/sql-reference/table-functions) spécifiée. La table créée fonctionnera également de la même manière que la table function correspondante spécifiée.

<div id="from-select-query">
  ### À partir d’une requête SELECT
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name[(name1 [type1], name2 [type2], ...)] ENGINE = engine AS SELECT ...
```

Crée une table dont la structure est semblable au résultat de la requête `SELECT`, en utilisant le moteur `engine`, puis la remplit avec les données de `SELECT`. Vous pouvez également définir explicitement les colonnes.

Si la table existe déjà et que `IF NOT EXISTS` est spécifié, la requête est sans effet.

D&#39;autres clauses peuvent figurer après la clause `ENGINE` dans la requête. Consultez la documentation détaillée sur la création de tables dans les descriptions des [moteurs de table](/fr/engines/table-engines).

**Exemple**

```sql title="Query"
CREATE TABLE t1 (x String) ENGINE = Memory AS SELECT 1;
SELECT x, toTypeName(x) FROM t1;
```

```text title="Response"
┌─x─┬─toTypeName(x)─┐
│ 1 │ String        │
└───┴───────────────┘
```

<div id="null-or-not-null-modifiers">
  ## Modificateurs `NULL` ou `NOT NULL`
</div>

Les modificateurs `NULL` et `NOT NULL` placés après le type de données dans la définition d&#39;une colonne permettent ou non de rendre ce type [Nullable](/fr/sql-reference/data-types/nullable).

Si le type n&#39;est pas `Nullable` et que `NULL` est spécifié, il sera traité comme `Nullable` ; si `NOT NULL` est spécifié, ce ne sera pas le cas. Par exemple, `INT NULL` est équivalent à `Nullable(INT)`. Si le type est `Nullable` et que les modificateurs `NULL` ou `NOT NULL` sont spécifiés, une exception sera levée.

Voir aussi le paramètre [data&#95;type&#95;default&#95;nullable](../../../operations/settings/settings.md#data_type_default_nullable).

<div id="default_values">
  ## Valeurs par défaut
</div>

La description d&#39;une colonne peut spécifier une expression de valeur par défaut sous la forme `DEFAULT expr`, `MATERIALIZED expr` ou `ALIAS expr`. Exemple : `URLDomain String DEFAULT domain(URL)`.

L&#39;expression `expr` est facultative. Si elle est omise, le type de la colonne doit être spécifié explicitement et la valeur par défaut sera `0` pour les colonnes numériques, `''` (la chaîne vide) pour les colonnes de type chaîne, `[]` (le tableau vide) pour les colonnes de type tableau, `1970-01-01` pour les colonnes de type date, ou `NULL` pour les colonnes Nullable.

Le type d&#39;une colonne avec une valeur par défaut peut être omis, auquel cas il est déduit du type de `expr`. Par exemple, le type de la colonne `EventDate DEFAULT toDate(EventTime)` sera Date.

Si un type de données et une expression de valeur par défaut sont tous deux spécifiés, une fonction implicite de conversion de type est insérée pour convertir l&#39;expression vers le type spécifié. Exemple : `Hits UInt32 DEFAULT 0` est représenté en interne sous la forme `Hits UInt32 DEFAULT toUInt32(0)`.

Une expression de valeur par défaut `expr` peut référencer des colonnes de table arbitraires et des constantes. ClickHouse vérifie que les modifications de la structure de la table n&#39;introduisent pas de boucles dans le calcul de l&#39;expression. Pour INSERT, il vérifie que les expressions peuvent être résolues, c&#39;est-à-dire que toutes les colonnes à partir desquelles elles peuvent être calculées ont bien été transmises.

<div id="default">
  ### DEFAULT
</div>

`DEFAULT expr`

Valeur par défaut standard. Si la valeur d’une telle colonne n’est pas spécifiée dans une requête `INSERT`, elle est calculée à partir de `expr`.

Exemple :

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    updated_at DateTime DEFAULT now(),
    updated_at_date Date DEFAULT toDate(updated_at)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test (id) VALUES (1);

SELECT * FROM test;
┌─id─┬──────────updated_at─┬─updated_at_date─┐
│  1 │ 2023-02-24 17:06:46 │      2023-02-24 │
└────┴─────────────────────┴─────────────────┘
```

<div id="materialized">
  ### MATERIALIZED
</div>

`MATERIALIZED expr`

Expression matérialisée. Les valeurs de ces colonnes sont automatiquement calculées d’après l’expression matérialisée spécifiée lors de l’insertion des lignes. Les valeurs ne peuvent pas être spécifiées explicitement lors des opérations `INSERT`.

De plus, les colonnes de ce type avec une valeur par défaut ne sont pas incluses dans le résultat de `SELECT *`. Cela permet de préserver l’invariant selon lequel le résultat d’un `SELECT *` peut toujours être réinséré dans la table à l’aide de `INSERT`. Ce comportement peut être désactivé avec le paramètre `asterisk_include_materialized_columns`.

Exemple :

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    updated_at DateTime MATERIALIZED now(),
    updated_at_date Date MATERIALIZED toDate(updated_at)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test VALUES (1);

SELECT * FROM test;
┌─id─┐
│  1 │
└────┘

SELECT id, updated_at, updated_at_date FROM test;
┌─id─┬──────────updated_at─┬─updated_at_date─┐
│  1 │ 2023-02-24 17:08:08 │      2023-02-24 │
└────┴─────────────────────┴─────────────────┘

SELECT * FROM test SETTINGS asterisk_include_materialized_columns=1;
┌─id─┬──────────updated_at─┬─updated_at_date─┐
│  1 │ 2023-02-24 17:08:08 │      2023-02-24 │
└────┴─────────────────────┴─────────────────┘
```

<div id="ephemeral">
  ### EPHEMERAL
</div>

`EPHEMERAL [expr]`

Colonne éphémère. Les colonnes de ce type ne sont pas stockées dans la table et il n&#39;est pas possible de les interroger avec `SELECT`. Le seul but des colonnes éphémères est de servir à construire, à partir d&#39;elles, les expressions de valeur par défaut d&#39;autres colonnes.

Un `INSERT` sans colonnes explicitement spécifiées ignorera les colonnes de ce type. Cela permet de préserver l&#39;invariant selon lequel le résultat d&#39;un `SELECT *` peut toujours être réinséré dans la table à l&#39;aide de `INSERT`.

Exemple :

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    unhexed String EPHEMERAL,
    hexed FixedString(4) DEFAULT unhex(unhexed)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test (id, unhexed) VALUES (1, '5a90b714');

SELECT
    id,
    hexed,
    hex(hexed)
FROM test
FORMAT Vertical;

Row 1:
──────
id:         1
hexed:      Z��
hex(hexed): 5A90B714
```

<div id="alias">
  ### ALIAS
</div>

`ALIAS expr`

Colonnes calculées (synonyme). Les colonnes de ce type ne sont pas stockées dans la table et il n&#39;est pas possible d&#39;y INSERT des valeurs.

Lorsque des requêtes SELECT référencent explicitement des colonnes de ce type, la valeur est calculée au moment de la requête à partir de `expr`. Par défaut, `SELECT *` exclut les colonnes ALIAS. Ce comportement peut être désactivé avec le paramètre `asterisk_include_alias_columns`.

Lorsque vous utilisez la requête ALTER pour ajouter de nouvelles colonnes, les anciennes données de ces colonnes ne sont pas écrites. À la place, lors de la lecture d&#39;anciennes données qui n&#39;ont pas de valeurs pour les nouvelles colonnes, les expressions sont calculées à la volée par défaut. Toutefois, si l&#39;évaluation des expressions nécessite d&#39;autres colonnes qui ne sont pas indiquées dans la requête, ces colonnes seront également lues, mais uniquement pour les blocs de données qui en ont besoin.

Si vous ajoutez une nouvelle colonne à une table puis modifiez plus tard son expression par défaut, les valeurs utilisées pour les anciennes données changeront (pour les données dont les valeurs n&#39;ont pas été stockées sur le disque). Notez que, lors de l&#39;exécution des fusions en arrière-plan, les données des colonnes absentes dans l&#39;une des parts en cours de fusion sont écrites dans la part fusionnée.

Il n&#39;est pas possible de définir des valeurs par défaut pour les éléments de structures de données imbriquées.

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    size_bytes Int64,
    size String ALIAS formatReadableSize(size_bytes)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test VALUES (1, 4678899);

SELECT id, size_bytes, size FROM test;
┌─id─┬─size_bytes─┬─size─────┐
│  1 │    4678899 │ 4.46 MiB │
└────┴────────────┴──────────┘

SELECT * FROM test SETTINGS asterisk_include_alias_columns=1;
┌─id─┬─size_bytes─┬─size─────┐
│  1 │    4678899 │ 4.46 MiB │
└────┴────────────┴──────────┘
```

<div id="primary-key">
  ## Clé primaire
</div>

Vous pouvez définir une [clé primaire](../../../engines/table-engines/mergetree-family/mergetree.md#primary-keys-and-indexes-in-queries) lors de la création d’une table. La clé primaire peut être spécifiée de deux manières :

* Dans la liste des colonnes

```sql
CREATE TABLE [db.]table_name
(
    name1 type1, name2 type2, ...,
    PRIMARY KEY(expr1[, expr2,...])
)
ENGINE = engine;
```

* Hors de la liste des colonnes

```sql
CREATE TABLE [db.]table_name
(
    name1 type1, name2 type2, ...
)
ENGINE = engine
PRIMARY KEY(expr1[, expr2,...]);
```

:::tip
Vous ne pouvez pas combiner ces deux approches dans une même requête.
:::

<div id="constraints">
  ## Contraintes
</div>

En plus de la description des colonnes, il est possible de définir des contraintes :

<div id="constraint">
  ### CONSTRAINT
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [compression_codec] [TTL expr1],
    ...
    CONSTRAINT constraint_name_1 CHECK boolean_expr_1,
    ...
) ENGINE = engine
```

`boolean_expr_1` peut être n’importe quelle expression booléenne. Si des contraintes sont définies pour la table, chacune d’elles sera vérifiée pour chaque ligne de la requête `INSERT`. Si l’une des contraintes n’est pas respectée, le serveur lèvera une exception avec le nom de la contrainte et l’expression de vérification.

L’ajout d’un grand nombre de contraintes peut avoir un impact négatif sur les performances des requêtes `INSERT` volumineuses.

Les contraintes existantes sur l’ensemble des tables peuvent être consultées dans la table [`system.constraints`](/fr/operations/system-tables/constraints).

<div id="assume">
  ### ASSUME
</div>

La clause `ASSUME` sert à définir une `CONSTRAINT` sur une table, en supposant que cette contrainte est vraie. Cette contrainte peut ensuite être utilisée par l’optimiseur pour améliorer les performances des requêtes SQL.

Prenons cet exemple, où `ASSUME CONSTRAINT` est utilisé lors de la création de la table `users_a` :

```sql
CREATE TABLE users_a (
    uid Int16, 
    name String, 
    age Int16, 
    name_len UInt8 MATERIALIZED length(name), 
    CONSTRAINT c1 ASSUME length(name) = name_len
) 
ENGINE=MergeTree 
ORDER BY (name_len, name);
```

Ici, `ASSUME CONSTRAINT` sert à affirmer que la fonction `length(name)` est toujours égale à la valeur de la colonne `name_len`. Cela signifie que chaque fois que `length(name)` est appelée dans une query, ClickHouse peut la remplacer par `name_len`, ce qui devrait être plus rapide, car cela évite d’appeler la fonction `length()`.

Ensuite, lors de l’exécution de la query `SELECT name FROM users_a WHERE length(name) < 5;`, ClickHouse peut l’optimiser en `SELECT name FROM users_a WHERE name_len < 5`; grâce à `ASSUME CONSTRAINT`. Cela peut accélérer l’exécution de la query, car cela évite de calculer la longueur de `name` pour chaque ligne.

`ASSUME CONSTRAINT` **ne fait pas respecter la contrainte** ; il informe simplement l’optimiseur que la contrainte est vérifiée. Si la contrainte n’est pas réellement vraie, les résultats des queries peuvent être incorrects. Par conséquent, vous ne devez utiliser `ASSUME CONSTRAINT` que si vous êtes certain que la contrainte est vraie.

<div id="ttl-expression">
  ## Expression TTL
</div>

Définit la durée de conservation des valeurs. Ne peut être spécifiée que pour les tables de la famille MergeTree. Pour une description détaillée, consultez [TTL pour les colonnes et les tables](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl).

<div id="column_compression_codec">
  ## Codecs de compression des colonnes
</div>

Par défaut, ClickHouse utilise la compression `lz4` dans la version autogérée, et `zstd` dans ClickHouse Cloud.

Pour la famille de moteurs `MergeTree`, vous pouvez modifier la méthode de compression par défaut dans la section [compression](/fr/operations/server-configuration-parameters/settings#compression) de la configuration du serveur.

Vous pouvez également définir la méthode de compression pour chaque colonne dans la requête `CREATE TABLE`.

```sql
CREATE TABLE codec_example
(
    dt Date CODEC(ZSTD),
    ts DateTime CODEC(LZ4HC),
    float_value Float32 CODEC(NONE),
    double_value Float64 CODEC(LZ4HC(9)),
    value Float32 CODEC(Delta, ZSTD)
)
ENGINE = <Engine>
...
```

Le codec `Default` peut être indiqué pour faire référence à la compression par défaut, qui peut dépendre de différents paramètres (et des propriétés des données) au moment de l’exécution.
Exemple : `value UInt64 CODEC(Default)` — équivaut à l’absence de spécification de codec.

Vous pouvez également supprimer le `CODEC` actuel de la colonne et utiliser la compression par défaut définie dans config.xml :

```sql
ALTER TABLE codec_example MODIFY COLUMN float_value CODEC(Default);
```

Les codecs peuvent être combinés en pipeline, par exemple `CODEC(Delta, Default)`.

:::tip
Vous ne pouvez pas décompresser les fichiers de la base de données ClickHouse avec des utilitaires externes comme `lz4`. Utilisez plutôt l’utilitaire spécifique [clickhouse-compressor](https://github.com/ClickHouse/ClickHouse/tree/master/programs/compressor).
:::

La compression est prise en charge pour les moteurs de table suivants :

* Famille [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md). Prend en charge les codecs de compression des colonnes ainsi que le choix de la méthode de compression par défaut via les paramètres [compression](/fr/operations/server-configuration-parameters/settings#compression).
* Famille [Log](../../../engines/table-engines/log-family/index.md). Utilise par défaut la méthode de compression `lz4` et prend en charge les codecs de compression des colonnes.
* [Set](../../../engines/table-engines/special/set.md). Seule la compression par défaut est prise en charge.
* [Join](../../../engines/table-engines/special/join.md). Seule la compression par défaut est prise en charge.

ClickHouse prend en charge des codecs à usage général et des codecs spécialisés.

<div id="general-purpose-codecs">
  ### Codecs d’usage général
</div>

<div id="none">
  #### NONE
</div>

`NONE` — Aucune compression.

<div id="lz4">
  #### LZ4
</div>

`LZ4` — [Algorithme de compression de données](https://github.com/lz4/lz4) sans perte utilisé par défaut. Applique la compression rapide de LZ4.

<div id="lz4hc">
  #### LZ4HC
</div>

`LZ4HC[(level)]` — algorithme LZ4 HC (forte compression) avec niveau configurable. Niveau par défaut : 9. Définir `level <= 0` applique le niveau par défaut. Niveaux possibles : [1, 12]. Plage de niveaux recommandée : [4, 9].

<div id="zstd">
  #### ZSTD
</div>

`ZSTD[(level)]` — [algorithme de compression ZSTD](https://en.wikipedia.org/wiki/Zstandard) avec `level` configurable. Niveaux possibles : [1, 22]. Niveau par défaut : 1.

Des niveaux de compression élevés sont utiles dans des scénarios asymétriques, par exemple lorsqu&#39;on compresse une fois puis qu&#39;on décompresse à plusieurs reprises. Des niveaux plus élevés offrent une meilleure compression, mais augmentent aussi l&#39;utilisation du CPU.

<div id="zstd_qat">
  #### Obsolète : ZSTD_QAT
</div>

<CloudNotSupportedBadge />

<div id="deflate_qpl">
  #### Obsolète : DEFLATE_QPL
</div>

<CloudNotSupportedBadge />

<div id="specialized-codecs">
  ### Codecs spécialisés
</div>

Ces codecs sont conçus pour rendre la compression plus efficace en exploitant des caractéristiques propres aux données. Certains de ces codecs ne compressent pas eux-mêmes les données ; ils les prétraitent de sorte qu’une seconde étape de compression, utilisant un codec générique, puisse atteindre un taux de compression plus élevé.

<div id="delta">
  #### Delta
</div>

`Delta(delta_bytes)` — Approche de compression dans laquelle les valeurs brutes sont remplacées par la différence entre deux valeurs adjacentes, à l’exception de la première, qui reste inchangée. `delta_bytes` correspond à la taille maximale des valeurs brutes ; la valeur par défaut est `sizeof(type)`. La spécification de `delta_bytes` comme argument est obsolète et sa prise en charge sera supprimée dans une prochaine version. Delta est un codec de préparation des données, c’est-à-dire qu’il ne peut pas être utilisé seul.

<div id="doubledelta">
  #### DoubleDelta
</div>

`DoubleDelta(bytes_size)` — Calcule les deltas de deltas et les écrit au format binaire compact. `bytes_size` a une signification similaire à `delta_bytes` dans le codec [Delta](#delta). Spécifier `bytes_size` comme argument est obsolète et cette prise en charge sera supprimée dans une prochaine version. Les taux de compression optimaux sont obtenus pour des séquences monotones avec un pas constant, telles que les données de séries temporelles. Peut être utilisé avec n’importe quel type numérique. Implémente l’algorithme utilisé dans Gorilla TSDB, en l’étendant pour prendre également en charge les types 64 bits. Utilise 1 bit supplémentaire pour les deltas 32 bits : des préfixes de 5 bits au lieu de préfixes de 4 bits. Pour plus d’informations, consultez « Compressing Time Stamps » dans [Gorilla: A Fast, Scalable, In-Memory Time Series Database](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf). DoubleDelta est un codec de préparation des données, c’est-à-dire qu’il ne peut pas être utilisé seul.

<div id="gcd">
  #### PGCD
</div>

`GCD()` - - Calcule le plus grand commun diviseur (PGCD) des valeurs de la colonne, puis divise chaque valeur par le PGCD. Peut être utilisé avec des colonnes d’entiers, de nombres décimaux et de date et heure. Ce codec est particulièrement adapté aux colonnes dont les valeurs varient (augmentent ou diminuent) par multiples du PGCD, par exemple 24, 28, 16, 24, 8, 24 (GCD = 4). GCD est un codec de préparation des données, c’est-à-dire qu’il ne peut pas être utilisé seul.

<div id="gorilla">
  #### Gorilla
</div>

`Gorilla(bytes_size)` — Calcule le XOR entre la valeur à virgule flottante actuelle et la précédente, puis l’écrit sous une forme binaire compacte. Plus l’écart entre des valeurs consécutives est faible, c’est-à-dire plus les valeurs de la série évoluent lentement, meilleur est le taux de compression. Implémente l’algorithme utilisé dans Gorilla TSDB, étendu pour prendre en charge les types 64 bits. Valeurs possibles de `bytes_size` : 1, 2, 4, 8 ; la valeur par défaut est `sizeof(type)` si elle est égale à 1, 2, 4 ou 8. Dans tous les autres cas, elle vaut 1. Pour plus d’informations, voir la section 4.1 de [Gorilla: A Fast, Scalable, In-Memory Time Series Database](https://doi.org/10.14778/2824032.2824078).

<div id="alp">
  #### ALP
</div>

<ExperimentalBadge />

`ALP(variant)` — Compression adaptative sans perte pour les données en virgule flottante. Prend en charge `Float32` et `Float64`. Pour plus de détails, voir [ALP: Adaptive lossless floating-point compression](https://ir.cwi.nl/pub/33334).

Le codec accepte un argument de variante facultatif :

* `ALP()` ou `ALP(AUTO)` (par défaut) — Utilise STD et se rabat sur RD selon la taille compressée estimée.
* `ALP(STD)` — Variante ALP standard. Représente chaque valeur sous la forme d’un entier exact mis à l’échelle à l’aide de puissances de dix, puis compresse les entiers obtenus avec Frame-of-Reference et l’empaquetage de bits. Les valeurs non représentables sont stockées telles quelles en tant qu’exceptions. Fonctionne le mieux pour les nombres issus de valeurs décimales (par ex., mesures, prix).
* `ALP(RD)` — Variante Real Doubles. Réinterprète le schéma de bits de chaque valeur et le divise en une partie haute (signe + exposant + bits de poids fort de la mantisse) et une partie basse. Les parties hautes sont encodées par dictionnaire (jusqu’à 8 entrées), les parties basses sont empaquetées au niveau des bits. Fonctionne le mieux lorsque de nombreuses valeurs partagent les mêmes bits de poids fort.

:::note
Ce codec est expérimental et nécessite `SET allow_experimental_codecs = 1` pour être utilisé.
:::

<div id="fpc">
  #### FPC
</div>

`FPC(level, float_size)` - Prédit de manière répétée la prochaine valeur en virgule flottante d&#39;une séquence à l&#39;aide du meilleur de deux prédicteurs, puis applique un XOR entre la valeur réelle et la valeur prédite, avant de compresser le résultat en tirant parti des zéros de tête. Comme Gorilla, cette méthode est efficace pour stocker une série de valeurs en virgule flottante qui évoluent lentement. Pour les valeurs 64 bits (double), FPC est plus rapide que Gorilla ; pour les valeurs 32 bits, les performances peuvent varier. Valeurs possibles de `level` : 1-28, la valeur par défaut est 12.  Valeurs possibles de `float_size` : 4, 8, la valeur par défaut est `sizeof(type)` si le type est Float. Dans tous les autres cas, elle est de 4. Pour une description détaillée de l&#39;algorithme, voir [High Throughput Compression of Double-Precision Floating-Point Data](https://userweb.cs.txstate.edu/~burtscher/papers/dcc07a.pdf).

<div id="t64">
  #### T64
</div>

`T64` — approche de compression qui tronque les bits de poids fort inutilisés des valeurs dans les types de données entiers (y compris `Enum`, `Date` et `DateTime`). À chaque étape de son algorithme, le codec prend un bloc de 64 valeurs, les place dans une matrice de 64x64 bits, la transpose, tronque les bits inutilisés des valeurs et renvoie le reste sous forme de séquence. Les bits inutilisés sont ceux qui ne diffèrent pas entre les valeurs minimale et maximale dans l’ensemble de la data part sur laquelle la compression est utilisée.

Les codecs `DoubleDelta` et `Gorilla` sont utilisés dans Gorilla TSDB comme composants de son algorithme de compression. L’approche Gorilla est efficace dans les scénarios où il existe une séquence de valeurs évoluant lentement avec leurs timestamps. Les timestamps sont efficacement compressés par le codec `DoubleDelta`, et les valeurs le sont efficacement par le codec `Gorilla`. Par exemple, pour obtenir une table stockée efficacement, vous pouvez la créer avec la configuration suivante :

```sql
CREATE TABLE codec_example
(
    timestamp DateTime CODEC(DoubleDelta),
    slow_values Float32 CODEC(Gorilla)
)
ENGINE = MergeTree()
```

<div id="encryption-codecs">
  ### Codecs de chiffrement
</div>

Ces codecs ne compressent pas réellement les données, mais les chiffrent sur le disque. Ils ne sont disponibles que lorsqu&#39;une clé de chiffrement est spécifiée dans les paramètres [encryption](/fr/operations/server-configuration-parameters/settings#encryption). Notez que le chiffrement n&#39;a de sens qu&#39;en fin de pipeline de codecs, car les données chiffrées ne peuvent généralement pas être compressées de façon significative.

Codecs de chiffrement :

<div id="aes_128_gcm_siv">
  #### AES_128_GCM_SIV
</div>

`CODEC('AES-128-GCM-SIV')` — Chiffre les données avec AES-128 en mode GCM-SIV défini dans la [RFC 8452](https://tools.ietf.org/html/rfc8452).

<div id="aes-256-gcm-siv">
  #### AES-256-GCM-SIV
</div>

`CODEC('AES-256-GCM-SIV')` — Chiffre les données avec AES-256 en mode GCM-SIV.

Ces codecs utilisent un nonce fixe et le chiffrement est donc déterministe. Ils sont ainsi compatibles avec les moteurs prenant en charge la déduplication, tels que [ReplicatedMergeTree](../../../engines/table-engines/mergetree-family/replication.md), mais cela présente une faiblesse : lorsqu’un même bloc de données est chiffré deux fois, le ciphertext obtenu sera exactement identique. Un adversaire capable de lire le disque peut donc constater cette équivalence (mais uniquement cette équivalence, sans en connaître le contenu).

:::note
La plupart des moteurs, y compris ceux de la famille &quot;*MergeTree&quot;, créent des fichiers d’index sur le disque sans appliquer de codecs. Cela signifie que des données en clair apparaîtront sur le disque si une colonne chiffrée est indexée.
:::

:::note
Si vous exécutez une requête SELECT mentionnant une valeur spécifique dans une colonne chiffrée (par exemple dans sa clause WHERE), la valeur peut apparaître dans [system.query&#95;log](../../../operations/system-tables/query_log.md). Vous pouvez envisager de désactiver la journalisation.
:::

**Exemple**

```sql
CREATE TABLE mytable
(
    x String CODEC(AES_128_GCM_SIV)
)
ENGINE = MergeTree ORDER BY x;
```

:::note
Si la compression doit être appliquée, elle doit être indiquée explicitement. Sinon, seules les données seront chiffrées.
:::

**Exemple**

```sql
CREATE TABLE mytable
(
    x String CODEC(Delta, LZ4, AES_128_GCM_SIV)
)
ENGINE = MergeTree ORDER BY x;
```

<div id="temporary-tables">
  ## Tables temporaires
</div>

:::note
Veuillez noter que les tables temporaires ne sont pas répliquées. Par conséquent, rien ne garantit que les données insérées dans une table temporaire seront disponibles sur d&#39;autres répliques. Le principal cas d&#39;usage des tables temporaires est d&#39;interroger ou de joindre de petits jeux de données externes au cours d&#39;une seule session.
:::

ClickHouse prend en charge les tables temporaires, qui présentent les caractéristiques suivantes :

* Les tables temporaires disparaissent lorsque la session se termine, y compris si la connexion est perdue.
* Une table temporaire utilise le moteur de table Memory lorsqu&#39;aucun moteur n&#39;est spécifié, et elle peut utiliser n&#39;importe quel moteur de table à l&#39;exception des moteurs Replicated et `KeeperMap`.
* La DB ne peut pas être spécifiée pour une table temporaire. Elle est créée en dehors des bases de données.
* Il est impossible de créer une table temporaire avec une requête DDL distribuée sur tous les serveurs du cluster (en utilisant `ON CLUSTER`) : cette table n&#39;existe que dans la session en cours.
* Si une table temporaire porte le même nom qu&#39;une autre table et qu&#39;une requête indique le nom de la table sans préciser la DB, la table temporaire sera utilisée.
* Pour le traitement distribué des requêtes, les tables temporaires utilisant le moteur Memory dans une requête sont transmises aux serveurs distants.

Pour créer une table temporaire, utilisez la syntaxe suivante :

```sql
CREATE [OR REPLACE] TEMPORARY TABLE [IF NOT EXISTS] table_name
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) [ENGINE = engine]
```

Dans la plupart des cas, les tables temporaires ne sont pas créées manuellement, mais elles le sont lors de l’utilisation de données externes pour une requête, ou pour un `(GLOBAL) IN` distribué. Pour plus d’informations, voir les sections appropriées

Il est possible d’utiliser des tables avec [ENGINE = Memory](../../../engines/table-engines/special/memory.md) au lieu des tables temporaires.

<div id="replace-table">
  ## REPLACE TABLE
</div>

L’instruction `REPLACE` vous permet de mettre à jour une table [atomiquement](/fr/concepts/glossary#atomicity).

:::note
Cette instruction est prise en charge par les moteurs de base de données [`Atomic`](../../../engines/database-engines/atomic.md) et [`Replicated`](../../../engines/database-engines/replicated.md),
qui sont respectivement les moteurs de base de données par défaut de ClickHouse et de ClickHouse Cloud.
:::

En règle générale, si vous devez supprimer certaines données d’une table,
vous pouvez créer une nouvelle table et la remplir à l’aide d’une instruction `SELECT` qui ne récupère pas les données indésirables,
puis supprimer l’ancienne table et renommer la nouvelle.
Cette approche est illustrée dans l’exemple ci-dessous :

```sql
CREATE TABLE myNewTable AS myOldTable;

INSERT INTO myNewTable
SELECT * FROM myOldTable 
WHERE CounterID <12345;

DROP TABLE myOldTable;

RENAME TABLE myNewTable TO myOldTable;
```

Plutôt que d’utiliser l’approche ci-dessus, il est également possible d’utiliser `REPLACE` (à condition d’utiliser les moteurs de base de données par défaut) pour obtenir le même résultat :

```sql
REPLACE TABLE myOldTable
ENGINE = MergeTree()
ORDER BY CounterID 
AS
SELECT * FROM myOldTable
WHERE CounterID <12345;
```

<div id="syntax">
  ### Syntaxe
</div>

```sql
{CREATE [OR REPLACE] | REPLACE} TABLE [db.]table_name
```

:::note
Toutes les formes de syntaxe de l’instruction `CREATE` s’appliquent également à cette instruction. Appeler `REPLACE` sur une table inexistante entraîne une erreur.
:::

<div id="examples">
  ### Exemples :
</div>

<Tabs>
  <TabItem value="clickhouse_replace_example" label="Local" default>
    Prenons la table suivante :

    ```sql
    CREATE DATABASE base 
    ENGINE = Atomic;

    CREATE OR REPLACE TABLE base.t1
    (
        n UInt64,
        s String
    )
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO base.t1 VALUES (1, 'test');

    SELECT * FROM base.t1;

    ┌─n─┬─s────┐
    │ 1 │ test │
    └───┴──────┘
    ```

    Nous pouvons utiliser l’instruction `REPLACE` pour supprimer toutes les données :

    ```sql
    CREATE OR REPLACE TABLE base.t1 
    (
        n UInt64,
        s Nullable(String)
    )
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO base.t1 VALUES (2, null);

    SELECT * FROM base.t1;

    ┌─n─┬─s──┐
    │ 2 │ \N │
    └───┴────┘
    ```

    Ou nous pouvons utiliser l’instruction `REPLACE` pour modifier la structure de la table :

    ```sql
    REPLACE TABLE base.t1 (n UInt64) 
    ENGINE = MergeTree 
    ORDER BY n;

    INSERT INTO base.t1 VALUES (3);

    SELECT * FROM base.t1;

    ┌─n─┐
    │ 3 │
    └───┘
    ```
  </TabItem>

  <TabItem value="cloud_replace_example" label="Cloud">
    Prenons la table suivante dans ClickHouse Cloud :

    ```sql
    CREATE DATABASE base;

    CREATE OR REPLACE TABLE base.t1 
    (
        n UInt64,
        s String
    )
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO base.t1 VALUES (1, 'test');

    SELECT * FROM base.t1;

    1    test
    ```

    Nous pouvons utiliser l’instruction `REPLACE` pour supprimer toutes les données :

    ```sql
    CREATE OR REPLACE TABLE base.t1 
    (
        n UInt64, 
        s Nullable(String)
    )
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO base.t1 VALUES (2, null);

    SELECT * FROM base.t1;

    2    
    ```

    Ou nous pouvons utiliser l’instruction `REPLACE` pour modifier la structure de la table :

    ```sql
    REPLACE TABLE base.t1 (n UInt64) 
    ENGINE = MergeTree 
    ORDER BY n;

    INSERT INTO base.t1 VALUES (3);

    SELECT * FROM base.t1;

    3
    ```
  </TabItem>
</Tabs>

<div id="comment-clause">
  ## Clause COMMENT
</div>

Vous pouvez ajouter un commentaire à une table au moment de sa création.

**Syntaxe**

```sql
CREATE TABLE [db.]table_name
(
    name1 type1, name2 type2, ...
)
ENGINE = engine
COMMENT 'Comment'
```

:::note
La clause `COMMENT` doit être spécifiée **après** toutes les clauses spécifiques au stockage, telles que `PARTITION BY`, `ORDER BY` et les `SETTINGS` propres au stockage.

Après la clause `COMMENT`, seuls les `SETTINGS` spécifiques aux requêtes (comme `max_threads`, etc.) seront interprétés, et non les paramètres liés au stockage.

Cela signifie que l’ordre correct des clauses est le suivant :

* `ENGINE`
* clauses de stockage
* `COMMENT`
* `SETTINGS` de requête (le cas échéant)
  :::

**Exemple**

```sql title="Query"
CREATE TABLE t1 (x String) ENGINE = Memory COMMENT 'The temporary table';
SELECT name, comment FROM system.tables WHERE name = 't1';
```

```text title="Response"
┌─name─┬─comment─────────────┐
│ t1   │ The temporary table │
└──────┴─────────────────────┘
```

<div id="related-content">
  ## Contenu connexe
</div>

* Blog : [Optimiser ClickHouse avec des schémas et des codecs](https://clickhouse.com/blog/optimize-clickhouse-codecs-compression-schema)
* Blog : [Utiliser des données de séries temporelles dans ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)