---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 35
toc_title: CREATE
---

# Créer des requêtes {#create-queries}

## CREATE DATABASE {#query-language-create-database}

Crée la base de données.

``` sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster] [ENGINE = engine(...)]
```

### Clause {#clauses}

-   `IF NOT EXISTS`
    Si l' `db_name` la base de données existe déjà, alors ClickHouse ne crée pas de nouvelle base de données et:

    -   Ne lance pas d'exception si la clause est spécifiée.
    -   Lève une exception si la clause n'est pas spécifiée.

-   `ON CLUSTER`
    Clickhouse crée le `db_name` base de données sur tous les serveurs d'un cluster spécifié.

-   `ENGINE`

    -   [MySQL](../../engines/database-engines/mysql.md)
        Vous permet de récupérer des données à partir du serveur MySQL distant.
        Par défaut, ClickHouse utilise son propre [moteur de base de données](../../engines/database-engines/index.md).

## CREATE TABLE {#create-table-query}

Le `CREATE TABLE` la requête peut avoir plusieurs formes.

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [compression_codec] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [compression_codec] [TTL expr2],
    ...
) ENGINE = engine
```

Crée une table nommée ‘name’ dans le ‘db’ base de données ou la base de données actuelle si ‘db’ n'est pas définie, avec la structure spécifiée entre parenthèses et l' ‘engine’ moteur.
La structure de la table est une liste de descriptions de colonnes. Si les index sont pris en charge par le moteur, ils sont indiqués comme paramètres pour le moteur de table.

Une description de colonne est `name type` dans le cas le plus simple. Exemple: `RegionID UInt32`.
Des Expressions peuvent également être définies pour les valeurs par défaut (voir ci-dessous).

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name AS [db2.]name2 [ENGINE = engine]
```

Crée une table avec la même structure qu'une autre table. Vous pouvez spécifier un moteur différent pour la table. Si le moteur n'est pas spécifié, le même moteur sera utilisé que pour la `db2.name2` table.

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name AS table_function()
```

Crée une table avec la structure et les données renvoyées par [fonction de table](../table-functions/index.md#table-functions).

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name ENGINE = engine AS SELECT ...
```

Crée une table avec une structure comme le résultat de l' `SELECT` une requête avec les ‘engine’ moteur, et le remplit avec des données de SELECT.

Dans tous les cas, si `IF NOT EXISTS` est spécifié, la requête ne renvoie pas une erreur si la table existe déjà. Dans ce cas, la requête ne font rien.

Il peut y avoir d'autres clauses après le `ENGINE` la clause dans la requête. Voir la documentation détaillée sur la façon de créer des tables dans les descriptions de [moteurs de table](../../engines/table-engines/index.md#table_engines).

### Les Valeurs Par Défaut {#create-default-values}

La description de colonne peut spécifier une expression pour une valeur par défaut, de l'une des manières suivantes:`DEFAULT expr`, `MATERIALIZED expr`, `ALIAS expr`.
Exemple: `URLDomain String DEFAULT domain(URL)`.

Si une expression pour la valeur par défaut n'est pas définie, les valeurs par défaut seront définies sur zéros pour les nombres, chaînes vides pour les chaînes, tableaux vides pour les tableaux et `1970-01-01` pour les dates ou zero unix timestamp pour les dates avec le temps. Les valeurs NULL ne sont pas prises en charge.

Si l'expression par défaut est définie, le type de colonne est facultatif. S'il n'y a pas de type explicitement défini, le type d'expression par défaut est utilisé. Exemple: `EventDate DEFAULT toDate(EventTime)` – the ‘Date’ type sera utilisé pour la ‘EventDate’ colonne.

Si le type de données et l'expression par défaut sont définis explicitement, cette expression sera convertie au type spécifié à l'aide des fonctions de conversion de type. Exemple: `Hits UInt32 DEFAULT 0` signifie la même chose que `Hits UInt32 DEFAULT toUInt32(0)`.

Default expressions may be defined as an arbitrary expression from table constants and columns. When creating and changing the table structure, it checks that expressions don't contain loops. For INSERT, it checks that expressions are resolvable – that all columns they can be calculated from have been passed.

`DEFAULT expr`

Valeur par défaut normale. Si la requête INSERT ne spécifie pas la colonne correspondante, elle sera remplie en calculant l'expression correspondante.

`MATERIALIZED expr`

Expression matérialisée. Une telle colonne ne peut pas être spécifiée pour INSERT, car elle est toujours calculée.
Pour un INSERT sans Liste de colonnes, ces colonnes ne sont pas prises en compte.
De plus, cette colonne n'est pas substituée lors de l'utilisation d'un astérisque dans une requête SELECT. C'est pour préserver l'invariant que le dump obtenu en utilisant `SELECT *` peut être inséré dans la table en utilisant INSERT sans spécifier la liste des colonnes.

`ALIAS expr`

Synonyme. Une telle colonne n'est pas du tout stockée dans la table.
Ses valeurs ne peuvent pas être insérées dans une table et elles ne sont pas substituées lors de l'utilisation d'un astérisque dans une requête SELECT.
Il peut être utilisé dans SELECTs si l'alias est développé pendant l'analyse des requêtes.

Lorsque vous utilisez la requête ALTER pour ajouter de nouvelles colonnes, les anciennes données de ces colonnes ne sont pas écrites. Au lieu de cela, lors de la lecture d'anciennes données qui n'ont pas de valeurs pour les nouvelles colonnes, les expressions sont calculées à la volée par défaut. Cependant, si l'exécution des expressions nécessite différentes colonnes qui ne sont pas indiquées dans la requête, ces colonnes seront en outre lues, mais uniquement pour les blocs de données qui en ont besoin.

Si vous ajoutez une nouvelle colonne à une table mais modifiez ultérieurement son expression par défaut, les valeurs utilisées pour les anciennes données changeront (pour les données où les valeurs n'ont pas été stockées sur le disque). Notez que lors de l'exécution de fusions d'arrière-plan, les données des colonnes manquantes dans l'une des parties de fusion sont écrites dans la partie fusionnée.

Il n'est pas possible de définir des valeurs par défaut pour les éléments dans les structures de données.

### Contraintes {#constraints}

Avec les descriptions de colonnes des contraintes peuvent être définies:

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [compression_codec] [TTL expr1],
    ...
    CONSTRAINT constraint_name_1 CHECK boolean_expr_1,
    ...
) ENGINE = engine
```

`boolean_expr_1` pourrait par n'importe quelle expression booléenne. Si les contraintes sont définies pour la table, chacun d'eux sera vérifiée pour chaque ligne `INSERT` query. If any constraint is not satisfied — server will raise an exception with constraint name and checking expression.

L'ajout d'une grande quantité de contraintes peut affecter négativement les performances de big `INSERT` requête.

### Expression TTL {#ttl-expression}

Définit la durée de stockage des valeurs. Peut être spécifié uniquement pour les tables mergetree-family. Pour la description détaillée, voir [TTL pour les colonnes et les tableaux](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl).

### Codecs De Compression De Colonne {#codecs}

Par défaut, ClickHouse applique le `lz4` méthode de compression. Pour `MergeTree`- famille de moteurs Vous pouvez modifier la méthode de compression par défaut dans le [compression](../../operations/server-configuration-parameters/settings.md#server-settings-compression) section d'une configuration de serveur. Vous pouvez également définir la méthode de compression pour chaque colonne `CREATE TABLE` requête.

``` sql
CREATE TABLE codec_example
(
    dt Date CODEC(ZSTD),
    ts DateTime CODEC(LZ4HC),
    float_value Float32 CODEC(NONE),
    double_value Float64 CODEC(LZ4HC(9))
    value Float32 CODEC(Delta, ZSTD)
)
ENGINE = <Engine>
...
```

Si un codec est spécifié, le codec par défaut ne s'applique pas. Les Codecs peuvent être combinés dans un pipeline, par exemple, `CODEC(Delta, ZSTD)`. Pour sélectionner la meilleure combinaison de codecs pour votre projet, passez des benchmarks similaires à ceux décrits dans Altinity [Nouveaux encodages pour améliorer L'efficacité du ClickHouse](https://www.altinity.com/blog/2019/7/new-encodings-to-improve-clickhouse) article.

!!! warning "Avertissement"
    Vous ne pouvez pas décompresser les fichiers de base de données ClickHouse avec des utilitaires externes tels que `lz4`. Au lieu de cela, utilisez le spécial [clickhouse-compresseur](https://github.com/ClickHouse/ClickHouse/tree/master/programs/compressor) utilitaire.

La Compression est prise en charge pour les moteurs de tableau suivants:

-   [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) famille. Prend en charge les codecs de compression de colonne et la sélection de la méthode de compression par défaut par [compression](../../operations/server-configuration-parameters/settings.md#server-settings-compression) paramètre.
-   [Journal](../../engines/table-engines/log-family/index.md) famille. Utilise le `lz4` méthode de compression par défaut et prend en charge les codecs de compression de colonne.
-   [Définir](../../engines/table-engines/special/set.md). Uniquement pris en charge la compression par défaut.
-   [Rejoindre](../../engines/table-engines/special/join.md). Uniquement pris en charge la compression par défaut.

ClickHouse prend en charge les codecs à usage commun et les codecs spécialisés.

#### Codecs Spécialisés {#create-query-specialized-codecs}

Ces codecs sont conçus pour rendre la compression plus efficace en utilisant des fonctionnalités spécifiques des données. Certains de ces codecs ne compressent pas les données eux-mêmes. Au lieu de cela, ils préparent les données pour un codec à usage commun, qui les compresse mieux que sans cette préparation.

Spécialisé codecs:

-   `Delta(delta_bytes)` — Compression approach in which raw values are replaced by the difference of two neighboring values, except for the first value that stays unchanged. Up to `delta_bytes` sont utilisés pour stocker des valeurs delta, donc `delta_bytes` est la taille maximale des valeurs brutes. Possible `delta_bytes` valeurs: 1, 2, 4, 8. La valeur par défaut pour `delta_bytes` être `sizeof(type)` si égale à 1, 2, 4 ou 8. Dans tous les autres cas, c'est 1.
-   `DoubleDelta` — Calculates delta of deltas and writes it in compact binary form. Optimal compression rates are achieved for monotonic sequences with a constant stride, such as time series data. Can be used with any fixed-width type. Implements the algorithm used in Gorilla TSDB, extending it to support 64-bit types. Uses 1 extra bit for 32-byte deltas: 5-bit prefixes instead of 4-bit prefixes. For additional information, see Compressing Time Stamps in [Gorilla: Une Base De Données De Séries Chronologiques Rapide, Évolutive Et En Mémoire](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf).
-   `Gorilla` — Calculates XOR between current and previous value and writes it in compact binary form. Efficient when storing a series of floating point values that change slowly, because the best compression rate is achieved when neighboring values are binary equal. Implements the algorithm used in Gorilla TSDB, extending it to support 64-bit types. For additional information, see Compressing Values in [Gorilla: Une Base De Données De Séries Chronologiques Rapide, Évolutive Et En Mémoire](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf).
-   `T64` — Compression approach that crops unused high bits of values in integer data types (including `Enum`, `Date` et `DateTime`). À chaque étape de son algorithme, le codec prend un bloc de 64 valeurs, les place dans une matrice de 64x64 bits, le transpose, recadre les bits de valeurs inutilisés et renvoie le reste sous forme de séquence. Les bits inutilisés sont les bits, qui ne diffèrent pas entre les valeurs maximum et minimum dans la partie de données entière pour laquelle la compression est utilisée.

`DoubleDelta` et `Gorilla` les codecs sont utilisés dans Gorilla TSDB comme composants de son algorithme de compression. L'approche Gorilla est efficace dans les scénarios où il y a une séquence de valeurs qui changent lentement avec leurs horodatages. Les horodatages sont effectivement compressés par le `DoubleDelta` codec, et les valeurs sont effectivement comprimé par le `Gorilla` codec. Par exemple, pour obtenir une table stockée efficacement, vous pouvez la créer dans la configuration suivante:

``` sql
CREATE TABLE codec_example
(
    timestamp DateTime CODEC(DoubleDelta),
    slow_values Float32 CODEC(Gorilla)
)
ENGINE = MergeTree()
```

#### Codecs À Usage Général {#create-query-general-purpose-codecs}

Codec:

-   `NONE` — No compression.
-   `LZ4` — Lossless [algorithme de compression de données](https://github.com/lz4/lz4) utilisé par défaut. Applique la compression rapide LZ4.
-   `LZ4HC[(level)]` — LZ4 HC (high compression) algorithm with configurable level. Default level: 9. Setting `level <= 0` s'applique le niveau par défaut. Niveaux possibles: \[1, 12\]. Plage de niveau recommandée: \[4, 9\].
-   `ZSTD[(level)]` — [Algorithme de compression ZSTD](https://en.wikipedia.org/wiki/Zstandard) avec configurables `level`. Niveaux possibles: \[1, 22\]. Valeur par défaut: 1.

Des niveaux de compression élevés sont utiles pour les scénarios asymétriques, comme compresser une fois, décompresser à plusieurs reprises. Des niveaux plus élevés signifient une meilleure compression et une utilisation plus élevée du processeur.

## Les Tables Temporaires {#temporary-tables}

Clickhouse prend en charge les tables temporaires qui ont les caractéristiques suivantes:

-   Les tables temporaires disparaissent à la fin de la session, y compris si la connexion est perdue.
-   Une table temporaire utilise uniquement le moteur de mémoire.
-   La base de données ne peut pas être spécifiée pour une table temporaire. Il est créé en dehors des bases de données.
-   Impossible de créer une table temporaire avec une requête DDL distribuée sur tous les serveurs de cluster (en utilisant `ON CLUSTER`): ce tableau n'existe que dans la session en cours.
-   Si une table temporaire a le même nom qu'une autre et qu'une requête spécifie le nom de la table sans spécifier la base de données, la table temporaire sera utilisée.
-   Pour le traitement des requêtes distribuées, les tables temporaires utilisées dans une requête sont transmises à des serveurs distants.

Pour créer une table temporaire, utilisez la syntaxe suivante:

``` sql
CREATE TEMPORARY TABLE [IF NOT EXISTS] table_name
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
)
```

Dans la plupart des cas, les tables temporaires ne sont pas créées manuellement, mais lors de l'utilisation de données externes pour une requête ou pour `(GLOBAL) IN`. Pour plus d'informations, consultez les sections appropriées

Il est possible d'utiliser des tables avec [Moteur = mémoire](../../engines/table-engines/special/memory.md) au lieu de tables temporaires.

## Requêtes DDL distribuées (sur la clause CLUSTER) {#distributed-ddl-queries-on-cluster-clause}

Le `CREATE`, `DROP`, `ALTER`, et `RENAME` les requêtes prennent en charge l'exécution distribuée sur un cluster.
Par exemple, la requête suivante crée la `all_hits` `Distributed` tableau sur chaque ordinateur hôte `cluster`:

``` sql
CREATE TABLE IF NOT EXISTS all_hits ON CLUSTER cluster (p Date, i Int32) ENGINE = Distributed(cluster, default, hits)
```

Pour exécuter ces requêtes correctement, chaque hôte doit avoir la même définition de cluster (pour simplifier la synchronisation des configs, vous pouvez utiliser des substitutions de ZooKeeper). Ils doivent également se connecter aux serveurs ZooKeeper.
La version locale de la requête sera finalement implémentée sur chaque hôte du cluster, même si certains hôtes ne sont actuellement pas disponibles. L'ordre d'exécution des requêtes au sein d'un seul hôte est garanti.

## CREATE VIEW {#create-view}

``` sql
CREATE [MATERIALIZED] VIEW [IF NOT EXISTS] [db.]table_name [TO[db.]name] [ENGINE = engine] [POPULATE] AS SELECT ...
```

Crée une vue. Il existe deux types de vues: normale et matérialisée.

Les vues normales ne stockent aucune donnée, mais effectuent simplement une lecture à partir d'une autre table. En d'autres termes, une vue normale n'est rien de plus qu'une requête enregistrée. Lors de la lecture à partir d'une vue, cette requête enregistrée est utilisée comme sous-requête dans la clause FROM.

Par exemple, supposons que vous avez créé une vue:

``` sql
CREATE VIEW view AS SELECT ...
```

et écrit une requête:

``` sql
SELECT a, b, c FROM view
```

Cette requête est entièrement équivalente à l'utilisation de la sous requête:

``` sql
SELECT a, b, c FROM (SELECT ...)
```

Les vues matérialisées stockent les données transformées par la requête SELECT correspondante.

Lors de la création d'une vue matérialisée sans `TO [db].[table]`, you must specify ENGINE – the table engine for storing data.

Lors de la création d'une vue matérialisée avec `TO [db].[table]` vous ne devez pas utiliser `POPULATE`.

Une vue matérialisée est agencée comme suit: lors de l'insertion de données dans la table spécifiée dans SELECT, une partie des données insérées est convertie par cette requête SELECT, et le résultat est inséré dans la vue.

Si vous spécifiez POPULATE, les données de table existantes sont insérées dans la vue lors de sa création, comme si `CREATE TABLE ... AS SELECT ...` . Sinon, la requête ne contient que les données insérées dans la table après la création de la vue. Nous ne recommandons pas D'utiliser POPULATE, car les données insérées dans la table lors de la création de la vue ne seront pas insérées dedans.

A `SELECT` la requête peut contenir `DISTINCT`, `GROUP BY`, `ORDER BY`, `LIMIT`… Note that the corresponding conversions are performed independently on each block of inserted data. For example, if `GROUP BY` est définie, les données sont agrégées lors de l'insertion, mais uniquement dans un seul paquet de données insérées. Les données ne seront pas agrégées davantage. L'exception concerne l'utilisation d'un moteur qui effectue indépendamment l'agrégation de données, par exemple `SummingMergeTree`.

L'exécution de `ALTER` les requêtes sur les vues matérialisées n'ont pas été complètement développées, elles pourraient donc être gênantes. Si la vue matérialisée utilise la construction `TO [db.]name` vous pouvez `DETACH` la vue, exécutez `ALTER` pour la table cible, puis `ATTACH` précédemment détaché (`DETACH`) vue.

Les vues ressemblent aux tables normales. Par exemple, ils sont répertoriés dans le résultat de la `SHOW TABLES` requête.

Il n'y a pas de requête séparée pour supprimer des vues. Pour supprimer une vue, utilisez `DROP TABLE`.

## CREATE DICTIONARY {#create-dictionary-query}

``` sql
CREATE DICTIONARY [IF NOT EXISTS] [db.]dictionary_name [ON CLUSTER cluster]
(
    key1 type1  [DEFAULT|EXPRESSION expr1] [HIERARCHICAL|INJECTIVE|IS_OBJECT_ID],
    key2 type2  [DEFAULT|EXPRESSION expr2] [HIERARCHICAL|INJECTIVE|IS_OBJECT_ID],
    attr1 type2 [DEFAULT|EXPRESSION expr3],
    attr2 type2 [DEFAULT|EXPRESSION expr4]
)
PRIMARY KEY key1, key2
SOURCE(SOURCE_NAME([param1 value1 ... paramN valueN]))
LAYOUT(LAYOUT_NAME([param_name param_value]))
LIFETIME([MIN val1] MAX val2)
```

Crée [externe dictionnaire](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md) avec le [structure](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md), [source](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md), [disposition](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md) et [vie](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md).

Structure de dictionnaire externe se compose d'attributs. Les attributs du dictionnaire sont spécifiés de la même manière que les colonnes du tableau. La seule propriété d'attribut requise est son type, toutes les autres propriétés peuvent avoir des valeurs par défaut.

Selon le dictionnaire [disposition](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md) un ou plusieurs attributs peuvent être spécifiés comme les clés de dictionnaire.

Pour plus d'informations, voir [Dictionnaires Externes](../dictionaries/external-dictionaries/external-dicts.md) section.

## CREATE USER {#create-user-statement}

Crée un [compte d'utilisateur](../../operations/access-rights.md#user-account-management).

### Syntaxe {#create-user-syntax}

``` sql
CREATE USER [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [IDENTIFIED [WITH {NO_PASSWORD|PLAINTEXT_PASSWORD|SHA256_PASSWORD|SHA256_HASH|DOUBLE_SHA1_PASSWORD|DOUBLE_SHA1_HASH}] BY {'password'|'hash'}]
    [HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [DEFAULT ROLE role [,...]]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```

#### Identification {#identification}

Il existe de multiples façons d'identification d'un utilisateur:

-   `IDENTIFIED WITH no_password`
-   `IDENTIFIED WITH plaintext_password BY 'qwerty'`
-   `IDENTIFIED WITH sha256_password BY 'qwerty'` ou `IDENTIFIED BY 'password'`
-   `IDENTIFIED WITH sha256_hash BY 'hash'`
-   `IDENTIFIED WITH double_sha1_password BY 'qwerty'`
-   `IDENTIFIED WITH double_sha1_hash BY 'hash'`

#### L'Utilisateur De L'Hôte {#user-host}

L'hôte utilisateur est un hôte à partir duquel une connexion au serveur ClickHouse peut être établie. Hôte peut être spécifié dans le `HOST` section de requête par les moyens suivants:

-   `HOST IP 'ip_address_or_subnetwork'` — User can connect to ClickHouse server only from the specified IP address or a [sous-réseau](https://en.wikipedia.org/wiki/Subnetwork). Exemple: `HOST IP '192.168.0.0/16'`, `HOST IP '2001:DB8::/32'`. Pour une utilisation en production, spécifiez uniquement `HOST IP` (adresses IP et leurs masques), depuis l'utilisation `host` et `host_regexp` peut causer une latence supplémentaire.
-   `HOST ANY` — User can connect from any location. This is default option.
-   `HOST LOCAL` — User can connect only locally.
-   `HOST NAME 'fqdn'` — User host can be specified as FQDN. For example, `HOST NAME 'mysite.com'`.
-   `HOST NAME REGEXP 'regexp'` — You can use [pcre](http://www.pcre.org/) expressions régulières lors de la spécification des hôtes utilisateur. Exemple, `HOST NAME REGEXP '.*\.mysite\.com'`.
-   `HOST LIKE 'template'` — Allows you use the [LIKE](../functions/string-search-functions.md#function-like) opérateur de filtre de l'utilisateur hôtes. Exemple, `HOST LIKE '%'` est équivalent à `HOST ANY`, `HOST LIKE '%.mysite.com'` filtre tous les hôtes dans le `mysite.com` domaine.

Une autre façon de spécifier l'hôte est d'utiliser `@` syntaxe avec le nom d'utilisateur. Exemple:

-   `CREATE USER mira@'127.0.0.1'` — Equivalent to the `HOST IP` syntaxe.
-   `CREATE USER mira@'localhost'` — Equivalent to the `HOST LOCAL` syntaxe.
-   `CREATE USER mira@'192.168.%.%'` — Equivalent to the `HOST LIKE` syntaxe.

!!! info "Avertissement"
    Clickhouse traite `user_name@'address'` comme un nom d'utilisateur dans son ensemble. Donc, techniquement, vous pouvez créer plusieurs utilisateurs avec `user_name` et différentes constructions après `@`. Nous ne recommandons pas de le faire.

### Exemple {#create-user-examples}

Créer le compte d'utilisateur `mira` protégé par le mot de passe `qwerty`:

``` sql
CREATE USER mira HOST IP '127.0.0.1' IDENTIFIED WITH sha256_password BY 'qwerty'
```

`mira` devrait démarrer l'application client sur l'hôte où le serveur ClickHouse s'exécute.

Créer le compte d'utilisateur `john`, attribuez-lui des rôles et définissez ces rôles par défaut:

``` sql
CREATE USER john DEFAULT ROLE role1, role2
```

Créer le compte d'utilisateur `john` et faire tous ses futurs rôles par défaut:

``` sql
ALTER USER user DEFAULT ROLE ALL
```

Quand un rôle sera attribué à `john` dans l'avenir, il deviendra automatiquement par défaut.

Créer le compte d'utilisateur `john` et faire tous ses futurs rôles par défaut sauf `role1` et `role2`:

``` sql
ALTER USER john DEFAULT ROLE ALL EXCEPT role1, role2
```

## CREATE ROLE {#create-role-statement}

Crée un [rôle](../../operations/access-rights.md#role-management).

### Syntaxe {#create-role-syntax}

``` sql
CREATE ROLE [IF NOT EXISTS | OR REPLACE] name
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```

### Description {#create-role-description}

Rôle est un ensemble de [privilège](grant.md#grant-privileges). Un utilisateur reçoit un rôle obtient tous les privilèges de ce rôle.

Un utilisateur peut être affecté à plusieurs rôles. Les utilisateurs peuvent appliquer leurs rôles accordés dans des combinaisons arbitraires par le [SET ROLE](misc.md#set-role-statement) déclaration. La finale de la portée des privilèges est un ensemble combiné de tous les privilèges de tous les rôles. Si un utilisateur a des privilèges accordés directement à son compte d'utilisateur, ils sont également combinés avec les privilèges accordés par les rôles.

L'utilisateur peut avoir des rôles par défaut qui s'appliquent à la connexion de l'utilisateur. Pour définir les rôles par défaut, utilisez [SET DEFAULT ROLE](misc.md#set-default-role-statement) - déclaration ou de la [ALTER USER](alter.md#alter-user-statement) déclaration.

Pour révoquer un rôle, utilisez [REVOKE](revoke.md) déclaration.

Pour supprimer le rôle, utilisez [DROP ROLE](misc.md#drop-role-statement) déclaration. Le rôle supprimé est automatiquement révoqué de tous les utilisateurs et rôles auxquels il a été accordé.

### Exemple {#create-role-examples}

``` sql
CREATE ROLE accountant;
GRANT SELECT ON db.* TO accountant;
```

Cette séquence de requêtes crée le rôle `accountant` cela a le privilège de lire les données du `accounting` la base de données.

Octroi du rôle à l'utilisateur `mira`:

``` sql
GRANT accountant TO mira;
```

Une fois le rôle accordé, l'utilisateur peut l'utiliser et effectuer les requêtes autorisées. Exemple:

``` sql
SET ROLE accountant;
SELECT * FROM db.*;
```

## CREATE ROW POLICY {#create-row-policy-statement}

Crée un [filtre pour les lignes](../../operations/access-rights.md#row-policy-management) qu'un utilisateur peut lire à partir d'une table.

### Syntaxe {#create-row-policy-syntax}

``` sql
CREATE [ROW] POLICY [IF NOT EXISTS | OR REPLACE] policy_name [ON CLUSTER cluster_name] ON [db.]table
    [AS {PERMISSIVE | RESTRICTIVE}]
    [FOR SELECT]
    [USING condition]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

#### Section AS {#create-row-policy-as}

À l'aide de cette section, vous pouvez créer des stratégies permissives ou restrictives.

La stratégie Permissive accorde l'accès aux lignes. Les stratégies permissives qui s'appliquent à la même table sont combinées ensemble en utilisant le booléen `OR` opérateur. Les stratégies sont permissives par défaut.

La politique Restrictive limite l'accès à la ligne. Les politiques restrictives qui s'appliquent à la même table sont combinées en utilisant le booléen `AND` opérateur.

Les stratégies restrictives s'appliquent aux lignes qui ont passé les filtres permissifs. Si vous définissez des stratégies restrictives mais aucune politique permissive, l'utilisateur ne peut obtenir aucune ligne de la table.

#### La Section DE {#create-row-policy-to}

Dans la section `TO` vous pouvez donner une liste mixte de rôles et d'utilisateurs, par exemple, `CREATE ROW POLICY ... TO accountant, john@localhost`.

Mot `ALL` signifie Tous les utilisateurs de ClickHouse, y compris l'utilisateur actuel. Mot `ALL EXCEPT` autoriser à exclure certains utilisateurs de la liste tous les utilisateurs, par exemple `CREATE ROW POLICY ... TO ALL EXCEPT accountant, john@localhost`

### Exemple {#examples}

-   `CREATE ROW POLICY filter ON mydb.mytable FOR SELECT USING a<1000 TO accountant, john@localhost`
-   `CREATE ROW POLICY filter ON mydb.mytable FOR SELECT USING a<1000 TO ALL EXCEPT mira`

## CREATE QUOTA {#create-quota-statement}

Crée un [quota](../../operations/access-rights.md#quotas-management) qui peut être attribué à un utilisateur ou un rôle.

### Syntaxe {#create-quota-syntax}

``` sql
CREATE QUOTA [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [KEYED BY {'none' | 'user name' | 'ip address' | 'client key' | 'client key or user name' | 'client key or ip address'}]
    [FOR [RANDOMIZED] INTERVAL number {SECOND | MINUTE | HOUR | DAY}
        {MAX { {QUERIES | ERRORS | RESULT ROWS | RESULT BYTES | READ ROWS | READ BYTES | EXECUTION TIME} = number } [,...] |
         NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

### Exemple {#create-quota-example}

Limiter le nombre maximum de requêtes pour l'utilisateur actuel avec 123 requêtes en 15 mois contrainte:

``` sql
CREATE QUOTA qA FOR INTERVAL 15 MONTH MAX QUERIES 123 TO CURRENT_USER
```

## CREATE SETTINGS PROFILE {#create-settings-profile-statement}

Crée un [les paramètres de profil](../../operations/access-rights.md#settings-profiles-management) qui peut être attribué à un utilisateur ou un rôle.

### Syntaxe {#create-settings-profile-syntax}

``` sql
CREATE SETTINGS PROFILE [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | INHERIT 'profile_name'] [,...]
```

# Exemple {#create-settings-profile-syntax}

Créer l' `max_memory_usage_profile` paramètres du profil avec valeur et contraintes pour `max_memory_usage` paramètre. L'affecter à `robin`:

``` sql
CREATE SETTINGS PROFILE max_memory_usage_profile SETTINGS max_memory_usage = 100000001 MIN 90000000 MAX 110000000 TO robin
```

[Article Original](https://clickhouse.tech/docs/en/query_language/create/) <!--hide-->
