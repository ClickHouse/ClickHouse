---
description: 'Documentation de SHOW'
sidebar_label: 'SHOW'
sidebar_position: 37
slug: /sql-reference/statements/show
title: 'Instructions SHOW'
doc_type: 'reference'
---

:::note

`SHOW CREATE (TABLE|DATABASE|USER)` masque les secrets, sauf si les paramètres suivants sont activés :

* [`display_secrets_in_show_and_select`](../../operations/server-configuration-parameters/settings/#display_secrets_in_show_and_select) (paramètre serveur)
* [`format_display_secrets_in_show_and_select` ](../../operations/settings/formats/#format_display_secrets_in_show_and_select) (paramètre de format)

De plus, l’utilisateur doit disposer du privilège [`displaySecretsInShowAndSelect`](grant.md/#displaysecretsinshowandselect).
:::

<div id="show-create-table--dictionary--view--database">
  ## SHOW CREATE TABLE | DICTIONARY | VIEW | DATABASE
</div>

Ces instructions renvoient une seule colonne de type String,
contenant la requête `CREATE` utilisée pour créer l&#39;objet spécifié.

<div id="syntax">
  ### Syntaxe
</div>

```sql title="Syntax"
SHOW [CREATE] TABLE | TEMPORARY TABLE | DICTIONARY | VIEW | DATABASE [db.]table|view [INTO OUTFILE filename] [FORMAT format]
```

:::note
Si vous utilisez cette instruction pour obtenir la requête `CREATE` des tables système,
vous obtiendrez une requête *fictive* qui ne fait que déclarer la structure de la table,
mais qui ne peut pas être utilisée pour créer une table.
:::

<div id="show-databases">
  ## SHOW DATABASES
</div>

Cette instruction affiche la liste de toutes les bases de données.

<div id="syntax">
  ### Syntaxe
</div>

```sql title="Syntax"
SHOW DATABASES [[NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE filename] [FORMAT format]
```

Elle est identique à la requête :

```sql
SELECT name FROM system.databases [WHERE name [NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE filename] [FORMAT format]
```

<div id="examples">
  ### Exemples
</div>

Dans cet exemple, nous utilisons `SHOW` pour obtenir les noms des bases de données dont le nom contient la séquence de caractères &#39;de&#39; :

```sql title="Query"
SHOW DATABASES LIKE '%de%'
```

```text title="Response"
┌─name────┐
│ default │
└─────────┘
```

Nous pouvons également le faire sans tenir compte de la casse :

```sql title="Query"
SHOW DATABASES ILIKE '%DE%'
```

```text title="Response"
┌─name────┐
│ default │
└─────────┘
```

Ou obtenez les noms des bases de données ne contenant pas &#39;de&#39; :

```sql title="Query"
SHOW DATABASES NOT LIKE '%de%'
```

```text title="Response"
┌─name───────────────────────────┐
│ _temporary_and_external_tables │
│ system                         │
│ test                           │
│ tutorial                       │
└────────────────────────────────┘
```

Enfin, nous pouvons n’obtenir que les noms des deux premières bases de données :

```sql title="Query"
SHOW DATABASES LIMIT 2
```

```text title="Response"
┌─name───────────────────────────┐
│ _temporary_and_external_tables │
│ default                        │
└────────────────────────────────┘
```

<div id="see-also">
  ### Voir aussi
</div>

* [`CREATE DATABASE`](/fr/sql-reference/statements/create/database)

<div id="show-tables">
  ## SHOW TABLES
</div>

L’instruction `SHOW TABLES` affiche la liste des tables.

<div id="syntax">
  ### Syntaxe
</div>

```sql title="Syntax"
SHOW [FULL] [TEMPORARY] TABLES [{FROM | IN} <db>] [[NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

Si la clause `FROM` n’est pas spécifiée, la requête renvoie la liste des tables de la base de données courante.

Cette instruction est identique à la requête :

```sql
SELECT name FROM system.tables [WHERE name [NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

<div id="examples">
  ### Exemples
</div>

Dans cet exemple, nous utilisons l’instruction `SHOW TABLES` pour répertorier toutes les tables dont le nom contient &#39;user&#39; :

```sql title="Query"
SHOW TABLES FROM system LIKE '%user%'
```

```text title="Response"
┌─name─────────────┐
│ user_directories │
│ users            │
└──────────────────┘
```

Nous pouvons également le faire sans tenir compte de la casse :

```sql title="Query"
SHOW TABLES FROM system ILIKE '%USER%'
```

```text title="Response"
┌─name─────────────┐
│ user_directories │
│ users            │
└──────────────────┘
```

Ou pour trouver des tables dont le nom ne contient pas la lettre &#39;s&#39; :

```sql title="Query"
SHOW TABLES FROM system NOT LIKE '%s%'
```

```text title="Response"
┌─name─────────┐
│ metric_log   │
│ metric_log_0 │
│ metric_log_1 │
└──────────────┘
```

Enfin, nous pouvons récupérer uniquement les noms des deux premières tables :

```sql title="Query"
SHOW TABLES FROM system LIMIT 2
```

```text title="Response"
┌─name───────────────────────────┐
│ aggregate_function_combinators │
│ asynchronous_metric_log        │
└────────────────────────────────┘
```

<div id="see-also">
  ### Voir aussi
</div>

* [`Create Tables`](/fr/sql-reference/statements/create/table)
* [`SHOW CREATE TABLE`](#show-create-table--dictionary--view--database)

<div id="show_columns">
  ## SHOW COLUMNS
</div>

L’instruction `SHOW COLUMNS` affiche la liste des colonnes.

<div id="syntax">
  ### Syntaxe
</div>

```sql title="Syntax"
SHOW [EXTENDED] [FULL] COLUMNS {FROM | IN} <table> [{FROM | IN} <db>] [{[NOT] {LIKE | ILIKE} '<pattern>' | WHERE <expr>}] [LIMIT <N>] [INTO
OUTFILE <filename>] [FORMAT <format>]
```

Le nom de la base de données et de la table peut être indiqué sous forme abrégée : `<db>.<table>`,
ce qui signifie que `FROM tab FROM db` et `FROM db.tab` sont équivalents.
Si aucune base de données n&#39;est indiquée, la requête renvoie la liste des colonnes de la base de données courante.

Il existe également deux mots-clés facultatifs : `EXTENDED` et `FULL`. Le mot-clé `EXTENDED` n&#39;a actuellement aucun effet
et existe pour assurer la compatibilité avec MySQL. Le mot-clé `FULL` fait en sorte que la sortie inclue les colonnes de collation, de commentaire et de privilège.

L&#39;instruction `SHOW COLUMNS` produit un tableau de résultats avec la structure suivante :

| Colonne     | Description                                                                                                                                                 | Type               |
| ----------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------ |
| `field`     | Le nom de la colonne                                                                                                                                        | `String`           |
| `type`      | Le type de données de la colonne. Si la requête a été effectuée via le MySQL wire protocol, le nom de type MySQL équivalent est affiché.                    | `String`           |
| `null`      | `YES` si le type de données de la colonne est Nullable, `NO` sinon                                                                                          | `String`           |
| `key`       | `PRI` si la colonne fait partie de la clé primaire, `SOR` si la colonne fait partie de la clé de tri, sinon vide                                            | `String`           |
| `default`   | Expression par défaut de la colonne si elle est de type `ALIAS`, `DEFAULT` ou `MATERIALIZED`, sinon `NULL`.                                                 | `Nullable(String)` |
| `extra`     | Informations supplémentaires, actuellement inutilisées                                                                                                      | `String`           |
| `collation` | (uniquement si le mot-clé `FULL` a été spécifié) Collation de la colonne, toujours `NULL`, car ClickHouse ne prend pas en charge les collations par colonne | `Nullable(String)` |
| `comment`   | (uniquement si le mot-clé `FULL` a été spécifié) Commentaire de la colonne                                                                                  | `String`           |
| `privilege` | (uniquement si le mot-clé `FULL` a été spécifié) Le privilège dont vous disposez sur cette colonne, actuellement indisponible                               | `String`           |

<div id="examples">
  ### Exemples
</div>

Dans cet exemple, nous utiliserons l’instruction `SHOW COLUMNS` pour obtenir des informations sur toutes les colonnes de la table &#39;orders&#39;,
commençant par &#39;delivery&#95;&#39;:

```sql title="Query"
SHOW COLUMNS FROM 'orders' LIKE 'delivery_%'
```

```text title="Response"
┌─field───────────┬─type─────┬─null─┬─key─────┬─default─┬─extra─┐
│ delivery_date   │ DateTime │    0 │ PRI SOR │ ᴺᵁᴸᴸ    │       │
│ delivery_status │ Bool     │    0 │         │ ᴺᵁᴸᴸ    │       │
└─────────────────┴──────────┴──────┴─────────┴─────────┴───────┘
```

<div id="see-also">
  ### Voir aussi
</div>

* [`system.columns`](../../operations/system-tables/columns.md)

<div id="show-dictionaries">
  ## SHOW DICTIONARIES
</div>

L’instruction `SHOW DICTIONARIES` affiche la liste des [Dictionnaires](./create/dictionary/overview.md).

<div id="syntax">
  ### Syntaxe
</div>

```sql title="Syntax"
SHOW DICTIONARIES [FROM <db>] [LIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

Si la clause `FROM` n’est pas spécifiée, la requête renvoie la liste des dictionnaires de la base de données courante.

Vous pouvez obtenir le même résultat qu’avec la requête `SHOW DICTIONARIES` de la manière suivante :

```sql
SELECT name FROM system.dictionaries WHERE database = <db> [AND name LIKE <pattern>] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

<div id="examples">
  ### Exemples
</div>

La requête suivante sélectionne les deux premières lignes de la liste des tables de la base de données `system` dont le nom contient `reg`.

```sql title="Query"
SHOW DICTIONARIES FROM db LIKE '%reg%' LIMIT 2
```

```text title="Response"
┌─name─────────┐
│ regions      │
│ region_names │
└──────────────┘
```

<div id="show-index">
  ## SHOW INDEX
</div>

Affiche la liste des clés primaires et des indexes de saut de données d&#39;une table.

Cette instruction existe principalement pour assurer la compatibilité avec MySQL. Les tables système [`system.tables`](../../operations/system-tables/tables.md) (pour les
clés primaires) et [`system.data_skipping_indices`](../../operations/system-tables/data_skipping_indices.md) (pour les indexes de saut de données)
fournissent des informations équivalentes, mais sous une forme plus naturelle pour ClickHouse.

<div id="syntax">
  ### Syntaxe
</div>

```sql title="Syntax"
SHOW [EXTENDED] {INDEX | INDEXES | INDICES | KEYS } {FROM | IN} <table> [{FROM | IN} <db>] [WHERE <expr>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

Les noms de la base de données et de la table peuvent être indiqués sous forme abrégée sous la forme `<db>.<table>` ; autrement dit, `FROM tab FROM db` et `FROM db.tab` sont
équivalents. Si aucune base de données n&#39;est spécifiée, la requête utilise la base de données courante.

Le mot-clé facultatif `EXTENDED` n&#39;a actuellement aucun effet et est présent pour la compatibilité avec MySQL.

L&#39;instruction produit une table de résultats ayant la structure suivante :

| Colonne         | Description                                                                                                                                       | Type               |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------ |
| `table`         | Le nom de la table.                                                                                                                               | `String`           |
| `non_unique`    | Toujours `1`, car ClickHouse ne prend pas en charge les contraintes d&#39;unicité.                                                                | `UInt8`            |
| `key_name`      | Le nom de l&#39;index, `PRIMARY` si l&#39;index est un index de clé primaire.                                                                     | `String`           |
| `seq_in_index`  | Pour un index de clé primaire, la position de la colonne à partir de `1`. Pour un index de saut de données : toujours `1`.                        | `UInt8`            |
| `column_name`   | Pour un index de clé primaire, le nom de la colonne. Pour un index de saut de données : `''` (chaîne vide), voir le champ &quot;expression&quot;. | `String`           |
| `collation`     | L&#39;ordre de tri de la colonne dans l&#39;index : `A` si croissant, `D` si décroissant, `NULL` si non trié.                                     | `Nullable(String)` |
| `cardinality`   | Estimation de la cardinalité de l&#39;index (nombre de valeurs uniques dans l&#39;index). Vaut actuellement toujours 0.                           | `UInt64`           |
| `sub_part`      | Toujours `NULL`, car ClickHouse ne prend pas en charge les préfixes d&#39;index comme MySQL.                                                      | `Nullable(String)` |
| `packed`        | Toujours `NULL`, car ClickHouse ne prend pas en charge les index compactés (comme MySQL).                                                         | `Nullable(String)` |
| `null`          | Actuellement inutilisé                                                                                                                            |                    |
| `index_type`    | Le type d&#39;index, par ex. `PRIMARY`, `MINMAX`, `BLOOM_FILTER`, etc.                                                                            | `String`           |
| `comment`       | Informations supplémentaires sur l&#39;index, actuellement toujours `''` (chaîne vide).                                                           | `String`           |
| `index_comment` | `''` (chaîne vide), car les index dans ClickHouse ne peuvent pas avoir de champ `COMMENT` (comme dans MySQL).                                     | `String`           |
| `visible`       | Si l&#39;index est visible par l&#39;optimiseur, toujours `YES`.                                                                                  | `String`           |
| `expression`    | Pour un index de saut de données, l&#39;expression de l&#39;index. Pour un index de clé primaire : `''` (chaîne vide).                            | `String`           |

<div id="examples">
  ### Exemples
</div>

Dans cet exemple, nous utilisons l’instruction `SHOW INDEX` pour obtenir des informations sur tous les index de la table &#39;tbl&#39;

```sql title="Query"
SHOW INDEX FROM 'tbl'
```

```text title="Response"
┌─table─┬─non_unique─┬─key_name─┬─seq_in_index─┬─column_name─┬─collation─┬─cardinality─┬─sub_part─┬─packed─┬─null─┬─index_type───┬─comment─┬─index_comment─┬─visible─┬─expression─┐
│ tbl   │          1 │ blf_idx  │ 1            │ 1           │ ᴺᵁᴸᴸ      │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ BLOOM_FILTER │         │               │ YES     │ d, b       │
│ tbl   │          1 │ mm1_idx  │ 1            │ 1           │ ᴺᵁᴸᴸ      │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ MINMAX       │         │               │ YES     │ a, c, d    │
│ tbl   │          1 │ mm2_idx  │ 1            │ 1           │ ᴺᵁᴸᴸ      │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ MINMAX       │         │               │ YES     │ c, d, e    │
│ tbl   │          1 │ PRIMARY  │ 1            │ c           │ A         │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ PRIMARY      │         │               │ YES     │            │
│ tbl   │          1 │ PRIMARY  │ 2            │ a           │ A         │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ PRIMARY      │         │               │ YES     │            │
│ tbl   │          1 │ set_idx  │ 1            │ 1           │ ᴺᵁᴸᴸ      │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ SET          │         │               │ YES     │ e          │
└───────┴────────────┴──────────┴──────────────┴─────────────┴───────────┴─────────────┴──────────┴────────┴──────┴──────────────┴─────────┴───────────────┴─────────┴────────────┘
```

<div id="see-also">
  ### Voir aussi
</div>

* [`system.tables`](../../operations/system-tables/tables.md)
* [`system.data_skipping_indices`](../../operations/system-tables/data_skipping_indices.md)

<div id="show-processlist">
  ## SHOW PROCESSLIST
</div>

Affiche le contenu de la table [`system.processes`](/fr/operations/system-tables/processes), qui contient la liste des requêtes en cours de traitement, à l’exception des requêtes `SHOW PROCESSLIST`.

<div id="syntax">
  ### Syntaxe
</div>

```sql title="Syntax"
SHOW PROCESSLIST [INTO OUTFILE filename] [FORMAT format]
```

La requête `SELECT * FROM system.processes` renvoie des informations sur toutes les requêtes en cours.

:::tip
Exécutez dans la console :

```bash
$ watch -n1 "clickhouse-client --query='SHOW PROCESSLIST'"
```

:::

<div id="show-grants">
  ## SHOW GRANTS
</div>

L’instruction `SHOW GRANTS` affiche les privilèges d’un utilisateur.

<div id="syntax">
  ### Syntaxe
</div>

```sql title="Syntax"
SHOW GRANTS [FOR user1 [, user2 ...]] [WITH IMPLICIT] [FINAL]
```

Si aucun utilisateur n&#39;est spécifié, la requête renvoie les privilèges de l&#39;utilisateur courant.

Le modificateur `WITH IMPLICIT` permet d&#39;afficher les privilèges implicites (par ex. `GRANT SELECT ON system.one`)

Le modificateur `FINAL` fusionne tous les privilèges de l&#39;utilisateur, ainsi que ceux des rôles qui lui ont été accordés (avec héritage)

<div id="show-create-user">
  ## SHOW CREATE USER
</div>

L’instruction `SHOW CREATE USER` affiche les paramètres utilisés lors de la [création de l’utilisateur](../../sql-reference/statements/create/user.md).

<div id="syntax">
  ### Syntaxe
</div>

```sql title="Syntax"
SHOW CREATE USER [name1 [, name2 ...] | CURRENT_USER]
```

<div id="show-create-role">
  ## SHOW CREATE ROLE
</div>

L’instruction `SHOW CREATE ROLE` affiche les paramètres utilisés lors de la [création du rôle](../../sql-reference/statements/create/role.md).

<div id="syntax">
  ### Syntaxe
</div>

```sql title="Syntax"
SHOW CREATE ROLE name1 [, name2 ...]
```

<div id="show-create-row-policy">
  ## SHOW CREATE ROW POLICY
</div>

L’instruction `SHOW CREATE ROW POLICY` affiche les paramètres utilisés lors de la [création de la ROW POLICY](../../sql-reference/statements/create/row-policy.md).

<div id="syntax">
  ### Syntaxe
</div>

```sql title="Syntax"
SHOW CREATE [ROW] POLICY name ON [database1.]table1 [, [database2.]table2 ...]
```

<div id="show-create-quota">
  ## SHOW CREATE QUOTA
</div>

L’instruction `SHOW CREATE QUOTA` affiche les paramètres utilisés lors de la [création d’un quota](../../sql-reference/statements/create/quota.md).

<div id="syntax">
  ### Syntaxe
</div>

```sql title="Syntax"
SHOW CREATE QUOTA [name1 [, name2 ...] | CURRENT]
```

<div id="show-create-settings-profile">
  ## SHOW CREATE SETTINGS PROFILE
</div>

L’instruction `SHOW CREATE SETTINGS PROFILE` affiche les paramètres utilisés lors de la [création du profil de paramètres](../../sql-reference/statements/create/settings-profile.md).

<div id="syntax">
  ### Syntaxe
</div>

```sql title="Syntax"
SHOW CREATE [SETTINGS] PROFILE name1 [, name2 ...]
```

<div id="show-users">
  ## SHOW USERS
</div>

L’instruction `SHOW USERS` renvoie la liste des noms des [comptes utilisateur](../../guides/sre/user-management/index.md#user-account-management).
Pour afficher les paramètres des comptes utilisateur, consultez la table système [`system.users`](/fr/operations/system-tables/users).

<div id="syntax">
  ### Syntaxe
</div>

```sql title="Syntax"
SHOW USERS
```

<div id="show-roles">
  ## SHOW ROLES
</div>

L’instruction `SHOW ROLES` renvoie une liste de [rôles](../../guides/sre/user-management/index.md#role-management).
Pour afficher d’autres paramètres,
consultez les tables système [`system.roles`](/fr/operations/system-tables/roles) et [`system.role_grants`](/fr/operations/system-tables/role_grants).

<div id="syntax">
  ### Syntaxe
</div>

```sql title="Syntax"
SHOW [CURRENT|ENABLED] ROLES
```

<div id="show-profiles">
  ## SHOW PROFILES
</div>

L’instruction `SHOW PROFILES` renvoie une liste de [profils de paramètres](../../guides/sre/user-management/index.md#settings-profiles-management).
Pour afficher les paramètres des comptes d’utilisateur, consultez la table système [`settings_profiles`](/fr/operations/system-tables/settings_profiles).

<div id="syntax">
  ### Syntaxe
</div>

```sql title="Syntax"
SHOW [SETTINGS] PROFILES
```

<div id="show-policies">
  ## SHOW POLICIES
</div>

L’instruction `SHOW POLICIES` renvoie une liste des [politiques de ligne](../../guides/sre/user-management/index.md#row-policy-management) pour la table spécifiée.
Pour afficher les paramètres des comptes d’utilisateur, consultez la table système [`system.row_policies`](/fr/operations/system-tables/row_policies).

<div id="syntax">
  ### Syntaxe
</div>

```sql title="Syntax"
SHOW [ROW] POLICIES [ON [db.]table]
```

<div id="show-quotas">
  ## SHOW QUOTAS
</div>

L’instruction `SHOW QUOTAS` renvoie une liste de [quotas](../../guides/sre/user-management/index.md#quotas-management).
Pour consulter les paramètres des quotas, voir la table système [`system.quotas`](/fr/operations/system-tables/quotas).

<div id="syntax">
  ### Syntaxe
</div>

```sql title="Syntax"
SHOW QUOTAS
```

<div id="show-quota">
  ## SHOW QUOTA
</div>

L’instruction `SHOW QUOTA` renvoie les informations de consommation du [quota](../../operations/quotas.md) pour tous les utilisateurs ou pour l’utilisateur courant.
Pour afficher d’autres paramètres, consultez les tables système [`system.quotas_usage`](/fr/operations/system-tables/quotas_usage) et [`system.quota_usage`](/fr/operations/system-tables/quota_usage).

<div id="syntax">
  ### Syntaxe
</div>

```sql title="Syntax"
SHOW [CURRENT] QUOTA
```

<div id="show-access">
  ## SHOW ACCESS
</div>

L’instruction `SHOW ACCESS` affiche tous les [utilisateurs](../../guides/sre/user-management/index.md#user-account-management), [rôles](../../guides/sre/user-management/index.md#role-management), [profils](../../guides/sre/user-management/index.md#settings-profiles-management), etc., ainsi que tous leurs [privilèges](../../sql-reference/statements/grant.md#privileges).

<div id="syntax">
  ### Syntaxe
</div>

```sql title="Syntax"
SHOW ACCESS
```

<div id="show-clusters">
  ## SHOW CLUSTER(S)
</div>

L’instruction `SHOW CLUSTER(S)` renvoie une liste de clusters.
Tous les clusters disponibles sont répertoriés dans la table [`system.clusters`](../../operations/system-tables/clusters.md).

:::note
La requête `SHOW CLUSTER name` affiche les valeurs `cluster`, `shard_num`, `replica_num`, `host_name`, `host_address` et `port` de la table `system.clusters` pour le nom de cluster spécifié.
:::

<div id="syntax">
  ### Syntaxe
</div>

```sql title="Syntax"
SHOW CLUSTER '<name>'
SHOW CLUSTERS [[NOT] LIKE|ILIKE '<pattern>'] [LIMIT <N>]
```

<div id="examples">
  ### Exemples
</div>

```sql title="Query"
SHOW CLUSTERS;
```

```text title="Response"
┌─cluster──────────────────────────────────────┐
│ test_cluster_two_shards                      │
│ test_cluster_two_shards_internal_replication │
│ test_cluster_two_shards_localhost            │
│ test_shard_localhost                         │
│ test_shard_localhost_secure                  │
│ test_unavailable_shard                       │
└──────────────────────────────────────────────┘
```

```sql title="Query"
SHOW CLUSTERS LIKE 'test%' LIMIT 1;
```

```text title="Response"
┌─cluster─────────────────┐
│ test_cluster_two_shards │
└─────────────────────────┘
```

```sql title="Query"
SHOW CLUSTER 'test_shard_localhost' FORMAT Vertical;
```

```text title="Response"
Row 1:
──────
cluster:                 test_shard_localhost
shard_num:               1
replica_num:             1
host_name:               localhost
host_address:            127.0.0.1
port:                    9000
```

<div id="show-settings">
  ## SHOW SETTINGS
</div>

L’instruction `SHOW SETTINGS` renvoie la liste des paramètres système et de leurs valeurs.
Elle extrait les données de la table [`system.settings`](../../operations/system-tables/settings.md).

<div id="syntax">
  ### Syntaxe
</div>

```sql title="Syntax"
SHOW [CHANGED] SETTINGS LIKE|ILIKE <name>
```

<div id="clauses">
  ### Clauses
</div>

`LIKE|ILIKE` permettent de spécifier un motif de correspondance pour le nom du paramètre. Il peut contenir des globs tels que `%` ou `_`. La clause `LIKE` est sensible à la casse, `ILIKE` est insensible à la casse.

Lorsque la clause `CHANGED` est utilisée, la requête renvoie uniquement les paramètres dont la valeur a été modifiée par rapport à la valeur par défaut.

<div id="examples">
  ### Exemples
</div>

Requête avec la clause `LIKE` :

```sql title="Query"
SHOW SETTINGS LIKE 'send_timeout';
```

```text title="Response"
┌─name─────────┬─type────┬─value─┐
│ send_timeout │ Seconds │ 300   │
└──────────────┴─────────┴───────┘
```

Requête utilisant la clause `ILIKE` :

```sql title="Query"
SHOW SETTINGS ILIKE '%CONNECT_timeout%'
```

```text title="Response"
┌─name────────────────────────────────────┬─type─────────┬─value─┐
│ connect_timeout                         │ Seconds      │ 10    │
│ connect_timeout_with_failover_ms        │ Milliseconds │ 50    │
│ connect_timeout_with_failover_secure_ms │ Milliseconds │ 100   │
└─────────────────────────────────────────┴──────────────┴───────┘
```

Requête avec la clause `CHANGED` :

```sql title="Query"
SHOW CHANGED SETTINGS ILIKE '%MEMORY%'
```

```text title="Response"
┌─name─────────────┬─type───┬─value───────┐
│ max_memory_usage │ UInt64 │ 10000000000 │
└──────────────────┴────────┴─────────────┘
```

<div id="show-setting">
  ## SHOW SETTING
</div>

L’instruction `SHOW SETTING` affiche la valeur du paramètre correspondant au nom de paramètre spécifié.

<div id="syntax">
  ### Syntaxe
</div>

```sql title="Syntax"
SHOW SETTING <name>
```

<div id="see-also">
  ### Voir aussi
</div>

* [`table system.settings`](../../operations/system-tables/settings.md)

<div id="show-filesystem-caches">
  ## SHOW FILESYSTEM CACHES
</div>

<div id="examples">
  ### Exemples
</div>

```sql title="Query"
SHOW FILESYSTEM CACHES
```

```text title="Response"
┌─Caches────┐
│ s3_cache  │
└───────────┘
```

<div id="see-also">
  ### Voir aussi
</div>

* la table [`system.settings`](../../operations/system-tables/settings.md)

<div id="show-engines">
  ## SHOW ENGINES
</div>

L’instruction `SHOW ENGINES` affiche le contenu de la table [`system.table_engines`](../../operations/system-tables/table_engines.md),
qui contient la description des moteurs de table pris en charge par le serveur, ainsi que des informations sur les fonctionnalités qu’ils prennent en charge.

<div id="syntax">
  ### Syntaxe
</div>

```sql title="Syntax"
SHOW ENGINES [INTO OUTFILE filename] [FORMAT format]
```

<div id="see-also">
  ### Voir aussi
</div>

* table [system.table&#95;engines](../../operations/system-tables/table_engines.md)

<div id="show-functions">
  ## SHOW FUNCTIONS
</div>

L’instruction `SHOW FUNCTIONS` affiche le contenu de la table [`system.functions`](../../operations/system-tables/functions.md).

<div id="syntax">
  ### Syntaxe
</div>

```sql title="Syntax"
SHOW FUNCTIONS [LIKE | ILIKE '<pattern>']
```

Si l’une des clauses `LIKE` ou `ILIKE` est spécifiée, la requête renvoie une liste des fonctions système dont le nom correspond au `<pattern>` fourni.

<div id="see-also">
  ### Voir aussi
</div>

* table [`system.functions`](../../operations/system-tables/functions.md)

<div id="show-merges">
  ## SHOW MERGES
</div>

L’instruction `SHOW MERGES` renvoie une liste des fusions.
Toutes les fusions sont répertoriées dans la table [`system.merges`](../../operations/system-tables/merges.md) :

| Colonne             | Description                                                 |
| ------------------- | ----------------------------------------------------------- |
| `table`             | Nom de la table.                                            |
| `database`          | Nom de la base de données contenant la table.               |
| `estimate_complete` | Temps estimé avant la fin de l’opération (en secondes).     |
| `elapsed`           | Temps écoulé (en secondes) depuis le début de la fusion.    |
| `progress`          | Pourcentage du travail effectué (de 0 à 100 %).             |
| `is_mutation`       | 1 si ce processus est une mutation de part.                 |
| `size_compressed`   | Taille totale des données compressées des parts fusionnées. |
| `memory_usage`      | Consommation de mémoire du processus de fusion.             |

<div id="syntax">
  ### Syntaxe
</div>

```sql title="Syntax"
SHOW MERGES [[NOT] LIKE|ILIKE '<table_name_pattern>'] [LIMIT <N>]
```

<div id="examples">
  ### Exemples
</div>

```sql title="Query"
SHOW MERGES;
```

```text title="Response"
┌─table──────┬─database─┬─estimate_complete─┬─elapsed─┬─progress─┬─is_mutation─┬─size_compressed─┬─memory_usage─┐
│ your_table │ default  │              0.14 │    0.36 │    73.01 │           0 │        5.40 MiB │    10.25 MiB │
└────────────┴──────────┴───────────────────┴─────────┴──────────┴─────────────┴─────────────────┴──────────────┘
```

```sql title="Query"
SHOW MERGES LIKE 'your_t%' LIMIT 1;
```

```text title="Response"
┌─table──────┬─database─┬─estimate_complete─┬─elapsed─┬─progress─┬─is_mutation─┬─size_compressed─┬─memory_usage─┐
│ your_table │ default  │              0.14 │    0.36 │    73.01 │           0 │        5.40 MiB │    10.25 MiB │
└────────────┴──────────┴───────────────────┴─────────┴──────────┴─────────────┴─────────────────┴──────────────┘
```

<div id="show-create-masking-policy">
  ## SHOW CREATE MASKING POLICY
</div>

L’instruction `SHOW CREATE MASKING POLICY` affiche les paramètres utilisés lors de la [création de la politique de masquage](../../sql-reference/statements/create/masking-policy.md).

<div id="syntax">
  ### Syntaxe
</div>

```sql title="Syntax"
SHOW CREATE MASKING POLICY name ON [database.]table
```