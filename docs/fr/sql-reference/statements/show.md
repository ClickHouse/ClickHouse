---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 38
toc_title: SHOW
---

# Afficher les requêtes {#show-queries}

## SHOW CREATE TABLE {#show-create-table}

``` sql
SHOW CREATE [TEMPORARY] [TABLE|DICTIONARY] [db.]table [INTO OUTFILE filename] [FORMAT format]
```

Renvoie un seul `String`-type ‘statement’ column, which contains a single value – the `CREATE` requête utilisée pour créer l'objet spécifié.

## SHOW DATABASES {#show-databases}

``` sql
SHOW DATABASES [INTO OUTFILE filename] [FORMAT format]
```

Imprime une liste de toutes les bases de données.
Cette requête est identique à `SELECT name FROM system.databases [INTO OUTFILE filename] [FORMAT format]`.

## SHOW PROCESSLIST {#show-processlist}

``` sql
SHOW PROCESSLIST [INTO OUTFILE filename] [FORMAT format]
```

Sorties le contenu de la [système.processus](../../operations/system-tables.md#system_tables-processes) table, qui contient une liste de requêtes en cours de traitement en ce moment, à l'exception `SHOW PROCESSLIST` requête.

Le `SELECT * FROM system.processes` requête renvoie des données sur toutes les requêtes en cours.

Astuce (exécuter dans la console):

``` bash
$ watch -n1 "clickhouse-client --query='SHOW PROCESSLIST'"
```

## SHOW TABLES {#show-tables}

Affiche une liste de tableaux.

``` sql
SHOW [TEMPORARY] TABLES [{FROM | IN} <db>] [LIKE '<pattern>' | WHERE expr] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

Si l' `FROM` la clause n'est pas spécifié, la requête renvoie la liste des tables de la base de données actuelle.

Vous pouvez obtenir les mêmes résultats que l' `SHOW TABLES` requête de la façon suivante:

``` sql
SELECT name FROM system.tables WHERE database = <db> [AND name LIKE <pattern>] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

**Exemple**

La requête suivante sélectionne les deux premières lignes de la liste des tables `system` base de données, dont les noms contiennent `co`.

``` sql
SHOW TABLES FROM system LIKE '%co%' LIMIT 2
```

``` text
┌─name───────────────────────────┐
│ aggregate_function_combinators │
│ collations                     │
└────────────────────────────────┘
```

## SHOW DICTIONARIES {#show-dictionaries}

Affiche une liste de [dictionnaires externes](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md).

``` sql
SHOW DICTIONARIES [FROM <db>] [LIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

Si l' `FROM` la clause n'est pas spécifié, la requête retourne la liste des dictionnaires de la base de données actuelle.

Vous pouvez obtenir les mêmes résultats que l' `SHOW DICTIONARIES` requête de la façon suivante:

``` sql
SELECT name FROM system.dictionaries WHERE database = <db> [AND name LIKE <pattern>] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

**Exemple**

La requête suivante sélectionne les deux premières lignes de la liste des tables `system` base de données, dont les noms contiennent `reg`.

``` sql
SHOW DICTIONARIES FROM db LIKE '%reg%' LIMIT 2
```

``` text
┌─name─────────┐
│ regions      │
│ region_names │
└──────────────┘
```

## SHOW GRANTS {#show-grants-statement}

Montre les privilèges d'un utilisateur.

### Syntaxe {#show-grants-syntax}

``` sql
SHOW GRANTS [FOR user]
```

Si l'utilisateur n'est pas spécifié, la requête renvoie les privilèges de l'utilisateur actuel.

## SHOW CREATE USER {#show-create-user-statement}

Affiche les paramètres qui ont été utilisés [la création d'un utilisateur](create.md#create-user-statement).

`SHOW CREATE USER` ne produit pas de mots de passe utilisateur.

### Syntaxe {#show-create-user-syntax}

``` sql
SHOW CREATE USER [name | CURRENT_USER]
```

## SHOW CREATE ROLE {#show-create-role-statement}

Affiche les paramètres qui ont été utilisés [la création de rôle](create.md#create-role-statement)

### Syntaxe {#show-create-role-syntax}

``` sql
SHOW CREATE ROLE name
```

## SHOW CREATE ROW POLICY {#show-create-row-policy-statement}

Affiche les paramètres qui ont été utilisés [création de stratégie de ligne](create.md#create-row-policy-statement)

### Syntaxe {#show-create-row-policy-syntax}

``` sql
SHOW CREATE [ROW] POLICY name ON [database.]table
```

## SHOW CREATE QUOTA {#show-create-quota-statement}

Affiche les paramètres qui ont été utilisés [quota de création](create.md#create-quota-statement)

### Syntaxe {#show-create-row-policy-syntax}

``` sql
SHOW CREATE QUOTA [name | CURRENT]
```

## SHOW CREATE SETTINGS PROFILE {#show-create-settings-profile-statement}

Affiche les paramètres qui ont été utilisés [configuration création de profil](create.md#create-settings-profile-statement)

### Syntaxe {#show-create-row-policy-syntax}

``` sql
SHOW CREATE [SETTINGS] PROFILE name
```

[Article Original](https://clickhouse.tech/docs/en/query_language/show/) <!--hide-->
