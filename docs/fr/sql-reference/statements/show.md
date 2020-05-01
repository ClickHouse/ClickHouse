---
machine_translated: true
machine_translated_rev: f865c9653f9df092694258e0ccdd733c339112f5
toc_priority: 38
toc_title: SHOW
---

# Afficher Les requêtes {#show-queries}

## SHOW CREATE TABLE {#show-create-table}

``` sql
SHOW CREATE [TEMPORARY] [TABLE|DICTIONARY] [db.]table [INTO OUTFILE filename] [FORMAT format]
```

Renvoie un seul `String`-type ‘statement’ column, which contains a single value – the `CREATE` requête utilisée pour créer l’objet spécifié.

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

Sorties le contenu de la [système.processus](../../operations/system-tables.md#system_tables-processes) table, qui contient une liste de requêtes en cours de traitement en ce moment, à l’exception `SHOW PROCESSLIST` requête.

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

Si l’ `FROM` la clause n’est pas spécifié, la requête renvoie la liste des tables de la base de données actuelle.

Vous pouvez obtenir les mêmes résultats que l’ `SHOW TABLES` requête de la façon suivante:

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

Si l’ `FROM` la clause n’est pas spécifié, la requête retourne la liste des dictionnaires de la base de données actuelle.

Vous pouvez obtenir les mêmes résultats que l’ `SHOW DICTIONARIES` requête de la façon suivante:

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

[Article Original](https://clickhouse.tech/docs/en/query_language/show/) <!--hide-->
