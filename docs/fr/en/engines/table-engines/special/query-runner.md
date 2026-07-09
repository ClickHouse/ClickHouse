---
description: 'Les enregistrements insérés dans une table `QueryRunner` correspondent à des requêtes que le moteur exécute, localement ou sur un cluster distant, en mode « fire and forget ».'
sidebar_label: 'QueryRunner'
sidebar_position: 55
slug: /engines/table-engines/special/query-runner
title: 'Moteur de table QueryRunner'
doc_type: 'reference'
---

<div id="queryrunner-table-engine">
  # Moteur de table QueryRunner
</div>

Les enregistrements insérés dans une table `QueryRunner` représentent les requêtes que le moteur exécute.
Le moteur peut être utilisé pour l’exécution asynchrone de requêtes, l’exécution par lots de requêtes générées,
l’acheminement de requêtes vers des clusters distants, les tests de performance, le fuzzing et les tests avec trafic miroir.

<div id="creating-a-table">
  ## Créer une table
</div>

```sql
CREATE TABLE runner
(
    query String,
    database String,
    settings Map(LowCardinality(String), String)
)
ENGINE = QueryRunner
SETTINGS
    cluster = 'cluster_name',
    shard = '1',
    mode = 'asynchronous',
    threads = 4,
    max_queue_size = 1000
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | INVOKER | NONE }];
```

La table doit être créée avec un sous-ensemble des colonnes autorisées : `query`, `database`, `settings`.
La colonne `query` est obligatoire, les autres colonnes sont facultatives.

| Colonne    | Type                  | Signification                                                                                                           |
| ---------- | --------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `query`    | `String`              | La requête à exécuter.                                                                                                  |
| `database` | `String`              | La base de données par défaut pour la requête. Si elle est vide, la base de données par défaut du serveur est utilisée. |
| `settings` | `Map(String, String)` | Paramètres appliqués à la requête.                                                                                      |

<div id="engine-settings">
  ## Paramètres du moteur
</div>

| Paramètre        | Par défaut       | Signification                                                                                                                                                                                                                                        |
| ---------------- | ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster`        | `''`             | Nom du cluster auquel envoyer les requêtes. S&#39;il est vide, les requêtes sont exécutées localement.                                                                                                                                               |
| `shard`          | `'1'`            | Index du shard du cluster, indexé à partir de 1, auquel envoyer les requêtes, ou `'random'` pour sélectionner un shard aléatoire pour chaque requête, ou `'all'` pour exécuter chaque requête sur tous les shards. Nécessite le paramètre `cluster`. |
| `mode`           | `'asynchronous'` | En mode `synchronous`, INSERT renvoie une fois que toutes les requêtes du lot inséré sont terminées. En mode `asynchronous`, INSERT renvoie dès que les requêtes sont mises en file d&#39;attente.                                                   |
| `threads`        | `4`              | Nombre de threads d&#39;arrière-plan qui exécutent les requêtes.                                                                                                                                                                                     |
| `max_queue_size` | `1000`           | Nombre maximal de requêtes en file d&#39;attente. Lorsque la file d&#39;attente est pleine, les nouvelles requêtes sont ignorées et une erreur est consignée dans le journal.                                                                        |

<div id="details">
  ## Détails
</div>

La table n&#39;autorise que les requêtes INSERT.
Les requêtes sont exécutées en mode « fire and forget » : en cas d&#39;exception, aucune nouvelle tentative n&#39;est effectuée,
et les résultats des requêtes SELECT sont ignorés (le seul moyen de conserver les résultats est `INSERT SELECT`).
La réussite de chaque requête peut être vérifiée dans la table `system.query_log`, où les requêtes initiées par
ce moteur sont marquées par `is_internal = 1` sur le serveur initiateur.

Les requêtes en file d&#39;attente sont conservées en mémoire et ne survivent pas à un redémarrage du serveur. Lors de l&#39;arrêt du serveur
(ou d&#39;un `DROP`/`DETACH` de la table), les requêtes qui n&#39;ont pas encore démarré sont abandonnées. Parmi les
requêtes déjà en cours d&#39;exécution, celles envoyées à un cluster sont annulées, tandis que l&#39;on attend la fin de celles exécutées
localement.

Lorsqu&#39;une requête à exécuter est elle-même un `INSERT`, ses données doivent être intégrées — `INSERT ... VALUES (...)`,
`INSERT ... SELECT ...` ou `INSERT ... FORMAT ...` avec les données dans le texte de la requête. Un `INSERT` qui
attend ses données d&#39;un flux distinct n&#39;est pas pris en charge.

<div id="local-mode-and-sql-security">
  ## Mode local et SQL SECURITY
</div>

Sans le paramètre `cluster`, les requêtes sont exécutées sur le serveur local.
L’utilisateur au nom duquel elles s’exécutent est déterminé par la clause `SQL SECURITY` :

* `INVOKER` (par défaut) : les requêtes s’exécutent au nom de l’utilisateur qui a effectué l’opération `INSERT`.
* `DEFINER` : les requêtes s’exécutent au nom de l’utilisateur `DEFINER` spécifié. Comme les requêtes insérées sont arbitraires, accorder `INSERT` sur une telle table délègue tous les privilèges de ce `DEFINER`.
* `NONE` : les requêtes s’exécutent avec un accès complet, sans utilisateur. Nécessite l’autorisation `ALLOW_SQL_SECURITY_NONE` lors de la création de la table.

<div id="cluster-mode">
  ## Mode cluster
</div>

Lorsque le paramètre `cluster` est spécifié, les requêtes sont envoyées au cluster indiqué.

Le shard cible est sélectionné par `shard` : un index fixe indexé à partir de 1 (`'1'` par défaut), `'random'` pour choisir un
shard aléatoire pour chaque requête, ou `'all'` pour exécuter chaque requête sur chaque shard du cluster. Une réplique au sein du
shard est choisie selon le paramètre `load_balancing` du serveur.

La colonne `database` définit la base de données par défaut de la connexion au serveur distant. Comme la
base de données par défaut n&#39;est définie qu&#39;une seule fois par connexion, chaque valeur distincte de `database` utilise son propre
pool de connexions, qui est créé lors de la première utilisation et réutilisé pendant toute la durée de vie de la table.

`DEFINER` et `SQL SECURITY` n&#39;ont d&#39;effet qu&#39;en mode local, et leur combinaison avec le
paramètre `cluster` constitue une erreur. Sur les serveurs distants, les requêtes sont authentifiées avec les
identifiants de la configuration du cluster et s&#39;exécutent comme des requêtes initiales ordinaires : elles sont enregistrées dans
`system.query_log` avec `is_initial_query = 1` et leur propre `query_id` (sans lien avec l&#39;INSERT qui
les a produites). Sur le serveur initiateur, les requêtes acheminées sont enregistrées dans `system.query_log`
avec `is_internal = 1`.

Comme le moteur ignore les résultats des requêtes, il exécute toujours les requêtes acheminées avec
`discard_query_data = 1`, de sorte que les données de résultat des requêtes SELECT ne sont pas transférées sur le réseau
(cela remplace toute valeur `discard_query_data` définie dans la colonne `settings`).

<div id="waiting-for-queries-to-finish">
  ## Attendre la fin des requêtes
</div>

En mode asynchrone, la requête suivante permet de bloquer jusqu&#39;à ce que toutes les requêtes soumises à la table jusqu&#39;à présent soient terminées :

```sql
SYSTEM WAIT QUERY RUNNER runner;
```

<div id="example">
  ## Exemple
</div>

Relance de requêtes `SELECT` récentes à partir du journal des requêtes :

```sql
INSERT INTO runner (query, database, settings)
SELECT query, current_database, Settings
FROM system.query_log
WHERE type = 'QueryFinish' AND is_initial_query AND NOT is_internal AND query_kind = 'Select'
  AND event_time > now() - INTERVAL 1 HOUR;
```