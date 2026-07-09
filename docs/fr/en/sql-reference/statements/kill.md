---
description: 'Documentation de KILL'
sidebar_label: 'KILL'
sidebar_position: 46
slug: /sql-reference/statements/kill
title: 'Instructions KILL'
doc_type: 'reference'
---

Il existe deux types d’instructions KILL : l’une pour arrêter une requête et l’autre pour arrêter une mutation

<div id="kill-query">
  ## KILL QUERY
</div>

```sql
KILL QUERY [ON CLUSTER cluster]
  WHERE <where expression to SELECT FROM system.processes query>
  [SYNC|ASYNC|TEST]
  [FORMAT format]
```

Permet d’interrompre de force les requêtes en cours d’exécution.
Les requêtes à interrompre sont sélectionnées dans la table system.processes selon les critères définis dans la clause `WHERE` de la requête `KILL`.

Exemples :

Commencez par obtenir la liste des requêtes incomplètes. Cette requête SQL les affiche en commençant par celles qui s’exécutent depuis le plus longtemps :

Liste à partir d’un seul nœud ClickHouse :

```sql
SELECT
  initial_query_id,
  query_id,
  formatReadableTimeDelta(elapsed) AS time_delta,
  query,
  *
  FROM system.processes
  WHERE query ILIKE 'SELECT%'
  ORDER BY time_delta DESC;
```

Liste d’un cluster ClickHouse :

```sql
SELECT
  initial_query_id,
  query_id,
  formatReadableTimeDelta(elapsed) AS time_delta,
  query,
  *
  FROM clusterAllReplicas(default, system.processes)
  WHERE query ILIKE 'SELECT%'
  ORDER BY time_delta DESC;
```

Arrêtez la requête :

```sql
-- Forcibly terminates all queries with the specified query_id:
KILL QUERY WHERE query_id='2-857d-4a57-9ee0-327da5d60a90'

-- Synchronously terminates all queries run by 'username':
KILL QUERY WHERE user='username' SYNC
```

:::tip
Si vous arrêtez une requête dans ClickHouse Cloud ou dans un cluster autogéré, veillez à utiliser l’option `ON CLUSTER [cluster-name]` afin de garantir l’arrêt de la requête sur toutes les répliques.
:::

Les utilisateurs en lecture seule ne peuvent arrêter que leurs propres requêtes.

Par défaut, c’est la version asynchrone des requêtes qui est utilisée (`ASYNC`) ; elle n’attend pas la confirmation de l’arrêt des requêtes.

La version synchrone (`SYNC`) attend l’arrêt de toutes les requêtes et affiche des informations sur chaque processus à mesure qu’il s’arrête.
La réponse contient la colonne `kill_status`, qui peut prendre les valeurs suivantes :

1. `finished` – La requête a été arrêtée avec succès.
2. `waiting` – En attente de la fin de la requête après l’envoi d’un signal d’arrêt.
3. Les autres valeurs indiquent pourquoi la requête ne peut pas être arrêtée.

Une requête de test (`TEST`) vérifie uniquement les droits de l’utilisateur et affiche la liste des requêtes à arrêter.

<div id="kill-mutation">
  ## KILL MUTATION
</div>

La présence de mutations en cours depuis longtemps ou incomplètes indique souvent qu’un service ClickHouse fonctionne mal. La nature asynchrone des mutations peut les conduire à consommer toutes les ressources disponibles sur un système. Vous devrez peut-être soit :

* Mettre en pause toutes les nouvelles mutations, les `INSERT` et les `SELECT`, puis laisser la file d’attente des mutations se vider.
* Ou interrompre manuellement certaines de ces mutations en envoyant une commande `KILL`.

```sql
KILL MUTATION
  WHERE <where expression to SELECT FROM system.mutations query>
  [TEST]
  [FORMAT format]
```

Tente d’annuler et de supprimer les [mutations](/fr/sql-reference/statements/alter#mutations) en cours d’exécution. Les mutations à annuler sont sélectionnées dans la table [`system.mutations`](/fr/operations/system-tables/mutations) à l’aide du filtre spécifié dans la clause `WHERE` de la requête `KILL`.

Une requête de test (`TEST`) vérifie uniquement les droits de l’utilisateur et affiche la liste des mutations à arrêter.

Exemples :

Obtenir le `count()` du nombre de mutations incomplètes :

Nombre de mutations sur un seul nœud ClickHouse :

```sql
SELECT count(*)
FROM system.mutations
WHERE is_done = 0;
```

Nombre de mutations d’un cluster de répliques ClickHouse :

```sql
SELECT count(*)
FROM clusterAllReplicas('default', system.mutations)
WHERE is_done = 0;
```

Affichez la liste des mutations incomplètes :

Liste des mutations sur un seul nœud ClickHouse :

```sql
SELECT mutation_id, *
FROM system.mutations
WHERE is_done = 0;
```

Liste des mutations d’un cluster ClickHouse :

```sql
SELECT mutation_id, *
FROM clusterAllReplicas('default', system.mutations)
WHERE is_done = 0;
```

Interrompez les mutations si nécessaire :

```sql
-- Cancel and remove all mutations of the single table:
KILL MUTATION WHERE database = 'default' AND table = 'table'

-- Cancel the specific mutation:
KILL MUTATION WHERE database = 'default' AND table = 'table' AND mutation_id = 'mutation_3.txt'
```

La requête est utile lorsqu&#39;une mutation est bloquée et ne peut pas se terminer (par ex. si une fonction de la requête de mutation déclenche une exception lorsqu&#39;elle est appliquée aux données contenues dans la table).

Les modifications déjà effectuées par la mutation ne sont pas annulées.

:::note
La colonne `is_killed=1` (ClickHouse Cloud uniquement) dans la table [system.mutations](/fr/operations/system-tables/mutations) ne signifie pas nécessairement que la mutation est entièrement terminée. Il est possible qu&#39;une mutation reste dans un état où `is_killed=1` et `is_done=0` pendant une période prolongée. Cela peut se produire si une autre mutation longue bloque la mutation interrompue. Il s&#39;agit d&#39;une situation normale.
:::