---
description: "Ce moteur permet d'intégrer ClickHouse à NATS pour publier sur des sujets de messages ou s'y abonner, et traiter les nouveaux messages dès qu'ils sont disponibles."
sidebar_label: 'NATS'
sidebar_position: 140
slug: /engines/table-engines/integrations/nats
title: 'Moteur de table NATS'
doc_type: 'guide'
---

Ce moteur permet d&#39;intégrer ClickHouse à [NATS](https://nats.io/).

`NATS` vous permet de :

* Publier sur des sujets de messages ou vous y abonner.
* Traiter les nouveaux messages dès qu&#39;ils sont disponibles.

<div id="creating-a-table">
  ## Création d’une table
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = NATS SETTINGS
    nats_url = 'host:port',
    nats_subjects = 'subject1,subject2,...',
    nats_format = 'data_format'[,]
    [nats_schema = '',]
    [nats_num_consumers = N,]
    [nats_queue_group = 'group_name',]
    [nats_secure = false,]
    [nats_max_reconnect = N,]
    [nats_reconnect_wait = N,]
    [nats_server_list = 'host1:port1,host2:port2,...',]
    [nats_skip_broken_messages = N,]
    [nats_max_block_size = N,]
    [nats_flush_interval_ms = N,]
    [nats_username = 'user',]
    [nats_password = 'password',]
    [nats_token = 'clickhouse',]
    [nats_credential_file = '/var/nats_credentials',]
    [nats_startup_connect_tries = 5,]
    [nats_max_rows_per_message = 1,]
    [nats_handle_error_mode = 'default']
```

Paramètres requis :

* `nats_url` – host:port (par exemple, `localhost:4222`)..
* `nats_subjects` – Liste des sujets auxquels la table NATS doit s’abonner ou publier. Prend en charge les sujets génériques comme `foo.*.bar` ou `baz.>`
* `nats_format` – Format du message. Utilise la même notation que la fonction SQL `FORMAT`, par exemple `JSONEachRow`. Pour plus d’informations, consultez la section [Formats](../../../interfaces/formats.md).

Paramètres facultatifs :

* `nats_schema` – Paramètre à utiliser si le format nécessite une définition de schéma. Par exemple, [Cap&#39;n Proto](https://capnproto.org/) requiert le chemin du fichier de schéma et le nom de l’objet racine `schema.capnp:Message`.
* `nats_stream` – Nom d’un stream existant dans NATS JetStream.
* `nats_consumer_name` – Nom d’un durable pull consumer existant dans NATS JetStream.
* `nats_num_consumers` – Nombre de consommateurs par table. Valeur par défaut : `1`. Spécifiez davantage de consommateurs si le débit d’un seul consommateur est insuffisant, uniquement pour NATS Core.
* `nats_queue_group` – Nom du groupe de file d’attente des abonnés NATS. La valeur par défaut est le nom de la table.
* `nats_max_reconnect` – Déprécié et sans effet : la reconnexion est effectuée en permanence avec le délai d’attente nats&#95;reconnect&#95;wait.
* `nats_reconnect_wait` – Temps d’attente en millisecondes entre chaque tentative de reconnexion. Valeur par défaut : `2000`.
* `nats_server_list` - Liste des serveurs pour la connexion. Peut être spécifiée pour se connecter à un cluster NATS.
* `nats_skip_broken_messages` - Tolérance de l’analyseur de messages NATS aux messages incompatibles avec le schéma par bloc. Valeur par défaut : `0`. Si `nats_skip_broken_messages = N`, alors le moteur ignore *N* messages NATS qui ne peuvent pas être analysés (un message équivaut à une ligne de données).
* `nats_max_block_size` - Nombre de lignes collectées par poll(s) pour vider les données de NATS. Valeur par défaut : [max&#95;insert&#95;block&#95;size](../../../operations/settings/settings.md#max_insert_block_size).
* `nats_flush_interval_ms` - Délai d’expiration pour vider les données lues depuis NATS. Valeur par défaut : [stream&#95;flush&#95;interval&#95;ms](/fr/operations/settings/settings#stream_flush_interval_ms).
* `nats_username` - Nom d’utilisateur NATS.
* `nats_password` - Mot de passe NATS.
* `nats_token` - Jeton d’authentification NATS.
* `nats_credential_file` - Chemin vers un fichier d’identifiants NATS.
* `nats_startup_connect_tries` - Nombre de tentatives de connexion au démarrage. Valeur par défaut : `5`.
* `nats_max_rows_per_message` — Nombre maximal de lignes écrites dans un message NATS pour les formats basés sur les lignes. (valeur par défaut : `1`).
* `nats_handle_error_mode` — Comment gérer les erreurs pour le moteur NATS. Valeurs possibles : default (une exception sera levée en cas d’échec de l’analyse d’un message), stream (le message d’exception et le message brut seront enregistrés dans les colonnes virtuelles `_error` et `_raw_message`).

Connexion SSL :

Pour une connexion sécurisée, utilisez `nats_secure = 1`.
La vérification du certificat est contrôlée par la variable d’environnement `CLICKHOUSE_NATS_TLS_SECURE` ;
Si le certificat est expiré, auto-signé, absent ou autrement invalide, désactivez la vérification en définissant `CLICKHOUSE_NATS_TLS_SECURE=0`.

Écriture dans la table NATS :

Si la table lit uniquement à partir d’un sujet, toute insertion sera publiée dans ce même sujet.
Cependant, si la table lit à partir de plusieurs sujets, vous devez préciser vers quel sujet publier.
C’est pourquoi, lors de toute insertion dans une table avec plusieurs sujets, le paramètre `stream_like_engine_insert_queue` est nécessaire.
Vous pouvez sélectionner l’un des sujets lus par la table et y publier vos données. Par exemple :

```sql
  CREATE TABLE queue (
    key UInt64,
    value UInt64
  ) ENGINE = NATS
    SETTINGS nats_url = 'localhost:4444',
             nats_subjects = 'subject1,subject2',
             nats_format = 'JSONEachRow';

  INSERT INTO queue
  SETTINGS stream_like_engine_insert_queue = 'subject2'
  VALUES (1, 1);
```

Les paramètres de format peuvent également être ajoutés en même temps que les paramètres liés à NATS.

Exemple :

```sql
  CREATE TABLE queue (
    key UInt64,
    value UInt64,
    date DateTime
  ) ENGINE = NATS
    SETTINGS nats_url = 'localhost:4444',
             nats_subjects = 'subject1',
             nats_format = 'JSONEachRow',
             date_time_input_format = 'best_effort';
```

La configuration du serveur NATS peut être ajoutée dans le fichier de configuration de ClickHouse.
Plus précisément, vous pouvez y ajouter votre mot de passe pour le moteur NATS :

```xml
<nats>
    <user>click</user>
    <password>house</password>
    <token>clickhouse</token>
</nats>
```

<div id="description">
  ## Description
</div>

`SELECT` n’est pas particulièrement utile pour lire des messages (sauf pour le débogage), car chaque message ne peut être lu qu’une seule fois. Il est plus pratique de créer des flux en temps réel à l’aide de [vues matérialisées](../../../sql-reference/statements/create/view.md). Pour ce faire :

1. Utilisez le moteur pour créer un consommateur NATS et le considérer comme un flux de données.
2. Créez une table avec la structure souhaitée.
3. Créez une vue matérialisée qui convertit les données du moteur et les insère dans une table créée précédemment.

Lorsque la `MATERIALIZED VIEW` se connecte au moteur, elle commence à collecter des données en arrière-plan. Cela vous permet de recevoir en continu des messages de NATS et de les convertir au format requis à l’aide de `SELECT`.
Une table NATS peut avoir autant de vues matérialisées que vous le souhaitez ; elles ne lisent pas les données directement depuis la table, mais reçoivent de nouveaux enregistrements (par blocs). Vous pouvez ainsi écrire dans plusieurs tables avec des niveaux de détail différents (avec regroupement/agrégation ou sans).

Exemple :

```sql
  CREATE TABLE queue (
    key UInt64,
    value UInt64
  ) ENGINE = NATS
    SETTINGS nats_url = 'localhost:4444',
             nats_subjects = 'subject1',
             nats_format = 'JSONEachRow',
             date_time_input_format = 'best_effort';

  CREATE TABLE daily (key UInt64, value UInt64)
    ENGINE = MergeTree() ORDER BY key;

  CREATE MATERIALIZED VIEW consumer TO daily
    AS SELECT key, value FROM queue;

  SELECT key, value FROM daily ORDER BY key;
```

Pour cesser de recevoir les données de flux ou pour modifier la logique de conversion, détachez la vue matérialisée :

```sql
  DETACH TABLE consumer;
  ATTACH TABLE consumer;
```

Si vous souhaitez modifier la table cible à l’aide de `ALTER`, nous vous recommandons de désactiver la vue matérialisée afin d’éviter toute incohérence entre la table cible et les données de la vue.

<div id="virtual-columns">
  ## Colonnes virtuelles
</div>

* `_subject` - Subject du message NATS. Type de données : `String`.

Colonnes virtuelles supplémentaires lorsque `nats_handle_error_mode='stream'` :

* `_raw_message` - Message brut qui n&#39;a pas pu être analysé correctement. Type de données : `Nullable(String)`.
* `_error` - Message d&#39;exception survenu lors de l&#39;échec de l&#39;analyse. Type de données : `Nullable(String)`.

Remarque : les colonnes virtuelles `_raw_message` et `_error` sont renseignées uniquement en cas d&#39;exception lors de l&#39;analyse ; elles valent toujours `NULL` lorsque le message a été analysé correctement.

<div id="data-formats-support">
  ## Prise en charge des formats de données
</div>

Le moteur NATS prend en charge tous les [formats](../../../interfaces/formats.md) pris en charge par ClickHouse.
Le nombre de lignes dans un message NATS dépend du fait que le format soit orienté lignes ou orienté blocs :

* Pour les formats orientés lignes, le nombre de lignes dans un message NATS peut être contrôlé en définissant `nats_max_rows_per_message`.
* Pour les formats orientés blocs, il n’est pas possible de diviser un block en parties plus petites, mais le nombre de lignes dans un block peut être contrôlé à l’aide du paramètre général [max&#95;block&#95;size](/fr/operations/settings/settings#max_block_size).

<div id="using-jetstream">
  ## Utilisation de JetStream
</div>

Avant d’utiliser le moteur NATS avec NATS JetStream, vous devez créer un stream NATS ainsi qu’un durable pull consumer. Pour cela, vous pouvez, par exemple, utiliser l’utilitaire nats du paquet [NATS CLI](https://github.com/nats-io/natscli) :

<details>
  <summary>création du stream</summary>

  ```bash
  $ nats stream add
  ? Stream Name stream_name
  ? Subjects stream_subject
  ? Storage file
  ? Replication 1
  ? Retention Policy Limits
  ? Discard Policy Old
  ? Stream Messages Limit -1
  ? Per Subject Messages Limit -1
  ? Total Stream Size -1
  ? Message TTL -1
  ? Max Message Size -1
  ? Duplicate tracking time window 2m0s
  ? Allow message Roll-ups No
  ? Allow message deletion Yes
  ? Allow purging subjects or the entire stream Yes
  Stream stream_name was created

  Information for Stream stream_name created 2025-10-03 14:12:51

                  Subjects: stream_subject
                  Replicas: 1
                   Storage: File

  Options:

                 Retention: Limits
           Acknowledgments: true
            Discard Policy: Old
          Duplicate Window: 2m0s
                Direct Get: true
         Allows Msg Delete: true
              Allows Purge: true
    Allows Per-Message TTL: false
            Allows Rollups: false

  Limits:

          Maximum Messages: unlimited
       Maximum Per Subject: unlimited
             Maximum Bytes: unlimited
               Maximum Age: unlimited
      Maximum Message Size: unlimited
         Maximum Consumers: unlimited

  State:

                  Messages: 0
                     Bytes: 0 B
            First Sequence: 0
             Last Sequence: 0
          Active Consumers: 0
  ```
</details>

<details>
  <summary>création du durable pull consumer</summary>

  ```bash
  $ nats consumer add
  ? Select a Stream stream_name
  ? Consumer name consumer_name
  ? Delivery target (empty for Pull Consumers) 
  ? Start policy (all, new, last, subject, 1h, msg sequence) all
  ? Acknowledgment policy explicit
  ? Replay policy instant
  ? Filter Stream by subjects (blank for all) 
  ? Maximum Allowed Deliveries -1
  ? Maximum Acknowledgments Pending 0
  ? Deliver headers only without bodies No
  ? Add a Retry Backoff Policy No
  Information for Consumer stream_name > consumer_name created 2025-10-03T14:13:51+03:00

  Configuration:

                      Name: consumer_name
                 Pull Mode: true
            Deliver Policy: All
                Ack Policy: Explicit
                  Ack Wait: 30.00s
             Replay Policy: Instant
           Max Ack Pending: 1,000
         Max Waiting Pulls: 512

  State:

    Last Delivered Message: Consumer sequence: 0 Stream sequence: 0
      Acknowledgment Floor: Consumer sequence: 0 Stream sequence: 0
          Outstanding Acks: 0 out of maximum 1,000
      Redelivered Messages: 0
      Unprocessed Messages: 0
             Waiting Pulls: 0 of maximum 512
  ```
</details>

Après avoir créé le stream et le durable pull consumer, vous pouvez créer une table avec le moteur NATS. Pour cela, vous devez initialiser : nats&#95;stream, nats&#95;consumer&#95;name et nats&#95;subjects :

```SQL
CREATE TABLE nats_jet_stream (
    key UInt64,
    value UInt64
  ) ENGINE NATS 
    SETTINGS  nats_url = 'localhost:4222',
              nats_stream = 'stream_name',
              nats_consumer_name = 'consumer_name',
              nats_subjects = 'stream_subject',
              nats_format = 'JSONEachRow';
```