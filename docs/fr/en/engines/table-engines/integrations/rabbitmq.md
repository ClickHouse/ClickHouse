---
description: 'Ce moteur permet d’intégrer ClickHouse à RabbitMQ.'
sidebar_label: 'RabbitMQ'
sidebar_position: 170
slug: /engines/table-engines/integrations/rabbitmq
title: 'Moteur de table RabbitMQ'
doc_type: 'guide'
---

Ce moteur permet d’intégrer ClickHouse à [RabbitMQ](https://www.rabbitmq.com).

`RabbitMQ` vous permet de :

* Publier ou vous abonner à des flux de données.
* Traiter les flux au fur et à mesure qu’ils deviennent disponibles.

<div id="creating-a-table">
  ## Créer une table
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1],
    name2 [type2],
    ...
) ENGINE = RabbitMQ SETTINGS
    rabbitmq_host_port = 'host:port' [or rabbitmq_address = 'amqp(s)://guest:guest@localhost/vhost'],
    rabbitmq_exchange_name = 'exchange_name',
    rabbitmq_format = 'data_format'[,]
    [rabbitmq_exchange_type = 'exchange_type',]
    [rabbitmq_routing_key_list = 'key1,key2,...',]
    [rabbitmq_secure = 0,]
    [rabbitmq_schema = '',]
    [rabbitmq_num_consumers = N,]
    [rabbitmq_num_queues = N,]
    [rabbitmq_queue_base = 'queue',]
    [rabbitmq_persistent = 0,]
    [rabbitmq_skip_broken_messages = N,]
    [rabbitmq_max_block_size = N,]
    [rabbitmq_flush_interval_ms = N,]
    [rabbitmq_queue_settings_list = 'x-dead-letter-exchange=my-dlx,x-max-length=10,x-overflow=reject-publish',]
    [rabbitmq_queue_consume = false,]
    [rabbitmq_address = '',]
    [rabbitmq_vhost = '/',]
    [rabbitmq_username = '',]
    [rabbitmq_password = '',]
    [rabbitmq_commit_on_select = false,]
    [rabbitmq_max_rows_per_message = 1,]
    [rabbitmq_handle_error_mode = 'default']
```

Paramètres requis :

* `rabbitmq_host_port` – hôte:port (par exemple, `localhost:5672`).
* `rabbitmq_exchange_name` – nom de l’exchange RabbitMQ.
* `rabbitmq_format` – format du message. Utilise la même notation que la fonction SQL `FORMAT`, par exemple `JSONEachRow`. Pour plus d’informations, consultez la section [Formats](../../../interfaces/formats.md).

Paramètres facultatifs :

* `rabbitmq_exchange_type` – Le type d’exchange RabbitMQ : `direct`, `fanout`, `topic`, `headers`, `consistent_hash`. Par défaut : `fanout`.
* `rabbitmq_routing_key_list` – Liste de clés de routage séparées par des virgules.
* `rabbitmq_schema` – Paramètre à utiliser si le format nécessite une définition de schéma. Par exemple, [Cap&#39;n Proto](https://capnproto.org/) nécessite le chemin vers le fichier de schéma et le nom de l’objet racine `schema.capnp:Message`.
* `rabbitmq_num_consumers` – Le nombre de consommateurs par table. Spécifiez davantage de consommateurs si le débit d’un seul consommateur est insuffisant. Par défaut : `1`
* `rabbitmq_num_queues` – Nombre total de files d’attente. Augmenter ce nombre peut améliorer considérablement les performances. Par défaut : `1`.
* `rabbitmq_queue_base` - Indique un préfixe pour les noms de file d’attente. Les cas d’usage de ce paramètre sont décrits ci-dessous.
* `rabbitmq_persistent` - Si cette option est définie sur 1 (true), le mode de livraison de la requête d’insertion sera défini sur 2 (ce qui marque les messages comme « persistants »). Par défaut : `0`.
* `rabbitmq_skip_broken_messages` – Tolérance de l’analyseur de messages RabbitMQ aux messages incompatibles avec le schéma, par bloc. Si `rabbitmq_skip_broken_messages = N`, le moteur ignore *N* messages RabbitMQ qui ne peuvent pas être analysés (un message correspond à une ligne de données). Par défaut : `0`.
* `rabbitmq_max_block_size` - Nombre de lignes collectées avant d’écrire les données de RabbitMQ. Par défaut : [max&#95;insert&#95;block&#95;size](../../../operations/settings/settings.md#max_insert_block_size).
* `rabbitmq_flush_interval_ms` - Timeout avant l’écriture des données depuis RabbitMQ. Par défaut : [stream&#95;flush&#95;interval&#95;ms](/fr/operations/settings/settings#stream_flush_interval_ms).
* `rabbitmq_queue_settings_list` - permet de définir les paramètres RabbitMQ lors de la création d’une file d’attente. Paramètres disponibles : `x-max-length`, `x-max-length-bytes`, `x-message-ttl`, `x-expires`, `x-priority`, `x-max-priority`, `x-overflow`, `x-dead-letter-exchange`, `x-queue-type`. Le paramètre `durable` est activé automatiquement pour la file d’attente.
* `rabbitmq_address` - Adresse de connexion. Utilisez soit ce paramètre, soit `rabbitmq_host_port`.
* `rabbitmq_vhost` - vhost RabbitMQ. Par défaut : `'/'`.
* `rabbitmq_queue_consume` - Utilise des files d’attente définies par l’utilisateur et n’effectue aucune configuration RabbitMQ : déclaration des exchanges, des files d’attente et des bindings. Par défaut : `false`.
* `rabbitmq_username` - Nom d’utilisateur RabbitMQ.
* `rabbitmq_password` - Mot de passe RabbitMQ.
* `reject_unhandled_messages` - Rejette les messages (envoie un accusé de réception négatif RabbitMQ) en cas d’erreur. Ce paramètre est automatiquement activé si `x-dead-letter-exchange` est défini dans `rabbitmq_queue_settings_list`.
* `rabbitmq_commit_on_select` - Valide les messages lorsqu’une requête SELECT est exécutée. Par défaut : `false`.
* `rabbitmq_max_rows_per_message` — Le nombre maximal de lignes écrites dans un message RabbitMQ pour les formats basés sur les lignes. Par défaut : `1`.
* `rabbitmq_empty_queue_backoff_start_ms` — Point de départ du backoff pour replanifier la lecture si la file d’attente RabbitMQ est vide.
* `rabbitmq_empty_queue_backoff_end_ms` — Point de fin du backoff pour replanifier la lecture si la file d’attente RabbitMQ est vide.
* `rabbitmq_empty_queue_backoff_step_ms` — Pas du backoff pour replanifier la lecture si la file d’attente RabbitMQ est vide.
* `rabbitmq_handle_error_mode` — Mode de gestion des erreurs pour le moteur RabbitMQ. Valeurs possibles : default (une exception est levée si l’analyse d’un message échoue), stream (le message d’exception et le message brut sont enregistrés dans les colonnes virtuelles `_error` et `_raw_message`), dead&#95;letter&#95;queue (les données liées à l’erreur sont enregistrées dans `system.dead_letter_queue`).

<div id="ssl-connection">
  ### Connexion SSL
</div>

Utilisez soit `rabbitmq_secure = 1`, soit `amqps` dans l’adresse de connexion : `rabbitmq_address = 'amqps://guest:guest@localhost/vhost'`.
Par défaut, la bibliothèque utilisée ne vérifie pas si la connexion TLS établie est suffisamment sécurisée. Que le certificat soit expiré, autosigné, absent ou invalide, la connexion est simplement autorisée. Une vérification plus stricte des certificats pourra être mise en œuvre à l’avenir.

Les paramètres de format peuvent également être ajoutés en plus des paramètres liés à RabbitMQ.

Exemple :

```sql
  CREATE TABLE queue (
    key UInt64,
    value UInt64,
    date DateTime
  ) ENGINE = RabbitMQ SETTINGS rabbitmq_host_port = 'localhost:5672',
                            rabbitmq_exchange_name = 'exchange1',
                            rabbitmq_format = 'JSONEachRow',
                            rabbitmq_num_consumers = 5,
                            date_time_input_format = 'best_effort';
```

La configuration du serveur RabbitMQ doit être ajoutée dans le fichier de configuration de ClickHouse.

Configuration requise :

```xml
 <rabbitmq>
    <username>root</username>
    <password>clickhouse</password>
 </rabbitmq>
```

Configuration supplémentaire :

```xml
 <rabbitmq>
    <vhost>clickhouse</vhost>
 </rabbitmq>
```

<div id="description">
  ## Description
</div>

`SELECT` n’est pas particulièrement utile pour lire des messages (sauf pour le débogage), car chaque message ne peut être lu qu’une seule fois. Il est plus pratique de créer des flux en temps réel à l’aide de [vues matérialisées](../../../sql-reference/statements/create/view.md). Pour ce faire :

1. Utilisez le moteur pour créer un consommateur RabbitMQ et considérez-le comme un flux de données.
2. Créez une table avec la structure souhaitée.
3. Créez une vue matérialisée qui convertit les données du moteur et les insère dans une table créée précédemment.

Lorsque la `MATERIALIZED VIEW` est associée au moteur, elle commence à collecter les données en arrière-plan. Cela vous permet de recevoir en continu des messages depuis RabbitMQ et de les convertir au format requis à l’aide de `SELECT`.
Une table RabbitMQ peut avoir autant de vues matérialisées que vous le souhaitez.

Les données peuvent être acheminées en fonction de `rabbitmq_exchange_type` et de la valeur spécifiée dans `rabbitmq_routing_key_list`.
Il ne peut pas y avoir plus d’un exchange par table. Un exchange peut être partagé entre plusieurs tables, ce qui permet un routage simultané vers plusieurs tables.

Options de type d’exchange :

* `direct` - Le routage repose sur une correspondance exacte des clés. Exemple de liste de clés de table : `key1,key2,key3,key4,key5` ; la clé du message peut correspondre à n’importe laquelle d’entre elles.
* `fanout` - Routage vers toutes les tables (où le nom de l’exchange est identique), indépendamment des clés.
* `topic` - Le routage repose sur des motifs avec des clés séparées par des points. Exemples : `*.logs`, `records.*.*.2020`, `*.2018,*.2019,*.2020`.
* `headers` - Le routage repose sur des correspondances `key=value` avec le paramètre `x-match=all` ou `x-match=any`. Exemple de liste de clés de table : `x-match=all,format=logs,type=report,year=2020`.
* `consistent_hash` - Les données sont réparties uniformément entre toutes les tables liées (où le nom de l’exchange est identique). Notez que ce type d’exchange doit être activé avec le plugin RabbitMQ : `rabbitmq-plugins enable rabbitmq_consistent_hash_exchange`.

Le paramètre `rabbitmq_queue_base` peut être utilisé dans les cas suivants :

* pour permettre à différentes tables de partager des files d’attente, afin que plusieurs consommateurs puissent être enregistrés sur les mêmes files d’attente, ce qui améliore les performances. Si vous utilisez les paramètres `rabbitmq_num_consumers` et/ou `rabbitmq_num_queues`, une correspondance exacte des files d’attente est obtenue lorsque ces paramètres sont identiques.
* pour pouvoir reprendre la lecture à partir de certaines files d’attente durables lorsque tous les messages n’ont pas été consommés avec succès. Pour reprendre la consommation à partir d’une file d’attente spécifique, définissez son nom dans le paramètre `rabbitmq_queue_base` et ne spécifiez ni `rabbitmq_num_consumers` ni `rabbitmq_num_queues` (la valeur par défaut est 1). Pour reprendre la consommation à partir de toutes les files d’attente déclarées pour une table spécifique, indiquez simplement les mêmes paramètres : `rabbitmq_queue_base`, `rabbitmq_num_consumers`, `rabbitmq_num_queues`. Par défaut, les noms des files d’attente sont uniques pour chaque table.
* pour réutiliser les files d’attente, car elles sont déclarées comme durables et ne sont pas supprimées automatiquement. (Elles peuvent être supprimées à l’aide de n’importe lequel des outils CLI de RabbitMQ.)

Pour améliorer les performances, les messages reçus sont regroupés en blocs de la taille de [max&#95;insert&#95;block&#95;size](/fr/operations/settings/settings#max_insert_block_size). Si le bloc n’a pas été formé dans les [stream&#95;flush&#95;interval&#95;ms](../../../operations/server-configuration-parameters/settings.md) millisecondes, les données seront écrites dans la table même si le bloc n’est pas complet.

Si les paramètres `rabbitmq_num_consumers` et/ou `rabbitmq_num_queues` sont spécifiés avec `rabbitmq_exchange_type`, alors :

* le plugin `rabbitmq-consistent-hash-exchange` doit être activé.
* la propriété `message_id` des messages publiés doit être spécifiée (unique pour chaque message/lot).

Pour la requête d’insertion, des métadonnées de message sont ajoutées pour chaque message publié : `messageID` et l’indicateur `republished` (`true` s’il a été publié plus d’une fois) — accessibles via les en-têtes du message.

N’utilisez pas la même table pour les insertions et les vues matérialisées.

Exemple :

```sql
  CREATE TABLE queue (
    key UInt64,
    value UInt64
  ) ENGINE = RabbitMQ SETTINGS rabbitmq_host_port = 'localhost:5672',
                            rabbitmq_exchange_name = 'exchange1',
                            rabbitmq_exchange_type = 'headers',
                            rabbitmq_routing_key_list = 'format=logs,type=report,year=2020',
                            rabbitmq_format = 'JSONEachRow',
                            rabbitmq_num_consumers = 5;

  CREATE TABLE daily (key UInt64, value UInt64)
    ENGINE = MergeTree() ORDER BY key;

  CREATE MATERIALIZED VIEW consumer TO daily
    AS SELECT key, value FROM queue;

  SELECT key, value FROM daily ORDER BY key;
```

<div id="virtual-columns">
  ## Colonnes virtuelles
</div>

* `_exchange_name` - Nom de l’exchange RabbitMQ. Type de données : `String`.
* `_channel_id` - ChannelID sur lequel le consumer ayant reçu le message a été déclaré. Type de données : `String`.
* `_delivery_tag` - DeliveryTag du message reçu. Propre à chaque canal. Type de données : `UInt64`.
* `_redelivered` - indicateur `redelivered` du message. Type de données : `UInt8`.
* `_message_id` - messageID du message reçu ; non vide s’il a été défini lors de la publication du message. Type de données : `String`.
* `_timestamp` - timestamp du message reçu ; non vide s’il a été défini lors de la publication du message. Type de données : `UInt64`.

Colonnes virtuelles supplémentaires lorsque `rabbitmq_handle_error_mode='stream'` :

* `_raw_message` - Message brut qui n’a pas pu être analysé correctement. Type de données : `Nullable(String)`.
* `_error` - Message d’exception survenu lors de l’échec de l’analyse. Type de données : `Nullable(String)`.

Remarque : les colonnes virtuelles `_raw_message` et `_error` sont renseignées uniquement en cas d’exception pendant l’analyse ; elles sont toujours à `NULL` lorsque le message a été analysé correctement.

<div id="caveats">
  ## Points à noter
</div>

Même si vous pouvez spécifier des [expressions par défaut de colonne](/fr/sql-reference/statements/create/table.md/#default_values) (telles que `DEFAULT`, `MATERIALIZED`, `ALIAS`) dans la définition de la table, elles seront ignorées. À la place, les colonnes seront renseignées avec les valeurs par défaut correspondant à leur type.

<div id="data-formats-support">
  ## Prise en charge des formats de données
</div>

Le moteur RabbitMQ prend en charge tous les [formats](../../../interfaces/formats.md) pris en charge par ClickHouse.
Le nombre de lignes dans un message RabbitMQ dépend du fait que le format soit basé sur les lignes ou sur les blocs :

* Pour les formats basés sur les lignes, le nombre de lignes dans un message RabbitMQ peut être contrôlé en définissant `rabbitmq_max_rows_per_message`.
* Pour les formats basés sur les blocs, il n’est pas possible de diviser un bloc en parties plus petites, mais le nombre de lignes dans un bloc peut être contrôlé par le paramètre général [max&#95;block&#95;size](/fr/operations/settings/settings#max_block_size).