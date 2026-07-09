---
description: 'Guide d’utilisation d’OpenTelemetry pour le traçage distribué et la collecte
  de métriques dans ClickHouse'
sidebar_label: 'Traçage de ClickHouse avec OpenTelemetry'
sidebar_position: 62
slug: /operations/opentelemetry
title: 'Traçage de ClickHouse avec OpenTelemetry'
doc_type: 'guide'
---

[OpenTelemetry](https://opentelemetry.io/) est une norme ouverte permettant de collecter des traces et des métriques à partir d’applications distribuées. ClickHouse prend en charge OpenTelemetry dans une certaine mesure.

<div id="supplying-trace-context-to-clickhouse">
  ## Fournir le contexte de trace à ClickHouse
</div>

ClickHouse accepte les en-têtes HTTP de contexte de trace, comme décrit dans la [recommandation du W3C](https://www.w3.org/TR/trace-context/). Il accepte également le contexte de trace via le protocole natif utilisé pour la communication entre les serveurs ClickHouse ou entre le client et le serveur. Pour les tests manuels, des en-têtes de contexte de trace conformes à la recommandation Trace Context peuvent être transmis à `clickhouse-client` à l&#39;aide des options `--opentelemetry-traceparent` et `--opentelemetry-tracestate`.

Si aucun contexte de trace parent n&#39;est fourni, ou si le contexte de trace fourni n&#39;est pas conforme à la norme W3C mentionnée ci-dessus, ClickHouse peut démarrer une nouvelle trace, avec une probabilité contrôlée par le paramètre [opentelemetry&#95;start&#95;trace&#95;probability](/fr/operations/settings/settings#opentelemetry_start_trace_probability).

<div id="propagating-the-trace-context">
  ## Propagation du contexte de trace
</div>

Le contexte de trace est propagé aux services en aval dans les cas suivants :

* Requêtes vers des serveurs ClickHouse distants, par exemple lors de l’utilisation du moteur de table [Distributed](../engines/table-engines/special/distributed.md).

* Fonction de table [url](../sql-reference/table-functions/url.md). Les informations du contexte de trace sont envoyées dans les en-têtes HTTP.

<div id="tracing-clickhouse-keeper-requests">
  ## Traçage des requêtes ClickHouse Keeper
</div>

ClickHouse prend en charge le traçage OpenTelemetry des requêtes [ClickHouse Keeper](../guides/sre/keeper/index.md) (service de coordination compatible avec ZooKeeper). Cette fonctionnalité offre une visibilité détaillée sur le cycle de vie des opérations Keeper, de l’envoi des requêtes par le client jusqu’au traitement côté serveur.

<div id="enabling-keeper-tracing">
  ### Activation du traçage de Keeper
</div>

Pour activer le traçage des requêtes Keeper, configurez les paramètres suivants dans la configuration de votre client ZooKeeper/Keeper :

```xml
<clickhouse>
    <zookeeper>
        <node>
            <host>keeper1</host>
            <port>9181</port>
        </node>
        <!-- Enable OpenTelemetry tracing context propagation -->
        <pass_opentelemetry_tracing_context>true</pass_opentelemetry_tracing_context>
    </zookeeper>
</clickhouse>
```

<div id="keeper-span-types">
  ### Types de spans Keeper
</div>

Lorsque le traçage est activé, ClickHouse crée des spans pour les opérations Keeper côté client et côté serveur :

**Spans côté client :**

* `zookeeper.create` — Créer un nouveau nœud
* `zookeeper.get` — Obtenir les données du nœud
* `zookeeper.set` — Définir les données du nœud
* `zookeeper.remove` — Supprimer un nœud
* `zookeeper.list` — Lister les nœuds enfants
* `zookeeper.exists` — Vérifier si un nœud existe
* `zookeeper.multi` — Exécuter plusieurs opérations de manière atomique
* `zookeeper.client.requests_queue` — Temps passé à mettre les requêtes en file d’attente avant leur envoi

**Spans côté serveur (Keeper) :**

* `keeper.receive_request` — Réception et analyse de la requête du client
* `keeper.dispatcher.requests_queue` — Mise en file d’attente des requêtes dans le dispatcher
* `keeper.write.pre_commit` — Prétraitement des requêtes d’écriture avant le Raft commit
* `keeper.write.commit` — Traitement des requêtes d’écriture après le Raft commit
* `keeper.read.wait_for_write` — Requêtes de lecture en attente d’écritures dont elles dépendent
* `keeper.read.process` — Traitement des requêtes de lecture
* `keeper.dispatcher.responses_queue` — Mise en file d’attente des réponses dans le dispatcher
* `keeper.send_response` — Envoi de la réponse au client

<div id="sampling-and-performance">
  ### Échantillonnage et performances
</div>

Pour gérer le surcoût du traçage, Keeper met en œuvre un échantillonnage dynamique. Le taux d’échantillonnage s’ajuste automatiquement entre 1/10 000 et 1/10 en fonction de la taille des requêtes. La durée de toutes les requêtes (échantillonnées ou non) est enregistrée dans des métriques de type histogramme pour surveiller les performances.

<div id="tracing-the-clickhouse-itself">
  ## Traçage de ClickHouse lui-même
</div>

ClickHouse crée des `trace spans` pour chaque requête et pour certaines étapes de son exécution, comme la planification des requêtes ou les requêtes distribuées.

Pour être utiles, les informations de traçage doivent être exportées vers un système de supervision compatible avec OpenTelemetry, comme [Jaeger](https://jaegertracing.io/) ou [Prometheus](https://prometheus.io/). ClickHouse évite toute dépendance à un système de supervision particulier et fournit uniquement les données de traçage via une table système. Les informations sur les `trace spans` OpenTelemetry [requises par la norme](https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/overview.md#span) sont stockées dans la table [system.opentelemetry&#95;span&#95;log](../operations/system-tables/opentelemetry_span_log.md).

La table doit être activée dans la configuration du serveur ; consultez l’élément `opentelemetry_span_log` dans le fichier de configuration par défaut `config.xml`. Elle est activée par défaut.

Les tags ou attributs sont enregistrés sous forme de deux tableaux parallèles, contenant les clés et les valeurs. Utilisez [ARRAY JOIN](../sql-reference/statements/select/array-join.md) pour les exploiter.

<div id="log-query-settings">
  ## Paramètres de journalisation des requêtes
</div>

Le paramètre [log&#95;query&#95;settings](settings/settings.md) permet de consigner les modifications apportées aux paramètres de requête pendant l&#39;exécution d&#39;une requête. Lorsqu&#39;il est activé, toute modification des paramètres de requête est enregistrée dans le journal du span OpenTelemetry. Cette fonctionnalité est particulièrement utile dans les environnements de production pour suivre les changements de configuration susceptibles d&#39;affecter les performances des requêtes.

<div id="integration-with-monitoring-systems">
  ## Intégration avec des systèmes de monitoring
</div>

À l&#39;heure actuelle, il n&#39;existe aucun outil prêt à l&#39;emploi permettant d&#39;exporter les données de traçage de ClickHouse vers un système de monitoring.

À des fins de test, il est possible de configurer l&#39;export à l&#39;aide d&#39;une vue matérialisée avec le moteur [URL](../engines/table-engines/special/url.md) sur la table [system.opentelemetry&#95;span&#95;log](../operations/system-tables/opentelemetry_span_log.md), afin d&#39;envoyer les données de log reçues vers un endpoint HTTP d&#39;un collector de traces. Par exemple, pour envoyer les données minimales de span vers une instance Zipkin exécutée sur `http://localhost:9411`, au format JSON Zipkin v2 :

```sql
CREATE MATERIALIZED VIEW default.zipkin_spans
ENGINE = URL('http://127.0.0.1:9411/api/v2/spans', 'JSONEachRow')
SETTINGS output_format_json_named_tuples_as_objects = 1,
    output_format_json_array_of_rows = 1 AS
SELECT
    lower(hex(trace_id)) AS traceId,
    CASE WHEN parent_span_id = 0 THEN '' ELSE lower(hex(parent_span_id)) END AS parentId,
    lower(hex(span_id)) AS id,
    operation_name AS name,
    start_time_us AS timestamp,
    finish_time_us - start_time_us AS duration,
    cast(tuple('clickhouse'), 'Tuple(serviceName text)') AS localEndpoint,
    cast(tuple(
        attribute.values[indexOf(attribute.names, 'db.statement')]),
        'Tuple("db.statement" text)') AS tags
FROM system.opentelemetry_span_log
```

En cas d’erreur, la partie des données de journalisation concernée sera perdue silencieusement. Si les données n’arrivent pas, consultez les logs du serveur pour voir les messages d’erreur.

<div id="related-content">
  ## Contenu connexe
</div>

* Blog : [Créer une solution d’observability avec ClickHouse - Partie 2 - Traces](https://clickhouse.com/blog/storing-traces-and-spans-open-telemetry-in-clickhouse)