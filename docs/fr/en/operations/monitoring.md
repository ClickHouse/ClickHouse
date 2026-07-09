---
description: 'Vous pouvez surveiller l''utilisation des ressources matérielles ainsi que les
  métriques du serveur ClickHouse.'
keywords: ['monitoring', 'observability', 'advanced dashboard', 'dashboard', 'observability
    dashboard']
sidebar_label: 'Monitoring'
sidebar_position: 45
slug: /operations/monitoring
title: 'Monitoring'
doc_type: 'reference'
---

import Image from '@theme/IdealImage';

<div id="monitoring">
  # Monitoring
</div>

:::note
Les données de monitoring décrites dans ce guide sont accessibles dans ClickHouse Cloud. En plus d’être affichées dans le tableau de bord intégré décrit ci-dessous, les métriques de performance, de base comme avancées, peuvent également être consultées directement dans la console principale du service.
:::

Vous pouvez surveiller :

* L’utilisation des ressources matérielles.
* Les métriques du serveur ClickHouse.

<div id="built-in-advanced-observability-dashboard">
  ## Tableau de bord d’observability avancé intégré
</div>

<Image img="https://github.com/ClickHouse/ClickHouse/assets/3936029/2bd10011-4a47-4b94-b836-d44557c7fdc1" alt="Capture d’écran 2023-11-12 à 6 08 58 PM" size="md" />

ClickHouse intègre une fonctionnalité de tableau de bord d’observability avancé, accessible via `$HOST:$PORT/dashboard` (nécessite un utilisateur et un mot de passe), qui affiche les métriques suivantes :

* Requêtes/seconde
* Utilisation du CPU (cœurs)
* Requêtes en cours
* Merges en cours
* Octets sélectionnés/seconde
* Attente d’IO
* Attente du CPU
* Utilisation CPU du système d’exploitation (userspace)
* Utilisation CPU du système d’exploitation (noyau)
* Lecture depuis le disque
* Lecture depuis le filesystem
* Mémoire (suivie)
* Lignes insérées/seconde
* Nombre total de parts MergeTree
* Nombre maximal de parts par partition

<div id="resource-utilization">
  ## Utilisation des ressources
</div>

ClickHouse surveille également lui-même l’état des ressources matérielles, notamment :

* La charge et la température des processeurs.
* L’utilisation du système de stockage, de la RAM et du réseau.

Ces données sont collectées dans la table `system.asynchronous_metric_log`.

<div id="clickhouse-server-metrics">
  ## Métriques du serveur ClickHouse
</div>

Le serveur ClickHouse dispose de mécanismes intégrés pour surveiller son propre état.

Pour suivre les événements du serveur, utilisez les logs du serveur. Consultez la section [logger](../operations/server-configuration-parameters/settings.md#logger) du fichier de configuration.

ClickHouse collecte :

* Différentes métriques sur l&#39;utilisation des ressources de calcul par le serveur.
* Des statistiques générales sur le traitement des requêtes.

Vous pouvez trouver ces métriques dans les tables [system.metrics](/fr/operations/system-tables/metrics), [system.events](/fr/operations/system-tables/events) et [system.asynchronous&#95;metrics](/fr/operations/system-tables/asynchronous_metrics).

Vous pouvez configurer ClickHouse pour exporter des métriques vers [Graphite](https://github.com/graphite-project). Consultez la [section Graphite](../operations/server-configuration-parameters/settings.md#graphite) dans le fichier de configuration du serveur ClickHouse. Avant de configurer l&#39;export des métriques, vous devez d&#39;abord configurer Graphite en suivant le [guide](https://graphite.readthedocs.io/en/latest/install.html) officiel.

Vous pouvez configurer ClickHouse pour exporter des métriques vers [Prometheus](https://prometheus.io). Consultez la [section Prometheus](../operations/server-configuration-parameters/settings.md#prometheus) dans le fichier de configuration du serveur ClickHouse. Avant de configurer l&#39;export des métriques, vous devez d&#39;abord configurer Prometheus en suivant le [guide](https://prometheus.io/docs/prometheus/latest/installation/) officiel.

De plus, vous pouvez surveiller la disponibilité du serveur via l&#39;API HTTP. Envoyez la requête `HTTP GET` à `/ping`. Si le serveur est disponible, il renvoie `200 OK`.

Pour surveiller les serveurs dans une configuration de cluster, vous devez définir le paramètre [max&#95;replica&#95;delay&#95;for&#95;distributed&#95;queries](../operations/settings/settings.md#max_replica_delay_for_distributed_queries) et utiliser la ressource HTTP `/replicas_status`. Une requête à `/replicas_status` renvoie `200 OK` si la réplique est disponible et n&#39;est pas en retard par rapport aux autres répliques. Si une réplique est en retard, elle renvoie `503 HTTP_SERVICE_UNAVAILABLE` avec des informations sur ce décalage.