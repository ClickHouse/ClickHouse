---
machine_translated: true
---

# Surveiller {#monitoring}

Vous pouvez surveiller:

-   L'utilisation des ressources matérielles.
-   Statistiques du serveur ClickHouse.

## L'Utilisation Des Ressources {#resource-utilization}

ClickHouse ne surveille pas l'état des ressources matérielles par lui-même.

Il est fortement recommandé de configurer la surveillance de:

-   Charge et température sur les processeurs.

    Vous pouvez utiliser [dmesg](https://en.wikipedia.org/wiki/Dmesg), [turbostat](https://www.linux.org/docs/man8/turbostat.html) ou d'autres instruments.

-   Utilisation du système de stockage, de la RAM et du réseau.

## Métriques Du Serveur ClickHouse {#clickhouse-server-metrics}

Clickhouse server a des instruments embarqués pour la surveillance de l'auto-état.

Pour suivre les événements du serveur, utilisez les journaux du serveur. Voir la [enregistreur](server_settings/settings.md#server_settings-logger) section du fichier de configuration.

Clickhouse recueille:

-   Différentes mesures de la façon dont le serveur utilise les ressources de calcul.
-   Statistiques communes sur le traitement des requêtes.

Vous pouvez trouver des mesures dans le [système.métrique](system_tables.md#system_tables-metrics), [système.événement](system_tables.md#system_tables-events), et [système.asynchronous\_metrics](system_tables.md#system_tables-asynchronous_metrics) table.

Vous pouvez configurer ClickHouse pour exporter des métriques vers [Graphite](https://github.com/graphite-project). Voir la [Graphite section](server_settings/settings.md#server_settings-graphite) dans le fichier de configuration du serveur ClickHouse. Avant de configurer l'exportation des métriques, vous devez configurer Graphite en suivant leur [guide](https://graphite.readthedocs.io/en/latest/install.html).

De plus, vous pouvez surveiller la disponibilité du serveur via L'API HTTP. Envoyer la `HTTP GET` demande à `/ping`. Si le serveur est disponible, il répond avec `200 OK`.

Pour surveiller les serveurs dans une configuration de cluster, vous devez [max\_replica\_delay\_for\_distributed\_queries](settings/settings.md#settings-max_replica_delay_for_distributed_queries) paramètre et utiliser la ressource HTTP `/replicas_status`. Une demande de `/replicas_status` retourner `200 OK` si la réplique est disponible et n'est pas retardé derrière les autres réplicas. Si une réplique est retardée, elle revient `503 HTTP_SERVICE_UNAVAILABLE` avec des informations sur l'écart.
