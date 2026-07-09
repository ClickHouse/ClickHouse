---
description: 'Vue d’ensemble des tables système et de leur utilité.'
keywords: ['tables système', 'vue d’ensemble']
sidebar_label: 'Vue d’ensemble'
sidebar_position: 52
slug: /operations/system-tables/overview
title: 'Vue d’ensemble des tables système'
doc_type: 'reference'
---

<div id="system-tables-introduction">
  ## Vue d’ensemble des tables système
</div>

Les tables système fournissent des informations sur :

* Les états du serveur, les processus et l’environnement.
* Les processus internes du serveur.
* Les options utilisées lors de la compilation du binaire ClickHouse.

Les tables système :

* Sont situées dans la base de données `system`.
* Sont disponibles uniquement en lecture.
* Ne peuvent pas être supprimées ni modifiées, mais peuvent être détachées.

La plupart des tables système stockent leurs données en RAM. Un serveur ClickHouse crée ces tables système au démarrage.

Contrairement aux autres tables système, les tables de journaux système [metric&#95;log](../../operations/system-tables/metric_log.md), [query&#95;log](../../operations/system-tables/query_log.md), [query&#95;thread&#95;log](../../operations/system-tables/query_thread_log.md), [trace&#95;log](../../operations/system-tables/trace_log.md), [part&#95;log](../../operations/system-tables/part_log.md), [crash&#95;log](../../operations/system-tables/crash_log.md), [text&#95;log](../../operations/system-tables/text_log.md) et [backup&#95;log](../../operations/system-tables/backup_log.md) utilisent le table engine [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) et stockent leurs données dans un système de fichiers par défaut. Si vous supprimez une table du système de fichiers, le serveur ClickHouse en recrée une vide lors de la prochaine écriture de données. Si le schéma d’une table système a changé dans une nouvelle version, ClickHouse renomme alors la table actuelle et en crée une nouvelle.

Les tables de journaux système peuvent être personnalisées en créant un fichier de configuration portant le même nom que la table sous `/etc/clickhouse-server/config.d/`, ou en définissant les éléments correspondants dans `/etc/clickhouse-server/config.xml`. Les éléments pouvant être personnalisés sont :

* `database` : base de données à laquelle appartient la table de journaux système. Cette option est désormais obsolète. Toutes les tables de journaux système se trouvent dans la base de données `system`.
* `table` : table dans laquelle insérer des données.
* `partition_by` : spécifie l’expression [PARTITION BY](../../engines/table-engines/mergetree-family/custom-partitioning-key.md).
* `ttl` : spécifie l’expression [TTL](../../sql-reference/statements/alter/ttl.md) de la table.
* `flush_interval_milliseconds` : intervalle de vidage des données sur le disque.
* `engine` : fournit l’expression complète de l’engine (commençant par `ENGINE =` ) avec ses paramètres. Cette option est incompatible avec `partition_by` et `ttl`. Si elles sont définies ensemble, le serveur lèvera une exception et s’arrêtera.

Un exemple :

```xml
<clickhouse>
    <query_log>
        <database>system</database>
        <table>query_log</table>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <ttl>event_date + INTERVAL 30 DAY DELETE</ttl>
        <!--
        <engine>ENGINE = MergeTree PARTITION BY toYYYYMM(event_date) ORDER BY (event_date, event_time) SETTINGS index_granularity = 1024</engine>
        -->
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </query_log>
</clickhouse>
```

Par défaut, la croissance des tables n’est pas limitée. Pour contrôler la taille d’une table, vous pouvez utiliser les paramètres [TTL](/fr/sql-reference/statements/alter/ttl) pour supprimer les anciens enregistrements de journal. Vous pouvez également utiliser le partitionnement des tables utilisant le moteur `MergeTree`.

<div id="system-tables-sources-of-system-metrics">
  ## Sources des métriques système
</div>

Pour collecter les métriques système, le serveur ClickHouse utilise :

* la capacité `CAP_NET_ADMIN`.
* [procfs](https://en.wikipedia.org/wiki/Procfs) (uniquement sous Linux).

**procfs**

Si le serveur ClickHouse ne dispose pas de la capacité `CAP_NET_ADMIN`, il essaie de basculer vers `ProcfsMetricsProvider`. `ProcfsMetricsProvider` permet de collecter des métriques système par requête (pour le CPU et les E/S).

Si procfs est pris en charge et activé sur le système, le serveur ClickHouse collecte les métriques suivantes :

* `OSCPUVirtualTimeMicroseconds`
* `OSCPUWaitMicroseconds`
* `OSIOWaitMicroseconds`
* `OSReadChars`
* `OSWriteChars`
* `OSReadBytes`
* `OSWriteBytes`

:::note
`OSIOWaitMicroseconds` est désactivé par défaut dans les noyaux Linux à partir de la version 5.14.x.
Vous pouvez l’activer avec `sudo sysctl kernel.task_delayacct=1` ou en créant un fichier `.conf` dans `/etc/sysctl.d/` avec `kernel.task_delayacct = 1`
:::

<div id="system-tables-in-clickhouse-cloud">
  ## Tables système dans ClickHouse Cloud
</div>

Dans ClickHouse Cloud, les tables système fournissent des informations essentielles sur l’état et les performances du service, tout comme dans les déploiements autogérés. Certaines tables système s’appliquent à l’échelle de l’ensemble du cluster, en particulier celles qui tirent leurs données des nœuds Keeper, chargés de gérer les métadonnées distribuées. Ces tables reflètent l’état global du cluster et doivent être cohérentes lorsqu’elles sont interrogées sur des nœuds individuels. Par exemple, les résultats de [`parts`](/fr/operations/system-tables/parts) doivent être cohérents, quel que soit le nœud interrogé :

```sql
SELECT hostname(), count()
FROM system.parts
WHERE `table` = 'pypi'

┌─hostname()────────────────────┬─count()─┐
│ c-ecru-qn-34-server-vccsrty-0 │      26 │
└───────────────────────────────┴─────────┘

1 row in set. Elapsed: 0.005 sec.

SELECT
 hostname(),
    count()
FROM system.parts
WHERE `table` = 'pypi'

┌─hostname()────────────────────┬─count()─┐
│ c-ecru-qn-34-server-w59bfco-0 │      26 │
└───────────────────────────────┴─────────┘

1 row in set. Elapsed: 0.004 sec.
```

À l’inverse, d’autres tables système sont spécifiques à un nœud, par exemple lorsqu’elles résident en mémoire ou persistent leurs données à l’aide du table engine MergeTree. C’est typiquement le cas pour des données telles que les logs et les métriques. Cette persistance garantit que les données historiques restent disponibles pour l’analyse. Toutefois, ces tables spécifiques à un nœud sont, par nature, propres à chaque nœud.

De manière générale, les règles suivantes permettent de déterminer si une table système est spécifique à un nœud :

* Les tables système avec le suffixe `_log`.
* Les tables système qui exposent des métriques, par exemple `metrics`, `asynchronous_metrics`, `events`.
* Les tables système qui exposent des processus en cours, par exemple `processes`, `merges`.

De plus, de nouvelles versions de tables système peuvent être créées à la suite de mises à niveau ou de modifications de leur schéma. Ces versions sont nommées à l’aide d’un suffixe numérique.

Par exemple, prenons les tables `system.query_log`, qui contiennent une ligne pour chaque requête exécutée par le nœud :

```sql
SHOW TABLES FROM system LIKE 'query_log%'

┌─name─────────┐
│ query_log    │
│ query_log_1  │
│ query_log_10 │
│ query_log_2  │
│ query_log_3  │
│ query_log_4  │
│ query_log_5  │
│ query_log_6  │
│ query_log_7  │
│ query_log_8  │
│ query_log_9  │
└──────────────┘

11 rows in set. Elapsed: 0.004 sec.
```

<div id="querying-multiple-versions">
  ### Interroger plusieurs versions
</div>

Nous pouvons interroger l’ensemble de ces tables à l’aide de la fonction [`merge`](/fr/sql-reference/table-functions/merge). Par exemple, la requête ci-dessous identifie, dans chaque table `query_log`, la dernière requête envoyée au nœud cible :

```sql
SELECT
    _table,
    max(event_time) AS most_recent
FROM merge('system', '^query_log')
GROUP BY _table
ORDER BY most_recent DESC

┌─_table───────┬─────────most_recent─┐
│ query_log    │ 2025-04-13 10:59:29 │
│ query_log_1  │ 2025-04-09 12:34:46 │
│ query_log_2  │ 2025-04-09 12:33:45 │
│ query_log_3  │ 2025-04-07 17:10:34 │
│ query_log_5  │ 2025-03-24 09:39:39 │
│ query_log_4  │ 2025-03-24 09:38:58 │
│ query_log_6  │ 2025-03-19 16:07:41 │
│ query_log_7  │ 2025-03-18 17:01:07 │
│ query_log_8  │ 2025-03-18 14:36:07 │
│ query_log_10 │ 2025-03-18 14:01:33 │
│ query_log_9  │ 2025-03-18 14:01:32 │
└──────────────┴─────────────────────┘

11 rows in set. Elapsed: 0.373 sec. Processed 6.44 million rows, 25.77 MB (17.29 million rows/s., 69.17 MB/s.)
Peak memory usage: 28.45 MiB.
```

:::note Ne vous fiez pas au suffixe numérique pour l’ordre
Même si le suffixe numérique des tables peut laisser penser qu’il reflète l’ordre des données, il ne faut jamais s’y fier. C’est pourquoi vous devez toujours utiliser la fonction de table merge avec un filtre de date pour cibler des plages de dates précises.
:::

Il est important de noter que ces tables restent **locales à chaque nœud**.

<div id="querying-across-nodes">
  ### Interroger l’ensemble des nœuds
</div>

Pour obtenir une vue complète du cluster, les utilisateurs peuvent exploiter la fonction [`clusterAllReplicas`](/fr/sql-reference/table-functions/cluster) en combinaison avec la fonction `merge`. La fonction `clusterAllReplicas` permet d’interroger les tables système sur toutes les répliques du cluster &quot;default&quot;, en regroupant les données propres à chaque nœud dans un résultat unifié. Combinée à la fonction `merge`, elle permet de cibler toutes les données système d’une table spécifique dans un cluster.

Cette approche est particulièrement utile pour la surveillance et le débogage des opérations à l’échelle du cluster, en permettant aux utilisateurs d’analyser efficacement l’état et les performances de leur déploiement ClickHouse Cloud.

:::note
ClickHouse Cloud fournit des clusters composés de plusieurs répliques pour assurer la redondance et le basculement. Cela permet des fonctionnalités telles que l’autoscaling dynamique et les mises à niveau sans interruption. À un instant donné, de nouveaux nœuds peuvent être en cours d’ajout au cluster ou de suppression. Pour ignorer ces nœuds, ajoutez `SETTINGS skip_unavailable_shards = 1` aux requêtes utilisant `clusterAllReplicas`, comme indiqué ci-dessous.
:::

Par exemple, observez la différence lors de l’interrogation de la table `query_log`, souvent essentielle pour l’analyse.

```sql
SELECT
    hostname() AS host,
    count()
FROM system.query_log
WHERE (event_time >= '2025-04-01 00:00:00') AND (event_time <= '2025-04-12 00:00:00')
GROUP BY host

┌─host──────────────────────────┬─count()─┐
│ c-ecru-qn-34-server-s5bnysl-0 │  650543 │
└───────────────────────────────┴─────────┘

1 row in set. Elapsed: 0.010 sec. Processed 17.87 thousand rows, 71.51 KB (1.75 million rows/s., 7.01 MB/s.)

SELECT
    hostname() AS host,
    count()
FROM clusterAllReplicas('default', system.query_log)
WHERE (event_time >= '2025-04-01 00:00:00') AND (event_time <= '2025-04-12 00:00:00')
GROUP BY host SETTINGS skip_unavailable_shards = 1

┌─host──────────────────────────┬─count()─┐
│ c-ecru-qn-34-server-s5bnysl-0 │  650543 │
│ c-ecru-qn-34-server-6em4y4t-0 │  656029 │
│ c-ecru-qn-34-server-iejrkg0-0 │  641155 │
└───────────────────────────────┴─────────┘

3 rows in set. Elapsed: 0.026 sec. Processed 1.97 million rows, 7.88 MB (75.51 million rows/s., 302.05 MB/s.)
```

<div id="querying-across-nodes-and-versions">
  ### Requêtes sur plusieurs nœuds et versions
</div>

En raison du versionnement des tables système, cela ne représente toujours pas l’ensemble des données du cluster. En combinant ce qui précède avec la fonction `merge`, nous obtenons un résultat exact pour notre plage de dates :

```sql
SELECT
    hostname() AS host,
    count()
FROM clusterAllReplicas('default', merge('system', '^query_log'))
WHERE (event_time >= '2025-04-01 00:00:00') AND (event_time <= '2025-04-12 00:00:00')
GROUP BY host SETTINGS skip_unavailable_shards = 1

┌─host──────────────────────────┬─count()─┐
│ c-ecru-qn-34-server-s5bnysl-0 │ 3008000 │
│ c-ecru-qn-34-server-6em4y4t-0 │ 3659443 │
│ c-ecru-qn-34-server-iejrkg0-0 │ 1078287 │
└───────────────────────────────┴─────────┘

3 rows in set. Elapsed: 0.462 sec. Processed 7.94 million rows, 31.75 MB (17.17 million rows/s., 68.67 MB/s.)
```

<div id="related-content">
  ## Contenu connexe
</div>

* Blog : [Tables système et aperçu des rouages internes de ClickHouse](https://clickhouse.com/blog/clickhouse-debugging-issues-with-system-tables)
* Blog : [Requêtes de monitoring essentielles - partie 1 - requêtes INSERT](https://clickhouse.com/blog/monitoring-troubleshooting-insert-queries-clickhouse)
* Blog : [Requêtes de monitoring essentielles - partie 2 - requêtes SELECT](https://clickhouse.com/blog/monitoring-troubleshooting-select-queries-clickhouse)