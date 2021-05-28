---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 27
toc_title: "Int\xE9gration"
---

# Bibliothèques d'intégration de développeurs tiers {#integration-libraries-from-third-party-developers}

!!! warning "Avertissement"
    Yandex ne **pas** maintenir les outils et les bibliothèques énumérés ci-dessous et n'ont pas fait de tests approfondis pour assurer leur qualité.

## Produits D'Infrastructure {#infrastructure-products}

-   Systèmes de gestion de bases de données relationnelles
    -   [MySQL](https://www.mysql.com)
        -   [mysql2ch](https://github.com/long2ice/mysql2ch)
        -   [ProxySQL](https://github.com/sysown/proxysql/wiki/ClickHouse-Support)
        -   [clickhouse-mysql-lecteur de données](https://github.com/Altinity/clickhouse-mysql-data-reader)
        -   [horgh-réplicateur](https://github.com/larsnovikov/horgh-replicator)
    -   [PostgreSQL](https://www.postgresql.org)
        -   [clickhousedb\_fdw](https://github.com/Percona-Lab/clickhousedb_fdw)
        -   [infi.clickhouse\_fdw](https://github.com/Infinidat/infi.clickhouse_fdw) (utiliser [infi.clickhouse\_orm](https://github.com/Infinidat/infi.clickhouse_orm))
        -   [pg2ch](https://github.com/mkabilov/pg2ch)
        -   [clickhouse\_fdw](https://github.com/adjust/clickhouse_fdw)
    -   [MSSQL](https://en.wikipedia.org/wiki/Microsoft_SQL_Server)
        -   [ClickHouseMigrator](https://github.com/zlzforever/ClickHouseMigrator)
-   Files d'attente de messages
    -   [Kafka](https://kafka.apache.org)
        -   [clickhouse\_sinker](https://github.com/housepower/clickhouse_sinker) (utiliser [Allez client](https://github.com/ClickHouse/clickhouse-go/))
        -   [stream-loader-clickhouse](https://github.com/adform/stream-loader)
-   Traitement de flux
    -   [Flink](https://flink.apache.org)
        -   [flink-clickhouse-évier](https://github.com/ivi-ru/flink-clickhouse-sink)
-   Objet de stockages
    -   [S3](https://en.wikipedia.org/wiki/Amazon_S3)
        -   [clickhouse-sauvegarde](https://github.com/AlexAkulov/clickhouse-backup)
-   Orchestration de conteneur
    -   [Kubernetes](https://kubernetes.io)
        -   [clickhouse-opérateur](https://github.com/Altinity/clickhouse-operator)
-   Gestion de la Configuration
    -   [marionnette](https://puppet.com)
        -   [innogames / clickhouse](https://forge.puppet.com/innogames/clickhouse)
        -   [mfedotov / clickhouse](https://forge.puppet.com/mfedotov/clickhouse)
-   Surveiller
    -   [Graphite](https://graphiteapp.org)
        -   [graphouse](https://github.com/yandex/graphouse)
        -   [carbone-clickhouse](https://github.com/lomik/carbon-clickhouse) +
        -   [graphite-clickhouse](https://github.com/lomik/graphite-clickhouse)
        -   [graphite-CH-optimizer](https://github.com/innogames/graphite-ch-optimizer) - optimise les partitions calées dans [\* GraphiteMergeTree](../../engines/table-engines/mergetree-family/graphitemergetree.md#graphitemergetree) si les règles de [configuration de cumul](../../engines/table-engines/mergetree-family/graphitemergetree.md#rollup-configuration) pourrait être appliquée
    -   [Grafana](https://grafana.com/)
        -   [clickhouse-grafana](https://github.com/Vertamedia/clickhouse-grafana)
    -   [Prometheus](https://prometheus.io/)
        -   [clickhouse\_exporter](https://github.com/f1yegor/clickhouse_exporter)
        -   [PromHouse](https://github.com/Percona-Lab/PromHouse)
        -   [clickhouse\_exporter](https://github.com/hot-wifi/clickhouse_exporter) (utiliser [Allez client](https://github.com/kshvakov/clickhouse/))
    -   [Nagios](https://www.nagios.org/)
        -   [check\_clickhouse](https://github.com/exogroup/check_clickhouse/)
        -   [check\_clickhouse.py](https://github.com/innogames/igmonplugins/blob/master/src/check_clickhouse.py)
    -   [Zabbix](https://www.zabbix.com)
        -   [clickhouse-Zabbix-modèle](https://github.com/Altinity/clickhouse-zabbix-template)
    -   [Sematext](https://sematext.com/)
        -   [intégration de clickhouse](https://github.com/sematext/sematext-agent-integrations/tree/master/clickhouse)
-   Journalisation
    -   [rsyslog](https://www.rsyslog.com/)
        -   [omclickhouse](https://www.rsyslog.com/doc/master/configuration/modules/omclickhouse.html)
    -   [fluentd](https://www.fluentd.org)
        -   [maison de bois](https://github.com/flant/loghouse) (pour [Kubernetes](https://kubernetes.io))
    -   [logagent](https://www.sematext.com/logagent)
        -   [sortie logagent-plugin-clickhouse](https://sematext.com/docs/logagent/output-plugin-clickhouse/)
-   Geo
    -   [MaxMind](https://dev.maxmind.com/geoip/)
        -   [clickhouse-maxmind-geoip](https://github.com/AlexeyKupershtokh/clickhouse-maxmind-geoip)

## Écosystèmes De Langage De Programmation {#programming-language-ecosystems}

-   Python
    -   [SQLAlchemy](https://www.sqlalchemy.org)
        -   [sqlalchemy-clickhouse](https://github.com/cloudflare/sqlalchemy-clickhouse) (utiliser [infi.clickhouse\_orm](https://github.com/Infinidat/infi.clickhouse_orm))
    -   [Panda](https://pandas.pydata.org)
        -   [pandahouse](https://github.com/kszucs/pandahouse)
-   PHP
    -   [Doctrine](https://www.doctrine-project.org/)
        -   [dbal-clickhouse](https://packagist.org/packages/friendsofdoctrine/dbal-clickhouse)
-   R
    -   [dplyr](https://db.rstudio.com/dplyr/)
        -   [RClickHouse](https://github.com/IMSMWU/RClickHouse) (utiliser [clickhouse-cpp](https://github.com/artpaul/clickhouse-cpp))
-   Java
    -   [Hadoop](http://hadoop.apache.org)
        -   [clickhouse-HDFS-chargeur](https://github.com/jaykelin/clickhouse-hdfs-loader) (utiliser [JDBC](../../sql-reference/table-functions/jdbc.md))
-   Scala
    -   [Akka](https://akka.io)
        -   [clickhouse-Scala-client](https://github.com/crobox/clickhouse-scala-client)
-   C\#
    -   [ADO.NET](https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/ado-net-overview)
        -   [ClickHouse.Ado](https://github.com/killwort/ClickHouse-Net)
        -   [ClickHouse.Client](https://github.com/DarkWanderer/ClickHouse.Client)
        -   [ClickHouse.Net](https://github.com/ilyabreev/ClickHouse.Net)
        -   [ClickHouse.Net.Les Migrations](https://github.com/ilyabreev/ClickHouse.Net.Migrations)
-   Elixir
    -   [Ecto](https://github.com/elixir-ecto/ecto)
        -   [clickhouse\_ecto](https://github.com/appodeal/clickhouse_ecto)
-   Ruby
    -   [Ruby on Rails](https://rubyonrails.org/)
        -   [activecube](https://github.com/bitquery/activecube)
        -   [ActiveRecord](https://github.com/PNixx/clickhouse-activerecord)
    -   [GraphQL](https://github.com/graphql)
        -   [activecube-graphql](https://github.com/bitquery/activecube-graphql)

[Article Original](https://clickhouse.tech/docs/en/interfaces/third-party/integrations/) <!--hide-->
