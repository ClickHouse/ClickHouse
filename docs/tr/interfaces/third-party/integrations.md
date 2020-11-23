---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 27
toc_title: Entegrasyonlar
---

# Üçüncü taraf geliştiricilerin entegrasyon kütüphaneleri {#integration-libraries-from-third-party-developers}

!!! warning "Uyarı"
    Yandex yapar **değil** Aşağıda listelenen araçları ve kütüphaneleri koruyun ve kalitelerini sağlamak için kapsamlı bir test yapmadınız.

## Altyapı Ürünleri {#infrastructure-products}

-   İlişkisel veritabanı yönetim sistemleri
    -   [MySQL](https://www.mysql.com)
        -   [mysql2ch](https://github.com/long2ice/mysql2ch)
        -   [ProxySQL](https://github.com/sysown/proxysql/wiki/ClickHouse-Support)
        -   [clickhouse-mysql-data-reader](https://github.com/Altinity/clickhouse-mysql-data-reader)
        -   [horgh-çoğaltıcı](https://github.com/larsnovikov/horgh-replicator)
    -   [PostgreSQL](https://www.postgresql.org)
        -   [clickhousedb_fdw](https://github.com/Percona-Lab/clickhousedb_fdw)
        -   [ınfi.clickhouse_fdw](https://github.com/Infinidat/infi.clickhouse_fdw) (kullanma [ınfi.clickhouse_orm](https://github.com/Infinidat/infi.clickhouse_orm))
        -   [pg2ch](https://github.com/mkabilov/pg2ch)
        -   [clickhouse_fdw](https://github.com/adjust/clickhouse_fdw)
    -   [MSSQL](https://en.wikipedia.org/wiki/Microsoft_SQL_Server)
        -   [ClickHouseMigrator](https://github.com/zlzforever/ClickHouseMigrator)
-   Mesaj kuyrukları
    -   [Kafka](https://kafka.apache.org)
        -   [clickhouse_sinker](https://github.com/housepower/clickhouse_sinker) (kullanma [Go client](https://github.com/ClickHouse/clickhouse-go/))
        -   [stream-loader-clickhouse](https://github.com/adform/stream-loader)
-   Akış işleme
    -   [Flink](https://flink.apache.org)
        -   [flink-clickhouse-lavabo](https://github.com/ivi-ru/flink-clickhouse-sink)
-   Nesne depoları
    -   [S3](https://en.wikipedia.org/wiki/Amazon_S3)
        -   [clickhouse-yedekleme](https://github.com/AlexAkulov/clickhouse-backup)
-   Konteyner orkestrasyonu
    -   [Kubernetes](https://kubernetes.io)
        -   [clickhouse-operatör](https://github.com/Altinity/clickhouse-operator)
-   Yapılandırma yönetimi
    -   [kuklacı](https://puppet.com)
        -   [ınnogames / clickhouse](https://forge.puppet.com/innogames/clickhouse)
        -   [mfedotov/clickhouse](https://forge.puppet.com/mfedotov/clickhouse)
-   İzleme
    -   [Grafit](https://graphiteapp.org)
        -   [graphouse](https://github.com/yandex/graphouse)
        -   [karbon-clickhouse](https://github.com/lomik/carbon-clickhouse) +
        -   [grafit-clickhouse](https://github.com/lomik/graphite-clickhouse)
        -   [grafit-ch-doktoru](https://github.com/innogames/graphite-ch-optimizer) - staled bölümleri optimize eder [\* Graphıtemergetree](../../engines/table-engines/mergetree-family/graphitemergetree.md#graphitemergetree) eğer kurallar [toplaması yapılandırması](../../engines/table-engines/mergetree-family/graphitemergetree.md#rollup-configuration) uygulanabilir
    -   [Grafana](https://grafana.com/)
        -   [clickhouse-grafana](https://github.com/Vertamedia/clickhouse-grafana)
    -   [Prometheus](https://prometheus.io/)
        -   [clickhouse_exporter](https://github.com/f1yegor/clickhouse_exporter)
        -   [PromHouse](https://github.com/Percona-Lab/PromHouse)
        -   [clickhouse_exporter](https://github.com/hot-wifi/clickhouse_exporter) (kullanma [Go client](https://github.com/kshvakov/clickhouse/))
    -   [Nagios](https://www.nagios.org/)
        -   [check_clickhouse](https://github.com/exogroup/check_clickhouse/)
        -   [check_clickhouse.py](https://github.com/innogames/igmonplugins/blob/master/src/check_clickhouse.py)
    -   [Zabbix](https://www.zabbix.com)
        -   [clickhouse-zabbix-şablon](https://github.com/Altinity/clickhouse-zabbix-template)
    -   [Sematext](https://sematext.com/)
        -   [clickhouse entegrasyonu](https://github.com/sematext/sematext-agent-integrations/tree/master/clickhouse)
-   Günlük
    -   [rsyslog](https://www.rsyslog.com/)
        -   [omclickhouse](https://www.rsyslog.com/doc/master/configuration/modules/omclickhouse.html)
    -   [fluentd](https://www.fluentd.org)
        -   [loghouse](https://github.com/flant/loghouse) (içinde [Kubernetes](https://kubernetes.io))
    -   [logagent](https://www.sematext.com/logagent)
        -   [logagent çıktı-eklenti-clickhouse](https://sematext.com/docs/logagent/output-plugin-clickhouse/)
-   G geoeo
    -   [MaxMind](https://dev.maxmind.com/geoip/)
        -   [clickhouse-maxmind-geoıp](https://github.com/AlexeyKupershtokh/clickhouse-maxmind-geoip)

## Programlama Dili {#programming-language-ecosystems}

-   Piton
    -   [SQLAlchemy](https://www.sqlalchemy.org)
        -   [sqlalchemy-clickhouse](https://github.com/cloudflare/sqlalchemy-clickhouse) (kullanma [ınfi.clickhouse_orm](https://github.com/Infinidat/infi.clickhouse_orm))
    -   [Pandalar](https://pandas.pydata.org)
        -   [pandahouse](https://github.com/kszucs/pandahouse)
-   PHP
    -   [Doktrin](https://www.doctrine-project.org/)
        -   [dbal-clickhouse](https://packagist.org/packages/friendsofdoctrine/dbal-clickhouse)
-   R
    -   [dplyr](https://db.rstudio.com/dplyr/)
        -   [RClickHouse](https://github.com/IMSMWU/RClickHouse) (kullanma [clickhouse-cpp](https://github.com/artpaul/clickhouse-cpp))
-   Java
    -   [Hadoop](http://hadoop.apache.org)
        -   [clickhouse-hdfs-loader](https://github.com/jaykelin/clickhouse-hdfs-loader) (kullanma [JDBC](../../sql-reference/table-functions/jdbc.md))
-   SC scalaala
    -   [Akka](https://akka.io)
        -   [clickhouse-Scala-istemci](https://github.com/crobox/clickhouse-scala-client)
-   C#
    -   [ADO.NET](https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/ado-net-overview)
        -   [ClickHouse.Gürültü](https://github.com/killwort/ClickHouse-Net)
        -   [ClickHouse.Müşteri](https://github.com/DarkWanderer/ClickHouse.Client)
        -   [ClickHouse.Net](https://github.com/ilyabreev/ClickHouse.Net)
        -   [ClickHouse. Net. Migrations](https://github.com/ilyabreev/ClickHouse.Net.Migrations)
-   İksir
    -   [Ecto](https://github.com/elixir-ecto/ecto)
        -   [clickhouse_ecto](https://github.com/appodeal/clickhouse_ecto)
-   Ruby
    -   [Ruby on Rails](https://rubyonrails.org/)
        -   [activecube](https://github.com/bitquery/activecube)
        -   [ActiveRecord](https://github.com/PNixx/clickhouse-activerecord)
    -   [GraphQL](https://github.com/graphql)
        -   [activecube-graphql](https://github.com/bitquery/activecube-graphql)

[Orijinal makale](https://clickhouse.tech/docs/en/interfaces/third-party/integrations/) <!--hide-->
