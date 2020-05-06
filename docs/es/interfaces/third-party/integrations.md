---
machine_translated: true
machine_translated_rev: 3e185d24c9fe772c7cf03d5475247fb829a21dfa
toc_priority: 27
toc_title: "Integraci\xF3n"
---

# Bibliotecas de integración de desarrolladores externos {#integration-libraries-from-third-party-developers}

!!! warning "Descargo"
    Yandex hace **ni** mantenga las herramientas y bibliotecas que se enumeran a continuación y no haya realizado ninguna prueba extensa para garantizar su calidad.

## Productos de infraestructura {#infrastructure-products}

-   Sistemas de gestión de bases de datos relacionales
    -   [MySQL](https://www.mysql.com)
        -   [Nombre de la red inalámbrica (SSID):](https://github.com/sysown/proxysql/wiki/ClickHouse-Support)
        -   [Casa de clic-mysql-lector de datos](https://github.com/Altinity/clickhouse-mysql-data-reader)
        -   [Horgh-replicador](https://github.com/larsnovikov/horgh-replicator)
    -   [PostgreSQL](https://www.postgresql.org)
        -   [Haga clickhousedb\_fdw](https://github.com/Percona-Lab/clickhousedb_fdw)
        -   [InformaciónSistema abierto.](https://github.com/Infinidat/infi.clickhouse_fdw) (utilizar [InformaciónSistema abierto.](https://github.com/Infinidat/infi.clickhouse_orm))
        -   [Descripción](https://github.com/mkabilov/pg2ch)
        -   [Sistema abierto.](https://github.com/adjust/clickhouse_fdw)
    -   [MSSQL](https://en.wikipedia.org/wiki/Microsoft_SQL_Server)
        -   [Método de codificación de datos:](https://github.com/zlzforever/ClickHouseMigrator)
-   Colas de mensajes
    -   [Kafka](https://kafka.apache.org)
        -   [clickhouse\_sinker](https://github.com/housepower/clickhouse_sinker) (usos [Go client](https://github.com/ClickHouse/clickhouse-go/))
-   Procesamiento de flujo
    -   [Flink](https://flink.apache.org)
        -   [flink-clickhouse-sink](https://github.com/ivi-ru/flink-clickhouse-sink)
-   Almacenamiento de objetos
    -   [S3](https://en.wikipedia.org/wiki/Amazon_S3)
        -   [Haga clic en el botón de copia de seguridad](https://github.com/AlexAkulov/clickhouse-backup)
-   Orquestación de contenedores
    -   [Kubernetes](https://kubernetes.io)
        -   [Operador de clickhouse](https://github.com/Altinity/clickhouse-operator)
-   Gestión de configuración
    -   [marioneta](https://puppet.com)
        -   [Bienvenidos al Portal de LicitaciÃ³n ElectrÃ³nica de LicitaciÃ³n ElectrÃ³nica](https://forge.puppet.com/innogames/clickhouse)
        -   [Sistema abierto.](https://forge.puppet.com/mfedotov/clickhouse)
-   Monitoreo
    -   [Grafito](https://graphiteapp.org)
        -   [graphouse](https://github.com/yandex/graphouse)
        -   [de carbono-clickhouse](https://github.com/lomik/carbon-clickhouse) +
        -   [Sistema abierto.](https://github.com/lomik/graphite-clickhouse)
        -   [Grafito-ch-optimizador](https://github.com/innogames/graphite-ch-optimizer) - optimiza las particiones [\*GraphiteMergeTree](../../engines/table_engines/mergetree_family/graphitemergetree.md#graphitemergetree) reglas de [Configuración de rollup](../../engines/table_engines/mergetree_family/graphitemergetree.md#rollup-configuration) podría ser aplicado
    -   [Grafana](https://grafana.com/)
        -   [Bienvenidos al Portal de LicitaciÃ³n ElectrÃ³nica de LicitaciÃ³n ElectrÃ³nica](https://github.com/Vertamedia/clickhouse-grafana)
    -   [Prometeo](https://prometheus.io/)
        -   [Sistema abierto.](https://github.com/f1yegor/clickhouse_exporter)
        -   [Bienvenido](https://github.com/Percona-Lab/PromHouse)
        -   [Sistema abierto.](https://github.com/hot-wifi/clickhouse_exporter) (utilizar [Ir cliente](https://github.com/kshvakov/clickhouse/))
    -   [Nagios](https://www.nagios.org/)
        -   [Bienvenidos al Portal de LicitaciÃ³n ElectrÃ³nica de LicitaciÃ³n ElectrÃ³nica](https://github.com/exogroup/check_clickhouse/)
        -   [Inicio](https://github.com/innogames/igmonplugins/blob/master/src/check_clickhouse.py)
    -   [Zabbix](https://www.zabbix.com)
        -   [Sistema abierto.](https://github.com/Altinity/clickhouse-zabbix-template)
    -   [Sematext](https://sematext.com/)
        -   [integración clickhouse](https://github.com/sematext/sematext-agent-integrations/tree/master/clickhouse)
-   Tala
    -   [rsyslog](https://www.rsyslog.com/)
        -   [Bienvenidos al Portal de LicitaciÃ³n ElectrÃ³nica de LicitaciÃ³n ElectrÃ³nica](https://www.rsyslog.com/doc/master/configuration/modules/omclickhouse.html)
    -   [fluentd](https://www.fluentd.org)
        -   [casa de campo](https://github.com/flant/loghouse) (para [Kubernetes](https://kubernetes.io))
    -   [Información](https://www.sematext.com/logagent)
        -   [Sistema de tabiquería interior y exterior](https://sematext.com/docs/logagent/output-plugin-clickhouse/)
-   Geo
    -   [MaxMind](https://dev.maxmind.com/geoip/)
        -   [Para que usted pueda encontrar](https://github.com/AlexeyKupershtokh/clickhouse-maxmind-geoip)

## Programación de ecosistemas de lenguaje {#programming-language-ecosystems}

-   Película
    -   [SQLAlchemy](https://www.sqlalchemy.org)
        -   [sqlalchemy-clickhouse](https://github.com/cloudflare/sqlalchemy-clickhouse) (utilizar [InformaciónSistema abierto.](https://github.com/Infinidat/infi.clickhouse_orm))
    -   [pandas](https://pandas.pydata.org)
        -   [Pandahouse](https://github.com/kszucs/pandahouse)
- PHP
    -   [Doctrine](https://www.doctrine-project.org/)
        -   [dbal-clickhouse](https://packagist.org/packages/friendsofdoctrine/dbal-clickhouse)
-   R
    -   [Dplyr](https://db.rstudio.com/dplyr/)
        -   [Bienvenidos al Portal de LicitaciÃ³n ElectrÃ³nica de LicitaciÃ³n ElectrÃ³nica](https://github.com/IMSMWU/RClickhouse) (utilizar [Bienvenidos](https://github.com/artpaul/clickhouse-cpp))
-   Java
    -   [Hadoop](http://hadoop.apache.org)
        -   [Sistema abierto.](https://github.com/jaykelin/clickhouse-hdfs-loader) (utilizar [JDBC](../../sql_reference/table_functions/jdbc.md))
-   Ciudad
    -   [Akka](https://akka.io)
        -   [Sistema abierto.](https://github.com/crobox/clickhouse-scala-client)
-   C\#
    -   [ADO.NET](https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/ado-net-overview)
        -   [Sistema abierto.Ado](https://github.com/killwort/ClickHouse-Net)
        -   [Sistema abierto.Cliente](https://github.com/DarkWanderer/ClickHouse.Client)
        -   [Sistema abierto.](https://github.com/ilyabreev/ClickHouse.Net)
        -   [Bienvenidos al Portal de LicitaciÃ³n ElectrÃ³nica de LicitaciÃ³n ElectrÃ³nica](https://github.com/ilyabreev/ClickHouse.Net.Migrations)
-   Elixir
    -   [Ecto](https://github.com/elixir-ecto/ecto)
        -   [Método de codificación de datos:](https://github.com/appodeal/clickhouse_ecto)
-   Ruby
    -   [Ruby on Rails](https://rubyonrails.org/)
        -   [activecube](https://github.com/bitquery/activecube)
        -   [ActiveRecord](https://github.com/PNixx/clickhouse-activerecord)
    -   [GraphQL](https://github.com/graphql)
        -   [activecube-graphql](https://github.com/bitquery/activecube-graphql)

[Artículo Original](https://clickhouse.tech/docs/en/interfaces/third-party/integrations/) <!--hide-->
