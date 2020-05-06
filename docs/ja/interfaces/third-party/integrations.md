---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 27
toc_title: "\u7D71\u5408"
---

# サードパーティ開発者からの統合ライブラリ {#integration-libraries-from-third-party-developers}

!!! warning "免責事項"
    Yandexは **ない** 以下に示すツールとライブラリを維持し、その品質を保証するための広範なテストを行っていません。

## インフラ製品 {#infrastructure-products}

-   リレーショナルデータベース管理システム
    -   [MySQL](https://www.mysql.com)
        -   [Proxysqlcomment](https://github.com/sysown/proxysql/wiki/ClickHouse-Support)
        -   [clickhouse-mysql-data-reader](https://github.com/Altinity/clickhouse-mysql-data-reader)
        -   [horgh-replicator](https://github.com/larsnovikov/horgh-replicator)
    -   [PostgreSQL](https://www.postgresql.org)
        -   [clickhousedb\_fdw](https://github.com/Percona-Lab/clickhousedb_fdw)
        -   [infi.clickhouse\_fdw](https://github.com/Infinidat/infi.clickhouse_fdw) (用途 [infi.clickhouse\_orm](https://github.com/Infinidat/infi.clickhouse_orm))
        -   [pg2ch](https://github.com/mkabilov/pg2ch)
        -   [clickhouse\_fdw](https://github.com/adjust/clickhouse_fdw)
    -   [MSSQL](https://en.wikipedia.org/wiki/Microsoft_SQL_Server)
        -   [ClickHouseMigrator](https://github.com/zlzforever/ClickHouseMigrator)
-   メッセージキュ
    -   [カフカname](https://kafka.apache.org)
        -   [clickhouse\_sinker](https://github.com/housepower/clickhouse_sinker) (用途 [クライアントへ](https://github.com/kshvakov/clickhouse/))
-   オブジェクト保存
    -   [S3](https://en.wikipedia.org/wiki/Amazon_S3)
        -   [clickhouse-バックアップ](https://github.com/AlexAkulov/clickhouse-backup)
-   容器の協奏
    -   [Kubernetes](https://kubernetes.io)
        -   [クリックハウス-演算子](https://github.com/Altinity/clickhouse-operator)
-   構成管理
    -   [人形](https://puppet.com)
        -   [innogames/clickhouse](https://forge.puppet.com/innogames/clickhouse)
        -   [mfedotov/clickhouse](https://forge.puppet.com/mfedotov/clickhouse)
-   監視
    -   [黒鉛](https://graphiteapp.org)
        -   [グラファウス](https://github.com/yandex/graphouse)
        -   [カーボンクリックハウス](https://github.com/lomik/carbon-clickhouse) +
        -   [黒鉛-clickhouse](https://github.com/lomik/graphite-clickhouse)
        -   [graphite-ch-optimizer](https://github.com/innogames/graphite-ch-optimizer) -staledパーティションを最適化します。 [\*グラフィットマージツリー](../../engines/table_engines/mergetree_family/graphitemergetree.md#graphitemergetree) からのルールの場合 [ロールアップ構成](../../engines/table_engines/mergetree_family/graphitemergetree.md#rollup-configuration) 適用できます
    -   [グラファナ](https://grafana.com/)
        -   [クリックハウス-グラファナ](https://github.com/Vertamedia/clickhouse-grafana)
    -   [プロメテウス](https://prometheus.io/)
        -   [clickhouse\_exporter](https://github.com/f1yegor/clickhouse_exporter)
        -   [プロムハウス](https://github.com/Percona-Lab/PromHouse)
        -   [clickhouse\_exporter](https://github.com/hot-wifi/clickhouse_exporter) (用途 [クライアントへ](https://github.com/kshvakov/clickhouse/))
    -   [Nagios](https://www.nagios.org/)
        -   [check\_clickhouse](https://github.com/exogroup/check_clickhouse/)
        -   [check\_clickhouse.py](https://github.com/innogames/igmonplugins/blob/master/src/check_clickhouse.py)
    -   [Zabbix](https://www.zabbix.com)
        -   [clickhouse-zabbix-テンプレート](https://github.com/Altinity/clickhouse-zabbix-template)
    -   [Sematextgenericname](https://sematext.com/)
        -   [クリックハウス統合](https://github.com/sematext/sematext-agent-integrations/tree/master/clickhouse)
-   ログ記録
    -   [rsyslog](https://www.rsyslog.com/)
        -   [omclickhouse](https://www.rsyslog.com/doc/master/configuration/modules/omclickhouse.html)
    -   [fluentd](https://www.fluentd.org)
        -   [ログハウス](https://github.com/flant/loghouse) （のために [Kubernetes](https://kubernetes.io))
    -   [logagent](https://www.sematext.com/logagent)
        -   [logagent出力-プラグイン-clickhouse](https://sematext.com/docs/logagent/output-plugin-clickhouse/)
-   ジオ
    -   [MaxMind](https://dev.maxmind.com/geoip/)
        -   [クリックハウス-maxmind-geoip](https://github.com/AlexeyKupershtokh/clickhouse-maxmind-geoip)

## プログラミング言語の生態系 {#programming-language-ecosystems}

-   Python
    -   [SQLAlchemy](https://www.sqlalchemy.org)
        -   [sqlalchemy-clickhouse](https://github.com/cloudflare/sqlalchemy-clickhouse) (用途 [infi.clickhouse\_orm](https://github.com/Infinidat/infi.clickhouse_orm))
    -   [パンダ](https://pandas.pydata.org)
        -   [パンダハウス](https://github.com/kszucs/pandahouse)
- PHP
    -   [Doctrine](https://www.doctrine-project.org/)
        -   [dbal-clickhouse](https://packagist.org/packages/friendsofdoctrine/dbal-clickhouse)
-   R
    -   [dplyr](https://db.rstudio.com/dplyr/)
        -   [Rクリックハウス](https://github.com/IMSMWU/RClickhouse) (用途 [クリックハウス-cpp](https://github.com/artpaul/clickhouse-cpp))
-   Java
    -   [Hadoop](http://hadoop.apache.org)
        -   [クリックハウス-hdfs-ローダー](https://github.com/jaykelin/clickhouse-hdfs-loader) (用途 [JDBC](../../sql_reference/table_functions/jdbc.md))
-   Scala
    -   [Akka](https://akka.io)
        -   [clickhouse-scala-クライアント](https://github.com/crobox/clickhouse-scala-client)
-   C\#
    -   [ADO.NET](https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/ado-net-overview)
        -   [クリックハウス。ado](https://github.com/killwort/ClickHouse-Net)
        -   [クリックハウス。お客様](https://github.com/DarkWanderer/ClickHouse.Client)
        -   [ClickHouse.Net](https://github.com/ilyabreev/ClickHouse.Net)
        -   [ClickHouse.Net.Migrations](https://github.com/ilyabreev/ClickHouse.Net.Migrations)
-   エリクサー
    -   [Ecto](https://github.com/elixir-ecto/ecto)
        -   [clickhouse\_ecto](https://github.com/appodeal/clickhouse_ecto)
-   Ruby
    -   [Ruby on rails](https://rubyonrails.org/)
        -   [activecube](https://github.com/bitquery/activecube)
        -   [ActiveRecord](https://github.com/PNixx/clickhouse-activerecord) 
    -   [GraphQL](https://github.com/graphql)
        -   [activecube-graphql](https://github.com/bitquery/activecube-graphql)

[元の記事](https://clickhouse.tech/docs/en/interfaces/third-party/integrations/) <!--hide-->
