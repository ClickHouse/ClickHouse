---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 27
toc_title: "\u7D71\u5408"
---

# サードパーティ開発者からの統合ライブラリ {#integration-libraries-from-third-party-developers}

!!! warning "免責事項"
    Yandexのは **ない** 以下のツールとライブラリを維持し、その品質を確保するための広範なテストを行っていません。

## インフラ製品 {#infrastructure-products}

-   リレーショナルデータベース管理システム
    -   [MySQL](https://www.mysql.com)
        -   [mysql2ch](https://github.com/long2ice/mysql2ch)
        -   [ProxySQL](https://github.com/sysown/proxysql/wiki/ClickHouse-Support)
        -   [clickhouse-mysql-データリーダー](https://github.com/Altinity/clickhouse-mysql-data-reader)
        -   [horgh-レプリケーター](https://github.com/larsnovikov/horgh-replicator)
    -   [PostgreSQL](https://www.postgresql.org)
        -   [clickhousedb_fdw](https://github.com/Percona-Lab/clickhousedb_fdw)
        -   [インフィclickhouse_fdw](https://github.com/Infinidat/infi.clickhouse_fdw) （用途 [インフィclickhouse_orm](https://github.com/Infinidat/infi.clickhouse_orm))
        -   [pg2ch](https://github.com/mkabilov/pg2ch)
        -   [clickhouse_fdw](https://github.com/adjust/clickhouse_fdw)
    -   [MSSQL](https://en.wikipedia.org/wiki/Microsoft_SQL_Server)
        -   [ClickHouseMigrator](https://github.com/zlzforever/ClickHouseMigrator)
-   メッセージキュ
    -   [カフカ](https://kafka.apache.org)
        -   [clickhouse_sinker](https://github.com/housepower/clickhouse_sinker) （用途 [Goクライアント](https://github.com/ClickHouse/clickhouse-go/))
        -   [stream-loader-clickhouse](https://github.com/adform/stream-loader)
-   ストリーム処理
    -   [フリンク](https://flink.apache.org)
        -   [フリンク-クリックハウス-シンク](https://github.com/ivi-ru/flink-clickhouse-sink)
-   オブジェクト保管
    -   [S3](https://en.wikipedia.org/wiki/Amazon_S3)
        -   [clickhouse-バックアップ](https://github.com/AlexAkulov/clickhouse-backup)
-   容器の協奏
    -   [Kubernetes](https://kubernetes.io)
        -   [clickhouse-演算子](https://github.com/Altinity/clickhouse-operator)
-   構成管理
    -   [パペット](https://puppet.com)
        -   [イノゲームズ/クリックハウス](https://forge.puppet.com/innogames/clickhouse)
        -   [mfedotov/クリックハウス](https://forge.puppet.com/mfedotov/clickhouse)
-   監視
    -   [黒鉛](https://graphiteapp.org)
        -   [グラファウス](https://github.com/yandex/graphouse)
        -   [カーボンクリックハウス](https://github.com/lomik/carbon-clickhouse) +
        -   [グラファイト-クリック](https://github.com/lomik/graphite-clickhouse)
        -   [黒鉛-ch-オプティマイザー](https://github.com/innogames/graphite-ch-optimizer) -staled仕切りを最大限に活用する [\*GraphiteMergeTree](../../engines/table-engines/mergetree-family/graphitemergetree.md#graphitemergetree) からのルールの場合 [ロールアップ構成](../../engines/table-engines/mergetree-family/graphitemergetree.md#rollup-configuration) 応用できます
    -   [グラファナ](https://grafana.com/)
        -   [クリックハウス-グラファナ](https://github.com/Vertamedia/clickhouse-grafana)
    -   [プロメテウス](https://prometheus.io/)
        -   [clickhouse_exporter](https://github.com/f1yegor/clickhouse_exporter)
        -   [プロムハウス](https://github.com/Percona-Lab/PromHouse)
        -   [clickhouse_exporter](https://github.com/hot-wifi/clickhouse_exporter) （用途 [Goクライアント](https://github.com/kshvakov/clickhouse/))
    -   [ナギオス](https://www.nagios.org/)
        -   [check_clickhouse](https://github.com/exogroup/check_clickhouse/)
        -   [check_clickhouse.py](https://github.com/innogames/igmonplugins/blob/master/src/check_clickhouse.py)
    -   [Zabbix](https://www.zabbix.com)
        -   [clickhouse-zabbix-テンプレート](https://github.com/Altinity/clickhouse-zabbix-template)
    -   [Sematext](https://sematext.com/)
        -   [clickhouseの統合](https://github.com/sematext/sematext-agent-integrations/tree/master/clickhouse)
-   ロギング
    -   [rsyslog](https://www.rsyslog.com/)
        -   [オムクリックハウス](https://www.rsyslog.com/doc/master/configuration/modules/omclickhouse.html)
    -   [フルエント](https://www.fluentd.org)
        -   [ログハウス](https://github.com/flant/loghouse) （のために [Kubernetes](https://kubernetes.io))
    -   [logagent](https://www.sematext.com/logagent)
        -   [logagent output-plugin-clickhouse](https://sematext.com/docs/logagent/output-plugin-clickhouse/)
-   Geo
    -   [MaxMind](https://dev.maxmind.com/geoip/)
        -   [クリックハウス-maxmind-geoip](https://github.com/AlexeyKupershtokh/clickhouse-maxmind-geoip)

## プログラミ {#programming-language-ecosystems}

-   Python
    -   [SQLAlchemy](https://www.sqlalchemy.org)
        -   [sqlalchemy-クリックハウス](https://github.com/cloudflare/sqlalchemy-clickhouse) （用途 [インフィclickhouse_orm](https://github.com/Infinidat/infi.clickhouse_orm))
    -   [パンダ](https://pandas.pydata.org)
        -   [パンダハウス](https://github.com/kszucs/pandahouse)
-   PHP
    -   [教義](https://www.doctrine-project.org/)
        -   [dbal-クリックハウス](https://packagist.org/packages/friendsofdoctrine/dbal-clickhouse)
-   R
    -   [dplyr](https://db.rstudio.com/dplyr/)
        -   [RClickHouse](https://github.com/IMSMWU/RClickHouse) （用途 [クリックハウス-cpp](https://github.com/artpaul/clickhouse-cpp))
-   Java
    -   [Hadoop](http://hadoop.apache.org)
        -   [clickhouse-hdfs-loader](https://github.com/jaykelin/clickhouse-hdfs-loader) （用途 [JDBC](../../sql-reference/table-functions/jdbc.md))
-   Scala
    -   [アッカ](https://akka.io)
        -   [clickhouse-scala-クライアント](https://github.com/crobox/clickhouse-scala-client)
-   C#
    -   [ADO.NET](https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/ado-net-overview)
        -   [クリックハウスAdo](https://github.com/killwort/ClickHouse-Net)
        -   [クリックハウスクライアン](https://github.com/DarkWanderer/ClickHouse.Client)
        -   [ClickHouse.Net](https://github.com/ilyabreev/ClickHouse.Net)
        -   [ClickHouse.Net.Migrations](https://github.com/ilyabreev/ClickHouse.Net.Migrations)
-   エリクサー
    -   [Ecto](https://github.com/elixir-ecto/ecto)
        -   [clickhouse_ecto](https://github.com/appodeal/clickhouse_ecto)
-   Ruby
    -   [Ruby on rails](https://rubyonrails.org/)
        -   [activecube](https://github.com/bitquery/activecube)
        -   [ActiveRecord](https://github.com/PNixx/clickhouse-activerecord) 
    -   [GraphQL](https://github.com/graphql)
        -   [activecube-graphql](https://github.com/bitquery/activecube-graphql)

[元の記事](https://clickhouse.tech/docs/en/interfaces/third-party/integrations/) <!--hide-->
