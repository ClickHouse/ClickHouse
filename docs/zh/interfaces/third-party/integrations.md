# 第三方集成库 {#di-san-fang-ji-cheng-ku}

!!! warning "声明"
    Yandex不维护下面列出的库，也没有进行任何广泛的测试以确保其质量。

## 基建产品 {#ji-jian-chan-pin}

-   关系数据库管理系统
    -   [MySQL](https://www.mysql.com)
        -   [mysql2ch](https://github.com/long2ice/mysql2ch)
        -   [ProxySQL](https://github.com/sysown/proxysql/wiki/ClickHouse-Support)
        -   [clickhouse-mysql-data-reader](https://github.com/Altinity/clickhouse-mysql-data-reader)
        -   [horgh-复制器](https://github.com/larsnovikov/horgh-replicator)
    -   [PostgreSQL](https://www.postgresql.org)
        -   [clickhousedb_fdw](https://github.com/Percona-Lab/clickhousedb_fdw)
        -   [infi.clickhouse_fdw](https://github.com/Infinidat/infi.clickhouse_fdw) (使用 [infi.clickhouse_orm](https://github.com/Infinidat/infi.clickhouse_orm))
        -   [pg2ch](https://github.com/mkabilov/pg2ch)
    -   [MSSQL](https://en.wikipedia.org/wiki/Microsoft_SQL_Server)
        -   [ClickHouseMightrator](https://github.com/zlzforever/ClickHouseMigrator)
-   消息队列
    -   [卡夫卡](https://kafka.apache.org)
        -   [clickhouse_sinker](https://github.com/housepower/clickhouse_sinker) (使用 [去客户](https://github.com/ClickHouse/clickhouse-go/))
        -   [stream-loader-clickhouse](https://github.com/adform/stream-loader)
-   流处理
    -   [Flink](https://flink.apache.org)
        -   [flink-clickhouse-sink](https://github.com/ivi-ru/flink-clickhouse-sink)
-   对象存储
    -   [S3](https://en.wikipedia.org/wiki/Amazon_S3)
    -   [ﾂ环板backupｮﾂ嘉ｯﾂ偲](https://github.com/AlexAkulov/clickhouse-backup)
-   容器编排
    -   [Kubernetes](https://kubernetes.io)
    -   [clickhouse-操](https://github.com/Altinity/clickhouse-operator)
-   配置管理
    -   [木偶](https://puppet.com)
    -   [ﾂ环板/ｮﾂ嘉ｯﾂ偲](https://forge.puppet.com/innogames/clickhouse)
    -   [mfedotov/clickhouse](https://forge.puppet.com/mfedotov/clickhouse)
-   监控
    -   [石墨](https://graphiteapp.org)
        -   [graphouse](https://github.com/yandex/graphouse)
        -   [ﾂ暗ｪﾂ氾环催ﾂ団](https://github.com/lomik/carbon-clickhouse) +
        -   [ﾂ环板-ｮﾂ嘉ｯﾂ偲](https://github.com/lomik/graphite-clickhouse)
        -   [石墨-ch-optimizer](https://github.com/innogames/graphite-ch-optimizer) -优化静态分区 [\*GraphiteMergeTree](../../engines/table-engines/mergetree-family/graphitemergetree.md#graphitemergetree) 如果从规则 [汇总配置](../../engines/table-engines/mergetree-family/graphitemergetree.md#rollup-configuration) 可以应用
    -   [Grafana](https://grafana.com/)
        -   [clickhouse-grafana](https://github.com/Vertamedia/clickhouse-grafana)
    -   [普罗米修斯号](https://prometheus.io/)
        -   [clickhouse_exporter](https://github.com/f1yegor/clickhouse_exporter)
        -   [PromHouse](https://github.com/Percona-Lab/PromHouse)
        -   [clickhouse_exporter](https://github.com/hot-wifi/clickhouse_exporter) （用途 [去客户](https://github.com/kshvakov/clickhouse/))
    -   [Nagios](https://www.nagios.org/)
        -   [check_clickhouse](https://github.com/exogroup/check_clickhouse/)
        -   [check_clickhouse.py](https://github.com/innogames/igmonplugins/blob/master/src/check_clickhouse.py)
    -   [Zabbix](https://www.zabbix.com)
        -   [ﾂ暗ｪﾂ氾环催ﾂ団ﾂ法ﾂ人](https://github.com/Altinity/clickhouse-zabbix-template)
    -   [Sematext](https://sematext.com/)
        -   [clickhouse积分](https://github.com/sematext/sematext-agent-integrations/tree/master/clickhouse)
-   记录
    -   [rsyslog](https://www.rsyslog.com/)
        -   [鹿茫house omhousee酶](https://www.rsyslog.com/doc/master/configuration/modules/omclickhouse.html)
    -   [fluentd](https://www.fluentd.org)
        -   [loghouse](https://github.com/flant/loghouse) (对于 [Kubernetes](https://kubernetes.io))
    -   [Sematext](https://www.sematext.com/logagent)
        -   [logagent输出-插件-clickhouse](https://sematext.com/docs/logagent/output-plugin-clickhouse/)
-   地理
    -   [MaxMind](https://dev.maxmind.com/geoip/)
        -   [ﾂ环板-ｮﾂ嘉ｯﾂ偲青clickｼﾂ氾ｶﾂ鉄ﾂ工ﾂ渉](https://github.com/AlexeyKupershtokh/clickhouse-maxmind-geoip)

## 编程语言生态系统 {#bian-cheng-yu-yan-sheng-tai-xi-tong}

-   Python
    -   [SQLAlchemy](https://www.sqlalchemy.org)
        -   [ﾂ暗ｪﾂ氾环催ﾂ団ﾂ法ﾂ人](https://github.com/cloudflare/sqlalchemy-clickhouse) (使用 [infi.clickhouse_orm](https://github.com/Infinidat/infi.clickhouse_orm))
    -   [熊猫](https://pandas.pydata.org)
        -   [pandahouse](https://github.com/kszucs/pandahouse)
-   PHP
    -   [Doctrine](https://www.doctrine-project.org/)
        -   [dbal-clickhouse](https://packagist.org/packages/friendsofdoctrine/dbal-clickhouse)
-   R
    -   [dplyr](https://db.rstudio.com/dplyr/)
    -   [RClickhouse](https://github.com/IMSMWU/RClickhouse) (使用 [ﾂ暗ｪﾂ氾环催ﾂ団](https://github.com/artpaul/clickhouse-cpp))
-   Java
    -   [Hadoop](http://hadoop.apache.org)
    -   [clickhouse-hdfs-装载机](https://github.com/jaykelin/clickhouse-hdfs-loader) (使用 [JDBC](../../sql-reference/table-functions/jdbc.md))
-   斯卡拉
    -   [Akka](https://akka.io)
    -   [掳胫client-禄脢鹿脷露胫鲁隆鹿-client酶](https://github.com/crobox/clickhouse-scala-client)
-   C#
    -   [ADO.NET](https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/ado-net-overview)
    -   [克莱克豪斯Ado](https://github.com/killwort/ClickHouse-Net)
    -   [ClickHouse.Net](https://github.com/ilyabreev/ClickHouse.Net)
    -   [ClickHouse.Net.Migrations](https://github.com/ilyabreev/ClickHouse.Net.Migrations)
-   仙丹
    -   [Ecto](https://github.com/elixir-ecto/ecto)
    -   [clickhouse_ecto](https://github.com/appodeal/clickhouse_ecto)
-   Ruby
    -   [Ruby on Rails](https://rubyonrails.org/)
        -   [activecube](https://github.com/bitquery/activecube)
    -   [GraphQL](https://github.com/graphql)
        -   [activecube-graphql](https://github.com/bitquery/activecube-graphql)

[来源文章](https://clickhouse.tech/docs/zh/interfaces/third-party/integrations/) <!--hide-->
