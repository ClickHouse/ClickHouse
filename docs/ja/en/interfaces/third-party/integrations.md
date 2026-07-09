---
description: 'ClickHouse をさまざまなサードパーティ製システムやツールと
  統合するためのドキュメント'
sidebar_label: 'インテグレーション'
sidebar_position: 27
slug: /interfaces/third-party/integrations
title: 'サードパーティ開発者によるインテグレーションライブラリ'
doc_type: 'reference'
---

:::warning 免責事項
ClickHouse, Inc. は以下に記載するツールやライブラリを保守しておらず、その品質を十分に検証するための広範なテストも実施していません。
公式のインテグレーションについては、[インテグレーションページ](/ja/integrations)を参照してください。
:::

<div id="infrastructure-products">
  ## インフラストラクチャ製品
</div>

<details>
  <summary>リレーショナルデータベース管理システム</summary>

  * [MySQL](https://www.mysql.com)
    * [mysql2ch](https://github.com/long2ice/mysql2ch)
    * [ProxySQL](https://github.com/sysown/proxysql/wiki/ClickHouse-Support)
    * [clickhouse-mysql-data-reader](https://github.com/Altinity/clickhouse-mysql-data-reader)
    * [horgh-replicator](https://github.com/larsnovikov/horgh-replicator)
  * [PostgreSQL](https://www.postgresql.org)
    * [clickhousedb&#95;fdw](https://github.com/Percona-Lab/clickhousedb_fdw)
    * [infi.clickhouse&#95;fdw](https://github.com/Infinidat/infi.clickhouse_fdw) ([infi.clickhouse&#95;orm](https://github.com/Infinidat/infi.clickhouse_orm) を使用)
    * [pg2ch](https://github.com/mkabilov/pg2ch)
    * [clickhouse&#95;fdw](https://github.com/adjust/clickhouse_fdw)
  * [MSSQL](https://en.wikipedia.org/wiki/Microsoft_SQL_Server)
    * [ClickHouseMigrator](https://github.com/zlzforever/ClickHouseMigrator)
</details>

<details>
  <summary>スキーマ移行ツール</summary>

  * [clicko](https://github.com/arsura/clicko) - クラスター対応で、Goとのインテグレーションを備えています。
</details>

<details>
  <summary>メッセージキュー</summary>

  * [Kafka](https://kafka.apache.org)
    * [clickhouse&#95;sinker](https://github.com/housepower/clickhouse_sinker) ([Go client](https://github.com/ClickHouse/clickhouse-go/) を使用)
    * [stream-loader-clickhouse](https://github.com/adform/stream-loader)
</details>

<details>
  <summary>バッチ処理</summary>

  * [Spark](https://spark.apache.org)
    * [spark-clickhouse-connector](https://github.com/housepower/spark-clickhouse-connector)
</details>

<details>
  <summary>ストリーム処理</summary>

  * [Flink](https://flink.apache.org)
    * [flink-clickhouse-sink](https://github.com/ivi-ru/flink-clickhouse-sink)
</details>

<details>
  <summary>オブジェクトストレージ</summary>

  * [S3](https://en.wikipedia.org/wiki/Amazon_S3)
    * [clickhouse-backup](https://github.com/AlexAkulov/clickhouse-backup)
</details>

<details>
  <summary>コンテナーオーケストレーション</summary>

  * [Kubernetes](https://kubernetes.io)
    * [clickhouse-operator](https://github.com/Altinity/clickhouse-operator)
</details>

<details>
  <summary>構成管理</summary>

  * [puppet](https://puppet.com)
    * [innogames/clickhouse](https://forge.puppet.com/innogames/clickhouse)
    * [mfedotov/clickhouse](https://forge.puppet.com/mfedotov/clickhouse)
</details>

<details>
  <summary>監視</summary>

  * [Graphite](https://graphiteapp.org)
    * [graphouse](https://github.com/ClickHouse/graphouse)
    * [carbon-clickhouse](https://github.com/lomik/carbon-clickhouse)
    * [graphite-clickhouse](https://github.com/lomik/graphite-clickhouse)
    * [graphite-ch-optimizer](https://github.com/innogames/graphite-ch-optimizer) - [ロールアップ設定](../../engines/table-engines/mergetree-family/graphitemergetree.md#rollup-configuration) のルールを適用できる場合、[*GraphiteMergeTree](/ja/engines/table-engines/mergetree-family/graphitemergetree) の古くなったパーティションを最適化します
  * [Grafana](https://grafana.com/)
    * [clickhouse-grafana](https://github.com/Altinity/clickhouse-grafana)
  * [Prometheus](https://prometheus.io/)
    * [clickhouse&#95;exporter](https://github.com/f1yegor/clickhouse_exporter)
    * [PromHouse](https://github.com/Percona-Lab/PromHouse)
    * [clickhouse&#95;exporter](https://github.com/hot-wifi/clickhouse_exporter) ([Go client](https://github.com/kshvakov/clickhouse/) を使用)
  * [Nagios](https://www.nagios.org/)
    * [check&#95;clickhouse](https://github.com/exogroup/check_clickhouse/)
    * [check&#95;clickhouse.py](https://github.com/innogames/igmonplugins/blob/master/src/check_clickhouse.py)
  * [Zabbix](https://www.zabbix.com)
    * [clickhouse-zabbix-template](https://github.com/Altinity/clickhouse-zabbix-template)
  * [Sematext](https://sematext.com/)
    * [ClickHouse 連携](https://github.com/sematext/sematext-agent-integrations/tree/master/clickhouse)
</details>

<details>
  <summary>ロギング</summary>

  * [rsyslog](https://www.rsyslog.com/)
    * [omclickhouse](https://www.rsyslog.com/doc/master/configuration/modules/omclickhouse.html)
  * [fluentd](https://www.fluentd.org)
    * [loghouse](https://github.com/flant/loghouse) ([Kubernetes](https://kubernetes.io) 向け)
  * [logagent](https://www.sematext.com/logagent)
    * [logagent output-plugin-clickhouse](https://sematext.com/docs/logagent/output-plugin-clickhouse/)
</details>

<details>
  <summary>Geo</summary>

  * [MaxMind](https://dev.maxmind.com/geoip/)
    * [clickhouse-maxmind-geoip](https://github.com/AlexeyKupershtokh/clickhouse-maxmind-geoip)
</details>

<details>
  <summary>AutoML</summary>

  * [MindsDB](https://mindsdb.com/)
    * [MindsDB](https://github.com/mindsdb/mindsdb) - ClickHouse と連携し、ClickHouse のデータをさまざまな AI/ML モデルで利用できるようにします。
</details>

<div id="programming-language-ecosystems">
  ## プログラミング言語エコシステム
</div>

<details>
  <summary>Python</summary>

  * [SQLAlchemy](https://www.sqlalchemy.org)
    * [sqlalchemy-clickhouse](https://github.com/cloudflare/sqlalchemy-clickhouse)  ([infi.clickhouse&#95;orm](https://github.com/Infinidat/infi.clickhouse_orm) を使用)
  * [PyArrow/Pandas](https://pandas.pydata.org)
    * [Ibis](https://github.com/ibis-project/ibis)
</details>

<details>
  <summary>PHP</summary>

  * [Doctrine](https://www.doctrine-project.org/)
    * [dbal-clickhouse](https://packagist.org/packages/friendsofdoctrine/dbal-clickhouse)
</details>

<details>
  <summary>R</summary>

  * [dplyr](https://db.rstudio.com/dplyr/)
    * [RClickHouse](https://github.com/IMSMWU/RClickHouse)  ([clickhouse-cpp](https://github.com/artpaul/clickhouse-cpp) を使用)
</details>

<details>
  <summary>Java</summary>

  * [Hadoop](http://hadoop.apache.org)
    * [clickhouse-hdfs-loader](https://github.com/jaykelin/clickhouse-hdfs-loader)  ([JDBC](../../sql-reference/table-functions/jdbc.md) を使用)
</details>

<details>
  <summary>Scala</summary>

  * [Akka](https://akka.io)
    * [clickhouse-scala-client](https://github.com/crobox/clickhouse-scala-client)
</details>

<details>
  <summary>C#</summary>

  * [ADO.NET](https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/ado-net-overview)
    * [ClickHouse.Ado](https://github.com/killwort/ClickHouse-Net)
    * [ClickHouse.Client](https://github.com/DarkWanderer/ClickHouse.Client)
    * [ClickHouse.Net](https://github.com/ilyabreev/ClickHouse.Net)
    * [ClickHouse.Net.Migrations](https://github.com/ilyabreev/ClickHouse.Net.Migrations)
    * [Linq To DB](https://github.com/linq2db/linq2db)
</details>

<details>
  <summary>Elixir</summary>

  * [Ecto](https://github.com/elixir-ecto/ecto)
    * [clickhouse&#95;ecto](https://github.com/appodeal/clickhouse_ecto)
</details>

<details>
  <summary>Ruby</summary>

  * [Ruby on Rails](https://rubyonrails.org/)
    * [activecube](https://github.com/bitquery/activecube)
    * [ActiveRecord](https://github.com/PNixx/clickhouse-activerecord)
  * [GraphQL](https://github.com/graphql)
    * [activecube-graphql](https://github.com/bitquery/activecube-graphql)
</details>