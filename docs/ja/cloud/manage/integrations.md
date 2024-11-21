---
sidebar_label: インテグレーション
slug: /ja/manage/integrations
title: インテグレーション
---

ClickHouseのインテグレーションの全リストを見るには、[こちらのページ](/ja/integrations)をご覧ください。

## ClickHouse Cloudの専用インテグレーション

ClickHouseには多数のインテグレーションが用意されていますが、ClickHouse Cloudにのみ利用可能な専用インテグレーションも存在します。

### ClickPipes

[ClickPipes](/ja/integrations/clickpipes)は、単純なウェブベースUIを使用して、ClickHouse Cloudにデータを取り込むための管理されたインテグレーションプラットフォームです。現在、Apache Kafka、S3、GCS、Amazon Kinesisをサポートしており、さらに多くのインテグレーションが近日中に追加予定です。

### ClickHouse Cloud用Looker Studio

[Looker Studio](https://lookerstudio.google.com/)は、Googleが提供する人気のビジネスインテリジェンスツールです。Looker Studioは現在、ClickHouseコネクタをサポートしておらず、代わりにMySQLワイヤープロトコルを使用してClickHouseに接続しています。

Looker StudioをClickHouse Cloudに接続するには、[MySQLインターフェース](/ja/interfaces/mysql)を有効にする必要があります。Looker StudioをClickHouse Cloudに接続する方法の詳細については、[こちらのページ](/ja/interfaces/mysql#enabling-the-mysql-interface-on-clickhouse-cloud)をご覧ください。

### MySQLインターフェース

一部のアプリケーションは、現在ClickHouseワイヤープロトコルをサポートしていません。これらのアプリケーションでClickHouse Cloudを使用するには、Cloud Consoleを通じてMySQLワイヤープロトコルを有効にすることができます。Cloud Consoleを通じてMySQLワイヤープロトコルを有効にする方法の詳細については、[こちらのページ](/ja/interfaces/mysql#enabling-the-mysql-interface-on-clickhouse-cloud)をご覧ください。

## 未対応のインテグレーション

以下のインテグレーション機能は現在、ClickHouse Cloudでは利用できません。これらはエクスペリメンタルな機能であるため、アプリケーションでこれらの機能をサポートする必要がある場合は、support@clickhouse.comまでご連絡ください。

- [MaterializedPostgreSQL](/ja/engines/table-engines/integrations/materialized-postgresql)
- [MaterializedMySQL](/ja/engines/database-engines/materialized-mysql)
