---
slug: /ja/migrations/postgresql/dataset
title: PostgreSQLからClickHouseへのデータロード
description: PostgreSQLからClickHouseへ移行するためのデータセット例
keywords: [postgres, postgresql, 移行, マイグレーション]
---

> これは、PostgreSQLからClickHouseへの移行に関するガイドの**パート1**です。この内容は入門編と見なされ、ユーザーがClickHouseのベストプラクティスに従った初期の機能的なシステムをデプロイするのを支援することを目的としています。複雑なトピックを避け、完全に最適化されたスキーマにはなりませんが、ユーザーが本番環境のシステムを構築し、学習の基盤を築くための堅実な基礎を提供します。

## データセット

PostgresからClickHouseへの典型的な移行を示す例として、[こちら](/ja/getting-started/example-datasets/stackoverflow)で文書化されているStack Overflowのデータセットを使用します。これには、2008年から2024年4月までにStack Overflowで行われたすべての`post`、`vote`、`user`、`comment`、および`badge`が含まれています。このデータのPostgreSQLスキーマは以下の通りです：

<br />

<img src={require('../images/postgres-stackoverflow-schema.png').default}    
  class="image"
  alt="NEEDS ALT"
  style={{width: '1000px', background: 'none'}} />

<br />

*PostgreSQLでテーブルを作成するためのDDLコマンドは[こちら](https://pastila.nl/?001c0102/eef2d1e4c82aab78c4670346acb74d83#TeGvJWX9WTA1V/5dVVZQjg==)にあります。*

このスキーマは必ずしも最も最適ではありませんが、主キー、外部キー、パーティショニング、およびインデックスを含むいくつかの人気のあるPostgreSQLの機能を活用しています。

これらの概念を、それぞれClickHouseの同等のものに移行します。

移行手順をテストするためにこのデータセットをPostgreSQLインスタンスに投入したいユーザーには、`pg_dump`形式でデータを提供しており、DDLおよびその後のデータロードコマンドは以下に示されています：

```bash
# users
wget https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/pdump/2024/users.sql.gz
gzip -d users.sql.gz
psql < users.sql

# posts
wget https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/pdump/2024/posts.sql.gz
gzip -d posts.sql.gz
psql < posts.sql

# posthistory
wget https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/pdump/2024/posthistory.sql.gz
gzip -d posthistory.sql.gz
psql < posthistory.sql

# comments
wget https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/pdump/2024/comments.sql.gz
gzip -d comments.sql.gz
psql < comments.sql

# votes
wget https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/pdump/2024/votes.sql.gz
gzip -d votes.sql.gz
psql < votes.sql

# badges
wget https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/pdump/2024/badges.sql.gz
gzip -d badges.sql.gz
psql < badges.sql

# postlinks
wget https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/pdump/2024/postlinks.sql.gz
gzip -d postlinks.sql.gz
psql < postlinks.sql
```

このデータセットはClickHouseにとっては小規模ですが、Postgresにとっては大規模です。上記は2024年の最初の3か月をカバーするサブセットを表しています。

> 我々の例では、PostgresとClickHouseのパフォーマンス差を示すために完全なデータセットを使用していますが、以下に記載されているすべての手順は、小さなサブセットでも機能的に同一です。Postgresに完全なデータセットをロードしたいユーザーは[こちら](https://pastila.nl/?00d47a08/1c5224c0b61beb480539f15ac375619d#XNj5vX3a7ZjkdiX7In8wqA==)を参照してください。上記のスキーマによって課される外部制約のため、PostgreSQLの完全なデータセットには参照整合性を満たす行のみが含まれます。[Parquetバージョン](/ja/getting-started/example-datasets/stackoverflow)は、そのような制約なしでClickHouseに直接簡単にロードできます。

## データの移行

ClickHouseとPostgres間のデータ移行は、2つの主要なワークロードタイプに分かれます：

- **初回の一括ロードと定期更新** - 初期データセットを移行し、毎日などの設定された間隔で定期更新を行います。ここでの更新は、変更された行を再送信することで処理されます。これは、比較に使用できるカラム（例：日付）やXMIN値によって識別できます。削除操作はデータセットの完全な定期リロードで処理されます。
- **リアルタイムレプリケーションまたはCDC** - 初期データセットを移行する必要があります。このデータセットへの変更は、数秒の遅延が許容される近リアルタイムでClickHouseに反映される必要があります。これは実質的に、PostgresのテーブルをClickHouseと同期させるChange Data Capture (CDC)プロセスです。つまり、Postgresテーブルの挿入、更新、削除がClickHouseの同等のテーブルに適用される必要があります。

### 初回の一括ロードと定期更新

このワークロードは、上記のワークロードの中でも簡単な方です。変更は定期的に適用できます。データセットの初回の一括ロードは以下で達成できます：

- **テーブル関数** - ClickHouseで[Postgresテーブル関数](/ja/sql-reference/table-functions/postgresql)を使用してPostgresからデータを`SELECT`し、ClickHouseテーブルに`INSERT`します。数百GBのデータセットまでの一括ロードに関連しています。
- **エクスポート** - CSVやSQLスクリプトファイルなどの中間形式にエクスポートします。これらのファイルは、クライアントからの`INSERT FROM INFILE`句を使用して、またはオブジェクトストレージとその関連機能（例：s3, gcs）を使用してClickHouseにロードできます。

増分ロードは、スケジュールして行うことができます。Postgresテーブルが挿入のみを受け取り、増加するIDまたはタイムスタンプが存在する場合、上記のテーブル関数アプローチを使用してインクリメントをロードできます。つまり、`SELECT`に`WHERE`句を適用できます。このアプローチは、同じカラムが更新されることが保証されている場合に更新をサポートするためにも使用できます。ただし、削除をサポートするためには完全なリロードが必要になり、テーブルが成長するにつれて困難になる可能性があります。

`CreationDate`を使用して初回ロードと増分ロードを示します（行が更新された場合、これが更新されると仮定します）。

```sql
-- 初回ロード
INSERT INTO stackoverflow.posts SELECT * FROM postgresql('<host>', 'postgres', 'posts', 'postgres', '<password')

INSERT INTO stackoverflow.posts SELECT * FROM postgresql('<host>', 'postgres', 'posts', 'postgres', '<password') WHERE CreationDate > ( SELECT (max(CreationDate) FROM stackoverflow.posts)
```

> ClickHouseは、`=`, `!=`, `>`, `>=`, `<`, `<=`, `IN`のような単純な`WHERE`句をPostgreSQLサーバーにプッシュダウンします。したがって、インクリメンタルロードは、変更セットを識別するために使用されるカラムにインデックスが存在することを確認することで、より効率的に行えます。

> クエリレプリケーションを使用してUPDATE操作を検出する可能な方法は、[XMINシステムカラム](https://www.postgresql.org/docs/9.1/ddl-system-columns.html)（トランザクションID）をウォーターマークとして使用することです。このカラムの変更は変化を示し、したがって宛先テーブルに適用することができます。このアプローチを使用するユーザーは、XMIN値がラップアラウンドする可能性があり、全表スキャンが必要であるため、変更を追跡することがより複雑になることを認識している必要があります。このアプローチの詳細については、「Change Data Capture (CDC)」を参照してください。

### リアルタイムレプリケーションまたはCDC

Change Data Capture (CDC)は、表が2つのデータベース間で同期されるプロセスです。これは、更新および削除をリアルタイムで処理する場合、かなり複雑になります。

いくつかのソリューションがあります：
1. **[ClickHouseによるPeerDB](https://docs.peerdb.io/connect/clickhouse/clickhouse-cloud)** - PeerDBはユーザーがセルフマネージドまたはSaaSソリューションとして実行できるオープンコード専門Postgres CDCソリューションを提供し、PostgresおよびClickHouseでスケールでの優れたパフォーマンスを示しています。このソリューションは、PostgresとClickHouse間の高性能なデータ転送および信頼性保証を実現するために低レベルの最適化に焦点を当てています。オンラインおよびオフラインの両方のロードをサポートします。

2. **独自に構築** - これは**Debezium + Kafka**で実現できます。DebeziumはPostgresテーブルのすべての変更をキャプチャし、これらをイベントとしてKafkaキューにフォワードできます。これらのイベントはその後、ClickHouse Kafkaコネクタまたは[Clickpipes in ClickHouse Cloud](https://clickhouse.com/cloud/clickpipes)によって消費され、ClickHouseへの挿入が行われます。これはChange Data Capture (CDC)を表しており、Debeziumは表の初回コピーを行うだけでなく、後続のすべての更新、削除、および挿入がPostgresで検出され、下流のイベントが発生することを保証します。これは、Postgres、Debezium、およびClickHouseの注意深い設定を必要とします。例は[こちら](https://clickhouse.com/blog/clickhouse-postgresql-change-data-capture-cdc-part-2)にあります。

このガイドの例では、データ探索と他のアプローチで使用可能な本番スキーマへの簡単な反復に焦点を当て、初回の一括ロードのみを前提としています。

[パート2はこちら](/ja/migrations/postgresql/designing-schemas).

