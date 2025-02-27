---
slug: /ja/integrations/postgresql/inserting-data
title: PostgreSQLからデータを挿入する方法
keywords: [postgres, postgresql, 挿入]
---

ClickHouseへのデータ挿入を最適化するためのベストプラクティスについては、[このガイド](/ja/guides/inserting-data)をご覧になることをお勧めします。

PostgreSQLからデータを一括ロードするには、次の方法があります：

- `PeerDB by ClickHouse`を使用する方法。これは、セルフマネージドのClickHouseおよびClickHouse CloudへのPostgreSQLデータベースレプリケーション専用のETLツールです。まず[PeerDB Cloud](https://www.peerdb.io/)でアカウントを作成し、セットアップ手順については[ドキュメント](https://docs.peerdb.io/connect/clickhouse/clickhouse-cloud)をご覧ください。
- [Postgresテーブル関数](/ja/sql-reference/table-functions/postgresql)を利用してデータを直接読み取る方法。この方法は、例えばタイムスタンプなどの既知のウォーターマークに基づくバッチレプリケーションが十分な場合や、一度限りの移行の場合に適しています。このアプローチは、数千万行にスケールでき、大規模データセットを移行したいユーザーは、データをチャンクごとに処理する複数のリクエストを検討すべきです。チャンクごとにステージングテーブルを使用し、そのパーティションが最終テーブルに移動される前に使用できます。これにより失敗したリクエストを再試行できます。この一括ロード戦略の詳細については、[こちら](https://clickhouse.com/blog/supercharge-your-clickhouse-data-loads-part3)をご覧ください。
- データを[CSV形式でPostgresからエクスポート](https://blog.n8n.io/postgres-export-to-csv/)することができます。そして、[ローカルファイル](/ja/integrations/data-ingestion/insert-local-files)から、あるいは[テーブル関数](/ja/sql-reference/statements/insert-into#inserting-using-a-table-function)を使用してオブジェクトストレージを介してClickHouseに挿入できます。
