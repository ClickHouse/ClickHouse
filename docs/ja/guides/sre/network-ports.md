---
slug: /ja/guides/sre/network-ports
sidebar_label: ネットワークポート
---

# ネットワークポート

:::note
**デフォルト**として記載されているポートは、ポート番号が`/etc/clickhouse-server/config.xml`に設定されています。設定をカスタマイズするには、`/etc/clickhouse-server/config.d/`にファイルを追加してください。詳細は[設定ファイル](../../operations/configuration-files.md#override)のドキュメントを参照してください。
:::

|ポート|説明|
|----|-----------|
|2181|ZooKeeper デフォルトサービスポート。 **注: ClickHouse Keeper の場合は `9181` を参照**|
|8123|HTTP デフォルトポート|
|8443|HTTP SSL/TLS デフォルトポート|
|9000|ネイティブプロトコルポート（ClickHouse TCP プロトコルとも呼ばれます）。`clickhouse-server`, `clickhouse-client` およびネイティブ ClickHouse ツールのような ClickHouse アプリケーションとプロセスで使用されます。分散クエリのためのサーバ間通信に使用されます。|
|9004|MySQL エミュレーションポート|
|9005|PostgreSQL エミュレーションポート（ClickHouse で SSL が有効になっている場合、セキュア通信にも使用されます）。|
|9009|低レベルデータアクセスのためのサーバ間通信ポート。データ交換、レプリケーション、サーバ間通信に使用されます。|
|9010|サーバ間通信のための SSL/TLS|
|9011|ネイティブプロトコル PROXYv1 プロトコルポート|
|9019|JDBC ブリッジ|
|9100|gRPC ポート|
|9181|推奨される ClickHouse Keeper ポート|
|9234|推奨される ClickHouse Keeper Raft ポート（`<secure>1</secure>`が有効な場合、セキュア通信にも使用）|
|9363|Prometheus デフォルトメトリックポート|
|9281|推奨されるセキュア SSL ClickHouse Keeper ポート|
|9440|ネイティブプロトコル SSL/TLS ポート|
|42000|Graphite デフォルトポート|
