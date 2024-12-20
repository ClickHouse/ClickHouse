---
sidebar_label: 概要
sidebar_position: 1
keywords: [clickhouse, connect, explo, tableau, grafana, metabase, mitzu, superset, deepnote, draxlr, rocketbi, omni, bi, visualization, tool]
---

# ClickHouseでのデータの可視化

<div class='vimeo-container'>
<iframe
   src="https://player.vimeo.com/video/754460217?h=3dcae2e1ca"
   width="640"
   height="360"
   frameborder="0"
   allow="autoplay; fullscreen; picture-in-picture"
   allowfullscreen>
</iframe>
</div>

<br/>

データがClickHouseに入りましたので、次はデータを分析する時です。これには、BIツールを使って可視化を構築することがよく含まれます。多くの人気のあるBIおよび可視化ツールは、ClickHouseに接続します。一部のツールはClickHouseにそのまま接続可能で、他のツールはコネクタのインストールが必要です。以下のツールに関するドキュメントがあります：

- [Explo](./data-visualization/explo-and-clickhouse.md)
- [Grafana](./data-visualization/grafana/index.md)
- [Tableau](./data-visualization/tableau-and-clickhouse.md)
- [Looker](./data-visualization/looker-and-clickhouse.md)
- [Metabase](./data-visualization/metabase-and-clickhouse.md)
- [Mitzu](./data-visualization/mitzu-and-clickhouse.md)
- [Omni](./data-visualization/omni-and-clickhouse.md)
- [Superset](./data-visualization/superset-and-clickhouse.md)
- [Deepnote](./data-visualization/deepnote.md)
- [Draxlr](./data-visualization/draxlr-and-clickhouse.md)
- [Rocket BI](./data-visualization/rocketbi-and-clickhouse.md)
- [Rill](https://docs.rilldata.com/reference/olap-engines/clickhouse)
- [Zing Data](./data-visualization/zingdata-and-clickhouse.md)

## ClickHouse Cloudとデータ可視化ツールの互換性

| ツール                                                                   | サポート via                 | テスト済み | ドキュメントあり | コメント                                                                                                                                 |
|-------------------------------------------------------------------------|-------------------------------|--------|------------|-------------------------------------------------------------------------------------------------------------------------------------|
| [Apache Superset](./data-visualization/superset-and-clickhouse.md)      | ClickHouse公式コネクタ        | ✅      | ✅          |                                                                                                                                       |
| [AWS QuickSight](./data-visualization/quicksight-and-clickhouse.md)     | MySQLインターフェース          | ✅      | ✅          | 一部制限あり、詳細は[ドキュメント](./data-visualization/quicksight-and-clickhouse.md)をご覧ください                                   |
| [Deepnote](./data-visualization/deepnote.md)                            | ネイティブコネクタ             | ✅      | ✅          |                                                                                                                                       |
| [Explo](./data-visualization/explo-and-clickhouse.md)                   | ネイティブコネクタ             | ✅      | ✅          |                                                                                                                                       |
| [Grafana](./data-visualization/grafana/index.md)                        | ClickHouse公式コネクタ        | ✅      | ✅          |                                                                                                                                       |
| [Hashboard](./data-visualization/hashboard-and-clickhouse.md)           | ネイティブコネクタ             | ✅      | ✅          |                                                                                                                                       |
| [Looker](./data-visualization/looker-and-clickhouse.md)                 | ネイティブコネクタ             | ✅      | ✅          | 一部制限あり、詳細は[ドキュメント](./data-visualization/looker-and-clickhouse.md)をご覧ください                                       |
| Looker                                                                  | MySQLインターフェース          | 🚧     | ❌          |                                                                                                                                       |
| [Looker Studio](./data-visualization/looker-studio-and-clickhouse.md)   | MySQLインターフェース          | ✅      | ✅          |                                                                                                                                       |
| [Metabase](./data-visualization/metabase-and-clickhouse.md)             | ClickHouse公式コネクタ        | ✅      | ✅          |                                                                                                            
| [Mitzu](./data-visualization/mitzu-and-clickhouse.md)                   | ネイティブコネクタ             | ✅      | ✅          |                                                                                                                                       |
| [Omni](./data-visualization/omni-and-clickhouse.md)                     | ネイティブコネクタ             | ✅      | ✅          |                                                                                                                                       |
| [Power BI Desktop](./data-visualization/powerbi-and-clickhouse.md)      | ClickHouse公式コネクタ        | ✅      | ✅          | ODBC経由で、直接クエリモードをサポート                                                                                                   |
| [Power BI service](https://clickhouse.com/docs/ja/integrations/powerbi#power-bi-service)                                                    | ClickHouse公式コネクタ        | ✅    | ✅          | [Microsoft Data Gateway](https://learn.microsoft.com/en-us/power-bi/connect-data/service-gateway-custom-connectors)の設定が必要       |
| [Rill](https://docs.rilldata.com/reference/olap-engines/clickhouse)     | ネイティブコネクタ             | ✅      | ✅          |        
| [Rocket BI](./data-visualization/rocketbi-and-clickhouse.md)            | ネイティブコネクタ             | ✅      | ❌          |                                                                                                                                       |
| [Tableau Desktop](./data-visualization/tableau-and-clickhouse.md)       | ClickHouse公式コネクタ        | ✅      | ✅          | 認証プロセス中                                                                                                                                 |
| [Tableau Online](./data-visualization/tableau-online-and-clickhouse.md) | MySQLインターフェース          | ✅      | ✅          | 一部制限あり、詳細は[ドキュメント](./data-visualization/tableau-online-and-clickhouse.md)をご覧ください                                   |
| [Zing Data](./data-visualization/zingdata-and-clickhouse.md)            | ネイティブコネクタ             | ✅      | ✅          |                                                                                                                                       |
