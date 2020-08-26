---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 28
toc_title: "\u30D3\u30B8\u30E5\u30A2\u30EB"
---

# サードパー {#visual-interfaces-from-third-party-developers}

## オープンソース {#open-source}

### Tabix {#tabix}

のclickhouseのための網インターフェイス [Tabix](https://github.com/tabixio/tabix) プロジェクト。

特徴:

-   追加のソフトウェアをインストールする必要なく、ブラウザから直接clickhouseで動作します。
-   構文強調表示のクエリエディター。
-   コマンドの自動補完。
-   クエリ実行のグラフ分析のためのツール。
-   配色オプション。

[Tabixドキュメント](https://tabix.io/doc/).

### HouseOps {#houseops}

[HouseOps](https://github.com/HouseOps/HouseOps) OSX、Linux、Windows用のUI/IDEです。

特徴:

-   構文の強調表示を使用したクエリビルダー。 テーブルまたはjsonビューで応答を表示します。
-   CSVまたはJSONとしてエクスポートクエリ結果。
-   説明付きのプロセスのリスト。 書き込みモード。 停止する能力 (`KILL`）プロセス。
-   データベースグラフ すべてのテーブルとその列に追加情報を表示します。
-   列サイズのクイックビュー。
-   サーバー構成。

次の機能は、開発のために計画されています:

-   データベース管理。
-   ユーザー管理。
-   リアルタイムデータ分析。
-   クラスタ監視。
-   クラスター管理。
-   監視レプリケートおよびkafkaテーブル。

### 灯台 {#lighthouse}

[灯台](https://github.com/VKCOM/lighthouse) ClickHouseのための軽量のwebインターフェイスです。

特徴:

-   テーブルのリストのフィルタリング、メタデータを指すものとします。
-   テーブルのプレビューとフィルタです。
-   読み取り専用クエリの実行。

### Redash {#redash}

[Redash](https://github.com/getredash/redash) めるためのプラットフォームのデータを可視化する。

サポート、多数のデータソースを含むclickhouse,redash参加できる結果のクエリからデータソースへの最終データセットである。

特徴:

-   クエリの強力なエディタ。
-   データベ
-   視覚化ツールを使用すると、さまざまな形式のデータを表現できます。

### デービーバーname {#dbeaver}

[デービーバーname](https://dbeaver.io/) -ユニバーサルデスクトップのデータベースのクライアントClickHouseます。

特徴:

-   構文ハイライトと自動補完によるクエリ開発。
-   テーブルリフィルとメタデータを検索する
-   表データプレビュー。
-   フルテキスト検索。

### クリックハウス-cli {#clickhouse-cli}

[クリックハウス-cli](https://github.com/hatarist/clickhouse-cli) Python3で書かれたClickHouseの代替コマンドラインクライアントです。

特徴:

-   自動補完。
-   クエリとデータ出力の構文強調表示。
-   データ出力のためのポケベルサポート。
-   カスタムpostgresqlのようなコマンド。

### clickhouse-flamegraph {#clickhouse-flamegraph}

[clickhouse-flamegraph](https://github.com/Slach/clickhouse-flamegraph) 視覚化する専門にされた用具はある `system.trace_log` として [flamegraph](http://www.brendangregg.com/flamegraphs.html).

## 商業 {#commercial}

### データグリップ {#datagrip}

[データグリップ](https://www.jetbrains.com/datagrip/) JetbrainsのデータベースIDEで、ClickHouse専用サポートがあります。 PyCharm、IntelliJ IDEA、GoLand、PhpStormなどの他のIntelliJベースのツールにも組み込まれています。

特徴:

-   非常に高速なコード補完。
-   ClickHouse構文の強調表示。
-   ClickHouse固有の機能のサポート。
-   データエディタ。
-   リファクタリング。
-   検索とナビゲーション。

### YandexのDataLens {#yandex-datalens}

[YandexのDataLens](https://cloud.yandex.ru/services/datalens) データの可視化と分析のサービスです。

特徴:

-   シンプルな棒グラフから複雑なダッシュボードまで、幅広い視覚化が可能です。
-   ダッシュボードは一般公開されます。
-   ClickHouseを含む複数のデータソースのサポート。
-   ClickHouseに基づく具体化されたデータのための貯蔵。

データレンスは [無料で利用可能](https://cloud.yandex.com/docs/datalens/pricing) 商業使用のための低負荷プロジェクトのため。

-   [DataLens書](https://cloud.yandex.com/docs/datalens/).
-   [Tutorial](https://cloud.yandex.com/docs/solutions/datalens/data-from-ch-visualization) ClickHouseデータベースからデータを視覚化する。

### Holisticsソフトウェア {#holistics-software}

[ホリスティクス](https://www.holistics.io/) フルスタックのデータプラットフォームは、ビジネスインツールです。

特徴:

-   自動メール、slackやグーグルシートのスケジュール。
-   SQLエディタと可視化、バージョン管理の自動完了し、再利用可能なクエリー部品、ダイナミックフィルター.
-   Iframe経由のレポートとダッシュボードの埋め込み分析。
-   データ準備およびetl機能。
-   SQLデータモデリング支援のためのリレーショナルマッピングのデータです。

### 見物人 {#looker}

[見物人](https://looker.com) ClickHouseを含む50以上のデータベース方言をサポートするdata platform and business intelligenceツールです。 LookerはSaaSプラットフォームとして利用でき、セルフホスト型です。 ユーザーが利用できLookerる場合は、vpnクライアントの直接探索、データの構築の可視化とダッシュボード、スケジュール、識農場管理について学んでいます。 Lookerのツールを埋め込むためのチャプターでは、これらの機能の他のアプリケーション、およびAPI
統合データを、他のアプリケーション

特徴:

-   簡単-アジャイル開発をlookml、言語に対応したキュレーション
    [データモデル](https://looker.com/platform/data-modeling) レポート作成者とエンドユーザーをサポートする。
-   Lookerのを経由して強力なワークフローの統合 [データ操作](https://looker.com/platform/actions).

[LookerでClickHouseを設定する方法。](https://docs.looker.com/setup-and-management/database-config/clickhouse)

[元の記事](https://clickhouse.tech/docs/en/interfaces/third-party/gui/) <!--hide-->
