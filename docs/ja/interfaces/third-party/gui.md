---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 28
toc_title: "\u8996\u754C\u9762"
---

# サードパーテ {#visual-interfaces-from-third-party-developers}

## オープンソース {#open-source}

### Tabix {#tabix}

のClickHouseのためのWebインターフェイス [Tabix](https://github.com/tabixio/tabix) プロジェクト

特徴:

-   追加のソフトウェアをインストールする必要なしに、ブラウザから直接ClickHouseで動作します。
-   構文の強調表示とクエリエディタ。
-   コマンドの自動補完。
-   ツールのためのグラフィカルに分析クエリを実行します。
-   配色オプション。

[Tabixドキュメント](https://tabix.io/doc/).

### ハウスオップ {#houseops}

[ハウスオップ](https://github.com/HouseOps/HouseOps) OSX、Linux、Windows用のUI/IDEです。

特徴:

-   構文の強調表示とクエリビルダー。 テーブルまたはJSONビューで応答を表示します。
-   CSVまたはJSONとしてエクスポートクエリ結果。
-   説明付きのプロセスのリスト。 書き込みモード。 停止する能力 (`KILL`）プロセス。
-   データベー すべてのテーブルとその列に追加情報が表示されます。
-   列サイズのクイックビュー。
-   サーバー構成。

以下の機能の開発が計画されています:

-   データベース管理。
-   ユーザー管理。
-   リアルタイムデータ解析。
-   クラスター監視。
-   クラスタ管理。
-   複製されたテーブルとカフカテーブルの監視。

### 灯台 {#lighthouse}

[灯台](https://github.com/VKCOM/lighthouse) ClickHouseのための軽量なwebインターフェイスです。

特徴:

-   テーブルのリストのフィルタリング、メタデータを指すものとします。
-   テーブルのプレビューとフィルタです。
-   読み取り専用クエリの実行。

### レダッシュ {#redash}

[レダッシュ](https://github.com/getredash/redash) めるためのプラットフォームのデータを可視化する。

サポート、多数のデータソースを含むClickHouse,Redash参加できる結果のクエリからデータソースへの最終データセットである。

特徴:

-   クエリの強力なエディタ。
-   エクスプローラ
-   さまざまな形でデータを表現できる視覚化ツール。

### DBeaver {#dbeaver}

[DBeaver](https://dbeaver.io/) -ユニバーサルデスクトップのデータベースのクライアントClickHouseます。

特徴:

-   構文の強調表示と自動補完によるクエリ開発。
-   テーブルリフィルとメタデータを検索する
-   表データプレビュー。
-   全文検索。

### clickhouse-cli {#clickhouse-cli}

[clickhouse-cli](https://github.com/hatarist/clickhouse-cli) Python3で書かれたClickHouseの代替コマンドラインクライアントです。

特徴:

-   自動補完。
-   クエリとデータ出力の構文強調表示。
-   データ出力のためのポケットベルサポート。
-   カスタムPostgreSQLのようなコマンド。

### クリックハウス-フラメグラフ {#clickhouse-flamegraph}

[クリックハウス-フラメグラフ](https://github.com/Slach/clickhouse-flamegraph) このツールは `system.trace_log` として [フラメグラフ](http://www.brendangregg.com/flamegraphs.html).

### クリックハウス-プランタム {#clickhouse-plantuml}

[チックハウス-プランタム](https://pypi.org/project/clickhouse-plantuml/) 生成するスクリプトです [プランタム](https://plantuml.com/) テーブルのスキームの図。

## 商業 {#commercial}

### DataGrip {#datagrip}

[DataGrip](https://www.jetbrains.com/datagrip/) ClickHouse専用のサポートを持つJetBrainsのデータベースIDEです。 PyCharm、IntelliJ IDEA、GoLand、PhpStormなど、他のIntelliJベースのツールにも埋め込まれています。

特徴:

-   非常に高速なコード補完。
-   ClickHouse構文の強調表示。
-   ネストされた列、テーブルエンジンなど、ClickHouse固有の機能のサポート。
-   データエディタ。
-   リファクタリング。
-   検索とナビゲーション。

### Yandexデータレンズ {#yandex-datalens}

[Yandexデータレンズ](https://cloud.yandex.ru/services/datalens) データの可視化と分析のサービスです。

特徴:

-   シンプルな棒グラフから複雑なダッシュボードまで、幅広い視覚化が可能です。
-   ダッシュボードを公開できます。
-   ClickHouseを含む複数のデータソースのサポート。
-   ClickHouseに基づく実体化されたデータのストレージ。

データレンズは [自由のために利用できる](https://cloud.yandex.com/docs/datalens/pricing) 商業使用の低負荷プロジェクトのため。

-   [DataLensドキュメント](https://cloud.yandex.com/docs/datalens/).
-   [チュートリ](https://cloud.yandex.com/docs/solutions/datalens/data-from-ch-visualization) 上の可視化するデータからClickHouseデータベースです。

### Holisticsソフトウェア {#holistics-software}

[ホリスティック](https://www.holistics.io/) フルスタックのデータプラットフォームは、ビジネスインツールです。

特徴:

-   自動メール、Slackやグーグルシートのスケジュール。
-   Visualizations、バージョン管理、自動補完、再利用可能なクエリコンポーネントと動的フィルタとSQLエディタ。
-   Iframeによるレポートとダッシュボードの組み込み分析。
-   データ準備とETL機能。
-   SQLデータモデリング支援のためのリレーショナルマッピングのデータです。

### ルッカー {#looker}

[ルッカー](https://looker.com) はデータプラットフォームは、ビジネスインツールをサポート50+データベースの方言を含むClickHouse. LookerはSaaSプラットフォームとして利用可能で、セルフホスト型です。 ユーザーが利用できLookerる場合は、vpnクライアントの直接探索、データの構築の可視化とダッシュボード、スケジュール、識農場管理について学んでいます。 Lookerのツールを埋め込むためのチャプターでは、これらの機能の他のアプリケーション、およびAPI
統合データを、他のアプリケーション

特徴:

-   LookMLを使った簡単でアジャイルな開発
    [データモデル化](https://looker.com/platform/data-modeling) レポート作成者とエンドユーザーをサポートする。
-   見物人による強力なワークフローの統合 [データ操作](https://looker.com/platform/actions).

[LookerでClickHouseを設定する方法。](https://docs.looker.com/setup-and-management/database-config/clickhouse)

[元の記事](https://clickhouse.tech/docs/en/interfaces/third-party/gui/) <!--hide-->
