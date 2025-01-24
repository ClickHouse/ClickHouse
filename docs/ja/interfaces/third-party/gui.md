---
slug: /ja/interfaces/third-party/gui
sidebar_position: 28
sidebar_label: Visual Interfaces
---

# サードパーティ開発者による視覚的インターフェース

## オープンソース {#open-source}

### ch-ui {#ch-ui}

[ch-ui](https://github.com/caioricciuti/ch-ui)は、クエリの実行とデータの可視化を目的としたClickHouseデータベース用の簡単なReact.jsアプリインターフェースです。Reactとウェブ用ClickHouseクライアントで構築され、スムーズでユーザーフレンドリーなUIを提供し、データベースとの簡単なインタラクションを実現します。

機能:

- ClickHouse統合: 接続を簡単に管理し、クエリを実行。
- レスポンシブなタブ管理: クエリおよびテーブルタブのような複数のタブを動的に管理。
- パフォーマンス最適化: 効率的なキャッシングと状態管理のためにIndexed DBを利用。
- ローカルデータストレージ: すべてのデータはブラウザ内でローカルに保存され、他の場所には送信されません。

### ChartDB {#chartdb}

[ChartDB](https://chartdb.io)は、クエリひとつでClickHouseを含むデータベーススキーマの可視化と設計ができる無料のオープンソースツールです。Reactで構築されており、データベース資格情報やサインアップなしで簡単に始められます。

機能:

- スキーマの可視化: Materialized Viewや標準Viewで、テーブルへの参照を一緒に示す、ClickHouseスキーマをインポートして視覚化。
- AI駆動のDDLエクスポート: スキーマ管理とドキュメント化を容易にするDDLスクリプトを生成。
- マルチSQL方言サポート: さまざまなデータベース環境で使える。
- サインアップや資格情報が不要: すべての機能がブラウザで直接アクセス可能。

[ChartDB ソースコード](https://github.com/chartdb/chartdb).

### Tabix {#tabix}

[Tabix](https://github.com/tabixio/tabix)プロジェクトのClickHouse用ウェブインターフェース。

機能:

- 追加ソフトウェアをインストールすることなく、ブラウザから直接ClickHouseと連携。
- シンタックスハイライト付きのクエリエディター。
- コマンドのオートコンプリート機能。
- クエリ実行のグラフィカル解析ツール。
- カラースキームオプション。

[Tabix ドキュメント](https://tabix.io/doc/).

### HouseOps {#houseops}

[HouseOps](https://github.com/HouseOps/HouseOps) は、OSX、Linux、Windows対応のUI/IDEです。

機能:

- シンタックスハイライト付きのクエリービルダー。テーブルまたはJSONビューで応答を表示。
- クエリ結果をCSVまたはJSONとしてエクスポート。
- 説明付きのプロセスリスト。書き込みモード。プロセスを停止（`KILL`）する機能。
- データベースのグラフ。全テーブルとそのカラムが追加情報とともに表示。
- カラムサイズのクイックビュー。
- サーバー設定。

開発予定の機能:

- データベース管理。
- ユーザー管理。
- リアルタイムデータ分析。
- クラスターモニタリング。
- クラスターマネジメント。
- レプリケートされたテーブルおよびKafkaテーブルのモニタリング。

### LightHouse {#lighthouse}

[LightHouse](https://github.com/VKCOM/lighthouse)は、ClickHouse用の軽量なウェブインターフェースです。

機能:

- フィルターとメタデータによるテーブルリスト。
- フィルターとソートによるテーブルプレビュー。
- 読み取り専用のクエリ実行。

### Redash {#redash}

[Redash](https://github.com/getredash/redash)はデータの視覚化プラットフォームです。

ClickHouseを含む複数のデータソースをサポートし、異なるデータソースからのクエリ結果を1つの最終データセットに結合できます。

機能:

- 強力なクエリエディタ。
- データベースエクスプローラー。
- データを異なる形式で表現する視覚化ツール。

### Grafana {#grafana}

[Grafana](https://grafana.com/grafana/plugins/grafana-clickhouse-datasource/)は、監視と視覚化のためのプラットフォームです。

Grafanaは、どこに保管された指標でもクエリし、視覚化し、アラートを設定し、理解することを可能にします。チームとダッシュボードを作成、探索、共有し、データ駆動型の文化を促進します。コミュニティに信頼され、愛されています &mdash; grafana.com。

ClickHouseデータソースプラグインは、バックエンドデータベースとしてClickHouseをサポートします。

### qryn {#qryn}

[qryn](https://metrico.in)は、ClickHouse用の高性能オブザーバビリティスタックで、ネイティブGrafana統合を備え、Loki/LogQL、Prometheus/PromQL、OTLP/Tempo、Elastic、InfluxDBなどをサポートする任意のエージェントからのログ、メトリクス、テレメトリトレースをインジェストして分析することができます。

機能:

- クエリ、抽出、データの視覚化のための内蔵Explore UIとLogQL CLI
- プラグインなしでクエリ、処理、インジェスト、トレース、アラートのネイティブGrafana APIサポート
- ログ、イベント、トレースなどから動的にデータを検索、フィルタリング、抽出する強力なパイプライン
- LogQL、PromQL、InfluxDB、Elasticなどと互換性のあるインジェストとPUSH API
- Promtail、Grafana-Agent、Vector、Logstash、Telegrafなどのエージェントと即時使用可能

### DBeaver {#dbeaver}

[DBeaver](https://dbeaver.io/) - ClickHouseサポートを持つユニバーサルデスクトップデータベースクライアント。

機能:

- シンタックスハイライトとオートコンプリート付きのクエリ開発。
- フィルターとメタデータ検索付きのテーブルリスト。
- テーブルデータプレビュー。
- 全文検索。

デフォルトでは、DBeaverはセッション（例としてCLIが行うような）を使用して接続しません。セッションサポートが必要な場合（例えば、セッションの設定を行うため）、ドライバー接続プロパティを編集して`session_id`をランダムな文字列に設定してください（内部ではhttp接続を使用します）。その後、クエリウィンドウから任意の設定を使用できます。

### clickhouse-cli {#clickhouse-cli}

[clickhouse-cli](https://github.com/hatarist/clickhouse-cli)は、Python 3で書かれたClickHouse用の代替コマンドラインクライアントです。

機能:

- オートコンプリート。
- クエリとデータ出力のシンタックスハイライト。
- データ出力のページャーサポート。
- カスタムPostgreSQLライクなコマンド。

### clickhouse-flamegraph {#clickhouse-flamegraph}

[clickhouse-flamegraph](https://github.com/Slach/clickhouse-flamegraph)は、`system.trace_log`を[flamegraph](http://www.brendangregg.com/flamegraphs.html)として視覚化するための特殊なツールです。

### clickhouse-plantuml {#clickhouse-plantuml}

[clickhouse-plantuml](https://pypi.org/project/clickhouse-plantuml/)は、テーブルのスキームの[PlantUML](https://plantuml.com/)ダイアグラムを生成するスクリプトです。

### xeus-clickhouse {#xeus-clickhouse}

[xeus-clickhouse](https://github.com/wangfenjin/xeus-clickhouse)は、ClickHouse用のJupyterカーネルであり、JupyterでSQLを使ってCHデータにクエリを実行することをサポートします。

### MindsDB Studio {#mindsdb}

[MindsDB](https://mindsdb.com/)は、最先端の機械学習モデルを簡単に開発、トレーニング、デプロイできる、ClickHouseを含むデータベース用のオープンソースAI層です。MindsDB Studio(GUI)は、データベースから新しいモデルをトレーニングし、モデルが行った予測を解釈し、潜在的なデータバイアスを識別し、Explainable AI機能を使用してモデルの正確性を評価および視覚化することで、機械学習モデルをより迅速に適応および調整することができます。

### DBM {#dbm}

[DBM](https://github.com/devlive-community/dbm) DBMは、ClickHouse用の視覚管理ツールです！

機能:

- クエリ履歴のサポート（ページネーション、すべてクリアなど）
- 選択したSQL句クエリをサポート
- クエリの終了をサポート
- テーブル管理（メタデータ、削除、プレビュー）のサポート
- データベース管理（削除、作成）のサポート
- カスタムクエリのサポート
- 複数データソースの管理（接続テスト、監視）のサポート
- モニター（プロセッサー、接続、クエリ）サポート
- データの移行サポート

### Bytebase {#bytebase}

[Bytebase](https://bytebase.com)は、チーム向けのウェブベースでオープンソースのスキーマ変更とバージョン管理ツールです。ClickHouseを含むさまざまなデータベースをサポートしています。

機能:

- 開発者とDBA間のスキーマレビュー。
- Database-as-Code、スキーマをGitLabのようなVCSでバージョン管理し、コードコミット時にデプロイメントをトリガー。
- 環境ごとのポリシー付きストリームラインドデプロイ。
- 完全な移行履歴。
- スキーマのドリフト検出。
- バックアップとリストア。
- RBAC。

### Zeppelin-Interpreter-for-ClickHouse {#zeppelin-interpreter-for-clickhouse}

[Zeppelin-Interpreter-for-ClickHouse](https://github.com/SiderZhang/Zeppelin-Interpreter-for-ClickHouse)は、ClickHouse用の[Zeppelin](https://zeppelin.apache.org)インタープリターです。JDBCインタープリターと比較すると、長時間実行されるクエリに対してより良いタイムアウト制御を提供できます。

### ClickCat {#clickcat}

[ClickCat](https://github.com/clickcat-project/ClickCat)は、ClickHouseデータを検索、探索、可視化するのに役立つフレンドリーなユーザーインターフェースです。

機能:

- インストールなしでSQLコードを実行できるオンラインSQLエディター。
- 全プロセスと変異を観察可能。未完のプロセスはUIから殺すことができます。
- メトリクスには、クラスタ解析、データ解析、クエリ解析が含まれます。

### ClickVisual {#clickvisual}

[ClickVisual](https://clickvisual.net/) はClickVisualは軽量のオープンソースのログクエリ、分析、アラームビジュアル化プラットフォームです。

機能:

- 分析ログライブラリのワンクリック作成をサポート
- ログ収集設定の管理をサポート
- ユーザー定義のインデックス設定をサポート
- アラーム設定をサポート
- ライブラリとテーブルの権限設定のサポート

### ClickHouse-Mate {#clickmate}

[ClickHouse-Mate](https://github.com/metrico/clickhouse-mate)は、ClickHouseでデータを検索し、探索するためのAngularウェブクライアント＋ユーザーインターフェースです。

機能:

- ClickHouse SQLクエリオートコンプリート
- 高速のデータベースおよびテーブルツリーナビゲーション
- 高度な結果フィルタリングおよびソート
- インラインClickHouse SQLドキュメント
- クエリプリセットと履歴
- 100％ブラウザベース、サーバー/バックエンドなし

クライアントはGitHubページを通じて即時使用可能: https://metrico.github.io/clickhouse-mate/

### Uptrace {#uptrace}

[Uptrace](https://github.com/uptrace/uptrace)は、OpenTelemetryとClickHouseにより分散トレースとメトリクスを提供するAPMツールです。

機能:

- [OpenTelemetryトレース](https://uptrace.dev/opentelemetry/distributed-tracing.html)、メトリクス、およびログ。
- AlertManagerを使用したEmail/Slack/PagerDuty通知。
- スパンを集約するためのSQLライクなクエリ言語。
- メトリクスをクエリするためのPromqlライクな言語。
- 事前構築されたメトリクスダッシュボード。
- YAML設定による複数ユーザー/プロジェクト。

### clickhouse-monitoring {#clickhouse-monitoring}

[clickhouse-monitoring](https://github.com/duyet/clickhouse-monitoring)は、`system.*`テーブルに基づいてClickHouseクラスターを監視し、その概要を提供するシンプルなNext.jsダッシュボードです。

機能:

- クエリモニター: 現在のクエリ、クエリ履歴、クエリリソース（メモリ、読み取り部、file_openなど）、最も高コストのクエリ、最も使用されるテーブルまたはカラムなど。
- クラスターモニター: 合計メモリ/CPU使用量、分散キュー、グローバル設定、mergetree設定、指標など。
- テーブルとパーツの情報: サイズ、行数、圧縮、パーツサイズなど、カラムレベルの詳細。
- 有用なツール: Zookeeperデータ探査、クエリEXPLAIN、クエリのキルなど。
- 視覚化メトリックチャート: クエリとリソース使用量、マージ/ミューテーションの数、マージパフォーマンス、クエリパフォーマンスなど。

### CKibana {#ckibana}

[CKibana](https://github.com/TongchengOpenSource/ckibana)は、ネイティブKibana UIを使用してClickHouseデータを検索、探索、可視化できる軽量サービスです。

機能:

- ネイティブKibana UIからのチャートリクエストをClickHouseクエリ構文に変換。
- サンプリングやキャッシングなどの高度な機能をサポートし、クエリパフォーマンスを向上。
- ElasticSearchからClickHouseに移行後の学習コストを最小限に。

## コマーシャル {#commercial}

### DataGrip {#datagrip}

[DataGrip](https://www.jetbrains.com/datagrip/)はJetBrains提供のデータベースIDEで、ClickHouseに特化したサポートがあります。これはその他のIntelliJベースのツールにも組み込まれています: PyCharm、IntelliJ IDEA、GoLand、PhpStormなど。

機能:

- 非常に高速なコード補完。
- ClickHouseのシンタックスハイライト。
- ClickHouseに特有の機能、例えば入れ子になったカラムやテーブルエンジンのサポート。
- データエディタ。
- リファクタリング。
- 検索とナビゲーション。

### Yandex DataLens {#yandex-datalens}

[Yandex DataLens](https://cloud.yandex.ru/services/datalens)はデータの視覚化と分析サービスです。

機能:

- シンプルな棒グラフから複雑なダッシュボードまで幅広い視覚化が可能。
- ダッシュボードは公開可能。
- ClickHouseを含む複数のデータソースのサポート。
- Materialized Viewに基づいたデータのストレージ。

DataLensは、[商用利用においても低負荷プロジェクトに対して無料](https://cloud.yandex.com/docs/datalens/pricing)で利用可能です。

- [DataLens ドキュメント](https://cloud.yandex.com/docs/datalens/)。
- [チュートリアル](https://cloud.yandex.com/docs/solutions/datalens/data-from-ch-visualization)で、ClickHouseデータベースからのデータの視覚化について学ぶ。

### Holistics Software {#holistics-software}

[Holistics](https://www.holistics.io/)は全スタックのデータプラットフォームとビジネスインテリジェンスツールです。

機能:

- レポートの自動メール、Slack、Googleシートスケジュール。
- バージョン管理、オートコンプリート、再利用可能なクエリコンポーネント、動的フィルター付きの視覚化付きSQLエディタ。
- iframeを介したレポートとダッシュボードの埋め込み分析。
- データ準備とETL機能。
- 関連付けによるデータモデリングのためのSQLデータモデリングサポート。

### Looker {#looker}

[Looker](https://looker.com)は、ClickHouseを含む50以上のデータベース方言をサポートするデータプラットフォームおよびビジネスインテリジェンスツールです。LookerはSaaSプラットフォームとして利用できるほか、自社でホスティングすることもできます。ユーザーはブラウザを介してLookerを使用し、データを探索し、ビジュアライゼーションとダッシュボードを構築し、レポートをスケジュールし、同僚と洞察を共有できます。Lookerはこれらの機能を他のアプリケーションに埋め込むための豊富なツールセットを提供し、データを他のアプリケーションと統合するためのAPIも提供します。

機能:

- [データモデリング](https://looker.com/platform/data-modeling)をサポートするLookMLの使用による簡単で機敏な開発。
- Lookerの[Data Actions](https://looker.com/platform/actions)を介した強力なワークフロー統合。

[LookerでClickHouseを設定する方法](https://docs.looker.com/setup-and-management/database-config/clickhouse)。

### SeekTable {#seektable}

[SeekTable](https://www.seektable.com)はデータ探索と運用レポートのためのセルフサービスBIツールです。クラウドサービスとセルフホステッドバージョンの両方で利用可能です。SeekTableからのレポートは任意のウェブアプリに埋め込むことができます。

機能:

- ビジネスユーザーに優しいレポートビルダー。
- SQLフィルタリングとレポート特有のクエリカスタマイズのための強力なレポートパラメータ。
- ClickHouseにはネイティブなTCP/IPエンドポイントとHTTP(S)インターフェースのどちらも接続可能（2つの異なるドライバー）。
- ClickHouse SQL方言のすべての力をディメンション/メジャーの定義で使用可能。
- レポートの自動生成のための[Web API](https://www.seektable.com/help/web-api-integration)を提供。
- アカウントデータの[バックアップ/リストア](https://www.seektable.com/help/self-hosted-backup-restore)により、レポート開発のフローをサポート; データモデル（キューブ）/レポート構成は人間が読めるXMLであり、バージョン管理システムで管理可能。

SeekTableは、個人または個人利用のために[無料](https://www.seektable.com/help/cloud-pricing)です。

[SeekTableでのClickHouse接続設定方法。](https://www.seektable.com/help/clickhouse-pivot-table)

### Chadmin {#chadmin}

[Chadmin](https://github.com/bun4uk/chadmin)は、ClickHouseクラスターで現在実行されているクエリを視覚化し、それらの情報を表示し、必要に応じてクエリを停止できるシンプルなUIです。

### TABLUM.IO {#tablum_io}

[TABLUM.IO](https://tablum.io/) は、ETLと視覚化のためのオンラインクエリおよび分析ツールです。ClickHouseに接続し、用途の広いSQLコンソールを介してデータをクエリしたり、静的ファイルやサードパーティサービスからデータをロードすることができます。TABLUM.IOはデータ結果をチャートやテーブルとして視覚化できます。

機能:
- ETL: 人気のあるデータベース、ローカルおよびリモートファイル、API呼び出しからのデータロード。
- シンタックスハイライト付きの用途の広いSQLコンソールとビジュアルクエリビルダー。
- チャートおよびテーブルとしてのデータ視覚化。
- データのマテリアライズおよびサブクエリ。
- Slack、Telegram、メールへのデータレポート。
- 独自のAPIを介したデータパイプライン。
- JSON、CSV、SQL、HTML形式でのデータエクスポート。
- ウェブベースインターフェース。

TABLUM.IOは、セルフホストソリューション（Dockerイメージとして）またはクラウドで実行可能です。
ライセンス: 3ヶ月無料期間の[商用製品](https://tablum.io/pricing)。

クラウドで無料で試用する [クラウドで試用](https://tablum.io/try)。
詳しくは、[TABLUM.IO](https://tablum.io/)で製品について学ぶ。

### CKMAN {#ckman}

[CKMAN](https://www.github.com/housepower/ckman)は、ClickHouseクラスタの管理と監視のためのツールです！

機能:

- ブラウザインターフェイスを通じた便利で迅速なクラスタの自動展開
- クラスタのスケールまたはスケールダウン
- クラスターデータのロードバランス
- クラスターをオンラインでアップグレード
- ページでクラスタ設定を変更
- クラスタノードの監視とZookeeper監視を提供
- テーブルとパーティションの状態や遅いSQLステートメントを監視
- 使いやすいSQL実行ページを提供
