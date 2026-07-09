---
description: 'ClickHouse を扱うためのサードパーティ製 GUI ツールおよびアプリケーションの一覧'
sidebar_label: 'ビジュアルインターフェイス'
sidebar_position: 28
slug: /interfaces/third-party/gui
title: 'サードパーティ開発者によるビジュアルインターフェイス'
doc_type: 'reference'
---

<div id="open-source">
  ## オープンソース
</div>

<div id="agx">
  ### agx
</div>

[agx](https://github.com/agnosticeng/agx) は、Tauri と SvelteKit で構築されたデスクトップアプリケーションで、ClickHouse の埋め込みデータベースエンジン (chdb) を使ってデータの探索やクエリを行うためのモダンなインターフェイスを提供します。

* ネイティブアプリケーションとして実行する場合は、ch-db を活用できます。
* Web 版として実行する場合は、ClickHouse インスタンスに接続できます。
* Monaco エディタを採用しているため、違和感なく使い始められます。
* 複数の可視化を備え、現在も進化を続けています。

<div id="ch-ui">
  ### ch-ui
</div>

[ch-ui](https://github.com/caioricciuti/ch-ui) は、クエリの実行やデータの可視化のために設計された、ClickHouse データベース向けのシンプルな React.js アプリケーションのインターフェイスです。React と Web 向けの ClickHouse client で構築されており、洗練された使いやすい UI によって、データベースを手軽に操作できます。

特長:

* ClickHouse 連携: 接続を簡単に管理し、クエリを実行できます。
* レスポンシブなタブ管理: クエリタブやテーブルタブなど、複数のタブを動的に扱えます。
* パフォーマンスの最適化: 効率的なキャッシュと状態管理のために Indexed DB を利用します。
* ローカルデータ保存: すべてのデータはブラウザ内にローカル保存されるため、他の場所へ送信されることはありません。

<div id="chartdb">
  ### ChartDB
</div>

[ChartDB](https://chartdb.io) は、ClickHouse を含むデータベースのスキーマを 1 つのクエリで可視化・設計できる、無料のオープンソースツールです。React で構築されており、使いやすくシームレスな操作性を備え、利用開始にあたってデータベースの認証情報や登録は不要です。

機能:

* スキーマの可視化: ClickHouse のスキーマを即座に取り込み、可視化できます。materialized view や標準ビューを含む ER 図にも対応しており、テーブルへの参照も表示されます。
* AI による DDL エクスポート: スキーマ管理やドキュメント作成に役立つ DDL スクリプトを簡単に生成できます。
* 複数の SQL方言 をサポート: 幅広い SQL方言 に対応しており、さまざまなデータベース環境で柔軟に利用できます。
* 登録や認証情報は不要: すべての機能をブラウザーから直接利用できるため、手軽で安全です。

[ChartDB ソースコード](https://github.com/chartdb/chartdb)。

<div id="datastoria">
  ### DataStoria
</div>

[DataStoria](https://github.com/FrankChen021/datastoria) は、複数の ClickHouse クラスターを一元管理できる、AI 搭載の web console アプリケーションです。

機能:

* **AI 搭載インテリジェンス**: 自然言語でデータを Explore し、SQL クエリの最適化や修正を行い、データを可視化できます。
* **公式 ClickHouse Agent Skills インテグレーション**: [公式のベストプラクティス](https://github.com/ClickHouse/agent-skills)を活用して、データベースの最適化や改善提案を AI に求められます。
* **スマートなエラー診断**: 行番号とカラムを正確にハイライトして構文エラーを即座に特定し、ワンクリックで AI による修正提案を受け取れます。
* **system table の調査**: 強力な可視化ダッシュボードとフィルターを使って、`system.query_log`、`system.query_views_log`、`system.zookeeper`、`system.ddl_distributed_queue`、`system.part_log`、`system.processes` を詳しく調査し、クラスターの状態をすばやく把握できます。
* **ワンクリック Explain**: AST とパイプラインの視覚的なビューで、クエリ実行計画をすぐに理解できます。
* **依存関係グラフ**: materialized view、分散テーブル、外部システムをまたぐテーブル間の関係やデータフローを可視化できます。
* **クラスター監視**: リアルタイムのメトリクス、マージ操作、レプリケーション状態、クエリパフォーマンスなどを通じて、すべてのノードを監視できます。
* **プライバシーとセキュリティ**: すべての SQL クエリはブラウザーから ClickHouse server に直接送信されるため、完全なプライバシーを確保できます。

[DataStoria documentation](https://docs.datastoria.app).

<div id="datapup">
  ### DataPup
</div>

[DataPup](https://github.com/DataPupOrg/DataPup) は、ClickHouse をネイティブサポートする、モダンな AI 支援型クロスプラットフォームデータベースクライアントです。

機能:

* インテリジェントな提案による AI 搭載の SQL クエリ支援
* 認証情報を安全に処理する、ClickHouse へのネイティブ接続サポート
* 複数のテーマ (Light、Dark、カラフルなバリアント) を備えた、美しくアクセシブルなインターフェイス
* 高度なクエリ結果の絞り込みと分析
* クロスプラットフォーム対応 (macOS、Windows、Linux) 
* 高速で応答性に優れたパフォーマンス
* オープンソースで、MIT ライセンスの下で提供

<div id="dory">
  ### Dory
</div>

[Dory](https://github.com/dorylab/dory) は、ClickHouse をネイティブにサポートし、AI を組み込んだ SQL ワークスペースです。

機能:

* SQL の生成、説明、デバッグに対応する AI Copilot
* 統合ワークスペースから複数の ClickHouse クラスターを管理し、クエリを実行
* スキーマ対応の SQL 自動補完とマルチタブのクエリワークスペース
* フィルタリングや可視化による、対話的なクエリ結果の探索
* データセットの理解に役立つ AI によるテーブル要約
* SSH トンネルをサポートする ClickHouse への直接接続
* ライト/ダークテーマに対応した、モダンで開発者向けの使いやすいインターフェイス
* macOS、Windows、Linux に対応したクロスプラットフォームのデスクトップアプリと Docker サポート
* オープンソースの MIT ライセンス

<div id="clickhouse-schemaflow-visualizer">
  ### ClickHouse Schema Flow Visualizer
</div>

[ClickHouse Schema Flow Visualizer](https://github.com/FulgerX2007/clickhouse-schemaflow-visualizer) は、ClickHouse テーブル間の関係を可視化するためのオープンソースの Web アプリケーションです。
ClickHouse インスタンスに接続し、`system.tables` のメタデータ (engine の種類、依存関係、materialized view の SELECT) を解析して、インタラクティブなテーブルレベルのデータフロー図と、各エッジに変換式が付いたカラムレベルの関係を描画します。図は Dagre でレイアウトされ、プレーンなインライン SVG としてレンダリングされます。client-side のダイアグラムランタイムは読み込まれません。

機能:

* 直感的なサイドバーで ClickHouse のデータベースとテーブルを閲覧
* Data Flow ビュー: テーブルレベルのアップストリームソースとダウンストリームの materialized view
* Relationships ビュー: 各エッジに解析済みの変換式 (例: `toStartOfHour(scheduled_departure)`, `avgState(delay_minutes)`) を表示したカラムレベルのマッピング
* `MergeTree`、`Replicated*`、`Distributed`、`MaterializedView`、`Dictionary` に対応した engine 別アイコンと色分け
* Relationships ビューでカラムをクリックすると、pipeline 全体にわたるそのデータパス全体をハイライト
* ライブサイドバーフィルターと `Ctrl+K` / `⌘K` コマンドパレットにより、任意のテーブル、カラム、または engine に移動
* テーブルごとの行数とディスク上のサイズを表示するオプションのメタデータオーバーレイ
* 現在の図を自己完結型の HTML ファイルとしてエクスポート
* ClickHouse への TLS connection。証明書検証のスキップやカスタム CA / client certificates にも任意で対応

[ClickHouse Schema Flow Visualizer - ソースコード](https://github.com/FulgerX2007/clickhouse-schemaflow-visualizer)

<div id="tabix">
  ### Tabix
</div>

[Tabix](https://github.com/tabixio/tabix) プロジェクトによる、ClickHouse 向けの Web インターフェイスです。

機能:

* 追加のソフトウェアをインストールしなくても、ブラウザから直接 ClickHouse を利用できます。
* 構文ハイライト機能付きのクエリエディタ。
* コマンドの自動補完。
* クエリ実行を視覚的に分析するためのツール。
* 配色テーマのオプション。

[Tabix documentation](https://tabix.io/doc/)。

<div id="houseops">
  ### HouseOps
</div>

[HouseOps](https://github.com/HouseOps/HouseOps) は、OSX、Linux、Windows 向けの UI/IDE です。

機能:

* 構文ハイライト付きのクエリビルダー。応答はテーブルビューまたは JSON ビューで表示できます。
* クエリ結果を CSV または JSON としてエクスポートできます。
* 説明付きのプロセス一覧。書き込みモード。プロセスを停止 (`KILL`) する機能。
* データベースグラフ。すべてのテーブルとそのカラムを追加情報付きで表示します。
* カラムサイズをすばやく確認できます。
* サーバー設定。

今後開発予定の機能:

* データベース管理。
* ユーザー管理。
* リアルタイムのデータ分析。
* クラスター監視。
* クラスター管理。
* レプリケーション対応テーブルおよび Kafka テーブルの監視。

<div id="lighthouse">
  ### LightHouse
</div>

[LightHouse](https://github.com/VKCOM/lighthouse) は、ClickHouse 向けの軽量な Web インターフェイスです。

機能:

* フィルタリングとメタデータ表示に対応したテーブル一覧。
* フィルタリングとソートに対応したテーブルプレビュー。
* 読み取り専用でのクエリ実行。

<div id="redash">
  ### Redash
</div>

[Redash](https://github.com/getredash/redash) は、データ可視化のためのプラットフォームです。

ClickHouse を含む複数のデータソースに対応しており、異なるデータソースのクエリ結果を結合して、1 つの最終的なデータセットにまとめることができます。

機能:

* 強力なクエリエディタ。
* データベースエクスプローラー。
* データをさまざまな形式で表現できる可視化ツール。

<div id="grafana">
  ### Grafana
</div>

[Grafana](https://grafana.com/grafana/plugins/grafana-clickhouse-datasource/) は、監視と可視化のためのプラットフォームです。

「Grafana を使えば、メトリクスがどこに保存されていても、クエリ、可視化、アラートを行い、把握できます。チームでダッシュボードを作成、確認、共有し、データドリブンな文化を育むことができます。コミュニティから信頼され、支持されています」 — grafana.com。

ClickHouse データソース plugin は、バックエンドデータベースとして ClickHouse をサポートします。

<div id="qryn">
  ### qryn
</div>

[qryn](https://metrico.in) は、ClickHouse 向けの多様なプロトコルに対応した高性能なオブザーバビリティスタック&#x20;*&#x20;(旧称 cLoki)&#x20;*&#x20;で、ネイティブな Grafana インテグレーションにより、Loki/LogQL、Prometheus/PromQL、OTLP/Tempo、Elastic、InfluxDB などに対応するあらゆるエージェントからログ、メトリクス、トレースのテレメトリーを取り込んで分析できます。

特長:

* データのクエリ、抽出、可視化に対応する組み込みの Explore UI と LogQL CLI
* プラグイン不要でクエリ、処理、インジェスト、トレーシング、アラートに対応するネイティブな Grafana API サポート
* ログ、イベント、トレースなどからデータを動的に検索、絞り込み、抽出できる強力なパイプライン
* LogQL、PromQL、InfluxDB、Elastic などと透過的な互換性を持つインジェストおよび PUSH API
* Promtail、Grafana-Agent、Vector、Logstash、Telegraf など、多くのエージェントですぐに利用可能

<div id="dbeaver">
  ### DBeaver
</div>

[DBeaver](https://dbeaver.io/) - ClickHouse をサポートする汎用デスクトップデータベースクライアントです。

機能:

* シンタックスハイライトと自動補完を備えたクエリ作成。
* フィルターとメタデータ検索に対応したテーブル一覧。
* テーブルデータのプレビュー。
* 全文検索。

デフォルトでは、DBeaver はセッションを使用して接続しません (たとえば CLI は使用します) 。セッションのサポートが必要な場合 (たとえば、セッションに設定を適用する場合) は、ドライバーの接続プロパティを編集し、`session_id` をランダムな文字列に設定してください (内部では HTTP 接続を使用します) 。その後は、クエリウィンドウから任意の設定を使用できます。

<div id="clickhouse-cli">
  ### clickhouse-cli
</div>

[clickhouse-cli](https://github.com/hatarist/clickhouse-cli) は、Python 3 で書かれた ClickHouse 用の代替コマンドラインクライアントです。

機能:

* 自動補完
* クエリとデータ出力の構文ハイライト
* データ出力でのページャー対応
* PostgreSQL 風のカスタムコマンド

<div id="clickhouse-flamegraph">
  ### clickhouse-flamegraph
</div>

[clickhouse-flamegraph](https://github.com/Slach/clickhouse-flamegraph) は、`system.trace_log` を[フレームグラフ](http://www.brendangregg.com/flamegraphs.html)として可視化する専用ツールです。

<div id="clickhouse-plantuml">
  ### clickhouse-plantuml
</div>

[cickhouse-plantuml](https://pypi.org/project/clickhouse-plantuml/) は、テーブルスキーマの [PlantUML](https://plantuml.com/) 図を生成するスクリプトです。

<div id="clickhouse-table-graph">
  ### ClickHouse table graph
</div>

[ClickHouse table graph](https://github.com/mbaksheev/clickhouse-table-graph) は、ClickHouseテーブル間の依存関係を可視化するためのシンプルな CLI ツールです。このツールは `system.tables` テーブルからテーブル間の接続関係を取得し、[mermaid](https://mermaid.js.org/syntax/flowchart.html) フォーマットで依存関係のフローチャートを生成します。このツールを使うことで、テーブルの依存関係を簡単に可視化し、ClickHouseデータベース内のデータフローを把握できます。mermaid により、生成されるフローチャートは見やすく、Markdown ドキュメントにも簡単に追加できます。

<div id="xeus-clickhouse">
  ### xeus-clickhouse
</div>

[xeus-clickhouse](https://github.com/wangfenjin/xeus-clickhouse) は ClickHouse 向けの Jupyter カーネルで、Jupyter 上で SQL を使って ClickHouse のデータをクエリできます。

<div id="mindsdb">
  ### MindsDB Studio
</div>

[MindsDB](https://mindsdb.com/) は、ClickHouse を含むデータベース向けのオープンソースの AI レイヤーであり、最先端の機械学習モデルを簡単に開発、学習、デプロイできます。MindsDB Studio (GUI) を使うと、データベースのデータから新しいモデルを学習させたり、モデルによる予測を解釈したり、潜在的なデータの偏りを特定したりできるほか、Explainable AI 機能を使ってモデルの精度を評価・可視化できるため、機械学習モデルをより迅速に適応・調整できます。

<div id="dbm">
  ### DBM
</div>

[DBM](https://github.com/devlive-community/dbm) は、ClickHouse 向けのビジュアル管理ツールです。

機能:

* クエリ履歴に対応 (ページネーション、すべてクリア など) 
* 選択した SQL 句でのクエリに対応
* クエリの終了に対応
* テーブル管理に対応 (メタデータ、削除、プレビュー) 
* データベース管理に対応 (削除、作成) 
* カスタムクエリに対応
* 複数のデータソース管理に対応 (接続テスト、監視) 
* 監視に対応 (プロセッサ、接続、クエリ) 
* データ移行に対応

<div id="bytebase">
  ### Bytebase
</div>

[Bytebase](https://bytebase.com) は、チーム向けの Web ベースのオープンソースのスキーマ変更およびバージョン管理ツールです。ClickHouse を含むさまざまなデータベースをサポートしています。

機能:

* 開発者と DBA 間でのスキーマレビュー。
* Database-as-Code。GitLab などの VCS でスキーマをバージョン管理し、code の commit をトリガーにデプロイメントを実行します。
* 環境ごとのポリシーによる効率的なデプロイメント。
* 完全な移行履歴。
* スキーマドリフトの検出。
* バックアップと復元。
* RBAC。

<div id="zeppelin-interpreter-for-clickhouse">
  ### Zeppelin-Interpreter-for-ClickHouse
</div>

[Zeppelin-Interpreter-for-ClickHouse](https://github.com/SiderZhang/Zeppelin-Interpreter-for-ClickHouse) は、ClickHouse 向けの [Zeppelin](https://zeppelin.apache.org) インタープリタです。JDBC インタープリタと比べて、長時間実行されるクエリの timeout をより適切に制御できます。

<div id="clickcat">
  ### ClickCat
</div>

[ClickCat](https://github.com/clickcat-project/ClickCat) は、ClickHouse のデータを検索、閲覧、可視化できる、使いやすいユーザーインターフェイスです。

機能:

* インストール不要で SQL コードを実行できるオンライン SQL エディタ。
* すべてのプロセスとミューテーションを確認できます。未完了のプロセスは、UI から強制終了できます。
* メトリクスには、クラスター分析、データ分析、クエリ分析が含まれます。

<div id="clickvisual">
  ### ClickVisual
</div>

[ClickVisual](https://clickvisual.net/) は、軽量なオープンソースのログクエリ、分析、アラート可視化プラットフォームです。

機能:

* ログ分析ライブラリをワンクリックで作成可能
* ログ収集設定の管理をサポート
* ユーザー定義の索引設定をサポート
* アラート設定をサポート
* ライブラリおよびテーブル単位でのきめ細かな権限設定をサポート

<div id="clickmate">
  ### ClickHouse-Mate
</div>

[ClickHouse-Mate](https://github.com/metrico/clickhouse-mate) は、ClickHouse 内のデータを検索・探索するための Angular 製 Webクライアント兼ユーザーインターフェイスです。

機能:

* ClickHouse SQL クエリの自動補完
* 高速なデータベース／テーブルツリーのナビゲーション
* 高度な結果のフィルタリングとソート
* インラインの ClickHouse SQL ドキュメント
* クエリプリセットと履歴
* 100% ブラウザベース、サーバー／バックエンド不要

このクライアントは GitHub Pages からすぐに利用できます: https://metrico.github.io/clickhouse-mate/

<div id="uptrace">
  ### Uptrace
</div>

[Uptrace](https://github.com/uptrace/uptrace) は、OpenTelemetry と ClickHouse を活用して分散トレーシングやメトリクスを提供する APM ツールです。

機能:

* [OpenTelemetry tracing](https://uptrace.dev/opentelemetry/distributed-tracing.html)、メトリクス、ログ。
* AlertManager を使用した Email/Slack/PagerDuty への通知。
* span を集計するための SQL ライクなクエリ言語。
* メトリクスをクエリするための PromQL ライクな言語。
* あらかじめ用意されたメトリクスダッシュボード。
* YAML config による複数ユーザー／プロジェクト対応。

<div id="clickhouse-monitoring">
  ### clickhouse-monitoring
</div>

[clickhouse-monitoring](https://github.com/duyet/clickhouse-monitoring) は、`system.*` テーブルを利用して ClickHouse クラスターの監視や概要の把握を支援する、シンプルな Next.js ダッシュボードです。

機能:

* クエリモニター: 現在のクエリ、クエリ履歴、クエリリソース (メモリ、読み取りパーツ数、file&#95;open など) 、最も高コストなクエリ、最もよく使用されるテーブルやカラムなど。
* クラスター モニター: 合計メモリ/CPU 使用量、分散キュー、グローバル設定、MergeTree settings、メトリクスなど。
* テーブルとパーツの情報: サイズ、行数、圧縮、パーツサイズなどをカラムレベルの詳細で表示。
* 便利なツール: ZooKeeper データの探索、クエリ EXPLAIN、クエリの強制終了など。
* メトリクスの可視化チャート: クエリとリソース使用量、merges/mutation の数、merge パフォーマンス、クエリパフォーマンスなど。

<div id="ckibana">
  ### CKibana
</div>

[CKibana](https://github.com/TongchengOpenSource/ckibana) は、Kibana ネイティブUIを使って ClickHouse のデータを手軽に検索、探索、可視化できる軽量なサービスです。

特長:

* Kibana ネイティブUIからのチャートリクエストを ClickHouse のクエリ構文に変換します。
* クエリパフォーマンスを向上させるため、sampling やキャッシュなどの高度な機能をサポートしています。
* Elasticsearch から ClickHouse へ移行した後のユーザーの学習コストを最小限に抑えます。

<div id="telescope">
  ### Telescope
</div>

[Telescope](https://iamtelescope.net/) は、ClickHouse に保存されたログを探索するためのモダンな Web インターフェイスです。きめ細かなアクセス制御のもとで、ログデータのクエリ、可視化、管理を行える、使いやすい UI を提供します。

機能:

* 強力なフィルターとカスタマイズ可能なフィールド選択を備えた、洗練されたレスポンシブ UI。
* 直感的で表現力の高いログフィルタリングを実現する FlyQL 構文。
* ネストされた JSON、Map、Array フィールドを含む、group-by 対応の時系列グラフ。
* 高度なフィルタリング向けに、オプションで Raw SQL `WHERE` クエリをサポート (権限チェックあり) 。
* 保存済みビュー: クエリやレイアウトのカスタム UI 設定を保存して共有できます。
* ロールベースのアクセス制御 (RBAC) と GitHub 認証のインテグレーション。
* ClickHouse 側で追加のエージェントやコンポーネントは不要です。

[Telescope ソースコード](https://github.com/iamtelescope/telescope) · [ライブデモ](https://demo.iamtelescope.net)

<div id="clicklens">
  ### ClickLens
</div>

[ClickLens](https://ntk148v.github.io/clicklens/) は、ClickHouse データベースの管理と監視のための、モダンで高機能かつ使いやすい Web インターフェイスです。開発者、アナリスト、管理者が ClickHouse クラスターを効率的に扱えるようにする包括的なツール群を提供します。ClickHouse は非常に優れた分析データベースですが、CLI や基本的なツールだけで管理するのは難しいことがあります。ClickLens は、次のような機能を提供することでそのギャップを埋めます。

* Discover - あらゆるテーブルに対応した、Kibana ライクで柔軟なデータ探索
* SQL Console - シンタックスハイライトやストリーミング結果に対応し、クエリの記述、実行、分析が可能
* Real-time Monitoring - クラスターの健全性、クエリパフォーマンス、リソース使用状況をリアルタイムで監視
* Schema Explorer - データベース、テーブル、カラム、パーツなどを参照して移動
* Access Control - UI からユーザーとロールを直接管理
* Native RBAC - UI の権限は ClickHouse の grant から直接導出

[ClickLens ソースコード](https://github.com/ntk148v/clicklens)

<div id="chouse-ui">
  ### CHouse UI
</div>

[CHouse UI](https://chouse-ui.com) は、**本番環境で ClickHouse を運用するチーム**向けに構築された、オープンソースのセルフホスト型 ClickHouse Web インターフェイスです。多くのツールは、クエリワークスペース、ダッシュボード、AI アシスタント、クラスター監視など、どれか 1 つに特化しています。一方 CHouse UI は、それらを*組み合わせた*存在です。チーム向けのアクセスレイヤーに、マルチクラスターのフリート監視と、自律型の読み取り専用 AI SRE を組み合わせています。データベースの認証情報を直接必要とするクライアントとは異なり、認証情報はサーバー側で暗号化して保存され、独自の **Role-Based Access Control (RBAC)** レイヤーでアクセスが制御されるため、ブラウザが ClickHouse のパスワードに触れることはありません。

機能:

* **チームアクセスとセキュリティ** - アプリケーションレベルの RBAC (定義済み + カスタムロール、データベース / テーブルごとのきめ細かなデータアクセスルール) 、実際のセッションコンテキストを伴う監査ログ、AES-256-GCM で暗号化されたサーバー側認証情報。
* **マルチクラスターのフリート** - 設定済みのすべてのクラスターを 1 つの画面で監視できます (ステータス、メモリ、実行中のクエリ、例外、トレンドのスパークライン) 。各カードは独立してポーリングし、バックエンドのスナップショットポーラーによって支えられています。
* **Chouse AI — Fleet Doctor** - 自律型の読み取り専用 AI SRE です。保護された `system.*` 専用の `SELECT` ツール (ClickHouse `readonly=1`) でフリートをスキャンし、根本原因を特定し、負荷の高いクエリの詳細分析と推奨される書き換えを含む構造化レポートを作成します。クラスターを変更することはありません。
* **監視タブ内の AI** - Query Logs の行にある &quot;Optimize with Chouse AI&quot; (書き換え + 変更前→変更後の `EXPLAIN` 見積もり + SQL ワークスペースで開く) に加え、`system.errors` の行や part-log エントリでワンクリックの &quot;Diagnose&quot; を利用できます。
* **しきい値アラート** - ノードのメモリ %、クエリごとのメモリ、長時間実行クエリのルールを Slack とメールに配信し、しきい値超過時には自律的な根本原因分析も添付されます。
* **フルワークスペース** - Monaco SQL エディタ、Schema Explorer、強制終了に対応したライブクエリビュー、ClickHouse ネイティブの監視 (メモリ内訳、パーツ / マージ、レプリカラグ、レイテンシのパーセンタイル) 、およびデータのインポート / エクスポート。

オープンソース (Apache 2.0) で、オンプレミス優先 — すべての機能が標準で含まれており、有料ティアはありません。

[CHouse UI Source Code](https://github.com/daun-gatal/chouse-ui)

<div id="clickhouse-flow">
  ### clickhouse-flow
</div>

[clickhouse-flow](https://github.com/MikeAmputer/clickhouse-flow) は、ClickHouse のテーブル、ビュー、materialized view 間のデータフローや依存関係を可視化するオープンソースツールです。

機能:

* ClickHouse のメタデータからスキーマグラフを自動生成します。
* materialized view を介したデータフローを可視化します。
* スキーマ構造を確認できる対話型 UI。
* ドキュメント作成や共有向けに、図を PDF または SVG としてエクスポートできます。
* 開発環境で迅速にセットアップできる Docker ベースのデプロイメント。

<div id="commercial">
  ## 商用
</div>

<div id="datagrip">
  ### DataGrip
</div>

[DataGrip](https://www.jetbrains.com/datagrip/) は JetBrains のデータベース IDE で、ClickHouse 専用のサポートを備えています。また、PyCharm、IntelliJ IDEA、GoLand、PhpStorm など、他の IntelliJ ベースのツールにも組み込まれています。

機能:

* 非常に高速なコード補完。
* ClickHouse の構文ハイライト。
* ネストされたカラムやテーブルエンジンなど、ClickHouse 固有の機能をサポート。
* データエディタ。
* リファクタリング。
* 検索とナビゲーション。

<div id="yandex-datalens">
  ### Yandex DataLens
</div>

[Yandex DataLens](https://yandex.cloud/en/services/datalens) は、データの可視化と分析のためのサービスです。

特長:

* シンプルな棒グラフから複雑なダッシュボードまで、幅広い可視化に対応しています。
* ダッシュボードを公開して利用できます。
* ClickHouse を含む複数のデータソースをサポートしています。
* ClickHouse ベースのマテリアライズドデータを保存できます。

DataLens は、低負荷のプロジェクトであれば、商用利用であっても[無料で利用できます](https://yandex.cloud/en/docs/datalens/pricing)。

* [DataLens ドキュメント](https://yandex.cloud/en/docs/datalens/)。
* ClickHouse データベースのデータを可視化する[チュートリアル](https://yandex.cloud/en/docs/solutions/datalens/data-from-ch-visualization)。

<div id="holistics-software">
  ### Holistics Software
</div>

[Holistics](https://www.holistics.io/) は、フルスタックのデータプラットフォーム兼ビジネスインテリジェンスツールです。

機能:

* レポートのメール、Slack、Google Sheets への自動配信スケジュール。
* 可視化、バージョン管理、自動補完、再利用可能なクエリコンポーネント、動的フィルターを備えた SQL エディタ。
* iframe を介したレポートやダッシュボードの埋め込み分析。
* データ準備および ETL 機能。
* データのリレーショナルマッピングに対応した SQL データモデリングのサポート。

<div id="looker">
  ### Looker
</div>

[Looker](https://looker.com) は、ClickHouse を含む 50 種類以上のデータベース dialect をサポートするデータプラットフォーム兼ビジネスインテリジェンスツールです。Looker は、SaaS プラットフォームとしてもセルフホストでも利用できます。ユーザーはブラウザから Looker を使用して、データを探索し、可視化やダッシュボードを作成し、レポートをスケジュールし、得られたインサイトを同僚と共有できます。Looker は、これらの機能を他のアプリケーションに埋め込むための豊富なツール群と、API
を備えており、他のアプリケーションとのデータ連携も可能です。

機能:

* LookML を使用した容易で俊敏な開発。LookML は、レポート作成者やエンドユーザーを支援するために整理された
  [データモデリング](https://looker.com/platform/data-modeling) をサポートする言語です。
* Looker の [Data Actions](https://looker.com/platform/actions) による強力なワークフロー連携。

[Looker で ClickHouse を設定する方法](https://docs.looker.com/setup-and-management/database-config/clickhouse)

<div id="seektable">
  ### SeekTable
</div>

[SeekTable](https://www.seektable.com) は、データ探索と運用レポートのためのセルフサービスBIツールです。Cloud サービス版とセルフホスト版の両方が提供されています。SeekTable のレポートは、任意の Web アプリに埋め込むことができます。

機能:

* ビジネスユーザーでも使いやすいレポートビルダー。
* SQL によるフィルタリングや、レポート固有のクエリカスタマイズに対応した強力なレポートパラメータ。
* ネイティブ TCP/IP エンドポイントと HTTP(S) インターフェイスの両方を使用して ClickHouse に接続できます (2 種類のドライバー) 。
* 次元やメジャーの定義では、ClickHouse SQL方言の機能を最大限に活用できます。
* レポート生成を自動化するための [Web API](https://www.seektable.com/help/web-api-integration)。
* アカウントデータの [バックアップ/復元](https://www.seektable.com/help/self-hosted-backup-restore) を含むレポート開発フローをサポートします。データモデル (キューブ) /レポートの設定は人が読める XML で、バージョン管理システムで管理できます。

SeekTable は、個人利用であれば [無料](https://www.seektable.com/help/cloud-pricing) です。

[SeekTable で ClickHouse 接続を設定する方法。](https://www.seektable.com/help/clickhouse-pivot-table)

<div id="chadmin">
  ### Chadmin
</div>

[Chadmin](https://github.com/bun4uk/chadmin) は、ClickHouseクラスターで現在実行中のクエリやその詳細を可視化し、必要に応じて終了できるシンプルなUIです。

<div id="tablum_io">
  ### TABLUM.IO
</div>

[TABLUM.IO](https://tablum.io/) — ETL と可視化のためのオンラインのクエリ／analyticsツールです。ClickHouse への接続、柔軟な SQL コンソールを使ったデータのクエリ、静的ファイルや 3rd party サービスからのデータの読み込みが可能です。TABLUM.IO は、データ結果をチャートやテーブルとして可視化できます。

機能:

* ETL: 一般的なデータベース、ローカルおよびリモートのファイル、API 呼び出しからのデータの読み込み。
* 構文ハイライトと視覚的なクエリビルダーを備えた柔軟な SQL コンソール。
* チャートやテーブルによるデータの可視化。
* データのマテリアライズとサブクエリ。
* Slack、Telegram、またはメールへのデータレポート。
* 独自 API によるデータパイプライン処理。
* JSON、CSV、SQL、HTML フォーマットでのデータエクスポート。
* Web ベースのインターフェイス。

TABLUM.IO は、セルフホストのソリューション (Docker イメージとして) またはクラウドで実行できます。
ライセンス: 3 か月の無料期間がある[商用](https://tablum.io/pricing)製品です。

[クラウド](https://tablum.io/try)で無料でお試しいただけます。
製品の詳細は [TABLUM.IO](https://tablum.io/) をご覧ください。

<div id="ckman">
  ### CKMAN
</div>

[CKMAN](https://www.github.com/housepower/ckman) は、ClickHouse クラスターの管理と監視を行うためのツールです。

機能:

* ブラウザーのインターフェイスから、クラスターを迅速かつ簡単に自動デプロイ
* クラスターのスケールアップおよびスケールダウンが可能
* クラスター内のデータを負荷分散
* クラスターをオンラインでアップグレード
* ページ上でクラスターの設定を変更
* クラスターのノード監視と ZooKeeper の監視を提供
* テーブルとパーティションの状態、および低速な SQL ステートメントを監視
* 使いやすい SQL 実行ページを提供

<div id="1bench">
  ### 1bench
</div>

[1bench](https://1bench.dev) は、複数のデータベースに対応し、ClickHouse を手厚くサポートするネイティブのデスクトップ GUI です。サーバーの概要表示、スキーマ管理、vector search、大規模な result-set の閲覧まで幅広くカバーしています。

機能:

* 接続時にサーバーの概要を表示 — バージョン、uptime、実行中のクエリ、進行中の merges、パーツ数とストレージサイズ、レプリカの status、クラスターとノードをひと目で確認できます。
* ビジュアルなクエリビルダー (カラムピッカー、filters、並び替え、Limit) に加え、構文ハイライト対応の Monaco SQL エディタと、connection ごとのクエリ履歴を備えています。
* `MergeTree` の各種バリアント、`ORDER BY`、`PARTITION BY`、`SETTINGS`、`Nullable()` の自動ラップに対応したビジュアルな `CREATE TABLE` ウィザード。
* ClickHouse のネイティブ型をサポート — `Nullable`、`Array`、`LowCardinality`、ネストされた object。
* vector search をサポート — `Array(Float32)` の埋め込みカラムを compact な vector cell として表示し、2D の埋め込み visualization や `cosineDistance` を使った類似検索に対応します。
* result table での Inline データ editing に対応し、Batch 保存に加えて、ClickHouse の native フォーマットを使った CSV/JSON/SQL のエクスポートとインポートが可能です。
* connection オプション: HTTP/HTTPS、ファイアウォールの内側にある Private クラスター向けの SSH トンネル、安全に production 環境を閲覧するためのオプションの read-only モード。
* ClickHouse Cloud とセルフホストの両方で利用できます。