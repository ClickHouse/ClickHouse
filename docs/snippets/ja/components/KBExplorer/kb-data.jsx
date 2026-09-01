export const kbIndex = {
  categories: [
    "Cloud",
    "設定",
    "データのインポートとエクスポート",
    "データ管理",
    "一般・よくある質問",
    "インテグレーションとクライアントライブラリ",
    "Materialized viewとプロジェクション",
    "監視とデバッグ",
    "パフォーマンスと最適化",
    "クエリとSQL",
    "セキュリティとアクセス制御",
    "セットアップとインストール",
    "テーブルとスキーマ",
    "トラブルシューティングとエラー"
  ],
  tags: [
    "ベストプラクティス",
    "コミュニティ",
    "コンセプト",
    "データの基本概念",
    "データエクスポート",
    "データフォーマット",
    "データ取り込み",
    "データモデリング",
    "データソース",
    "デプロイとスケーリング",
    "エラーと例外",
    "関数",
    "言語クライアント",
    "Cloudの管理",
    "データの管理",
    "ネイティブクライアントとインターフェース",
    "パフォーマンスと最適化",
    "セキュリティと認証",
    "サーバー管理",
    "設定",
    "システムテーブル",
    "ツールとユーティリティ",
    "トラブルシューティング",
    "ユースケース"
  ],
  articles: [
    {
      id: "integrations/python-clickhouse-connect-example",
      title: "ClickHouse Cloud Serviceへの接続：Pythonクライアントの実例",
      description: "clickhouse-connectドライバーを使ったステップバイステップの例を通じて、PythonでClickHouse Cloud Serviceに接続する方法を学びます。",
      href: "/ja/resources/support-center/knowledge-base/integrations/python-clickhouse-connect-example",
      category: "Integrations & client libraries",
      tags: ["Language Clients"]
    },
    {
      id: "configuration-settings/about-quotas-and-query-complexity",
      title: "クォータとクエリの複雑さについて",
      description:
        "クォータとクエリの複雑さは、ClickHouseでユーザーの操作を制限する強力な手段です。このナレッジベース記事では、これら2つのアプローチの適用例を紹介します。",
      href: "/ja/resources/support-center/knowledge-base/configuration-settings/about-quotas-and-query-complexity",
      category: "Configuration & settings",
      tags: ["Managing Cloud"]
    },
    {
      id: "data-import-export/achieving-atomic-inserts",
      title: "ClickHouse Cloudにおけるアトミックな挿入と複数テーブルの整合性の実現",
      description: "ステージングテーブルとパーティションレベルの操作を使用して、複数ステートメントのトランザクションなしにClickHouse Cloudでデータをアトミックに挿入し、複数テーブルの整合性を維持する方法。",
      href: "/ja/resources/support-center/knowledge-base/data-import-export/achieving-atomic-inserts",
      category: "Data import & export",
      tags: ["Data Ingestion", "Best Practices"]
    },
    {
      id: "tables-schema/add-column",
      title: "テーブルへのカラムの追加",
      description: "このガイドでは、既存のテーブルにカラムを追加する方法を説明します。",
      href: "/ja/resources/support-center/knowledge-base/tables-schema/add-column",
      category: "Tables & schema",
      tags: ["Data Modelling"]
    },
    {
      id: "configuration-settings/alter-user-settings-exception",
      title: "ユーザー設定変更時の例外",
      description: "ユーザー設定の変更時にスローされる例外の処理方法",
      href: "/ja/resources/support-center/knowledge-base/configuration-settings/alter-user-settings-exception",
      category: "Configuration & settings",
      tags: ["Settings", "Errors and Exceptions"]
    },
    {
      id: "materialized-views/are-materialized-views-inserted-asynchronously",
      title: "Materialized Viewは同期的に挿入されますか？",
      description: "このナレッジベース記事では、Materialized Viewへの挿入が同期的に行われるかどうかを検証します。",
      href: "/ja/resources/support-center/knowledge-base/materialized-views/are-materialized-views-inserted-asynchronously",
      category: "Materialized views & projections",
      tags: ["Data Modelling"]
    },
    {
      id: "tables-schema/schema-migration-tools",
      title: "ClickHouseの自動スキーママイグレーションツール",
      description: "ClickHouseの自動スキーママイグレーションツールと、データベーススキーマの変更を継続的に管理する方法について学びます。",
      href: "/ja/resources/support-center/knowledge-base/tables-schema/schema-migration-tools",
      category: "Tables & schema",
      tags: ["Tools and Utilities"]
    },
    {
      id: "cloud-services/aws-privatelink-setup-for-msk-clickpipes",
      title: "ClickPipes向けにMSKを公開するためのAWS PrivateLinkのセットアップ",
      description: "MSKのマルチVPC接続を介してプライベートMSKをClickPipesに公開するためのセットアップ手順。",
      href: "/ja/resources/support-center/knowledge-base/cloud-services/aws-privatelink-setup-for-msk-clickpipes",
      category: "Cloud",
      tags: ["Security and Authentication", "Managing Cloud"]
    },
    {
      id: "cloud-services/aws-privatelink-setup-for-clickpipes",
      title: "ClickPipes向けにプライベートRDSを公開するためのAWS PrivateLinkのセットアップ",
      description: "AWS PrivateLinkを介してプライベートRDSをClickPipesに公開するためのセットアップ手順。",
      href: "/ja/resources/support-center/knowledge-base/cloud-services/aws-privatelink-setup-for-clickpipes",
      category: "Cloud",
      tags: ["Security and Authentication", "Managing Cloud"]
    },
    {
      id: "data-management/backing-up-a-specific-partition",
      title: "特定のパーティションのバックアップ",
      description: "ClickHouseで特定のパーティションをバックアップする方法。",
      href: "/ja/resources/support-center/knowledge-base/data-management/backing-up-a-specific-partition",
      category: "Data management",
      tags: ["Managing Data"]
    },
    {
      id: "general-faqs/key-value",
      title: "ClickHouseをキーバリューストレージとして使用できますか？",
      description: "ClickHouseをキーバリューストレージとして使用できるかどうかというよくある質問への回答。",
      href: "/ja/resources/support-center/knowledge-base/general-faqs/key-value",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/time-series",
      title: "ClickHouseを時系列データベースとして使用できますか？",
      description: "ClickHouseを時系列データベースとして使用する方法を解説するページ。",
      href: "/ja/resources/support-center/knowledge-base/general-faqs/time-series",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "queries-sql/pivot",
      title: "ClickHouseでPIVOTは使用できますか？",
      description:
        "ClickHouseにはPIVOT句はありませんが、集計関数コンビネーターを使用して同様の機能を実現できます。英国の住宅価格データセットを使ってその方法を見てみましょう。",
      href: "/ja/resources/support-center/knowledge-base/queries-sql/pivot",
      category: "Queries & SQL",
      tags: ["Data Modelling", "Core Data Concepts"]
    },
    {
      id: "general-faqs/vector-search",
      title: "ClickHouseをベクトル検索に使用できますか？",
      description: "埋め込みの保存やコサイン類似度などの距離関数を使った検索など、ClickHouseをベクトル検索に活用する方法を学びます。",
      href: "/ja/resources/support-center/knowledge-base/general-faqs/vector-search",
      category: "General & FAQs",
      tags: ["Use Cases", "Concepts"]
    },
    {
      id: "monitoring-debugging/send-logs-level",
      title: "クライアントでのクエリのサーバーログのキャプチャ",
      description: "`send_logs_level`クライアント設定を使用して、ログ設定が異なる場合でもクライアントレベルでサーバーログをキャプチャする方法を学びます。",
      href: "/ja/resources/support-center/knowledge-base/monitoring-debugging/send-logs-level",
      category: "Monitoring & debugging",
      tags: ["Server Admin"]
    },
    {
      id: "configuration-settings/change-the-prompt-in-clickhouse-client",
      title: "clickhouse-clientのプロンプトの変更",
      description: "この記事では、ClickHouseクライアントおよびclickhouse-localのターミナルウィンドウのプロンプトを:)からプレフィックス付きの:)に変更する方法を説明します。",
      href: "/ja/resources/support-center/knowledge-base/configuration-settings/change-the-prompt-in-clickhouse-client",
      category: "Configuration & settings",
      tags: ["Settings", "Native Clients and Interfaces"]
    },
    {
      id: "security/common-rbac-queries",
      title: "よく使われるRBACクエリ",
      description: "ユーザーに特定の権限を付与するためのクエリ集。",
      href: "/ja/resources/support-center/knowledge-base/security/common-rbac-queries",
      category: "Security & access control",
      tags: ["Security and Authentication", "Managing Cloud"]
    },
    {
      id: "queries-sql/comparing-metrics-between-queries",
      title: "デシベルによるクエリ間のメトリクスの比較",
      description: "ClickHouseで2つのクエリ間のメトリクスを比較するクエリ。",
      href: "/ja/resources/support-center/knowledge-base/queries-sql/comparing-metrics-between-queries",
      category: "Queries & SQL",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "configuration-settings/configure-cap-ipc-lock-and-cap-sys-nice-in-docker",
      title: "Configuring CAP_IPC_LOCK and CAP_SYS_NICE Capabilities in Docker",
      description: "Learn how to resolve Docker capability warnings for `CAP_IPC_LOCK` and `CAP_SYS_NICE` when running ClickHouse in a container.",
      href: "/ja/resources/support-center/knowledge-base/configuration-settings/configure-cap-ipc-lock-and-cap-sys-nice-in-docker",
      category: "Configuration & settings",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "troubleshooting/configure-cap-ipc-lock-and-cap-sys-nice-in-docker",
      title: "Configuring CAP_IPC_LOCK and CAP_SYS_NICE Capabilities in Docker",
      description: "Learn how to resolve Docker capability warnings for `CAP_IPC_LOCK` and `CAP_SYS_NICE` when running ClickHouse in a container.",
      href: "/ja/resources/support-center/knowledge-base/troubleshooting/configure-cap-ipc-lock-and-cap-sys-nice-in-docker",
      category: "Troubleshooting & errors",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "cloud-services/custom-dns-alias-for-instance",
      title: "Create a custom DNS alias by setting up a reverse proxy",
      description: "Learn how to set up a custom DNS alias for your instance using a reverse proxy",
      href: "/ja/resources/support-center/knowledge-base/cloud-services/custom-dns-alias-for-instance",
      category: "Cloud",
      tags: ["Server Admin", "Security and Authentication"]
    },
    {
      id: "troubleshooting/part-intersects-previous-part",
      title: "DB::Exception: Part XXXXX intersects previous part YYYYY. It is a bug or a result of manual intervention in the ZooKeeper data.",
      description:
        "この記事では、ClickHouseにおけるパーツの重複に関するDB::Exceptionエラーの解決方法を説明します。このエラーは、競合状態またはZooKeeperデータへの手動介入が原因で発生することがあります。",
      href: "/ja/resources/support-center/knowledge-base/troubleshooting/part-intersects-previous-part",
      category: "トラブルシューティングとエラー",
      tags: ["エラーと例外", "システムテーブル"]
    },
    {
      id: "setup-installation/difference-between-official-builds-and-3rd-party",
      title: "Differences Between Official and 3rd-Party ClickHouse Builds",
      description: "Understand the key differences between official ClickHouse builds and 3rd-party builds, including updates, compatibility, and security considerations.",
      href: "/ja/resources/support-center/knowledge-base/setup-installation/difference-between-official-builds-and-3rd-party",
      category: "Setup & installation",
      tags: ["Concepts"]
    },
    {
      id: "general-faqs/cost-based",
      title: "Does ClickHouse have a cost-based optimizer",
      description: "ClickHouse has certain cost-based optimization mechanics",
      href: "/ja/resources/support-center/knowledge-base/general-faqs/cost-based",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/datalake",
      title: "Does ClickHouse support data lakes?",
      description: "ClickHouse supports data lakes, including Iceberg, Delta Lake, Apache Hudi, Apache Paimon, Hive",
      href: "/ja/resources/support-center/knowledge-base/general-faqs/datalake",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/distributed-join",
      title: "Does ClickHouse support distributed JOIN?",
      description: "ClickHouse supports distributed JOIN",
      href: "/ja/resources/support-center/knowledge-base/general-faqs/distributed-join",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/federated",
      title: "Does ClickHouse support federated queries?",
      description: "ClickHouse supports a wide range for federated and hybrid queries",
      href: "/ja/resources/support-center/knowledge-base/general-faqs/federated",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/concurrency",
      title: "Does ClickHouse support frequent, concurrent queries?",
      description: "ClickHouse supports high QPS and high concurrency",
      href: "/ja/resources/support-center/knowledge-base/general-faqs/concurrency",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "cloud-services/multi-region-replication",
      title: "Does ClickHouse support multi-region replication?",
      description: "This page answers whether ClickHouse supports multi-region replication",
      href: "/ja/resources/support-center/knowledge-base/cloud-services/multi-region-replication",
      category: "Cloud",
      tags: []
    },
    {
      id: "general-faqs/updates",
      title: "Does ClickHouse support real-time updates?",
      description: "ClickHouse supports lightweight real-time updates",
      href: "/ja/resources/support-center/knowledge-base/general-faqs/updates",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "security/row-column-policy",
      title: "Does ClickHouse support row-level and column-level security?",
      description: "Learn about row-level and column-level access restrictions in ClickHouse and ClickHouse Cloud, and how to implement role-based access control (RBAC) with policies.",
      href: "/ja/resources/support-center/knowledge-base/security/row-column-policy",
      category: "Security & access control",
      tags: ["Security and Authentication"]
    },
    {
      id: "cloud-services/execute-system-queries-in-cloud",
      title: "Execute SYSTEM Statements on All Nodes in ClickHouse Cloud",
      description: "Learn how to use `ON CLUSTER` and `clusterAllReplicas` to execute SYSTEM statements and queries across all nodes in a ClickHouse Cloud service.",
      href: "/ja/resources/support-center/knowledge-base/cloud-services/execute-system-queries-in-cloud",
      category: "Cloud",
      tags: ["Deployments and Scaling"]
    },
    {
      id: "troubleshooting/count-parts-by-type",
      title: "Find counts and sizes of wide or compact parts",
      description: "This knowledgebase article shows you how to find part counts by the type of part - wide or compact.",
      href: "/ja/resources/support-center/knowledge-base/troubleshooting/count-parts-by-type",
      category: "Troubleshooting & errors",
      tags: ["Troubleshooting"]
    },
    {
      id: "troubleshooting/fix-developer-verification-error-in-macos",
      title: "Fix the Developer Verification Error in MacOS",
      description: "Learn how to resolve the MacOS developer verification error when running ClickHouse commands, using either System Settings or the terminal.",
      href: "/ja/resources/support-center/knowledge-base/troubleshooting/fix-developer-verification-error-in-macos",
      category: "Troubleshooting & errors",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "data-import-export/s3-export-data-year-month-folders",
      title: "How can I do partitioned writes by year and month on S3?",
      description: "Learn how to write partitioned data by year and month to an S3 bucket in ClickHouse, using a custom path structure for organizing the data.",
      href: "/ja/resources/support-center/knowledge-base/data-import-export/s3-export-data-year-month-folders",
      category: "Data import & export",
      tags: ["Data Export", "Native Clients and Interfaces"]
    },
    {
      id: "data-import-export/kafka-clickhouse-json",
      title: "How can I use the new JSON Data Type with Kafka?",
      description: "Learn how to load JSON messages from Apache Kafka directly into a single JSON column in ClickHouse using the Kafka table engine and JSON data type.",
      href: "/ja/resources/support-center/knowledge-base/data-import-export/kafka-clickhouse-json",
      category: "Data import & export",
      tags: ["Data Formats", "Data Ingestion"]
    },
    {
      id: "cloud-services/change-billing-email",
      title: "How do I change my Billing Contact in ClickHouse Cloud?",
      description: "Let's learn how to change your billing address in ClickHouse Cloud.",
      href: "/ja/resources/support-center/knowledge-base/cloud-services/change-billing-email",
      category: "Cloud",
      tags: ["Managing Cloud"]
    },
    {
      id: "general-faqs/how-do-i-contribute-code-to-clickhouse",
      title: "How do I contribute code to ClickHouse?",
      description: "ClickHouseはGitHub上で開発されているオープンソースプロジェクトです。慣例に従い、コントリビューション手順はソースコードリポジトリのルートにあるCONTRIBUTINGファイルに記載されています。",
      href: "/ja/resources/support-center/knowledge-base/general-faqs/how-do-i-contribute-code-to-clickhouse",
      category: "一般・よくある質問",
      tags: ["コミュニティ"]
    },
    {
      id: "data-import-export/parquet-to-csv-json",
      title: "ParquetファイルをCSVまたはJSONに変換するには？",
      description: "ClickHouseの`clickhouse-local`ツールを使用して、ParquetファイルをCSVまたはJSON形式に変換する方法を説明します。",
      href: "/ja/resources/support-center/knowledge-base/data-import-export/parquet-to-csv-json",
      category: "データのインポートとエクスポート",
      tags: ["データソース", "データフォーマット"]
    },
    {
      id: "data-import-export/mysql-to-parquet-csv-json",
      title: "ClickHouseを使用してMySQLデータをParquet、CSV、またはJSONにエクスポートするには？",
      description: "`clickhouse-local`ツールを使用して、MySQLデータをParquet、CSV、またはJSON形式に素早くエクスポートする方法を説明します。",
      href: "/ja/resources/support-center/knowledge-base/data-import-export/mysql-to-parquet-csv-json",
      category: "データのインポートとエクスポート",
      tags: ["データフォーマット", "データエクスポート"]
    },
    {
      id: "data-import-export/postgresql-to-parquet-csv-json",
      title: "PostgreSQLデータをParquet、CSV、またはJSONにエクスポートするには？",
      description: "`clickhouse-local`を使用してPostgreSQLデータをParquet、CSV、またはJSON形式にエクスポートする方法を、豊富な例を交えて説明します。",
      href: "/ja/resources/support-center/knowledge-base/data-import-export/postgresql-to-parquet-csv-json",
      category: "データのインポートとエクスポート",
      tags: ["データエクスポート", "データフォーマット"]
    },
    {
      id: "setup-installation/install-clickhouse-windows10",
      title: "Windows 10にClickHouseをインストールするには？",
      description: "WSL 2を使用してWindows 10にClickHouseをインストールし、動作確認する方法を説明します。セットアップ、トラブルシューティング、テスト環境の構築方法も含みます。",
      href: "/ja/resources/support-center/knowledge-base/setup-installation/install-clickhouse-windows10",
      category: "セットアップとインストール",
      tags: ["ツールとユーティリティ"]
    },
    {
      id: "security/remove-default-user",
      title: "デフォルトユーザーを削除するには？",
      description: "ClickHouse Serverの実行時にデフォルトユーザーを削除する方法を説明します。",
      href: "/ja/resources/support-center/knowledge-base/security/remove-default-user",
      category: "セキュリティとアクセス制御",
      tags: ["サーバー管理"]
    },
    {
      id: "cloud-services/ingest-failures-23-9-release",
      title: "ClickHouse 23.9リリース後の取り込みエラーを解決するには？",
      description: "`async_inserts`を使用するテーブルに対してClickHouse 23.9で導入された厳格なグラント確認によって発生する取り込みエラーの解決方法を説明します。グラントを更新してエラーを修正する手順も含みます。",
      href: "/ja/resources/support-center/knowledge-base/cloud-services/ingest-failures-23-9-release",
      category: "Cloud",
      tags: ["エラーと例外"]
    },
    {
      id: "performance-optimization/insert-select-settings-tuning",
      title: "INSERT...SELECT実行中のTOO MANY PARTSエラーを解決するには？",
      description: "`INSERT...SELECT`実行中にClickHouseで発生するTOO_MANY_PARTSエラーを、大きなブロック向けの上級設定のチューニングとパーティションしきい値の引き上げによって解決する方法を説明します。",
      href: "/ja/resources/support-center/knowledge-base/performance-optimization/insert-select-settings-tuning",
      category: "パフォーマンスと最適化",
      tags: ["設定", "エラーと例外"]
    },
    {
      id: "integrations/node-js-example",
      title: "NodeJSで@clickhouse/clientを使用するには？",
      description: "Node.jsアプリケーションで@clickhouse/clientを使用してClickHouseと連携し、クエリを実行する方法を説明します。",
      href: "/ja/resources/support-center/knowledge-base/integrations/node-js-example",
      category: "インテグレーションとクライアントライブラリ",
      tags: ["言語クライアント"]
    },
    {
      id: "monitoring-debugging/view-number-of-active-mutations",
      title: "アクティブまたはキュー待ちのミューテーション数を確認するには？",
      description:
        "特に`ALTER`や`UPDATE`操作を実行する際に、ClickHouseでアクティブまたはキュー待ちのミューテーション数を監視する方法を説明します。ミューテーションの追跡には`system.mutations`テーブルを使用します。",
      href: "/ja/resources/support-center/knowledge-base/monitoring-debugging/view-number-of-active-mutations",
      category: "監視とデバッグ",
      tags: ["システムテーブル"]
    },
    {
      id: "data-management/read-consistency",
      title: "ClickHouseでデータ読み取りの一貫性を確保するには？",
      description: "同一ノードまたは任意のノードに接続している場合でも、ClickHouseからの読み取り時にデータの一貫性を確保する方法を説明します。",
      href: "/ja/resources/support-center/knowledge-base/data-management/read-consistency",
      category: "データ管理",
      tags: ["パフォーマンスと最適化"]
    },
    {
      id: "setup-installation/llvm-clang-up-to-date",
      title: "LinuxでLLVMとclangをビルドするには",
      description: "LinuxでLLVMとclangをビルドするためのコマンドです。",
      href: "/ja/resources/support-center/knowledge-base/setup-installation/llvm-clang-up-to-date",
      category: "セットアップとインストール",
      tags: ["コミュニティ", "ツールとユーティリティ"]
    },
    {
      id: "data-management/calculate-ratio-of-zero-sparse-serialization",
      title: "テーブルの各カラムにおける空/ゼロ値の比率を計算するには",
      description: "スパースカラムのシリアライゼーションを最適化するために、ClickHouseテーブルの各カラムにおける空またはゼロ値の比率を計算する方法を説明します。",
      href: "/ja/resources/support-center/knowledge-base/data-management/calculate-ratio-of-zero-sparse-serialization",
      category: "データ管理",
      tags: ["パフォーマンスと最適化"]
    },
    {
      id: "security/check-users-roles",
      title: "ロールに割り当てられたユーザーとその逆を確認するには",
      description: "ClickHouseの`system.role_grants`をクエリして、ロールに割り当てられたユーザーや特定のユーザーに割り当てられたロールを確認する方法を説明します。",
      href: "/ja/resources/support-center/knowledge-base/security/check-users-roles",
      category: "セキュリティとアクセス制御",
      tags: ["サーバー管理", "システムテーブル", "Cloud管理"]
    },
    {
      id: "monitoring-debugging/which-processes-are-currently-running",
      title: "サーバーで現在実行中のコードを確認するには？",
      description:
        "ClickHouseは`system.stack_trace`などのイントロスペクションツールを提供しており、各サーバースレッドで現在実行中のコードを検査できます。デバッグやパフォーマンス監視に役立ちます。",
      href: "/ja/resources/support-center/knowledge-base/monitoring-debugging/which-processes-are-currently-running",
      category: "監視とデバッグ",
      tags: ["サーバー管理"]
    },
    {
      id: "cloud-services/how-to-check-my-clickhouse-cloud-sevice-state",
      title: "ClickHouse Cloudサービスの状態を確認するには",
      description: "ClickHouse Cloud APIを使用して、サービスを起動させることなく停止中、アイドル中、または実行中かどうかを確認する方法を説明します。",
      href: "/ja/resources/support-center/knowledge-base/cloud-services/how-to-check-my-clickhouse-cloud-sevice-state",
      category: "Cloud",
      tags: ["Cloud管理"]
    },
    {
      id: "configuration-settings/configure-a-user-setting",
      title: "ClickHouseでユーザーの設定を構成するには",
      description: "`SET`および`ALTER USER`コマンドを使用して、個別のクエリ、クライアントセッション、または特定のユーザー向けにClickHouseの設定を定義する方法を説明します。",
      href: "/ja/resources/support-center/knowledge-base/configuration-settings/configure-a-user-setting",
      category: "構成と設定",
      tags: ["設定"]
    },
    {
      id: "materialized-views/projection-example",
      title: "クエリでPROJECTIONが使用されているかを確認するには？",
      description: "サンプルデータを使ったテストとEXPLAINを使用して、ClickHouseクエリでPROJECTIONが使用されているかどうかを確認する方法を説明します。",
      href: "/ja/resources/support-center/knowledge-base/materialized-views/projection-example",
      category: "materialized viewとPROJECTION",
      tags: ["データモデリング"]
    },
    {
      id: "cloud-services/how-to-connect-to-ch-cloud-using-ssh-keys",
      title: "SSHキーを使用してClickHouseに接続するには",
      description: "SSHキーを使用してClickHouseおよびClickHouse Cloudに接続する方法",
      href: "/ja/resources/support-center/knowledge-base/cloud-services/how-to-connect-to-ch-cloud-using-ssh-keys",
      category: "Cloud",
      tags: ["Managing Cloud", "Security and Authentication"]
    },
    {
      id: "data-management/dictionary-using-strings",
      title: "How to Create a ClickHouse Dictionary with String Keys and Values",
      description: "Learn how to create a ClickHouse dictionary using string keys and values from a MergeTree table as the source, with examples of setup and usage.",
      href: "/ja/resources/support-center/knowledge-base/data-management/dictionary-using-strings",
      category: "Data management",
      tags: ["Data Modelling"]
    },
    {
      id: "tables-schema/how-to-create-table-to-query-multiple-remote-clusters",
      title: "How to create a table that can query multiple remote clusters",
      description: "How to create a table that can query multiple remote clusters",
      href: "/ja/resources/support-center/knowledge-base/tables-schema/how-to-create-table-to-query-multiple-remote-clusters",
      category: "Tables & schema",
      tags: ["Deployments and Scaling"]
    },
    {
      id: "setup-installation/enabling-ssl-with-lets-encrypt",
      title: "How to Enable SSL with Let's Encrypt on a Single ClickHouse Server",
      description: "Learn how to set up SSL for a single ClickHouse server using Let's Encrypt, including certificate issuance, configuration, and validation.",
      href: "/ja/resources/support-center/knowledge-base/setup-installation/enabling-ssl-with-lets-encrypt",
      category: "Setup & installation",
      tags: ["Security and Authentication"]
    },
    {
      id: "data-import-export/file-export",
      title: "How to Export Data from ClickHouse to a File",
      description: "Learn various methods to export data from ClickHouse, including `INTO OUTFILE`, the File table engine, and command-line redirection.",
      href: "/ja/resources/support-center/knowledge-base/data-import-export/file-export",
      category: "Data import & export",
      tags: ["Data Export"]
    },
    {
      id: "queries-sql/how-to-filter-a-clickhouse-table-by-an-array-column",
      title: "How to filter a ClickHouse table by an array-column?",
      description: "Knowledgebase article on how to filter a ClickHouse table by an array-column.",
      href: "/ja/resources/support-center/knowledge-base/queries-sql/how-to-filter-a-clickhouse-table-by-an-array-column",
      category: "Queries & SQL",
      tags: ["Data Modelling", "Functions"]
    },
    {
      id: "monitoring-debugging/generate-har-file",
      title: "How to Generate a HAR file for support",
      description: "A HAR (HTTP Archive) file captures the network activity in your browser. It can help our support team diagnose slow page loads, failed requests, or other network issues.",
      href: "/ja/resources/support-center/knowledge-base/monitoring-debugging/generate-har-file",
      category: "Monitoring & debugging",
      tags: ["Tools and Utilities"]
    },
    {
      id: "materialized-views/how-to-display-queries-using-mv",
      title: "How to Identify Queries Using Materialized Views in ClickHouse",
      description: "Learn how to query ClickHouse logs to identify all queries involving Materialized Views within a specified time range.",
      href: "/ja/resources/support-center/knowledge-base/materialized-views/how-to-display-queries-using-mv",
      category: "Materialized views & projections",
      tags: ["System Tables"]
    },
    {
      id: "performance-optimization/find-expensive-queries",
      title: "How to Identify the Most Expensive Queries in ClickHouse",
      description: "Learn how to use the `query_log` table in ClickHouse to identify the most memory and CPU-intensive queries across distributed nodes.",
      href: "/ja/resources/support-center/knowledge-base/performance-optimization/find-expensive-queries",
      category: "Performance & optimization",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "configuration-settings/ignoring-incorrect-settings",
      title: "How to Ignore Incorrect Settings in ClickHouse",
      description: "Learn how to use the `skip_check_for_incorrect_settings` option to allow ClickHouse to start even when user-level settings are specified incorrectly.",
      href: "/ja/resources/support-center/knowledge-base/configuration-settings/ignoring-incorrect-settings",
      category: "Configuration & settings",
      tags: ["Settings"]
    },
    {
      id: "data-import-export/json-import",
      title: "How to import JSON into ClickHouse?",
      description: "This page shows you how to import JSON into ClickHouse",
      href: "/ja/resources/support-center/knowledge-base/data-import-export/json-import",
      category: "Data import & export",
      tags: []
    },
    {
      id: "setup-installation/how-to-increase-thread-pool-size",
      title: "How to Increase the Number of Threads in ClickHouse",
      description: "Learn how to configure the Global Thread pool in ClickHouse by adjusting settings like `max_thread_pool_size`, `thread_pool_queue_size`, and `max_thread_pool_free_size`.",
      href: "/ja/resources/support-center/knowledge-base/setup-installation/how-to-increase-thread-pool-size",
      category: "Setup & installation",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "data-import-export/kafka-to-clickhouse-setup",
      title: "How to Ingest Data from Kafka into ClickHouse",
      description: "Learn how to ingest data from a Kafka topic into ClickHouse using the Kafka table engine, materialized views, and MergeTree tables.",
      href: "/ja/resources/support-center/knowledge-base/data-import-export/kafka-to-clickhouse-setup",
      category: "Data import & export",
      tags: ["Data Ingestion"]
    },
    {
      id: "data-import-export/ingest-parquet-files-in-s3",
      title: "How to ingest Parquet files from an S3 bucket",
      description: "Learn the basics of using the S3 table engine in ClickHouse to ingest and query Parquet files from an S3 bucket, including setup, access permissions, and data import examples.",
      href: "/ja/resources/support-center/knowledge-base/data-import-export/ingest-parquet-files-in-s3",
      category: "Data import & export",
      tags: ["Data Ingestion"]
    },
    {
      id: "queries-sql/how-to-insert-all-rows-from-another-table",
      title: "How to insert all rows from one table to another?",
      description: "Knowledgebase article on how to insert all rows from one table to another.",
      href: "/ja/resources/support-center/knowledge-base/queries-sql/how-to-insert-all-rows-from-another-table",
      category: "Queries & SQL",
      tags: ["Data Ingestion"]
    },
    {
      id: "performance-optimization/check-query-processing-time-only",
      title: "How to Measure Query Processing Time Without Returning Rows",
      description: "Learn how to use the `FORMAT Null` option in ClickHouse to measure query processing time without returning any rows to the client.",
      href: "/ja/resources/support-center/knowledge-base/performance-optimization/check-query-processing-time-only",
      category: "Performance & optimization",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "monitoring-debugging/outputSendLogsLevelTracesToFile",
      title: "How to output send logs level traces to file using the clickhouse-client",
      description: "How to output send logs level traces to file using the clickhouse-client",
      href: "/ja/resources/support-center/knowledge-base/monitoring-debugging/outputSendLogsLevelTracesToFile",
      category: "Monitoring & debugging",
      tags: ["Data Export"]
    },
    {
      id: "tables-schema/recreate-table-across-terminals",
      title: "How to quickly recreate a small table across different terminals",
      description: "Learn how to quickly recreate a small table and its data across different terminals using copy/paste for development environments.",
      href: "/ja/resources/support-center/knowledge-base/tables-schema/recreate-table-across-terminals",
      category: "Tables & schema",
      tags: ["Tools and Utilities"]
    },
    {
      id: "integrations/how-to-set-up-ch-on-docker-odbc-connect-mssql",
      title: "ODBCを使用してDocker上のClickHouseをMicrosoft SQL Server（MSSQL）データベースに接続する方法",
      description: "ODBCを使用してDocker上のClickHouseをMicrosoft SQL Server（MSSQL）データベースに接続する方法",
      href: "/ja/resources/support-center/knowledge-base/integrations/how-to-set-up-ch-on-docker-odbc-connect-mssql",
      category: "Integrations & client libraries",
      tags: ["Native Clients and Interfaces"]
    },
    {
      id: "queries-sql/using-array-join-to-extract-and-query-attributes",
      title: "How to use array join to extract and query varying attributes using map keys and values",
      description: "マップのキーと値を使用して、array joinで可変属性を抽出・クエリする方法を示す簡単な例",
      href: "/ja/resources/support-center/knowledge-base/queries-sql/using-array-join-to-extract-and-query-attributes",
      category: "クエリ & SQL",
      tags: ["Functions"]
    },
    {
      id: "materialized-views/how-to-use-parametrised-views",
      title: "ClickHouseでパラメーター化ビューを使用する方法",
      description: "クエリ時パラメーターに基づく動的なデータスライシングのために、ClickHouseでパラメーター化ビューを作成・クエリする方法を学びます。",
      href: "/ja/resources/support-center/knowledge-base/materialized-views/how-to-use-parametrised-views",
      category: "materialized view & プロジェクション",
      tags: ["ユースケース"]
    },
    {
      id: "tables-schema/exchangeStatementToSwitchTables",
      title: "exchangeコマンドを使用してテーブルを切り替える方法",
      description: "exchangeコマンドを使用してテーブルを切り替える方法",
      href: "/ja/resources/support-center/knowledge-base/tables-schema/exchangeStatementToSwitchTables",
      category: "テーブル & スキーマ",
      tags: ["データ管理"]
    },
    {
      id: "queries-sql/compare-resultsets",
      title: "2つのクエリが同じ結果セットを返すかを検証する方法",
      description: "ハッシュ関数と比較手法を使用して、2つのClickHouseクエリが同一の結果セットを生成することを検証する方法を学びます。",
      href: "/ja/resources/support-center/knowledge-base/queries-sql/compare-resultsets",
      category: "クエリ & SQL",
      tags: ["Functions"]
    },
    {
      id: "monitoring-debugging/check-query-cache-in-use",
      title: "ClickHouseでクエリキャッシュの使用状況を確認する方法",
      description: "`clickhouse-client`のトレースログまたはSQLコマンドを使用して、ClickHouseでクエリキャッシュが利用されているかどうかを確認する方法を学びます。",
      href: "/ja/resources/support-center/knowledge-base/monitoring-debugging/check-query-cache-in-use",
      category: "監視 & デバッグ",
      tags: ["パフォーマンスと最適化"]
    },
    {
      id: "cloud-services/unable-to-access-cloud-service",
      title: "ClickHouse Cloud サービスにアクセスできない",
      description: "IPアクセスリストの設定を含む、ClickHouse Cloud サービスへのアクセス問題のトラブルシューティング",
      href: "/ja/resources/support-center/knowledge-base/cloud-services/unable-to-access-cloud-service",
      category: "Cloud",
      tags: ["エラーと例外", "Cloud管理"]
    },
    {
      id: "performance-optimization/finding-expensive-queries-by-memory-usage",
      title: "ClickHouseでメモリ使用量によるコストの高いクエリを特定する",
      description: "`system.query_log`テーブルを使用して、クラスター構成およびスタンドアロン構成の例を交えながら、ClickHouseで最もメモリを消費するクエリを見つける方法を学びます。",
      href: "/ja/resources/support-center/knowledge-base/performance-optimization/finding-expensive-queries-by-memory-usage",
      category: "パフォーマンス & 最適化",
      tags: ["パフォーマンスと最適化"]
    },
    {
      id: "data-import-export/importing-and-working-with-json-array-objects",
      title: "ClickHouseでのJSON配列オブジェクトのインポートとクエリ",
      description: "JSON関数とArray操作を使用して、ClickHouseにJSON配列オブジェクトをインポートし、高度なクエリを実行する方法を学びます。",
      href: "/ja/resources/support-center/knowledge-base/data-import-export/importing-and-working-with-json-array-objects",
      category: "データのインポート & エクスポート",
      tags: ["データフォーマット"]
    },
    {
      id: "data-import-export/importing-geojason-with-nested-object-array",
      title: "深くネストされたオブジェクト配列を持つGeoJSONのインポート",
      description: "「深くネストされたオブジェクト配列を持つGeoJSONのインポート」",
      href: "/ja/resources/support-center/knowledge-base/data-import-export/importing-geojason-with-nested-object-array",
      category: "データのインポート & エクスポート",
      tags: ["データフォーマット"]
    },
    {
      id: "performance-optimization/improve-map-performance",
      title: "ClickHouseでのMapルックアップパフォーマンスの改善",
      description: "特定のキーを独立したカラムとしてマテリアライズすることで、ClickHouseのMapカラムルックアップを最適化してクエリパフォーマンスを向上させる方法を学びます。",
      href: "/ja/resources/support-center/knowledge-base/performance-optimization/improve-map-performance",
      category: "パフォーマンス & 最適化",
      tags: ["パフォーマンスと最適化"]
    },
    {
      id: "tables-schema/delete-old-data",
      title: "ClickHouseテーブルから古いレコードを削除することは可能ですか？",
      description: "このページでは、ClickHouseテーブルから古いレコードを削除できるかどうかという質問に回答します。",
      href: "/ja/resources/support-center/knowledge-base/tables-schema/delete-old-data",
      category: "テーブル & スキーマ",
      tags: []
    },
    {
      id: "general-faqs/separate-storage",
      title: "ClickHouseをストレージとコンピュートを分離してデプロイすることは可能ですか？",
      description: "このページでは、ClickHouseをストレージとコンピュートを分離してデプロイできるかどうかについて回答します。",
      href: "/ja/resources/support-center/knowledge-base/general-faqs/separate-storage",
      category: "一般 & よくある質問",
      tags: []
    },
    {
      id: "data-import-export/json-extract-example",
      title: "JSONの抽出例",
      description: "JSONから基本型を抽出する方法の簡単な例",
      href: "/ja/resources/support-center/knowledge-base/data-import-export/json-extract-example",
      category: "データのインポート & エクスポート",
      tags: ["データフォーマット"]
    },
    {
      id: "queries-sql/calculate-pi-using-sql",
      title: "SQLを使って円周率を計算しよう",
      description: "円周率の日！ClickHouse SQLを使って円周率を計算しましょう",
      href: "/ja/resources/support-center/knowledge-base/queries-sql/calculate-pi-using-sql",
      category: "クエリ & SQL",
      tags: ["ユースケース"]
    },
    {
      id: "cloud-services/clickhouse-cloud-api-usage",
      title: "APIとcURLを使用したClickHouse Cloud サービスの管理",
      description: "APIエンドポイントとcURLコマンドを使用して、ClickHouse Cloud サービスを起動、停止、再開する方法を学びます。",
      href: "/ja/resources/support-center/knowledge-base/cloud-services/clickhouse-cloud-api-usage",
      category: "Cloud",
      tags: ["Cloud管理", "ツールとユーティリティ"]
    },
    {
      id: "monitoring-debugging/mapping-of-system-metrics-to-prometheus-metrics",
      title: "system.dashboardsで使用されるメトリクスから`system.custom_metrics`のPrometheusメトリクスへのマッピング",
      description: "system.dashboardsで使用されるメトリクスからsystem.custom_metricsのPrometheusメトリクスへのマッピング",
      href: "/ja/resources/support-center/knowledge-base/monitoring-debugging/mapping-of-system-metrics-to-prometheus-metrics",
      category: "監視 & デバッグ",
      tags: ["システムテーブル"]
    },
    {
      id: "security/windows-active-directory-to-ch-roles",
      title: "Windows Active DirectoryセキュリティグループをClickHouseロールにマッピングする",
      description: "Windows Active DirectoryセキュリティグループをClickHouseロールにマッピングする例",
      href: "/ja/resources/support-center/knowledge-base/security/windows-active-directory-to-ch-roles",
      category: "セキュリティ & アクセス制御",
      tags: ["ツールとユーティリティ"]
    },
    {
      id: "performance-optimization/memory-limit-exceeded-for-query",
      title: "クエリのメモリ制限超過",
      description: "クエリのメモリ制限超過エラーのトラブルシューティング",
      href: "/ja/resources/support-center/knowledge-base/performance-optimization/memory-limit-exceeded-for-query",
      category: "パフォーマンス & 最適化",
      tags: ["エラーと例外"]
    },
    {
      id: "integrations/ODBC-authentication-failed-error-using-PowerBI-CH-connector",
      title: "Power BI ClickHouseコネクターを使用する際のODBC認証失敗エラー",
      description: "Power BI ClickHouseコネクターを使用する際のODBC認証失敗エラー",
      href: "/ja/resources/support-center/knowledge-base/integrations/ODBC-authentication-failed-error-using-PowerBI-CH-connector",
      category: "インテグレーション & クライアントライブラリ",
      tags: ["ネイティブクライアントとインターフェース", "エラーと例外"]
    },
    {
      id: "monitoring-debugging/profiling-clickhouse-with-llvm-xray",
      title: "LLVMのXRayを使用したClickHouseのプロファイリング",
      description: "LLVMのXRayインストルメンテーションプロファイラーを使用してClickHouseをプロファイリングし、トレースを可視化してパフォーマンスを分析する方法を学びます。",
      href: "/ja/resources/support-center/knowledge-base/monitoring-debugging/profiling-clickhouse-with-llvm-xray",
      category: "監視 & デバッグ",
      tags: ["パフォーマンスと最適化", "ツールとユーティリティ"]
    },
    {
      id: "integrations/python-http-requests",
      title: "Python quick example using HTTP requests module",
      description: "An example using Python and requests module to write and read to ClickHouse",
      href: "/ja/resources/support-center/knowledge-base/integrations/python-http-requests",
      category: "Integrations & client libraries",
      tags: ["Native Clients and Interfaces"]
    },
    {
      id: "configuration-settings/maximum-number-of-tables-and-databases",
      title: "Recommended Maximum Databases, Tables, Partitions, and Parts in ClickHouse",
      description: "Learn the recommended maximum limits for databases, tables, partitions, and parts in a ClickHouse cluster to ensure optimal performance.",
      href: "/ja/resources/support-center/knowledge-base/configuration-settings/maximum-number-of-tables-and-databases",
      category: "Configuration & settings",
      tags: ["Performance and Optimizations", "Deployments and Scaling"]
    },
    {
      id: "data-import-export/cannot-append-data-to-parquet-format",
      title: 'Resolving "Cannot Append Data in Parquet Format" Error in ClickHouse',
      description: 'Are you getting the error "Cannot append data in format Parquet to file" error in ClickHouse? Let\'s take a look at how to resolve it.',
      href: "/ja/resources/support-center/knowledge-base/data-import-export/cannot-append-data-to-parquet-format",
      category: "Data import & export",
      tags: ["Errors and Exceptions", "Data Formats"]
    },
    {
      id: "troubleshooting/exception-too-many-parts",
      title: 'Resolving "Too Many Parts" Error in ClickHouse',
      description: 'Learn how to address the "Too many parts" error in ClickHouse by optimizing insert rates, configuring MergeTree settings, and managing partitions effectively.',
      href: "/ja/resources/support-center/knowledge-base/troubleshooting/exception-too-many-parts",
      category: "Troubleshooting & errors",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "troubleshooting/certificate-verify-failed-error",
      title: "Resolving SSL Certificate Verify Error in ClickHouse",
      description: "Learn how to resolve the SSL Exception CERTIFICATE_VERIFY_FAILED error.",
      href: "/ja/resources/support-center/knowledge-base/troubleshooting/certificate-verify-failed-error",
      category: "トラブルシューティングとエラー",
      tags: ["セキュリティと認証", "エラーと例外"]
    },
    {
      id: "troubleshooting/connection-timeout-remote-remoteSecure",
      title: "`remote`および`remoteSecure`テーブル関数のタイムアウトエラーを解決する",
      description: "接続タイムアウト設定を調整して、ClickHouseの`remote`または`remoteSecure`テーブル関数使用時に発生するタイムアウトエラーを修正する方法を説明します。",
      href: "/ja/resources/support-center/knowledge-base/troubleshooting/connection-timeout-remote-remoteSecure",
      category: "Troubleshooting & errors",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "tables-schema/search-across-node-for-tables-with-a-wildcard",
      title: "Searching across nodes for tables with a wildcard",
      description: "Learn how to search across nodes for tables with a wildcard.",
      href: "/ja/resources/support-center/knowledge-base/tables-schema/search-across-node-for-tables-with-a-wildcard",
      category: "Tables & schema",
      tags: ["Deployments and Scaling"]
    },
    {
      id: "performance-optimization/query-max-execution-time",
      title: "Setting a limit on query execution time",
      description: "How to enforce limit on max query execution time",
      href: "/ja/resources/support-center/knowledge-base/performance-optimization/query-max-execution-time",
      category: "Performance & optimization",
      tags: ["Managing Cloud", "Settings"]
    },
    {
      id: "data-import-export/json-simple-example",
      title: "Simple example flow for extracting JSON data using a landing table with a Materialized View",
      description: "Simple example flow for extracting JSON data using a landing table with a Materialized View",
      href: "/ja/resources/support-center/knowledge-base/data-import-export/json-simple-example",
      category: "Data import & export",
      tags: ["Data Formats"]
    },
    {
      id: "performance-optimization/async-vs-optimize-read-in-order",
      title: "Synchronous data reading",
      description:
        "The new setting `allow_asynchronous_read_from_io_pool_for_merge_tree` allows the number of reading threads (streams) to be higher than the number of threads in the rest of the query execution pipeline.",
      href: "/ja/resources/support-center/knowledge-base/performance-optimization/async-vs-optimize-read-in-order",
      category: "Performance & optimization",
      tags: ["Settings", "Performance and Optimizations"]
    },
    {
      id: "integrations/terraform-example",
      title: "Terraform example on how to use Cloud API",
      description: "This covers an example of how you can use terraform to create/delete clusters using the API",
      href: "/ja/resources/support-center/knowledge-base/integrations/terraform-example",
      category: "Integrations & client libraries",
      tags: ["Native Clients and Interfaces"]
    },
    {
      id: "performance-optimization/tips-tricks-optimizing-basic-data-types-in-clickhouse",
      title: "Tips and tricks on optimizing basic data types in ClickHouse",
      description: "Tips and tricks on optimizing basic data types in ClickHouse",
      href: "/ja/resources/support-center/knowledge-base/performance-optimization/tips-tricks-optimizing-basic-data-types-in-clickhouse",
      category: "Performance & optimization",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "queries-sql/useful-queries-for-troubleshooting",
      title: "Useful queries for troubleshooting",
      description: "A collection of handy queries for troubleshooting ClickHouse, including monitoring table sizes, long-running queries, and errors.",
      href: "/ja/resources/support-center/knowledge-base/queries-sql/useful-queries-for-troubleshooting",
      category: "Queries & SQL",
      tags: ["Settings"]
    },
    {
      id: "general-faqs/use-clickhouse-for-log-analytics",
      title: "Using ClickHouse for Log Analytics",
      description: "ClickHouse is popular for logs and metrics analysis because of the real-time analytics capabilities provided. Ready to find out more?",
      href: "/ja/resources/support-center/knowledge-base/general-faqs/use-clickhouse-for-log-analytics",
      category: "General & FAQs",
      tags: ["Use Cases"]
    },
    {
      id: "queries-sql/filtered-aggregates",
      title: "Using Filtered Aggregates in ClickHouse",
      description: "Learn how to use filtered aggregates in ClickHouse with `-If` and `-Distinct` aggregate combinators to simplify query syntax and enhance analytics.",
      href: "/ja/resources/support-center/knowledge-base/queries-sql/filtered-aggregates",
      category: "Queries & SQL",
      tags: ["Functions"]
    },
    {
      id: "general-faqs/dependencies",
      title: "What are the 3rd-party dependencies for running ClickHouse?",
      description: "ClickHouse is self-contained and has no runtime dependencies",
      href: "/ja/resources/support-center/knowledge-base/general-faqs/dependencies",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/dbms-naming",
      title: 'What does "ClickHouse" mean?',
      description: 'Learn about What does "ClickHouse" mean?',
      href: "/ja/resources/support-center/knowledge-base/general-faqs/dbms-naming",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/ne-tormozit",
      title: "What does “не тормозит” mean?",
      description: 'This page explains what "Не тормозит" means',
      href: "/ja/resources/support-center/knowledge-base/general-faqs/ne-tormozit",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "integrations/oracle-odbc",
      title: "What if I have a problem with encodings when using Oracle via ODBC?",
      description: "This page provides guidance on what to do if you have a problem with encodings when using Oracle via ODBC",
      href: "/ja/resources/support-center/knowledge-base/integrations/oracle-odbc",
      category: "Integrations & client libraries",
      tags: []
    },
    {
      id: "general-faqs/columnar-database",
      title: "What is a columnar database?",
      description: "This page describes what a columnar database is",
      href: "/ja/resources/support-center/knowledge-base/general-faqs/columnar-database",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/olap",
      title: "What is OLAP?",
      description: "An explainer on what Online Analytical Processing is",
      href: "/ja/resources/support-center/knowledge-base/general-faqs/olap",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "performance-optimization/optimize-final-vs-final",
      title: "What is the difference between OPTIMIZE FINAL and FINAL?",
      description: "Discusses the differences between OPTIMIZE FINAL and FINAL, and when to use and avoid them.",
      href: "/ja/resources/support-center/knowledge-base/performance-optimization/optimize-final-vs-final",
      category: "Performance & optimization",
      tags: ["Core Data Concepts"]
    },
    {
      id: "general-faqs/sql",
      title: "What SQL syntax does ClickHouse support?",
      description: "ClickHouse supports 100% of SQL syntax",
      href: "/ja/resources/support-center/knowledge-base/general-faqs/sql",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "data-management/when-is-ttl-applied",
      title: "TTLルールはいつ適用されますか？また、適用タイミングを制御できますか？",
      description:
        "ClickHouseのTTLルールは非同期で適用されますが、`merge_with_ttl_timeout`設定を使用して実行タイミングを制御できます。TTLを強制適用する方法や、TTL実行に使用するバックグラウンドスレッドの管理方法について説明します。",
      href: "/ja/resources/support-center/knowledge-base/data-management/when-is-ttl-applied",
      category: "Data management",
      tags: ["Core Data Concepts"]
    },
    {
      id: "setup-installation/production",
      title: "Which ClickHouse version to use in production?",
      description: "This page provides guidance on which ClickHouse version to use in production",
      href: "/ja/resources/support-center/knowledge-base/setup-installation/production",
      category: "Setup & installation",
      tags: []
    },
    {
      id: "general-faqs/who-is-using-clickhouse",
      title: "Who is using ClickHouse?",
      description: "Describes who is using ClickHouse",
      href: "/ja/resources/support-center/knowledge-base/general-faqs/who-is-using-clickhouse",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "data-management/dictionaries-consistent-state",
      title: "Why can't I see my data in a dictionary in ClickHouse Cloud?",
      description: "There is an issue where data in dictionaries may not be visible immediately after creation.",
      href: "/ja/resources/support-center/knowledge-base/data-management/dictionaries-consistent-state",
      category: "Data management",
      tags: ["Managing Cloud", "Data Modelling"]
    },
    {
      id: "general-faqs/why-recommend-clickhouse-keeper-over-zookeeper",
      title: "Why is ClickHouse Keeper recommended over ZooKeeper?",
      description:
        "ClickHouse Keeper improves upon ZooKeeper with features like reduced disk space usage, faster recovery, and less memory consumption, offering better performance for ClickHouse clusters.",
      href: "/ja/resources/support-center/knowledge-base/general-faqs/why-recommend-clickhouse-keeper-over-zookeeper",
      category: "General & FAQs",
      tags: ["Core Data Concepts"]
    },
    {
      id: "monitoring-debugging/why-default-logging-verbose",
      title: "Why is ClickHouse logging so verbose by default?",
      description: "Learn why the ClickHouse developers chose to set a verbose logging level by default.",
      href: "/ja/resources/support-center/knowledge-base/monitoring-debugging/why-default-logging-verbose",
      category: "Monitoring & debugging",
      tags: ["Settings"]
    },
    {
      id: "performance-optimization/why-is-my-primary-key-not-used",
      title: "Why is my primary key not used? How can I check?",
      description: "Covers a common reason why a primary key is not used in ordering and how we can confirm",
      href: "/ja/resources/support-center/knowledge-base/performance-optimization/why-is-my-primary-key-not-used",
      category: "Performance & optimization",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "general-faqs/mapreduce",
      title: "Why not use something like MapReduce?",
      description: "This page explains why you would use ClickHouse over MapReduce",
      href: "/ja/resources/support-center/knowledge-base/general-faqs/mapreduce",
      category: "General & FAQs",
      tags: []
    }
  ]
}