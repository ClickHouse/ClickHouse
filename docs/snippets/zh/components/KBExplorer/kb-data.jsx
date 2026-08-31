export const kbIndex = {
  categories: [
    "Cloud",
    "配置与设置",
    "数据导入与导出",
    "数据管理",
    "常规与常见问题",
    "集成与客户端库",
    "Materialized views 与 projections",
    "监控与调试",
    "性能与优化",
    "查询与 SQL",
    "安全与访问控制",
    "安装与部署",
    "表与 schema",
    "故障排查与错误"
  ],
  tags: [
    "最佳实践",
    "社区",
    "概念",
    "核心数据概念",
    "数据导出",
    "数据格式",
    "数据摄取",
    "数据建模",
    "数据源",
    "部署与扩展",
    "错误与异常",
    "函数",
    "语言客户端",
    "管理 Cloud",
    "数据管理",
    "原生客户端与接口",
    "性能与优化",
    "安全与身份验证",
    "服务器管理",
    "设置",
    "系统表",
    "工具与实用程序",
    "故障排查",
    "使用场景"
  ],
  articles: [
    {
      id: "integrations/python-clickhouse-connect-example",
      title: "连接 ClickHouse Cloud Service 的 Python 客户端示例",
      description: "通过使用 clickhouse-connect 驱动的分步示例，了解如何使用 Python 连接 ClickHouse Cloud Service。",
      href: "/zh/resources/support-center/knowledge-base/integrations/python-clickhouse-connect-example",
      category: "Integrations & client libraries",
      tags: ["Language Clients"]
    },
    {
      id: "configuration-settings/about-quotas-and-query-complexity",
      title: "关于配额与查询复杂度",
      description:
        "配额与查询复杂度是限制用户在 ClickHouse 中操作权限的有效手段。本知识库文章通过示例展示如何应用这两种不同的方式。",
      href: "/zh/resources/support-center/knowledge-base/configuration-settings/about-quotas-and-query-complexity",
      category: "Configuration & settings",
      tags: ["Managing Cloud"]
    },
    {
      id: "data-import-export/achieving-atomic-inserts",
      title: "在 ClickHouse Cloud 中实现原子插入与多表一致性",
      description: "如何在不使用多语句事务的情况下，通过暂存表和分区级操作，在 ClickHouse Cloud 中原子性地加载数据并保持多表一致性。",
      href: "/zh/resources/support-center/knowledge-base/data-import-export/achieving-atomic-inserts",
      category: "Data import & export",
      tags: ["Data Ingestion", "Best Practices"]
    },
    {
      id: "tables-schema/add-column",
      title: "向表中添加列",
      description: "本指南将介绍如何向现有表添加列。",
      href: "/zh/resources/support-center/knowledge-base/tables-schema/add-column",
      category: "Tables & schema",
      tags: ["Data Modelling"]
    },
    {
      id: "configuration-settings/alter-user-settings-exception",
      title: "修改用户设置时的异常处理",
      description: "处理修改用户设置时抛出的异常",
      href: "/zh/resources/support-center/knowledge-base/configuration-settings/alter-user-settings-exception",
      category: "Configuration & settings",
      tags: ["Settings", "Errors and Exceptions"]
    },
    {
      id: "materialized-views/are-materialized-views-inserted-asynchronously",
      title: "Materialized Views 是同步插入的吗？",
      description: "本知识库文章探讨 Materialized Views 是否以同步方式插入",
      href: "/zh/resources/support-center/knowledge-base/materialized-views/are-materialized-views-inserted-asynchronously",
      category: "Materialized views & projections",
      tags: ["Data Modelling"]
    },
    {
      id: "tables-schema/schema-migration-tools",
      title: "ClickHouse 的自动 schema 迁移工具",
      description: "了解 ClickHouse 的自动 schema 迁移工具，以及如何随时间推移管理不断变化的数据库 schema。",
      href: "/zh/resources/support-center/knowledge-base/tables-schema/schema-migration-tools",
      category: "Tables & schema",
      tags: ["Tools and Utilities"]
    },
    {
      id: "cloud-services/aws-privatelink-setup-for-msk-clickpipes",
      title: "配置 AWS PrivateLink 以将 MSK 暴露给 ClickPipes",
      description: "通过 MSK 多 VPC 连接将私有 MSK 暴露给 ClickPipes 的配置步骤。",
      href: "/zh/resources/support-center/knowledge-base/cloud-services/aws-privatelink-setup-for-msk-clickpipes",
      category: "Cloud",
      tags: ["Security and Authentication", "Managing Cloud"]
    },
    {
      id: "cloud-services/aws-privatelink-setup-for-clickpipes",
      title: "配置 AWS PrivateLink 以将私有 RDS 暴露给 ClickPipes",
      description: "通过 AWS PrivateLink 将私有 RDS 暴露给 ClickPipes 的配置步骤。",
      href: "/zh/resources/support-center/knowledge-base/cloud-services/aws-privatelink-setup-for-clickpipes",
      category: "Cloud",
      tags: ["Security and Authentication", "Managing Cloud"]
    },
    {
      id: "data-management/backing-up-a-specific-partition",
      title: "备份特定分区",
      description: "如何在 ClickHouse 中备份特定分区？",
      href: "/zh/resources/support-center/knowledge-base/data-management/backing-up-a-specific-partition",
      category: "Data management",
      tags: ["Managing Data"]
    },
    {
      id: "general-faqs/key-value",
      title: "ClickHouse 能用作键值存储吗？",
      description: "解答 ClickHouse 是否可用作键值存储这一常见问题。",
      href: "/zh/resources/support-center/knowledge-base/general-faqs/key-value",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/time-series",
      title: "ClickHouse 能用作时序数据库吗？",
      description: "介绍如何将 ClickHouse 用作时序数据库",
      href: "/zh/resources/support-center/knowledge-base/general-faqs/time-series",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "queries-sql/pivot",
      title: "ClickHouse 支持 PIVOT 吗？",
      description:
        "ClickHouse 没有 PIVOT 子句，但可以使用聚合函数组合器实现类似功能。下面以英国房价数据集为例进行演示。",
      href: "/zh/resources/support-center/knowledge-base/queries-sql/pivot",
      category: "Queries & SQL",
      tags: ["Data Modelling", "Core Data Concepts"]
    },
    {
      id: "general-faqs/vector-search",
      title: "ClickHouse 能用于向量搜索吗？",
      description: "了解如何使用 ClickHouse 进行向量搜索，包括存储嵌入向量以及使用余弦相似度等距离函数进行搜索。",
      href: "/zh/resources/support-center/knowledge-base/general-faqs/vector-search",
      category: "General & FAQs",
      tags: ["Use Cases", "Concepts"]
    },
    {
      id: "monitoring-debugging/send-logs-level",
      title: "在客户端捕获查询的服务器日志",
      description: "了解如何使用 `send_logs_level` 客户端设置在客户端层面捕获服务器日志，即使日志配置各异也同样适用。",
      href: "/zh/resources/support-center/knowledge-base/monitoring-debugging/send-logs-level",
      category: "Monitoring & debugging",
      tags: ["Server Admin"]
    },
    {
      id: "configuration-settings/change-the-prompt-in-clickhouse-client",
      title: "修改 clickhouse-client 的提示符",
      description: "本文介绍如何将 ClickHouse 客户端和 clickhouse-local 终端窗口中的提示符从 :) 修改为带前缀的 :)",
      href: "/zh/resources/support-center/knowledge-base/configuration-settings/change-the-prompt-in-clickhouse-client",
      category: "Configuration & settings",
      tags: ["Settings", "Native Clients and Interfaces"]
    },
    {
      id: "security/common-rbac-queries",
      title: "常用 RBAC 查询",
      description: "用于向用户授予特定权限的查询语句。",
      href: "/zh/resources/support-center/knowledge-base/security/common-rbac-queries",
      category: "Security & access control",
      tags: ["Security and Authentication", "Managing Cloud"]
    },
    {
      id: "queries-sql/comparing-metrics-between-queries",
      title: "以分贝为单位比较查询间的指标",
      description: "用于在 ClickHouse 中比较两个查询指标的查询语句。",
      href: "/zh/resources/support-center/knowledge-base/queries-sql/comparing-metrics-between-queries",
      category: "Queries & SQL",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "configuration-settings/configure-cap-ipc-lock-and-cap-sys-nice-in-docker",
      title: "Configuring CAP_IPC_LOCK and CAP_SYS_NICE Capabilities in Docker",
      description: "Learn how to resolve Docker capability warnings for `CAP_IPC_LOCK` and `CAP_SYS_NICE` when running ClickHouse in a container.",
      href: "/zh/resources/support-center/knowledge-base/configuration-settings/configure-cap-ipc-lock-and-cap-sys-nice-in-docker",
      category: "Configuration & settings",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "troubleshooting/configure-cap-ipc-lock-and-cap-sys-nice-in-docker",
      title: "Configuring CAP_IPC_LOCK and CAP_SYS_NICE Capabilities in Docker",
      description: "Learn how to resolve Docker capability warnings for `CAP_IPC_LOCK` and `CAP_SYS_NICE` when running ClickHouse in a container.",
      href: "/zh/resources/support-center/knowledge-base/troubleshooting/configure-cap-ipc-lock-and-cap-sys-nice-in-docker",
      category: "Troubleshooting & errors",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "cloud-services/custom-dns-alias-for-instance",
      title: "Create a custom DNS alias by setting up a reverse proxy",
      description: "Learn how to set up a custom DNS alias for your instance using a reverse proxy",
      href: "/zh/resources/support-center/knowledge-base/cloud-services/custom-dns-alias-for-instance",
      category: "Cloud",
      tags: ["Server Admin", "Security and Authentication"]
    },
    {
      id: "troubleshooting/part-intersects-previous-part",
      title: "DB::Exception: Part XXXXX intersects previous part YYYYY. It is a bug or a result of manual intervention in the ZooKeeper data.",
      description:
        "本文介绍如何解决 ClickHouse 中 parts 相交导致的 DB::Exception 错误，该错误通常由竞态条件或对 ZooKeeper 数据的手动干预引起。",
      href: "/zh/resources/support-center/knowledge-base/troubleshooting/part-intersects-previous-part",
      category: "故障排查与错误",
      tags: ["错误与异常", "系统表"]
    },
    {
      id: "setup-installation/difference-between-official-builds-and-3rd-party",
      title: "Differences Between Official and 3rd-Party ClickHouse Builds",
      description: "Understand the key differences between official ClickHouse builds and 3rd-party builds, including updates, compatibility, and security considerations.",
      href: "/zh/resources/support-center/knowledge-base/setup-installation/difference-between-official-builds-and-3rd-party",
      category: "Setup & installation",
      tags: ["Concepts"]
    },
    {
      id: "general-faqs/cost-based",
      title: "Does ClickHouse have a cost-based optimizer",
      description: "ClickHouse has certain cost-based optimization mechanics",
      href: "/zh/resources/support-center/knowledge-base/general-faqs/cost-based",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/datalake",
      title: "Does ClickHouse support data lakes?",
      description: "ClickHouse supports data lakes, including Iceberg, Delta Lake, Apache Hudi, Apache Paimon, Hive",
      href: "/zh/resources/support-center/knowledge-base/general-faqs/datalake",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/distributed-join",
      title: "Does ClickHouse support distributed JOIN?",
      description: "ClickHouse supports distributed JOIN",
      href: "/zh/resources/support-center/knowledge-base/general-faqs/distributed-join",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/federated",
      title: "Does ClickHouse support federated queries?",
      description: "ClickHouse supports a wide range for federated and hybrid queries",
      href: "/zh/resources/support-center/knowledge-base/general-faqs/federated",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/concurrency",
      title: "Does ClickHouse support frequent, concurrent queries?",
      description: "ClickHouse supports high QPS and high concurrency",
      href: "/zh/resources/support-center/knowledge-base/general-faqs/concurrency",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "cloud-services/multi-region-replication",
      title: "Does ClickHouse support multi-region replication?",
      description: "This page answers whether ClickHouse supports multi-region replication",
      href: "/zh/resources/support-center/knowledge-base/cloud-services/multi-region-replication",
      category: "Cloud",
      tags: []
    },
    {
      id: "general-faqs/updates",
      title: "Does ClickHouse support real-time updates?",
      description: "ClickHouse supports lightweight real-time updates",
      href: "/zh/resources/support-center/knowledge-base/general-faqs/updates",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "security/row-column-policy",
      title: "Does ClickHouse support row-level and column-level security?",
      description: "Learn about row-level and column-level access restrictions in ClickHouse and ClickHouse Cloud, and how to implement role-based access control (RBAC) with policies.",
      href: "/zh/resources/support-center/knowledge-base/security/row-column-policy",
      category: "Security & access control",
      tags: ["Security and Authentication"]
    },
    {
      id: "cloud-services/execute-system-queries-in-cloud",
      title: "Execute SYSTEM Statements on All Nodes in ClickHouse Cloud",
      description: "Learn how to use `ON CLUSTER` and `clusterAllReplicas` to execute SYSTEM statements and queries across all nodes in a ClickHouse Cloud service.",
      href: "/zh/resources/support-center/knowledge-base/cloud-services/execute-system-queries-in-cloud",
      category: "Cloud",
      tags: ["Deployments and Scaling"]
    },
    {
      id: "troubleshooting/count-parts-by-type",
      title: "Find counts and sizes of wide or compact parts",
      description: "This knowledgebase article shows you how to find part counts by the type of part - wide or compact.",
      href: "/zh/resources/support-center/knowledge-base/troubleshooting/count-parts-by-type",
      category: "Troubleshooting & errors",
      tags: ["Troubleshooting"]
    },
    {
      id: "troubleshooting/fix-developer-verification-error-in-macos",
      title: "Fix the Developer Verification Error in MacOS",
      description: "Learn how to resolve the MacOS developer verification error when running ClickHouse commands, using either System Settings or the terminal.",
      href: "/zh/resources/support-center/knowledge-base/troubleshooting/fix-developer-verification-error-in-macos",
      category: "Troubleshooting & errors",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "data-import-export/s3-export-data-year-month-folders",
      title: "How can I do partitioned writes by year and month on S3?",
      description: "Learn how to write partitioned data by year and month to an S3 bucket in ClickHouse, using a custom path structure for organizing the data.",
      href: "/zh/resources/support-center/knowledge-base/data-import-export/s3-export-data-year-month-folders",
      category: "Data import & export",
      tags: ["Data Export", "Native Clients and Interfaces"]
    },
    {
      id: "data-import-export/kafka-clickhouse-json",
      title: "How can I use the new JSON Data Type with Kafka?",
      description: "Learn how to load JSON messages from Apache Kafka directly into a single JSON column in ClickHouse using the Kafka table engine and JSON data type.",
      href: "/zh/resources/support-center/knowledge-base/data-import-export/kafka-clickhouse-json",
      category: "Data import & export",
      tags: ["Data Formats", "Data Ingestion"]
    },
    {
      id: "cloud-services/change-billing-email",
      title: "How do I change my Billing Contact in ClickHouse Cloud?",
      description: "Let's learn how to change your billing address in ClickHouse Cloud.",
      href: "/zh/resources/support-center/knowledge-base/cloud-services/change-billing-email",
      category: "Cloud",
      tags: ["Managing Cloud"]
    },
    {
      id: "general-faqs/how-do-i-contribute-code-to-clickhouse",
      title: "How do I contribute code to ClickHouse?",
      description: "ClickHouse 是一个在 GitHub 上开发的开源项目。按照惯例，贡献说明发布在源代码仓库根目录的 CONTRIBUTING 文件中。",
      href: "/zh/resources/support-center/knowledge-base/general-faqs/how-do-i-contribute-code-to-clickhouse",
      category: "常规与常见问题",
      tags: ["社区"]
    },
    {
      id: "data-import-export/parquet-to-csv-json",
      title: "如何将文件从 Parquet 转换为 CSV 或 JSON？",
      description: "了解如何使用 ClickHouse 的 `clickhouse-local` 工具，轻松将 Parquet 文件转换为 CSV 或 JSON 格式。",
      href: "/zh/resources/support-center/knowledge-base/data-import-export/parquet-to-csv-json",
      category: "数据导入与导出",
      tags: ["数据源", "数据格式"]
    },
    {
      id: "data-import-export/mysql-to-parquet-csv-json",
      title: "如何使用 ClickHouse 将 MySQL 数据导出为 Parquet、CSV 或 JSON",
      description: "了解如何使用 `clickhouse-local` 工具，快速高效地将 MySQL 数据导出为 Parquet、CSV 或 JSON 等格式。",
      href: "/zh/resources/support-center/knowledge-base/data-import-export/mysql-to-parquet-csv-json",
      category: "数据导入与导出",
      tags: ["数据格式", "数据导出"]
    },
    {
      id: "data-import-export/postgresql-to-parquet-csv-json",
      title: "如何将 PostgreSQL 数据导出为 Parquet、CSV 或 JSON？",
      description: "通过多个示例，了解如何使用 `clickhouse-local` 将 PostgreSQL 数据导出为 Parquet、CSV 或 JSON 格式。",
      href: "/zh/resources/support-center/knowledge-base/data-import-export/postgresql-to-parquet-csv-json",
      category: "数据导入与导出",
      tags: ["数据导出", "数据格式"]
    },
    {
      id: "setup-installation/install-clickhouse-windows10",
      title: "如何在 Windows 10 上安装 ClickHouse？",
      description: "了解如何使用 WSL 2 在 Windows 10 上安装和测试 ClickHouse，包括安装配置、故障排查以及运行测试环境。",
      href: "/zh/resources/support-center/knowledge-base/setup-installation/install-clickhouse-windows10",
      category: "安装与配置",
      tags: ["工具与实用程序"]
    },
    {
      id: "security/remove-default-user",
      title: "如何删除默认用户？",
      description: "了解如何在运行 ClickHouse Server 时删除默认用户。",
      href: "/zh/resources/support-center/knowledge-base/security/remove-default-user",
      category: "安全与访问控制",
      tags: ["服务器管理"]
    },
    {
      id: "cloud-services/ingest-failures-23-9-release",
      title: "如何解决 ClickHouse 23.9 版本发布后的数据摄取失败问题？",
      description: "了解如何解决 ClickHouse 23.9 中针对使用 `async_inserts` 的表引入更严格权限检查所导致的数据摄取失败问题，通过更新授权来修复错误。",
      href: "/zh/resources/support-center/knowledge-base/cloud-services/ingest-failures-23-9-release",
      category: "Cloud",
      tags: ["错误与异常"]
    },
    {
      id: "performance-optimization/insert-select-settings-tuning",
      title: "如何解决 INSERT...SELECT 期间出现的 TOO MANY PARTS 错误？",
      description: "通过调整高级设置以增大数据块并提高分区阈值，解决 ClickHouse 在执行 `INSERT...SELECT` 时出现的 TOO_MANY_PARTS 错误。",
      href: "/zh/resources/support-center/knowledge-base/performance-optimization/insert-select-settings-tuning",
      category: "性能与优化",
      tags: ["设置", "错误与异常"]
    },
    {
      id: "integrations/node-js-example",
      title: "如何在 NodeJS 中使用 @clickhouse/client",
      description: "了解如何在 Node.js 应用程序中使用 @clickhouse/client 与 ClickHouse 交互并执行查询。",
      href: "/zh/resources/support-center/knowledge-base/integrations/node-js-example",
      category: "集成与客户端库",
      tags: ["语言客户端"]
    },
    {
      id: "monitoring-debugging/view-number-of-active-mutations",
      title: "如何查看活跃或排队中的 mutation 数量？",
      description:
        "监控 ClickHouse 中活跃或排队中的 mutation 数量，尤其是在执行 `ALTER` 或 `UPDATE` 操作时。使用 `system.mutations` 表跟踪 mutation。",
      href: "/zh/resources/support-center/knowledge-base/monitoring-debugging/view-number-of-active-mutations",
      category: "监控与调试",
      tags: ["系统表"]
    },
    {
      id: "data-management/read-consistency",
      title: "如何在 ClickHouse 中实现数据读取一致性？",
      description: "了解如何在从 ClickHouse 读取数据时确保数据一致性，无论连接的是同一节点还是随机节点。",
      href: "/zh/resources/support-center/knowledge-base/data-management/read-consistency",
      category: "数据管理",
      tags: ["性能与优化"]
    },
    {
      id: "setup-installation/llvm-clang-up-to-date",
      title: "如何在 Linux 上构建 LLVM 和 clang",
      description: "在 Linux 上构建 LLVM 和 clang 的命令。",
      href: "/zh/resources/support-center/knowledge-base/setup-installation/llvm-clang-up-to-date",
      category: "安装与配置",
      tags: ["社区", "工具与实用程序"]
    },
    {
      id: "data-management/calculate-ratio-of-zero-sparse-serialization",
      title: "如何计算表中每列空值/零值的比例",
      description: "了解如何计算 ClickHouse 表中每列空值或零值的比例，以优化稀疏列序列化。",
      href: "/zh/resources/support-center/knowledge-base/data-management/calculate-ratio-of-zero-sparse-serialization",
      category: "数据管理",
      tags: ["性能与优化"]
    },
    {
      id: "security/check-users-roles",
      title: "如何查看角色与用户的双向分配关系",
      description: "了解如何查询 ClickHouse 的 `system.role_grants`，以查找分配给角色的用户以及分配给特定用户的角色。",
      href: "/zh/resources/support-center/knowledge-base/security/check-users-roles",
      category: "安全与访问控制",
      tags: ["服务器管理", "系统表", "Cloud 管理"]
    },
    {
      id: "monitoring-debugging/which-processes-are-currently-running",
      title: "如何查看服务器上当前正在运行的代码？",
      description:
        "ClickHouse 提供了 `system.stack_trace` 等内省工具，用于检查每个服务器线程上当前正在运行的代码，有助于调试和性能监控。",
      href: "/zh/resources/support-center/knowledge-base/monitoring-debugging/which-processes-are-currently-running",
      category: "监控与调试",
      tags: ["服务器管理"]
    },
    {
      id: "cloud-services/how-to-check-my-clickhouse-cloud-sevice-state",
      title: "如何查看 ClickHouse Cloud 服务状态",
      description: "了解如何使用 ClickHouse Cloud API 检查服务是否已停止、空闲或正在运行，且无需唤醒服务。",
      href: "/zh/resources/support-center/knowledge-base/cloud-services/how-to-check-my-clickhouse-cloud-sevice-state",
      category: "Cloud",
      tags: ["Cloud 管理"]
    },
    {
      id: "configuration-settings/configure-a-user-setting",
      title: "如何在 ClickHouse 中为用户配置设置",
      description: "了解如何使用 `SET` 和 `ALTER USER` 命令在 ClickHouse 中为单个查询、客户端会话或特定用户定义设置。",
      href: "/zh/resources/support-center/knowledge-base/configuration-settings/configure-a-user-setting",
      category: "配置与设置",
      tags: ["设置"]
    },
    {
      id: "materialized-views/projection-example",
      title: "如何确认查询是否使用了 Projection？",
      description: "了解如何通过测试样本数据并使用 EXPLAIN 来验证 ClickHouse 查询中是否使用了 projection。",
      href: "/zh/resources/support-center/knowledge-base/materialized-views/projection-example",
      category: "Materialized view 与 projection",
      tags: ["数据建模"]
    },
    {
      id: "cloud-services/how-to-connect-to-ch-cloud-using-ssh-keys",
      title: "如何使用 SSH 密钥连接到 ClickHouse",
      description: "如何使用 SSH 密钥连接到 ClickHouse 和 ClickHouse Cloud",
      href: "/zh/resources/support-center/knowledge-base/cloud-services/how-to-connect-to-ch-cloud-using-ssh-keys",
      category: "Cloud",
      tags: ["Managing Cloud", "Security and Authentication"]
    },
    {
      id: "data-management/dictionary-using-strings",
      title: "如何使用字符串键和值创建 ClickHouse 字典",
      description: "了解如何以 MergeTree 表为数据源，使用字符串键和值创建 ClickHouse 字典，并附有配置和使用示例。",
      href: "/zh/resources/support-center/knowledge-base/data-management/dictionary-using-strings",
      category: "数据管理",
      tags: ["Data Modelling"]
    },
    {
      id: "tables-schema/how-to-create-table-to-query-multiple-remote-clusters",
      title: "如何创建可查询多个远程集群的表",
      description: "如何创建可查询多个远程集群的表",
      href: "/zh/resources/support-center/knowledge-base/tables-schema/how-to-create-table-to-query-multiple-remote-clusters",
      category: "表与 schema",
      tags: ["Deployments and Scaling"]
    },
    {
      id: "setup-installation/enabling-ssl-with-lets-encrypt",
      title: "如何在单台 ClickHouse 服务器上使用 Let's Encrypt 启用 SSL",
      description: "了解如何使用 Let's Encrypt 为单台 ClickHouse 服务器配置 SSL，包括证书签发、配置和验证。",
      href: "/zh/resources/support-center/knowledge-base/setup-installation/enabling-ssl-with-lets-encrypt",
      category: "安装与配置",
      tags: ["Security and Authentication"]
    },
    {
      id: "data-import-export/file-export",
      title: "如何将 ClickHouse 中的数据导出到文件",
      description: "了解从 ClickHouse 导出数据的多种方法，包括 `INTO OUTFILE`、File 表引擎以及命令行重定向。",
      href: "/zh/resources/support-center/knowledge-base/data-import-export/file-export",
      category: "数据导入与导出",
      tags: ["Data Export"]
    },
    {
      id: "queries-sql/how-to-filter-a-clickhouse-table-by-an-array-column",
      title: "如何按 Array 列过滤 ClickHouse 表？",
      description: "关于如何按 Array 列过滤 ClickHouse 表的知识库文章。",
      href: "/zh/resources/support-center/knowledge-base/queries-sql/how-to-filter-a-clickhouse-table-by-an-array-column",
      category: "查询与 SQL",
      tags: ["Data Modelling", "Functions"]
    },
    {
      id: "monitoring-debugging/generate-har-file",
      title: "如何生成 HAR 文件以供支持团队使用",
      description: "HAR（HTTP Archive）文件记录浏览器中的网络活动，可帮助 ClickHouse 支持团队诊断页面加载缓慢、请求失败或其他网络问题。",
      href: "/zh/resources/support-center/knowledge-base/monitoring-debugging/generate-har-file",
      category: "监控与调试",
      tags: ["Tools and Utilities"]
    },
    {
      id: "materialized-views/how-to-display-queries-using-mv",
      title: "如何识别 ClickHouse 中使用 Materialized View 的查询",
      description: "了解如何查询 ClickHouse 日志，以识别指定时间范围内涉及 materialized view 的所有查询。",
      href: "/zh/resources/support-center/knowledge-base/materialized-views/how-to-display-queries-using-mv",
      category: "Materialized view 与 PROJECTION",
      tags: ["System Tables"]
    },
    {
      id: "performance-optimization/find-expensive-queries",
      title: "如何识别 ClickHouse 中开销最大的查询",
      description: "了解如何使用 ClickHouse 中的 `query_log` 表，识别分布式节点中内存和 CPU 消耗最高的查询。",
      href: "/zh/resources/support-center/knowledge-base/performance-optimization/find-expensive-queries",
      category: "性能与优化",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "configuration-settings/ignoring-incorrect-settings",
      title: "如何忽略 ClickHouse 中的错误配置项",
      description: "了解如何使用 `skip_check_for_incorrect_settings` 选项，使 ClickHouse 在用户级配置项设置有误时仍能正常启动。",
      href: "/zh/resources/support-center/knowledge-base/configuration-settings/ignoring-incorrect-settings",
      category: "配置与设置",
      tags: ["Settings"]
    },
    {
      id: "data-import-export/json-import",
      title: "如何将 JSON 导入 ClickHouse？",
      description: "本页介绍如何将 JSON 数据导入 ClickHouse。",
      href: "/zh/resources/support-center/knowledge-base/data-import-export/json-import",
      category: "数据导入与导出",
      tags: []
    },
    {
      id: "setup-installation/how-to-increase-thread-pool-size",
      title: "如何增加 ClickHouse 的线程数",
      description: "了解如何通过调整 `max_thread_pool_size`、`thread_pool_queue_size` 和 `max_thread_pool_free_size` 等参数来配置 ClickHouse 全局线程池。",
      href: "/zh/resources/support-center/knowledge-base/setup-installation/how-to-increase-thread-pool-size",
      category: "安装与配置",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "data-import-export/kafka-to-clickhouse-setup",
      title: "如何将 Kafka 中的数据摄取到 ClickHouse",
      description: "了解如何使用 Kafka 表引擎、materialized view 和 MergeTree 表将 Kafka topic 中的数据摄取到 ClickHouse。",
      href: "/zh/resources/support-center/knowledge-base/data-import-export/kafka-to-clickhouse-setup",
      category: "数据导入与导出",
      tags: ["Data Ingestion"]
    },
    {
      id: "data-import-export/ingest-parquet-files-in-s3",
      title: "如何从 S3 存储桶摄取 Parquet 文件",
      description: "了解如何使用 ClickHouse 中的 S3 表引擎从 S3 存储桶摄取和查询 Parquet 文件，包括配置、访问权限和数据导入示例。",
      href: "/zh/resources/support-center/knowledge-base/data-import-export/ingest-parquet-files-in-s3",
      category: "数据导入与导出",
      tags: ["Data Ingestion"]
    },
    {
      id: "queries-sql/how-to-insert-all-rows-from-another-table",
      title: "如何将一张表中的所有行插入另一张表？",
      description: "关于如何将一张表中的所有行插入另一张表的知识库文章。",
      href: "/zh/resources/support-center/knowledge-base/queries-sql/how-to-insert-all-rows-from-another-table",
      category: "查询与 SQL",
      tags: ["Data Ingestion"]
    },
    {
      id: "performance-optimization/check-query-processing-time-only",
      title: "如何在不返回行的情况下测量查询处理时间",
      description: "了解如何使用 ClickHouse 中的 `FORMAT Null` 选项，在不向客户端返回任何行的情况下测量查询处理时间。",
      href: "/zh/resources/support-center/knowledge-base/performance-optimization/check-query-processing-time-only",
      category: "性能与优化",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "monitoring-debugging/outputSendLogsLevelTracesToFile",
      title: "如何使用 clickhouse-client 将日志级别追踪信息输出到文件",
      description: "如何使用 clickhouse-client 将日志级别追踪信息输出到文件",
      href: "/zh/resources/support-center/knowledge-base/monitoring-debugging/outputSendLogsLevelTracesToFile",
      category: "监控与调试",
      tags: ["Data Export"]
    },
    {
      id: "tables-schema/recreate-table-across-terminals",
      title: "如何在不同终端间快速重建小表",
      description: "了解如何在开发环境中通过复制粘贴的方式，在不同终端间快速重建小表及其数据。",
      href: "/zh/resources/support-center/knowledge-base/tables-schema/recreate-table-across-terminals",
      category: "表与 schema",
      tags: ["Tools and Utilities"]
    },
    {
      id: "integrations/how-to-set-up-ch-on-docker-odbc-connect-mssql",
      title: "如何在 Docker 上配置 ClickHouse 并通过 ODBC 连接 Microsoft SQL Server (MSSQL) 数据库",
      description: "如何在 Docker 上配置 ClickHouse 并通过 ODBC 连接 Microsoft SQL Server (MSSQL) 数据库",
      href: "/zh/resources/support-center/knowledge-base/integrations/how-to-set-up-ch-on-docker-odbc-connect-mssql",
      category: "集成与客户端库",
      tags: ["Native Clients and Interfaces"]
    },
    {
      id: "queries-sql/using-array-join-to-extract-and-query-attributes",
      title: "如何使用 array join 通过 map 键和值提取并查询动态属性",
      description: "简单示例，演示如何使用 array join 通过 map 键和值提取并查询不同属性",
      href: "/zh/resources/support-center/knowledge-base/queries-sql/using-array-join-to-extract-and-query-attributes",
      category: "查询与 SQL",
      tags: ["Functions"]
    },
    {
      id: "materialized-views/how-to-use-parametrised-views",
      title: "如何在 ClickHouse 中使用参数化视图",
      description: "了解如何在 ClickHouse 中创建和查询参数化视图，以便根据查询时参数对数据进行动态切片。",
      href: "/zh/resources/support-center/knowledge-base/materialized-views/how-to-use-parametrised-views",
      category: "Materialized views 与 projections",
      tags: ["使用场景"]
    },
    {
      id: "tables-schema/exchangeStatementToSwitchTables",
      title: "如何使用 exchange 命令切换表",
      description: "如何使用 exchange 命令切换表",
      href: "/zh/resources/support-center/knowledge-base/tables-schema/exchangeStatementToSwitchTables",
      category: "表与 schema",
      tags: ["数据管理"]
    },
    {
      id: "queries-sql/compare-resultsets",
      title: "如何验证两个查询是否返回相同的结果集",
      description: "了解如何使用哈希函数和比较技术验证两个 ClickHouse 查询是否返回相同的结果集。",
      href: "/zh/resources/support-center/knowledge-base/queries-sql/compare-resultsets",
      category: "查询与 SQL",
      tags: ["Functions"]
    },
    {
      id: "monitoring-debugging/check-query-cache-in-use",
      title: "如何验证 ClickHouse 中的查询缓存使用情况",
      description: "了解如何通过 `clickhouse-client` 追踪日志或 SQL 命令检查 ClickHouse 中的查询缓存是否正在被使用。",
      href: "/zh/resources/support-center/knowledge-base/monitoring-debugging/check-query-cache-in-use",
      category: "监控与调试",
      tags: ["性能与优化"]
    },
    {
      id: "cloud-services/unable-to-access-cloud-service",
      title: "无法访问 ClickHouse Cloud 服务",
      description: "排查 ClickHouse Cloud 服务的访问问题，包括 IP 访问列表配置",
      href: "/zh/resources/support-center/knowledge-base/cloud-services/unable-to-access-cloud-service",
      category: "Cloud",
      tags: ["错误与异常", "Cloud 管理"]
    },
    {
      id: "performance-optimization/finding-expensive-queries-by-memory-usage",
      title: "在 ClickHouse 中通过内存使用量识别高开销查询",
      description: "了解如何使用 `system.query_log` 表找出 ClickHouse 中内存消耗最大的查询，并提供集群和单机部署的示例。",
      href: "/zh/resources/support-center/knowledge-base/performance-optimization/finding-expensive-queries-by-memory-usage",
      category: "性能与优化",
      tags: ["性能与优化"]
    },
    {
      id: "data-import-export/importing-and-working-with-json-array-objects",
      title: "在 ClickHouse 中导入和查询 JSON 数组对象",
      description: "了解如何将 JSON 数组对象导入 ClickHouse，并使用 JSON 函数和数组操作执行高级查询。",
      href: "/zh/resources/support-center/knowledge-base/data-import-export/importing-and-working-with-json-array-objects",
      category: "数据导入与导出",
      tags: ["数据格式"]
    },
    {
      id: "data-import-export/importing-geojason-with-nested-object-array",
      title: "导入包含深层嵌套对象数组的 GeoJSON",
      description: "导入包含深层嵌套对象数组的 GeoJSON",
      href: "/zh/resources/support-center/knowledge-base/data-import-export/importing-geojason-with-nested-object-array",
      category: "数据导入与导出",
      tags: ["数据格式"]
    },
    {
      id: "performance-optimization/improve-map-performance",
      title: "提升 ClickHouse 中 Map 查找性能",
      description: "了解如何通过将特定键物化为独立列来优化 ClickHouse 中 Map 列的查找性能，从而提升查询效率。",
      href: "/zh/resources/support-center/knowledge-base/performance-optimization/improve-map-performance",
      category: "性能与优化",
      tags: ["性能与优化"]
    },
    {
      id: "tables-schema/delete-old-data",
      title: "能否从 ClickHouse 表中删除旧记录？",
      description: "本页解答能否从 ClickHouse 表中删除旧记录的问题",
      href: "/zh/resources/support-center/knowledge-base/tables-schema/delete-old-data",
      category: "表与 schema",
      tags: []
    },
    {
      id: "general-faqs/separate-storage",
      title: "能否将 ClickHouse 部署为存储与计算分离的架构？",
      description: "本页解答能否将 ClickHouse 部署为存储与计算分离架构的问题",
      href: "/zh/resources/support-center/knowledge-base/general-faqs/separate-storage",
      category: "常规与 FAQ",
      tags: []
    },
    {
      id: "data-import-export/json-extract-example",
      title: "JSON 提取示例",
      description: "一个简短示例，演示如何从 JSON 中提取基础类型",
      href: "/zh/resources/support-center/knowledge-base/data-import-export/json-extract-example",
      category: "数据导入与导出",
      tags: ["数据格式"]
    },
    {
      id: "queries-sql/calculate-pi-using-sql",
      title: "用 SQL 计算圆周率",
      description: "今天是圆周率日！让我们用 ClickHouse SQL 来计算圆周率",
      href: "/zh/resources/support-center/knowledge-base/queries-sql/calculate-pi-using-sql",
      category: "查询与 SQL",
      tags: ["使用场景"]
    },
    {
      id: "cloud-services/clickhouse-cloud-api-usage",
      title: "使用 API 和 cURL 管理 ClickHouse Cloud 服务",
      description: "了解如何使用 API 端点和 cURL 命令启动、停止和恢复 ClickHouse Cloud 服务。",
      href: "/zh/resources/support-center/knowledge-base/cloud-services/clickhouse-cloud-api-usage",
      category: "Cloud",
      tags: ["Cloud 管理", "工具与实用程序"]
    },
    {
      id: "monitoring-debugging/mapping-of-system-metrics-to-prometheus-metrics",
      title: "system.dashboards 中的指标与 `system.custom_metrics` 中 Prometheus 指标的映射关系",
      description: "system.dashboards 中的指标与 system.custom_metrics 中 Prometheus 指标的映射关系",
      href: "/zh/resources/support-center/knowledge-base/monitoring-debugging/mapping-of-system-metrics-to-prometheus-metrics",
      category: "监控与调试",
      tags: ["系统表"]
    },
    {
      id: "security/windows-active-directory-to-ch-roles",
      title: "将 Windows Active Directory 安全组映射到 ClickHouse 角色",
      description: "将 Windows Active Directory 安全组映射到 ClickHouse 角色的示例",
      href: "/zh/resources/support-center/knowledge-base/security/windows-active-directory-to-ch-roles",
      category: "安全与访问控制",
      tags: ["工具与实用程序"]
    },
    {
      id: "performance-optimization/memory-limit-exceeded-for-query",
      title: "查询内存限制超出",
      description: "排查查询内存限制超出错误",
      href: "/zh/resources/support-center/knowledge-base/performance-optimization/memory-limit-exceeded-for-query",
      category: "性能与优化",
      tags: ["错误与异常"]
    },
    {
      id: "integrations/ODBC-authentication-failed-error-using-PowerBI-CH-connector",
      title: "使用 Power BI ClickHouse 连接器时出现 ODBC 身份验证失败错误",
      description: "使用 Power BI ClickHouse 连接器时出现 ODBC 身份验证失败错误",
      href: "/zh/resources/support-center/knowledge-base/integrations/ODBC-authentication-failed-error-using-PowerBI-CH-connector",
      category: "集成与客户端库",
      tags: ["原生客户端与接口", "错误与异常"]
    },
    {
      id: "monitoring-debugging/profiling-clickhouse-with-llvm-xray",
      title: "使用 LLVM 的 XRay 对 ClickHouse 进行性能分析",
      description: "了解如何使用 LLVM 的 XRay 插桩分析器对 ClickHouse 进行性能分析、可视化追踪并分析性能。",
      href: "/zh/resources/support-center/knowledge-base/monitoring-debugging/profiling-clickhouse-with-llvm-xray",
      category: "监控与调试",
      tags: ["性能与优化", "工具与实用程序"]
    },
    {
      id: "integrations/python-http-requests",
      title: "使用 HTTP requests 模块的 Python 快速示例",
      description: "使用 Python 和 requests 模块向 ClickHouse 读写数据的示例",
      href: "/zh/resources/support-center/knowledge-base/integrations/python-http-requests",
      category: "集成与客户端库",
      tags: ["原生客户端与接口"]
    },
    {
      id: "configuration-settings/maximum-number-of-tables-and-databases",
      title: "ClickHouse 中推荐的最大数据库、表、分区和 parts 数量",
      description: "了解 ClickHouse 集群中推荐的数据库、表、分区和 parts 的最大限制，以确保最佳性能。",
      href: "/zh/resources/support-center/knowledge-base/configuration-settings/maximum-number-of-tables-and-databases",
      category: "配置与设置",
      tags: ["性能与优化", "部署与扩展"]
    },
    {
      id: "data-import-export/cannot-append-data-to-parquet-format",
      title: '解决 ClickHouse 中的 "Cannot Append Data in Parquet Format" 错误',
      description: '您在 ClickHouse 中是否遇到了 "Cannot append data in format Parquet to file" 错误？让我们来看看如何解决它。',
      href: "/zh/resources/support-center/knowledge-base/data-import-export/cannot-append-data-to-parquet-format",
      category: "数据导入与导出",
      tags: ["错误与异常", "数据格式"]
    },
    {
      id: "troubleshooting/exception-too-many-parts",
      title: '解决 ClickHouse 中的 "Too Many Parts" 错误',
      description: '了解如何通过优化插入速率、配置 MergeTree 设置以及有效管理分区来解决 ClickHouse 中的 "Too many parts" 错误。',
      href: "/zh/resources/support-center/knowledge-base/troubleshooting/exception-too-many-parts",
      category: "故障排查与错误",
      tags: ["错误与异常"]
    },
    {
      id: "troubleshooting/certificate-verify-failed-error",
      title: "解决 ClickHouse 中的 SSL 证书验证错误",
      description: "了解如何解决 SSL 异常 CERTIFICATE_VERIFY_FAILED 错误。",
      href: "/zh/resources/support-center/knowledge-base/troubleshooting/certificate-verify-failed-error",
      category: "故障排查与错误",
      tags: ["安全与身份验证", "错误与异常"]
    },
    {
      id: "troubleshooting/connection-timeout-remote-remoteSecure",
      title: "解决使用 `remote` 和 `remoteSecure` 表函数时的超时错误",
      description: "了解如何通过调整连接超时设置来修复在 ClickHouse 中使用 `remote` 或 `remoteSecure` 表函数时的超时错误。",
      href: "/zh/resources/support-center/knowledge-base/troubleshooting/connection-timeout-remote-remoteSecure",
      category: "故障排查与错误",
      tags: ["错误与异常"]
    },
    {
      id: "tables-schema/search-across-node-for-tables-with-a-wildcard",
      title: "使用通配符跨节点搜索表",
      description: "了解如何使用通配符跨节点搜索表。",
      href: "/zh/resources/support-center/knowledge-base/tables-schema/search-across-node-for-tables-with-a-wildcard",
      category: "表与 schema",
      tags: ["部署与扩展"]
    },
    {
      id: "performance-optimization/query-max-execution-time",
      title: "设置查询执行时间限制",
      description: "如何强制限制最大查询执行时间",
      href: "/zh/resources/support-center/knowledge-base/performance-optimization/query-max-execution-time",
      category: "性能与优化",
      tags: ["管理 Cloud", "设置"]
    },
    {
      id: "data-import-export/json-simple-example",
      title: "使用带有 materialized view 的落地表提取 JSON 数据的简单示例流程",
      description: "使用带有 materialized view 的落地表提取 JSON 数据的简单示例流程",
      href: "/zh/resources/support-center/knowledge-base/data-import-export/json-simple-example",
      category: "数据导入与导出",
      tags: ["数据格式"]
    },
    {
      id: "performance-optimization/async-vs-optimize-read-in-order",
      title: "同步数据读取",
      description:
        "新设置 `allow_asynchronous_read_from_io_pool_for_merge_tree` 允许读取线程（流）的数量高于查询执行管道其余部分中的线程数量。",
      href: "/zh/resources/support-center/knowledge-base/performance-optimization/async-vs-optimize-read-in-order",
      category: "性能与优化",
      tags: ["设置", "性能与优化"]
    },
    {
      id: "integrations/terraform-example",
      title: "使用 Cloud API 的 Terraform 示例",
      description: "本文提供了一个如何使用 Terraform 通过 API 创建/删除集群的示例",
      href: "/zh/resources/support-center/knowledge-base/integrations/terraform-example",
      category: "集成与客户端库",
      tags: ["原生客户端与接口"]
    },
    {
      id: "performance-optimization/tips-tricks-optimizing-basic-data-types-in-clickhouse",
      title: "在 ClickHouse 中优化基本数据类型的提示和技巧",
      description: "在 ClickHouse 中优化基本数据类型的提示和技巧",
      href: "/zh/resources/support-center/knowledge-base/performance-optimization/tips-tricks-optimizing-basic-data-types-in-clickhouse",
      category: "性能与优化",
      tags: ["性能与优化"]
    },
    {
      id: "queries-sql/useful-queries-for-troubleshooting",
      title: "用于故障排查的实用查询",
      description: "用于 ClickHouse 故障排查的便捷查询集合，包括监控表大小、长时间运行的查询和错误。",
      href: "/zh/resources/support-center/knowledge-base/queries-sql/useful-queries-for-troubleshooting",
      category: "查询与 SQL",
      tags: ["设置"]
    },
    {
      id: "general-faqs/use-clickhouse-for-log-analytics",
      title: "使用 ClickHouse 进行日志分析",
      description: "ClickHouse 因其提供的实时分析功能而在日志和指标分析中广受欢迎。准备好了解更多了吗？",
      href: "/zh/resources/support-center/knowledge-base/general-faqs/use-clickhouse-for-log-analytics",
      category: "常规与常见问题解答",
      tags: ["用例"]
    },
    {
      id: "queries-sql/filtered-aggregates",
      title: "在 ClickHouse 中使用过滤聚合",
      description: "了解如何在 ClickHouse 中使用带有 `-If` 和 `-Distinct` 聚合组合器的过滤聚合，以简化查询语法并增强分析能力。",
      href: "/zh/resources/support-center/knowledge-base/queries-sql/filtered-aggregates",
      category: "查询与 SQL",
      tags: ["函数"]
    },
    {
      id: "general-faqs/dependencies",
      title: "运行 ClickHouse 有哪些第三方依赖？",
      description: "ClickHouse 是自包含的，没有运行时依赖",
      href: "/zh/resources/support-center/knowledge-base/general-faqs/dependencies",
      category: "常规与常见问题解答",
      tags: []
    },
    {
      id: "general-faqs/dbms-naming",
      title: '"ClickHouse" 是什么意思？',
      description: '了解 "ClickHouse" 的含义',
      href: "/zh/resources/support-center/knowledge-base/general-faqs/dbms-naming",
      category: "常规与常见问题解答",
      tags: []
    },
    {
      id: "general-faqs/ne-tormozit",
      title: "“не тормозит” 是什么意思？",
      description: '本页解释了 "Не тормозит" 的含义',
      href: "/zh/resources/support-center/knowledge-base/general-faqs/ne-tormozit",
      category: "常规与常见问题解答",
      tags: []
    },
    {
      id: "integrations/oracle-odbc",
      title: "如果通过 ODBC 使用 Oracle 时遇到编码问题怎么办？",
      description: "本页提供了有关通过 ODBC 使用 Oracle 时遇到编码问题时的处理指南",
      href: "/zh/resources/support-center/knowledge-base/integrations/oracle-odbc",
      category: "集成与客户端库",
      tags: []
    },
    {
      id: "general-faqs/columnar-database",
      title: "什么是列式数据库？",
      description: "本页描述了什么是列式数据库",
      href: "/zh/resources/support-center/knowledge-base/general-faqs/columnar-database",
      category: "常规与常见问题解答",
      tags: []
    },
    {
      id: "general-faqs/olap",
      title: "What is OLAP?",
      description: "An explainer on what Online Analytical Processing is",
      href: "/zh/resources/support-center/knowledge-base/general-faqs/olap",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "performance-optimization/optimize-final-vs-final",
      title: "What is the difference between OPTIMIZE FINAL and FINAL?",
      description: "Discusses the differences between OPTIMIZE FINAL and FINAL, and when to use and avoid them.",
      href: "/zh/resources/support-center/knowledge-base/performance-optimization/optimize-final-vs-final",
      category: "Performance & optimization",
      tags: ["Core Data Concepts"]
    },
    {
      id: "general-faqs/sql",
      title: "What SQL syntax does ClickHouse support?",
      description: "ClickHouse supports 100% of SQL syntax",
      href: "/zh/resources/support-center/knowledge-base/general-faqs/sql",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "data-management/when-is-ttl-applied",
      title: "TTL 规则何时生效？我们能否对其进行控制？",
      description:
        "ClickHouse 中的 TTL 规则会在后台异步执行，您可以通过 `merge_with_ttl_timeout` 设置来控制其执行时机。了解如何手动触发 TTL 以及如何管理 TTL 执行的后台线程。",
      href: "/zh/resources/support-center/knowledge-base/data-management/when-is-ttl-applied",
      category: "Data management",
      tags: ["Core Data Concepts"]
    },
    {
      id: "setup-installation/production",
      title: "Which ClickHouse version to use in production?",
      description: "This page provides guidance on which ClickHouse version to use in production",
      href: "/zh/resources/support-center/knowledge-base/setup-installation/production",
      category: "Setup & installation",
      tags: []
    },
    {
      id: "general-faqs/who-is-using-clickhouse",
      title: "Who is using ClickHouse?",
      description: "Describes who is using ClickHouse",
      href: "/zh/resources/support-center/knowledge-base/general-faqs/who-is-using-clickhouse",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "data-management/dictionaries-consistent-state",
      title: "Why can't I see my data in a dictionary in ClickHouse Cloud?",
      description: "There is an issue where data in dictionaries may not be visible immediately after creation.",
      href: "/zh/resources/support-center/knowledge-base/data-management/dictionaries-consistent-state",
      category: "Data management",
      tags: ["Managing Cloud", "Data Modelling"]
    },
    {
      id: "general-faqs/why-recommend-clickhouse-keeper-over-zookeeper",
      title: "Why is ClickHouse Keeper recommended over ZooKeeper?",
      description:
        "ClickHouse Keeper improves upon ZooKeeper with features like reduced disk space usage, faster recovery, and less memory consumption, offering better performance for ClickHouse clusters.",
      href: "/zh/resources/support-center/knowledge-base/general-faqs/why-recommend-clickhouse-keeper-over-zookeeper",
      category: "General & FAQs",
      tags: ["Core Data Concepts"]
    },
    {
      id: "monitoring-debugging/why-default-logging-verbose",
      title: "Why is ClickHouse logging so verbose by default?",
      description: "Learn why the ClickHouse developers chose to set a verbose logging level by default.",
      href: "/zh/resources/support-center/knowledge-base/monitoring-debugging/why-default-logging-verbose",
      category: "Monitoring & debugging",
      tags: ["Settings"]
    },
    {
      id: "performance-optimization/why-is-my-primary-key-not-used",
      title: "Why is my primary key not used? How can I check?",
      description: "Covers a common reason why a primary key is not used in ordering and how we can confirm",
      href: "/zh/resources/support-center/knowledge-base/performance-optimization/why-is-my-primary-key-not-used",
      category: "Performance & optimization",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "general-faqs/mapreduce",
      title: "Why not use something like MapReduce?",
      description: "This page explains why you would use ClickHouse over MapReduce",
      href: "/zh/resources/support-center/knowledge-base/general-faqs/mapreduce",
      category: "General & FAQs",
      tags: []
    }
  ]
}