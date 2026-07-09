export const kbIndex = {
  categories: [
    "Cloud",
    "Configuration & settings",
    "Data import & export",
    "Data management",
    "General & FAQs",
    "Integrations & client libraries",
    "Materialized views & projections",
    "Monitoring & debugging",
    "Performance & optimization",
    "Queries & SQL",
    "Security & access control",
    "Setup & installation",
    "Tables & schema",
    "Troubleshooting & errors"
  ],
  tags: [
    "Best Practices",
    "Community",
    "Concepts",
    "Core Data Concepts",
    "Data Export",
    "Data Formats",
    "Data Ingestion",
    "Data Modelling",
    "Data Sources",
    "Deployments and Scaling",
    "Errors and Exceptions",
    "Functions",
    "Language Clients",
    "Managing Cloud",
    "Managing Data",
    "Native Clients and Interfaces",
    "Performance and Optimizations",
    "Security and Authentication",
    "Server Admin",
    "Settings",
    "System Tables",
    "Tools and Utilities",
    "Troubleshooting",
    "Use Cases"
  ],
  articles: [
    {
      id: "integrations/python-clickhouse-connect-example",
      title: "Exemplo funcional de cliente Python para conexão ao ClickHouse Cloud Service",
      description: "Learn how to connect to ClickHouse Cloud Service using Python with a step-by-step example using the clickhouse-connect driver.",
      href: "/resources/support-center/knowledge-base/integrations/python-clickhouse-connect-example",
      category: "Integrations & client libraries",
      tags: ["Language Clients"]
    },
    {
      id: "configuration-settings/about-quotas-and-query-complexity",
      title: "Sobre cotas e complexidade de consultas",
      description:
        "Quotas and Query Complexity are powerful ways to limit and restrict what users can do in ClickHouse. This KB article shows examples on how to apply these two different approaches.",
      href: "/resources/support-center/knowledge-base/configuration-settings/about-quotas-and-query-complexity",
      category: "Configuration & settings",
      tags: ["Managing Cloud"]
    },
    {
      id: "data-import-export/achieving-atomic-inserts",
      title: "Achieving atomic inserts and multi-table consistency in ClickHouse Cloud",
      description: "How to load data atomically and keep multiple tables consistent in ClickHouse Cloud without multi-statement transactions, using staging tables and partition-level operations.",
      href: "/resources/support-center/knowledge-base/data-import-export/achieving-atomic-inserts",
      category: "Data import & export",
      tags: ["Data Ingestion", "Best Practices"]
    },
    {
      id: "tables-schema/add-column",
      title: "Adding a column to a table",
      description: "In this guide, we'll learn how to add a column to an existing table.",
      href: "/resources/support-center/knowledge-base/tables-schema/add-column",
      category: "Tables & schema",
      tags: ["Data Modelling"]
    },
    {
      id: "configuration-settings/alter-user-settings-exception",
      title: "Exceção ao alterar configurações de usuário",
      description: "Handing the an exception thrown when altering user settings",
      href: "/resources/support-center/knowledge-base/configuration-settings/alter-user-settings-exception",
      category: "Configuration & settings",
      tags: ["Settings", "Errors and Exceptions"]
    },
    {
      id: "materialized-views/are-materialized-views-inserted-asynchronously",
      title: "Are Materialized Views inserted synchronously?",
      description: "This KB article explores whether Materialized Views are inserted synchronously",
      href: "/resources/support-center/knowledge-base/materialized-views/are-materialized-views-inserted-asynchronously",
      category: "Materialized views & projections",
      tags: ["Data Modelling"]
    },
    {
      id: "tables-schema/schema-migration-tools",
      title: "Automatic schema migration tools for ClickHouse",
      description: "Learn about automatic schema migration tools for ClickHouse and how to manage changing database schemas over time.",
      href: "/resources/support-center/knowledge-base/tables-schema/schema-migration-tools",
      category: "Tables & schema",
      tags: ["Tools and Utilities"]
    },
    {
      id: "cloud-services/aws-privatelink-setup-for-msk-clickpipes",
      title: "AWS PrivateLink setup to expose MSK for ClickPipes",
      description: "Setup steps to expose a private MSK via MSK multi-VPC connectivity to ClickPipes.",
      href: "/resources/support-center/knowledge-base/cloud-services/aws-privatelink-setup-for-msk-clickpipes",
      category: "Cloud",
      tags: ["Security and Authentication", "Managing Cloud"]
    },
    {
      id: "cloud-services/aws-privatelink-setup-for-clickpipes",
      title: "AWS PrivateLink setup to expose private RDS for ClickPipes",
      description: "Setup steps to expose a private RDS via AWS PrivateLink to ClickPipes.",
      href: "/resources/support-center/knowledge-base/cloud-services/aws-privatelink-setup-for-clickpipes",
      category: "Cloud",
      tags: ["Security and Authentication", "Managing Cloud"]
    },
    {
      id: "data-management/backing-up-a-specific-partition",
      title: "Backing up a specific partition",
      description: "How can I backup a specific partition in ClickHouse?",
      href: "/resources/support-center/knowledge-base/data-management/backing-up-a-specific-partition",
      category: "Data management",
      tags: ["Managing Data"]
    },
    {
      id: "general-faqs/key-value",
      title: "Can I use ClickHouse as a key-value storage?",
      description: "Answers the frequently asked question of whether or not ClickHouse can be used as a key-value storage?",
      href: "/resources/support-center/knowledge-base/general-faqs/key-value",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/time-series",
      title: "Can I use ClickHouse as a time-series database?",
      description: "Page describing how to use ClickHouse as a time-series database",
      href: "/resources/support-center/knowledge-base/general-faqs/time-series",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "queries-sql/pivot",
      title: "Can you PIVOT in ClickHouse?",
      description:
        "ClickHouse doesn't have a PIVOT clause, but we can get close to this functionality using aggregate function combinators. Let's see how to do this using the UK housing prices dataset.",
      href: "/resources/support-center/knowledge-base/queries-sql/pivot",
      category: "Queries & SQL",
      tags: ["Data Modelling", "Core Data Concepts"]
    },
    {
      id: "general-faqs/vector-search",
      title: "Can you use ClickHouse for vector search?",
      description: "Learn how to use ClickHouse for vector search, including storing embeddings and searching with distance functions like cosine similarity.",
      href: "/resources/support-center/knowledge-base/general-faqs/vector-search",
      category: "General & FAQs",
      tags: ["Use Cases", "Concepts"]
    },
    {
      id: "monitoring-debugging/send-logs-level",
      title: "Capturing server logs of queries at the client",
      description: "Learn how to capture server logs at the client level, even with different log settings, using the `send_logs_level` client setting.",
      href: "/resources/support-center/knowledge-base/monitoring-debugging/send-logs-level",
      category: "Monitoring & debugging",
      tags: ["Server Admin"]
    },
    {
      id: "configuration-settings/change-the-prompt-in-clickhouse-client",
      title: "Change the prompt in clickhouse-client",
      description: "This article explains how to change the prompt in your Clickhouse client and clickhouse-local terminal window from :) to a prefix followed by :)",
      href: "/resources/support-center/knowledge-base/configuration-settings/change-the-prompt-in-clickhouse-client",
      category: "Configuration & settings",
      tags: ["Settings", "Native Clients and Interfaces"]
    },
    {
      id: "security/common-rbac-queries",
      title: "Common RBAC queries",
      description: "Queries to help grant specific permissions to users.",
      href: "/resources/support-center/knowledge-base/security/common-rbac-queries",
      category: "Security & access control",
      tags: ["Security and Authentication", "Managing Cloud"]
    },
    {
      id: "queries-sql/comparing-metrics-between-queries",
      title: "Comparing metrics between queries in decibels",
      description: "A query to compare metrics between two queries in ClickHouse.",
      href: "/resources/support-center/knowledge-base/queries-sql/comparing-metrics-between-queries",
      category: "Queries & SQL",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "configuration-settings/configure-cap-ipc-lock-and-cap-sys-nice-in-docker",
      title: "Configuring CAP_IPC_LOCK and CAP_SYS_NICE Capabilities in Docker",
      description: "Learn how to resolve Docker capability warnings for `CAP_IPC_LOCK` and `CAP_SYS_NICE` when running ClickHouse in a container.",
      href: "/resources/support-center/knowledge-base/configuration-settings/configure-cap-ipc-lock-and-cap-sys-nice-in-docker",
      category: "Configuration & settings",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "troubleshooting/configure-cap-ipc-lock-and-cap-sys-nice-in-docker",
      title: "Configuring CAP_IPC_LOCK and CAP_SYS_NICE Capabilities in Docker",
      description: "Learn how to resolve Docker capability warnings for `CAP_IPC_LOCK` and `CAP_SYS_NICE` when running ClickHouse in a container.",
      href: "/resources/support-center/knowledge-base/troubleshooting/configure-cap-ipc-lock-and-cap-sys-nice-in-docker",
      category: "Troubleshooting & errors",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "cloud-services/custom-dns-alias-for-instance",
      title: "Create a custom DNS alias by setting up a reverse proxy",
      description: "Learn how to set up a custom DNS alias for your instance using a reverse proxy",
      href: "/resources/support-center/knowledge-base/cloud-services/custom-dns-alias-for-instance",
      category: "Cloud",
      tags: ["Server Admin", "Security and Authentication"]
    },
    {
      id: "troubleshooting/part-intersects-previous-part",
      title: "DB::Exception: Part XXXXX intersects previous part YYYYY. It is a bug or a result of manual intervention in the ZooKeeper data.",
      description:
        "This article explains how to resolve the DB::Exception error related to intersecting parts in ClickHouse, often caused by a race condition or manual intervention in the ZooKeeper data.",
      href: "/resources/support-center/knowledge-base/troubleshooting/part-intersects-previous-part",
      category: "Troubleshooting & errors",
      tags: ["Errors and Exceptions", "System Tables"]
    },
    {
      id: "setup-installation/difference-between-official-builds-and-3rd-party",
      title: "Differences Between Official and 3rd-Party ClickHouse Builds",
      description: "Understand the key differences between official ClickHouse builds and 3rd-party builds, including updates, compatibility, and security considerations.",
      href: "/resources/support-center/knowledge-base/setup-installation/difference-between-official-builds-and-3rd-party",
      category: "Setup & installation",
      tags: ["Concepts"]
    },
    {
      id: "general-faqs/cost-based",
      title: "Does ClickHouse have a cost-based optimizer",
      description: "ClickHouse has certain cost-based optimization mechanics",
      href: "/resources/support-center/knowledge-base/general-faqs/cost-based",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/datalake",
      title: "Does ClickHouse support data lakes?",
      description: "ClickHouse supports data lakes, including Iceberg, Delta Lake, Apache Hudi, Apache Paimon, Hive",
      href: "/resources/support-center/knowledge-base/general-faqs/datalake",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/distributed-join",
      title: "Does ClickHouse support distributed JOIN?",
      description: "ClickHouse supports distributed JOIN",
      href: "/resources/support-center/knowledge-base/general-faqs/distributed-join",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/federated",
      title: "Does ClickHouse support federated queries?",
      description: "ClickHouse supports a wide range for federated and hybrid queries",
      href: "/resources/support-center/knowledge-base/general-faqs/federated",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/concurrency",
      title: "Does ClickHouse support frequent, concurrent queries?",
      description: "ClickHouse supports high QPS and high concurrency",
      href: "/resources/support-center/knowledge-base/general-faqs/concurrency",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "cloud-services/multi-region-replication",
      title: "Does ClickHouse support multi-region replication?",
      description: "This page answers whether ClickHouse supports multi-region replication",
      href: "/resources/support-center/knowledge-base/cloud-services/multi-region-replication",
      category: "Cloud",
      tags: []
    },
    {
      id: "general-faqs/updates",
      title: "Does ClickHouse support real-time updates?",
      description: "ClickHouse supports lightweight real-time updates",
      href: "/resources/support-center/knowledge-base/general-faqs/updates",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "security/row-column-policy",
      title: "Does ClickHouse support row-level and column-level security?",
      description: "Learn about row-level and column-level access restrictions in ClickHouse and ClickHouse Cloud, and how to implement role-based access control (RBAC) with policies.",
      href: "/resources/support-center/knowledge-base/security/row-column-policy",
      category: "Security & access control",
      tags: ["Security and Authentication"]
    },
    {
      id: "cloud-services/execute-system-queries-in-cloud",
      title: "Execute Instruções SYSTEM em Todos os Nós no ClickHouse Cloud",
      description: "Learn how to use `ON CLUSTER` and `clusterAllReplicas` to execute SYSTEM statements and queries across all nodes in a ClickHouse Cloud service.",
      href: "/resources/support-center/knowledge-base/cloud-services/execute-system-queries-in-cloud",
      category: "Cloud",
      tags: ["Deployments and Scaling"]
    },
    {
      id: "troubleshooting/count-parts-by-type",
      title: "Find counts and sizes of wide or compact parts",
      description: "This knowledgebase article shows you how to find part counts by the type of part - wide or compact.",
      href: "/resources/support-center/knowledge-base/troubleshooting/count-parts-by-type",
      category: "Troubleshooting & errors",
      tags: ["Troubleshooting"]
    },
    {
      id: "troubleshooting/fix-developer-verification-error-in-macos",
      title: "Corrija o Erro de Verificação de Desenvolvedor no macOS",
      description: "Learn how to resolve the MacOS developer verification error when running ClickHouse commands, using either System Settings or the terminal.",
      href: "/resources/support-center/knowledge-base/troubleshooting/fix-developer-verification-error-in-macos",
      category: "Troubleshooting & errors",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "data-import-export/s3-export-data-year-month-folders",
      title: "How can I do partitioned writes by year and month on S3?",
      description: "Learn how to write partitioned data by year and month to an S3 bucket in ClickHouse, using a custom path structure for organizing the data.",
      href: "/resources/support-center/knowledge-base/data-import-export/s3-export-data-year-month-folders",
      category: "Data import & export",
      tags: ["Data Export", "Native Clients and Interfaces"]
    },
    {
      id: "data-import-export/kafka-clickhouse-json",
      title: "Como posso usar o novo tipo de dado JSON com o Kafka?",
      description: "Learn how to load JSON messages from Apache Kafka directly into a single JSON column in ClickHouse using the Kafka table engine and JSON data type.",
      href: "/resources/support-center/knowledge-base/data-import-export/kafka-clickhouse-json",
      category: "Data import & export",
      tags: ["Data Formats", "Data Ingestion"]
    },
    {
      id: "cloud-services/change-billing-email",
      title: "Como altero meu contato de faturamento no ClickHouse Cloud?",
      description: "Let's learn how to change your billing address in ClickHouse Cloud.",
      href: "/resources/support-center/knowledge-base/cloud-services/change-billing-email",
      category: "Cloud",
      tags: ["Managing Cloud"]
    },
    {
      id: "general-faqs/how-do-i-contribute-code-to-clickhouse",
      title: "How do I contribute code to ClickHouse?",
      description: "ClickHouse is an open-source project developed on GitHub. As customary, contribution instructions are published in CONTRIBUTING file in the root of the source code repository.",
      href: "/resources/support-center/knowledge-base/general-faqs/how-do-i-contribute-code-to-clickhouse",
      category: "General & FAQs",
      tags: ["Community"]
    },
    {
      id: "data-import-export/parquet-to-csv-json",
      title: "Como converter arquivos de Parquet para CSV ou JSON?",
      description: "Learn how to use ClickHouse's `clickhouse-local` tool to easily convert Parquet files to CSV or JSON formats.",
      href: "/resources/support-center/knowledge-base/data-import-export/parquet-to-csv-json",
      category: "Data import & export",
      tags: ["Data Sources", "Data Formats"]
    },
    {
      id: "data-import-export/mysql-to-parquet-csv-json",
      title: "Como exportar dados do MySQL para Parquet, CSV ou JSON usando o ClickHouse",
      description: "Learn how to use the `clickhouse-local` tool to export MySQL data into formats like Parquet, CSV, or JSON quickly and efficiently.",
      href: "/resources/support-center/knowledge-base/data-import-export/mysql-to-parquet-csv-json",
      category: "Data import & export",
      tags: ["Data Formats", "Data Export"]
    },
    {
      id: "data-import-export/postgresql-to-parquet-csv-json",
      title: "How do I export PostgreSQL data to Parquet, CSV or JSON?",
      description: "Learn how to export PostgreSQL data to Parquet, CSV, or JSON formats using `clickhouse-local` with various examples.",
      href: "/resources/support-center/knowledge-base/data-import-export/postgresql-to-parquet-csv-json",
      category: "Data import & export",
      tags: ["Data Export", "Data Formats"]
    },
    {
      id: "setup-installation/install-clickhouse-windows10",
      title: "Como instalar o ClickHouse no Windows 10?",
      description: "Learn how to install and test ClickHouse on Windows 10 using WSL 2. Includes setup, troubleshooting, and running a test environment.",
      href: "/resources/support-center/knowledge-base/setup-installation/install-clickhouse-windows10",
      category: "Setup & installation",
      tags: ["Tools and Utilities"]
    },
    {
      id: "security/remove-default-user",
      title: "How do I remove the default user?",
      description: "Learn how to remove the default user when running ClickHouse Server.",
      href: "/resources/support-center/knowledge-base/security/remove-default-user",
      category: "Security & access control",
      tags: ["Server Admin"]
    },
    {
      id: "cloud-services/ingest-failures-23-9-release",
      title: "Como resolver falhas de ingestão após o lançamento do ClickHouse 23.9?",
      description: "Learn how to resolve ingest failures caused by stricter grant checking introduced in ClickHouse 23.9 for tables using `async_inserts`. Update grants to fix errors.",
      href: "/resources/support-center/knowledge-base/cloud-services/ingest-failures-23-9-release",
      category: "Cloud",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "performance-optimization/insert-select-settings-tuning",
      title: "How do I solve TOO MANY PARTS error during an INSERT...SELECT?",
      description: "Resolve the TOO_MANY_PARTS error in ClickHouse during an `INSERT...SELECT` by tuning expert-level settings for larger blocks and increasing partition thresholds.",
      href: "/resources/support-center/knowledge-base/performance-optimization/insert-select-settings-tuning",
      category: "Performance & optimization",
      tags: ["Settings", "Errors and Exceptions"]
    },
    {
      id: "integrations/node-js-example",
      title: "How do I use NodeJS with @clickhouse/client",
      description: "Learn how to use @clickhouse/client in a Node.js application to interact with ClickHouse and perform queries.",
      href: "/resources/support-center/knowledge-base/integrations/node-js-example",
      category: "Integrations & client libraries",
      tags: ["Language Clients"]
    },
    {
      id: "monitoring-debugging/view-number-of-active-mutations",
      title: "How do I view the number of active or queued mutations?",
      description:
        "Monitor the number of active or queued mutations in ClickHouse, especially when performing `ALTER` or `UPDATE` operations. Use the `system.mutations` table for tracking mutations.",
      href: "/resources/support-center/knowledge-base/monitoring-debugging/view-number-of-active-mutations",
      category: "Monitoring & debugging",
      tags: ["System Tables"]
    },
    {
      id: "data-management/read-consistency",
      title: "How to achieve data read consistency in ClickHouse?",
      description: "Learn how to ensure data consistency when reading from ClickHouse, whether you're connected to the same node or a random node.",
      href: "/resources/support-center/knowledge-base/data-management/read-consistency",
      category: "Data management",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "setup-installation/llvm-clang-up-to-date",
      title: "How to build LLVM and clang on Linux",
      description: "Commands to build LLVM and clang on Linux.",
      href: "/resources/support-center/knowledge-base/setup-installation/llvm-clang-up-to-date",
      category: "Setup & installation",
      tags: ["Community", "Tools and Utilities"]
    },
    {
      id: "data-management/calculate-ratio-of-zero-sparse-serialization",
      title: "How to calculate the ratio of empty/zero values in every column in a table",
      description: "Learn how to calculate the ratio of empty or zero values in every column of a ClickHouse table to optimize sparse column serialization.",
      href: "/resources/support-center/knowledge-base/data-management/calculate-ratio-of-zero-sparse-serialization",
      category: "Data management",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "security/check-users-roles",
      title: "How to Check Users Assigned to Roles and Vice Versa",
      description: "Learn how to query ClickHouse's `system.role_grants` to find users assigned to roles and roles assigned to specific users.",
      href: "/resources/support-center/knowledge-base/security/check-users-roles",
      category: "Security & access control",
      tags: ["Server Admin", "System Tables", "Managing Cloud"]
    },
    {
      id: "monitoring-debugging/which-processes-are-currently-running",
      title: "How to check what code is currently running on a server?",
      description:
        "ClickHouse provides introspection tools like `system.stack_trace` for inspecting what code is currently running on each server thread, helping with debugging and performance monitoring.",
      href: "/resources/support-center/knowledge-base/monitoring-debugging/which-processes-are-currently-running",
      category: "Monitoring & debugging",
      tags: ["Server Admin"]
    },
    {
      id: "cloud-services/how-to-check-my-clickhouse-cloud-sevice-state",
      title: "Como verificar o estado do seu serviço ClickHouse Cloud",
      description: "Learn how to use the ClickHouse Cloud API to check if your service is stopped, idle, or running without waking it up.",
      href: "/resources/support-center/knowledge-base/cloud-services/how-to-check-my-clickhouse-cloud-sevice-state",
      category: "Cloud",
      tags: ["Managing Cloud"]
    },
    {
      id: "configuration-settings/configure-a-user-setting",
      title: "Como configurar definições para um usuário no ClickHouse",
      description: "Learn how to define settings in ClickHouse for individual queries, client sessions, or specific users using `SET` and `ALTER USER` commands.",
      href: "/resources/support-center/knowledge-base/configuration-settings/configure-a-user-setting",
      category: "Configuration & settings",
      tags: ["Settings"]
    },
    {
      id: "materialized-views/projection-example",
      title: "How to confirm if a Projection is used by the query?",
      description: "Learn how to check if a projection is used in ClickHouse queries by testing with sample data and using EXPLAIN to verify projection usage.",
      href: "/resources/support-center/knowledge-base/materialized-views/projection-example",
      category: "Materialized views & projections",
      tags: ["Data Modelling"]
    },
    {
      id: "cloud-services/how-to-connect-to-ch-cloud-using-ssh-keys",
      title: "Como conectar ao ClickHouse usando chaves SSH",
      description: "How to connect to ClickHouse and ClickHouse Cloud using SSH Keys",
      href: "/resources/support-center/knowledge-base/cloud-services/how-to-connect-to-ch-cloud-using-ssh-keys",
      category: "Cloud",
      tags: ["Managing Cloud", "Security and Authentication"]
    },
    {
      id: "data-management/dictionary-using-strings",
      title: "How to Create a ClickHouse Dictionary with String Keys and Values",
      description: "Learn how to create a ClickHouse dictionary using string keys and values from a MergeTree table as the source, with examples of setup and usage.",
      href: "/resources/support-center/knowledge-base/data-management/dictionary-using-strings",
      category: "Data management",
      tags: ["Data Modelling"]
    },
    {
      id: "tables-schema/how-to-create-table-to-query-multiple-remote-clusters",
      title: "How to create a table that can query multiple remote clusters",
      description: "How to create a table that can query multiple remote clusters",
      href: "/resources/support-center/knowledge-base/tables-schema/how-to-create-table-to-query-multiple-remote-clusters",
      category: "Tables & schema",
      tags: ["Deployments and Scaling"]
    },
    {
      id: "setup-installation/enabling-ssl-with-lets-encrypt",
      title: "Como Habilitar SSL com Let's Encrypt em um Único Servidor ClickHouse",
      description: "Learn how to set up SSL for a single ClickHouse server using Let's Encrypt, including certificate issuance, configuration, and validation.",
      href: "/resources/support-center/knowledge-base/setup-installation/enabling-ssl-with-lets-encrypt",
      category: "Setup & installation",
      tags: ["Security and Authentication"]
    },
    {
      id: "data-import-export/file-export",
      title: "Como Exportar Dados do ClickHouse para um Arquivo",
      description: "Learn various methods to export data from ClickHouse, including `INTO OUTFILE`, the File table engine, and command-line redirection.",
      href: "/resources/support-center/knowledge-base/data-import-export/file-export",
      category: "Data import & export",
      tags: ["Data Export"]
    },
    {
      id: "queries-sql/how-to-filter-a-clickhouse-table-by-an-array-column",
      title: "How to filter a ClickHouse table by an array-column?",
      description: "Knowledgebase article on how to filter a ClickHouse table by an array-column.",
      href: "/resources/support-center/knowledge-base/queries-sql/how-to-filter-a-clickhouse-table-by-an-array-column",
      category: "Queries & SQL",
      tags: ["Data Modelling", "Functions"]
    },
    {
      id: "monitoring-debugging/generate-har-file",
      title: "Como Gerar um Arquivo HAR para Suporte",
      description: "A HAR (HTTP Archive) file captures the network activity in your browser. It can help our support team diagnose slow page loads, failed requests, or other network issues.",
      href: "/resources/support-center/knowledge-base/monitoring-debugging/generate-har-file",
      category: "Monitoring & debugging",
      tags: ["Tools and Utilities"]
    },
    {
      id: "materialized-views/how-to-display-queries-using-mv",
      title: "How to Identify Queries Using Materialized Views in ClickHouse",
      description: "Learn how to query ClickHouse logs to identify all queries involving Materialized Views within a specified time range.",
      href: "/resources/support-center/knowledge-base/materialized-views/how-to-display-queries-using-mv",
      category: "Materialized views & projections",
      tags: ["System Tables"]
    },
    {
      id: "performance-optimization/find-expensive-queries",
      title: "Como Identificar as Consultas Mais Custosas no ClickHouse",
      description: "Learn how to use the `query_log` table in ClickHouse to identify the most memory and CPU-intensive queries across distributed nodes.",
      href: "/resources/support-center/knowledge-base/performance-optimization/find-expensive-queries",
      category: "Performance & optimization",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "configuration-settings/ignoring-incorrect-settings",
      title: "Como Ignorar Configurações Incorretas no ClickHouse",
      description: "Learn how to use the `skip_check_for_incorrect_settings` option to allow ClickHouse to start even when user-level settings are specified incorrectly.",
      href: "/resources/support-center/knowledge-base/configuration-settings/ignoring-incorrect-settings",
      category: "Configuration & settings",
      tags: ["Settings"]
    },
    {
      id: "data-import-export/json-import",
      title: "How to import JSON into ClickHouse?",
      description: "This page shows you how to import JSON into ClickHouse",
      href: "/resources/support-center/knowledge-base/data-import-export/json-import",
      category: "Data import & export",
      tags: []
    },
    {
      id: "setup-installation/how-to-increase-thread-pool-size",
      title: "Como Aumentar o Número de Threads no ClickHouse",
      description: "Learn how to configure the Global Thread pool in ClickHouse by adjusting settings like `max_thread_pool_size`, `thread_pool_queue_size`, and `max_thread_pool_free_size`.",
      href: "/resources/support-center/knowledge-base/setup-installation/how-to-increase-thread-pool-size",
      category: "Setup & installation",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "data-import-export/kafka-to-clickhouse-setup",
      title: "Como Realizar a Ingestão de Dados do Kafka no ClickHouse",
      description: "Learn how to ingest data from a Kafka topic into ClickHouse using the Kafka table engine, materialized views, and MergeTree tables.",
      href: "/resources/support-center/knowledge-base/data-import-export/kafka-to-clickhouse-setup",
      category: "Data import & export",
      tags: ["Data Ingestion"]
    },
    {
      id: "data-import-export/ingest-parquet-files-in-s3",
      title: "Como realizar a ingestão de arquivos Parquet de um bucket S3",
      description: "Learn the basics of using the S3 table engine in ClickHouse to ingest and query Parquet files from an S3 bucket, including setup, access permissions, and data import examples.",
      href: "/resources/support-center/knowledge-base/data-import-export/ingest-parquet-files-in-s3",
      category: "Data import & export",
      tags: ["Data Ingestion"]
    },
    {
      id: "queries-sql/how-to-insert-all-rows-from-another-table",
      title: "How to insert all rows from one table to another?",
      description: "Knowledgebase article on how to insert all rows from one table to another.",
      href: "/resources/support-center/knowledge-base/queries-sql/how-to-insert-all-rows-from-another-table",
      category: "Queries & SQL",
      tags: ["Data Ingestion"]
    },
    {
      id: "performance-optimization/check-query-processing-time-only",
      title: "How to Measure Query Processing Time Without Returning Rows",
      description: "Learn how to use the `FORMAT Null` option in ClickHouse to measure query processing time without returning any rows to the client.",
      href: "/resources/support-center/knowledge-base/performance-optimization/check-query-processing-time-only",
      category: "Performance & optimization",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "monitoring-debugging/outputSendLogsLevelTracesToFile",
      title: "How to output send logs level traces to file using the clickhouse-client",
      description: "How to output send logs level traces to file using the clickhouse-client",
      href: "/resources/support-center/knowledge-base/monitoring-debugging/outputSendLogsLevelTracesToFile",
      category: "Monitoring & debugging",
      tags: ["Data Export"]
    },
    {
      id: "tables-schema/recreate-table-across-terminals",
      title: "How to quickly recreate a small table across different terminals",
      description: "Learn how to quickly recreate a small table and its data across different terminals using copy/paste for development environments.",
      href: "/resources/support-center/knowledge-base/tables-schema/recreate-table-across-terminals",
      category: "Tables & schema",
      tags: ["Tools and Utilities"]
    },
    {
      id: "integrations/how-to-set-up-ch-on-docker-odbc-connect-mssql",
      title: "How to set up ClickHouse on Docker with ODBC to connect to a Microsoft SQL Server (MSSQL) database",
      description: "How to set up ClickHouse on Docker with ODBC to connect to a Microsoft SQL Server (MSSQL) database",
      href: "/resources/support-center/knowledge-base/integrations/how-to-set-up-ch-on-docker-odbc-connect-mssql",
      category: "Integrations & client libraries",
      tags: ["Native Clients and Interfaces"]
    },
    {
      id: "queries-sql/using-array-join-to-extract-and-query-attributes",
      title: "How to use array join to extract and query varying attributes using map keys and values",
      description: "Exemplo simples para ilustrar como usar array join para extrair e consultar atributos variados usando chaves e valores de mapa",
      href: "/resources/support-center/knowledge-base/queries-sql/using-array-join-to-extract-and-query-attributes",
      category: "Queries & SQL",
      tags: ["Functions"]
    },
    {
      id: "materialized-views/how-to-use-parametrised-views",
      title: "Como usar Views Parametrizadas no ClickHouse",
      description: "Aprenda a criar e consultar views parametrizadas no ClickHouse para fatiamento dinâmico de dados com base em parâmetros de tempo de consulta.",
      href: "/resources/support-center/knowledge-base/materialized-views/how-to-use-parametrised-views",
      category: "Materialized views & projections",
      tags: ["Use Cases"]
    },
    {
      id: "tables-schema/exchangeStatementToSwitchTables",
      title: "Como usar o comando exchange para trocar tabelas",
      description: "Como usar o comando exchange para trocar tabelas",
      href: "/resources/support-center/knowledge-base/tables-schema/exchangeStatementToSwitchTables",
      category: "Tables & schema",
      tags: ["Managing Data"]
    },
    {
      id: "queries-sql/compare-resultsets",
      title: "Como Validar se Duas Consultas Retornam os Mesmos Conjuntos de Resultados",
      description: "Aprenda a validar que duas consultas do ClickHouse produzem conjuntos de resultados idênticos usando funções de hash e técnicas de comparação.",
      href: "/resources/support-center/knowledge-base/queries-sql/compare-resultsets",
      category: "Queries & SQL",
      tags: ["Functions"]
    },
    {
      id: "monitoring-debugging/check-query-cache-in-use",
      title: "Como verificar o uso do cache de consultas no ClickHouse",
      description: "Aprenda a verificar se o cache de consultas está sendo utilizado no ClickHouse usando logs de rastreamento do `clickhouse-client` ou comandos SQL.",
      href: "/resources/support-center/knowledge-base/monitoring-debugging/check-query-cache-in-use",
      category: "Monitoring & debugging",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "cloud-services/unable-to-access-cloud-service",
      title: "Não consigo acessar um serviço do ClickHouse Cloud",
      description: "Solução de problemas de acesso a serviços do ClickHouse Cloud, incluindo configuração da Lista de Acesso por IP",
      href: "/resources/support-center/knowledge-base/cloud-services/unable-to-access-cloud-service",
      category: "Cloud",
      tags: ["Errors and Exceptions", "Managing Cloud"]
    },
    {
      id: "performance-optimization/finding-expensive-queries-by-memory-usage",
      title: "Identificando Consultas Custosas por Uso de Memória no ClickHouse",
      description: "Aprenda a usar a tabela `system.query_log` para encontrar as consultas que mais consomem memória no ClickHouse, com exemplos para configurações em cluster e standalone.",
      href: "/resources/support-center/knowledge-base/performance-optimization/finding-expensive-queries-by-memory-usage",
      category: "Performance & optimization",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "data-import-export/importing-and-working-with-json-array-objects",
      title: "Importando e Consultando Objetos de Array JSON no ClickHouse",
      description: "Aprenda a importar objetos de array JSON no ClickHouse e realizar consultas avançadas usando funções JSON e operações de array.",
      href: "/resources/support-center/knowledge-base/data-import-export/importing-and-working-with-json-array-objects",
      category: "Data import & export",
      tags: ["Data Formats"]
    },
    {
      id: "data-import-export/importing-geojason-with-nested-object-array",
      title: "Importando GeoJSON com um array de objetos profundamente aninhado",
      description: "Aprenda a importar arquivos GeoJSON com arrays de objetos profundamente aninhados no ClickHouse e consultar os dados de recursos aninhados.",
      href: "/resources/support-center/knowledge-base/data-import-export/importing-geojason-with-nested-object-array",
      category: "Data import & export",
      tags: ["Data Formats"]
    },
    {
      id: "performance-optimization/improve-map-performance",
      title: "Melhorando o desempenho de busca em Map no ClickHouse",
      description: "Aprenda a otimizar buscas em colunas Map no ClickHouse para melhor desempenho de consultas materializando chaves específicas como colunas independentes.",
      href: "/resources/support-center/knowledge-base/performance-optimization/improve-map-performance",
      category: "Performance & optimization",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "tables-schema/delete-old-data",
      title: "É possível excluir registros antigos de uma tabela do ClickHouse?",
      description: "Esta página responde à pergunta sobre se é possível excluir registros antigos de uma tabela do ClickHouse",
      href: "/resources/support-center/knowledge-base/tables-schema/delete-old-data",
      category: "Tables & schema",
      tags: []
    },
    {
      id: "general-faqs/separate-storage",
      title: "É possível implantar o ClickHouse com armazenamento e computação separados?",
      description: "Esta página fornece uma resposta sobre se é possível implantar o ClickHouse com armazenamento e computação separados",
      href: "/resources/support-center/knowledge-base/general-faqs/separate-storage",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "data-import-export/json-extract-example",
      title: "Exemplo de extração JSON",
      description: "Um exemplo curto de como extrair tipos base de JSON",
      href: "/resources/support-center/knowledge-base/data-import-export/json-extract-example",
      category: "Data import & export",
      tags: ["Data Formats"]
    },
    {
      id: "queries-sql/calculate-pi-using-sql",
      title: "Vamos calcular pi usando SQL",
      description: "É o Dia do Pi! Vamos calcular pi usando o SQL do ClickHouse",
      href: "/resources/support-center/knowledge-base/queries-sql/calculate-pi-using-sql",
      category: "Queries & SQL",
      tags: ["Use Cases"]
    },
    {
      id: "cloud-services/clickhouse-cloud-api-usage",
      title: "Gerenciando o Serviço do ClickHouse Cloud com API e cURL",
      description: "Aprenda a iniciar, parar e retomar um serviço do ClickHouse Cloud usando endpoints de API e comandos cURL.",
      href: "/resources/support-center/knowledge-base/cloud-services/clickhouse-cloud-api-usage",
      category: "Cloud",
      tags: ["Managing Cloud", "Tools and Utilities"]
    },
    {
      id: "monitoring-debugging/mapping-of-system-metrics-to-prometheus-metrics",
      title: "Mapeamento de métricas usadas em system.dashboards para métricas do Prometheus em `system.custom_metrics`",
      description: "Mapeamento de métricas usadas em system.dashboards para métricas do Prometheus em system.custom_metrics",
      href: "/resources/support-center/knowledge-base/monitoring-debugging/mapping-of-system-metrics-to-prometheus-metrics",
      category: "Monitoring & debugging",
      tags: ["System Tables"]
    },
    {
      id: "security/windows-active-directory-to-ch-roles",
      title: "Mapeando grupos de segurança do Windows Active Directory para funções do ClickHouse",
      description: "Exemplo de mapeamento de grupos de segurança do Windows Active Directory para funções do ClickHouse",
      href: "/resources/support-center/knowledge-base/security/windows-active-directory-to-ch-roles",
      category: "Security & access control",
      tags: ["Tools and Utilities"]
    },
    {
      id: "performance-optimization/memory-limit-exceeded-for-query",
      title: "Limite de memória excedido para consulta",
      description: "Solução de problemas de erros de limite de memória excedido para uma consulta",
      href: "/resources/support-center/knowledge-base/performance-optimization/memory-limit-exceeded-for-query",
      category: "Performance & optimization",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "integrations/ODBC-authentication-failed-error-using-PowerBI-CH-connector",
      title: "Erro de falha de autenticação ODBC ao usar o conector Power BI do ClickHouse",
      description: "Erro de falha de autenticação ODBC ao usar o conector Power BI do ClickHouse",
      href: "/resources/support-center/knowledge-base/integrations/ODBC-authentication-failed-error-using-PowerBI-CH-connector",
      category: "Integrations & client libraries",
      tags: ["Native Clients and Interfaces", "Errors and Exceptions"]
    },
    {
      id: "monitoring-debugging/profiling-clickhouse-with-llvm-xray",
      title: "Perfilando o ClickHouse com o XRay do LLVM",
      description: "Aprenda a perfilar o ClickHouse usando o profiler de instrumentação XRay do LLVM, visualizar rastreamentos e analisar desempenho.",
      href: "/resources/support-center/knowledge-base/monitoring-debugging/profiling-clickhouse-with-llvm-xray",
      category: "Monitoring & debugging",
      tags: ["Performance and Optimizations", "Tools and Utilities"]
    },
    {
      id: "integrations/python-http-requests",
      title: "Python quick example using HTTP requests module",
      description: "An example using Python and requests module to write and read to ClickHouse",
      href: "/resources/support-center/knowledge-base/integrations/python-http-requests",
      category: "Integrations & client libraries",
      tags: ["Native Clients and Interfaces"]
    },
    {
      id: "configuration-settings/maximum-number-of-tables-and-databases",
      title: "Recommended Maximum Databases, Tables, Partitions, and Parts in ClickHouse",
      description: "Learn the recommended maximum limits for databases, tables, partitions, and parts in a ClickHouse cluster to ensure optimal performance.",
      href: "/resources/support-center/knowledge-base/configuration-settings/maximum-number-of-tables-and-databases",
      category: "Configuration & settings",
      tags: ["Performance and Optimizations", "Deployments and Scaling"]
    },
    {
      id: "data-import-export/cannot-append-data-to-parquet-format",
      title: 'Resolving "Cannot Append Data in Parquet Format" error in ClickHouse',
      description: 'Are you getting the error "Cannot append data in format Parquet to file" error in ClickHouse? Let\'s take a look at how to resolve it.',
      href: "/resources/support-center/knowledge-base/data-import-export/cannot-append-data-to-parquet-format",
      category: "Data import & export",
      tags: ["Errors and Exceptions", "Data Formats"]
    },
    {
      id: "troubleshooting/exception-too-many-parts",
      title: 'Resolving "Too Many Parts" error in ClickHouse',
      description: 'Learn how to address the "Too many parts" error in ClickHouse by optimizing insert rates, configuring MergeTree settings, and managing partitions effectively.',
      href: "/resources/support-center/knowledge-base/troubleshooting/exception-too-many-parts",
      category: "Troubleshooting & errors",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "troubleshooting/certificate-verify-failed-error",
      title: "Resolving SSL Certificate Verify Error in ClickHouse",
      description: "Learn how to resolve the SSL Exception CERTIFICATE_VERIFY_FAILED error.",
      href: "/resources/support-center/knowledge-base/troubleshooting/certificate-verify-failed-error",
      category: "Troubleshooting & errors",
      tags: ["Security and Authentication", "Errors and Exceptions"]
    },
    {
      id: "troubleshooting/connection-timeout-remote-remoteSecure",
      title: "Resolving Timeout Errors with `remote` and `remoteSecure` Table Functions",
      description: "Learn how to fix timeout errors when using `remote` or `remoteSecure` table functions in ClickHouse by adjusting the connection timeout settings.",
      href: "/resources/support-center/knowledge-base/troubleshooting/connection-timeout-remote-remoteSecure",
      category: "Troubleshooting & errors",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "tables-schema/search-across-node-for-tables-with-a-wildcard",
      title: "Searching across nodes for tables with a wildcard",
      description: "Learn how to search across nodes for tables with a wildcard.",
      href: "/resources/support-center/knowledge-base/tables-schema/search-across-node-for-tables-with-a-wildcard",
      category: "Tables & schema",
      tags: ["Deployments and Scaling"]
    },
    {
      id: "performance-optimization/query-max-execution-time",
      title: "Setting a limit on query execution time",
      description: "How to enforce limit on max query execution time",
      href: "/resources/support-center/knowledge-base/performance-optimization/query-max-execution-time",
      category: "Performance & optimization",
      tags: ["Managing Cloud", "Settings"]
    },
    {
      id: "data-import-export/json-simple-example",
      title: "Simple example flow for extracting JSON data using a landing table with a Materialized View",
      description: "Simple example flow for extracting JSON data using a landing table with a Materialized View",
      href: "/resources/support-center/knowledge-base/data-import-export/json-simple-example",
      category: "Data import & export",
      tags: ["Data Formats"]
    },
    {
      id: "performance-optimization/async-vs-optimize-read-in-order",
      title: "Synchronous data reading",
      description:
        "The new setting `allow_asynchronous_read_from_io_pool_for_merge_tree` allows the number of reading threads (streams) to be higher than the number of threads in the rest of the query execution pipeline.",
      href: "/resources/support-center/knowledge-base/performance-optimization/async-vs-optimize-read-in-order",
      category: "Performance & optimization",
      tags: ["Settings", "Performance and Optimizations"]
    },
    {
      id: "integrations/terraform-example",
      title: "Terraform example on how to use Cloud API",
      description: "This covers an example of how you can use terraform to create/delete clusters using the API",
      href: "/resources/support-center/knowledge-base/integrations/terraform-example",
      category: "Integrations & client libraries",
      tags: ["Native Clients and Interfaces"]
    },
    {
      id: "performance-optimization/tips-tricks-optimizing-basic-data-types-in-clickhouse",
      title: "Tips and tricks on optimizing basic data types in ClickHouse",
      description: "Tips and tricks on optimizing basic data types in ClickHouse",
      href: "/resources/support-center/knowledge-base/performance-optimization/tips-tricks-optimizing-basic-data-types-in-clickhouse",
      category: "Performance & optimization",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "queries-sql/useful-queries-for-troubleshooting",
      title: "Useful queries for troubleshooting",
      description: "A collection of handy queries for troubleshooting ClickHouse, including monitoring table sizes, long-running queries, and errors.",
      href: "/resources/support-center/knowledge-base/queries-sql/useful-queries-for-troubleshooting",
      category: "Queries & SQL",
      tags: ["Settings"]
    },
    {
      id: "general-faqs/use-clickhouse-for-log-analytics",
      title: "Usando o ClickHouse para análise de logs",
      description: "ClickHouse is popular for logs and metrics analysis because of the real-time analytics capabilities provided. Ready to find out more?",
      href: "/resources/support-center/knowledge-base/general-faqs/use-clickhouse-for-log-analytics",
      category: "General & FAQs",
      tags: ["Use Cases"]
    },
    {
      id: "queries-sql/filtered-aggregates",
      title: "Usando agregações filtradas no ClickHouse",
      description: "Learn how to use filtered aggregates in ClickHouse with `-If` and `-Distinct` aggregate combinators to simplify query syntax and enhance analytics.",
      href: "/resources/support-center/knowledge-base/queries-sql/filtered-aggregates",
      category: "Queries & SQL",
      tags: ["Functions"]
    },
    {
      id: "general-faqs/dependencies",
      title: "What are the 3rd-party dependencies for running ClickHouse?",
      description: "ClickHouse is self-contained and has no runtime dependencies",
      href: "/resources/support-center/knowledge-base/general-faqs/dependencies",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/dbms-naming",
      title: 'What does "ClickHouse" mean?',
      description: 'Learn about What does "ClickHouse" mean?',
      href: "/resources/support-center/knowledge-base/general-faqs/dbms-naming",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/ne-tormozit",
      title: "What does “не тормозит” mean?",
      description: 'This page explains what "Не тормозит" means',
      href: "/resources/support-center/knowledge-base/general-faqs/ne-tormozit",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "integrations/oracle-odbc",
      title: "What if I have a problem with encodings when using Oracle via ODBC?",
      description: "This page provides guidance on what to do if you have a problem with encodings when using Oracle via ODBC",
      href: "/resources/support-center/knowledge-base/integrations/oracle-odbc",
      category: "Integrations & client libraries",
      tags: []
    },
    {
      id: "general-faqs/columnar-database",
      title: "What is a columnar database?",
      description: "Um banco de dados colunar armazena os dados de cada coluna de forma independente. Isso permite ler dados do disco apenas para as colunas utilizadas em uma determinada consulta.",
      href: "/resources/support-center/knowledge-base/general-faqs/columnar-database",
      category: "General & FAQs",
      tags: ["Core Data Concepts"]
    },
    {
      id: "general-faqs/olap",
      title: "O que é OLAP?",
      description: "Uma explicação sobre o que é Processamento Analítico Online (Online Analytical Processing)",
      href: "/resources/support-center/knowledge-base/general-faqs/olap",
      category: "Geral e FAQs",
      tags: []
    },
    {
      id: "performance-optimization/optimize-final-vs-final",
      title: "Qual é a diferença entre OPTIMIZE FINAL e FINAL?",
      description: "Discute as diferenças entre OPTIMIZE FINAL e FINAL, e quando usá-los ou evitá-los.",
      href: "/resources/support-center/knowledge-base/performance-optimization/optimize-final-vs-final",
      category: "Desempenho e otimização",
      tags: ["Conceitos Principais de Dados"]
    },
    {
      id: "general-faqs/sql",
      title: "Qual sintaxe SQL o ClickHouse suporta?",
      description: "O ClickHouse suporta 100% da sintaxe SQL",
      href: "/resources/support-center/knowledge-base/general-faqs/sql",
      category: "Geral e FAQs",
      tags: []
    },
    {
      id: "data-management/when-is-ttl-applied",
      title: "Quando as regras de TTL são aplicadas e temos controle sobre isso?",
      description:
        "As regras de TTL no ClickHouse acabam sendo aplicadas em algum momento, e você pode controlar quando elas são executadas usando a configuração `merge_with_ttl_timeout`. Aprenda a forçar a aplicação de TTL e a gerenciar threads em segundo plano para a execução de TTL.",
      href: "/resources/support-center/knowledge-base/data-management/when-is-ttl-applied",
      category: "Gerenciamento de dados",
      tags: ["Conceitos Principais de Dados"]
    },
    {
      id: "setup-installation/production",
      title: "Qual versão do ClickHouse usar em produção?",
      description: "Esta página fornece orientações sobre qual versão do ClickHouse usar em produção",
      href: "/resources/support-center/knowledge-base/setup-installation/production",
      category: "Configuração e instalação",
      tags: []
    },
    {
      id: "general-faqs/who-is-using-clickhouse",
      title: "Quem está usando o ClickHouse?",
      description: "Descreve quem está usando o ClickHouse",
      href: "/resources/support-center/knowledge-base/general-faqs/who-is-using-clickhouse",
      category: "Geral e FAQs",
      tags: []
    },
    {
      id: "data-management/dictionaries-consistent-state",
      title: "Por que não consigo ver meus dados em um dicionário no ClickHouse Cloud?",
      description: "Existe um problema em que os dados em dicionários podem não ficar visíveis imediatamente após a criação.",
      href: "/resources/support-center/knowledge-base/data-management/dictionaries-consistent-state",
      category: "Gerenciamento de dados",
      tags: ["Gerenciamento do Cloud", "Modelagem de Dados"]
    },
    {
      id: "general-faqs/why-recommend-clickhouse-keeper-over-zookeeper",
      title: "Por que o ClickHouse Keeper é recomendado em vez do ZooKeeper?",
      description:
        "O ClickHouse Keeper aprimora o ZooKeeper com recursos como uso reduzido de espaço em disco, recuperação mais rápida e menor consumo de memória, oferecendo melhor desempenho para clusters do ClickHouse.",
      href: "/resources/support-center/knowledge-base/general-faqs/why-recommend-clickhouse-keeper-over-zookeeper",
      category: "Geral e FAQs",
      tags: ["Conceitos Principais de Dados"]
    },
    {
      id: "monitoring-debugging/why-default-logging-verbose",
      title: "Por que os logs do ClickHouse são tão detalhados por padrão?",
      description: "Entenda por que os desenvolvedores do ClickHouse optaram por definir um nível de log detalhado por padrão.",
      href: "/resources/support-center/knowledge-base/monitoring-debugging/why-default-logging-verbose",
      category: "Monitoramento e depuração",
      tags: ["Configurações"]
    },
    {
      id: "performance-optimization/why-is-my-primary-key-not-used",
      title: "Por que minha chave primária não é usada? Como posso verificar?",
      description: "Aborda um motivo comum pelo qual uma chave primária não é usada na ordenação e como podemos confirmar isso",
      href: "/resources/support-center/knowledge-base/performance-optimization/why-is-my-primary-key-not-used",
      category: "Desempenho e otimização",
      tags: ["Desempenho e Otimizações"]
    },
    {
      id: "general-faqs/mapreduce",
      title: "Por que não usar algo como o MapReduce?",
      description: "Esta página explica por que você usaria o ClickHouse em vez do MapReduce",
      href: "/resources/support-center/knowledge-base/general-faqs/mapreduce",
      category: "Geral e FAQs",
      tags: []
    }
  ]
}