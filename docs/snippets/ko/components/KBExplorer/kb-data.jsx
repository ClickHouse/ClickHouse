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
    "Runbooks",
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
      title: "A Python client working example for connecting to ClickHouse Cloud service",
      description: "Learn how to connect to ClickHouse Cloud Service using Python with a step-by-step example using the clickhouse-connect driver.",
      href: "/ko/resources/support-center/knowledge-base/integrations/python-clickhouse-connect-example",
      category: "Integrations & client libraries",
      tags: ["Language Clients"]
    },
    {
      id: "configuration-settings/about-quotas-and-query-complexity",
      title: "About quotas and query complexity",
      description:
        "Quotas and Query Complexity are powerful ways to limit and restrict what users can do in ClickHouse. This KB article shows examples on how to apply these two different approaches.",
      href: "/ko/resources/support-center/knowledge-base/configuration-settings/about-quotas-and-query-complexity",
      category: "Configuration & settings",
      tags: ["Managing Cloud"]
    },
    {
      id: "data-import-export/achieving-atomic-inserts",
      title: "Achieving atomic inserts and multi-table consistency in ClickHouse Cloud",
      description: "How to load data atomically and keep multiple tables consistent in ClickHouse Cloud without multi-statement transactions, using staging tables and partition-level operations.",
      href: "/ko/resources/support-center/knowledge-base/data-import-export/achieving-atomic-inserts",
      category: "Data import & export",
      tags: ["Data Ingestion", "Best Practices"]
    },
    {
      id: "tables-schema/add-column",
      title: "Adding a column to a table",
      description: "In this guide, we'll learn how to add a column to an existing table.",
      href: "/ko/resources/support-center/knowledge-base/tables-schema/add-column",
      category: "Tables & schema",
      tags: ["Data Modelling"]
    },
    {
      id: "configuration-settings/alter-user-settings-exception",
      title: "Alter user settings exception",
      description: "Handing the an exception thrown when altering user settings",
      href: "/ko/resources/support-center/knowledge-base/configuration-settings/alter-user-settings-exception",
      category: "Configuration & settings",
      tags: ["Settings", "Errors and Exceptions"]
    },
    {
      id: "materialized-views/are-materialized-views-inserted-asynchronously",
      title: "Are Materialized Views inserted synchronously?",
      description: "This KB article explores whether Materialized Views are inserted synchronously",
      href: "/ko/resources/support-center/knowledge-base/materialized-views/are-materialized-views-inserted-asynchronously",
      category: "Materialized views & projections",
      tags: ["Data Modelling"]
    },
    {
      id: "tables-schema/schema-migration-tools",
      title: "Automatic schema migration tools for ClickHouse",
      description: "Learn about automatic schema migration tools for ClickHouse and how to manage changing database schemas over time.",
      href: "/ko/resources/support-center/knowledge-base/tables-schema/schema-migration-tools",
      category: "Tables & schema",
      tags: ["Tools and Utilities"]
    },
    {
      id: "cloud-services/aws-privatelink-setup-for-msk-clickpipes",
      title: "AWS PrivateLink setup to expose MSK for ClickPipes",
      description: "Setup steps to expose a private MSK via MSK multi-VPC connectivity to ClickPipes.",
      href: "/ko/resources/support-center/knowledge-base/cloud-services/aws-privatelink-setup-for-msk-clickpipes",
      category: "Cloud",
      tags: ["Security and Authentication", "Managing Cloud"]
    },
    {
      id: "cloud-services/aws-privatelink-setup-for-clickpipes",
      title: "AWS PrivateLink setup to expose private RDS for ClickPipes",
      description: "Setup steps to expose a private RDS via AWS PrivateLink to ClickPipes.",
      href: "/ko/resources/support-center/knowledge-base/cloud-services/aws-privatelink-setup-for-clickpipes",
      category: "Cloud",
      tags: ["Security and Authentication", "Managing Cloud"]
    },
    {
      id: "cloud-services/aws-privatelink-vpc-endpoint-service-for-msk-cluster",
      title: "MSK 클러스터를 위한 AWS PrivateLink VPC 엔드포인트 서비스",
      description: "AWS PrivateLink VPC 엔드포인트 서비스를 통해 MSK 클러스터를 ClickPipes에 노출하는 설정 단계입니다.",
      href: "/ko/resources/support-center/knowledge-base/cloud-services/aws-privatelink-vpc-endpoint-service-for-msk-cluster",
      category: "Cloud",
      tags: ["Security and Authentication", "Managing Cloud"]
    },
    {
      id: "data-management/backing-up-a-specific-partition",
      title: "Backing up a specific partition",
      description: "How can I backup a specific partition in ClickHouse?",
      href: "/ko/resources/support-center/knowledge-base/data-management/backing-up-a-specific-partition",
      category: "Data management",
      tags: ["Managing Data"]
    },
    {
      id: "general-faqs/key-value",
      title: "Can I use ClickHouse as a key-value storage?",
      description: "Answers the frequently asked question of whether or not ClickHouse can be used as a key-value storage?",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/key-value",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/time-series",
      title: "Can I use ClickHouse as a time-series database?",
      description: "Page describing how to use ClickHouse as a time-series database",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/time-series",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "queries-sql/pivot",
      title: "Can you PIVOT in ClickHouse?",
      description:
        "ClickHouse doesn't have a PIVOT clause, but we can get close to this functionality using aggregate function combinators. Let's see how to do this using the UK housing prices dataset.",
      href: "/ko/resources/support-center/knowledge-base/queries-sql/pivot",
      category: "Queries & SQL",
      tags: ["Data Modelling", "Core Data Concepts"]
    },
    {
      id: "general-faqs/vector-search",
      title: "Can you use ClickHouse for vector search?",
      description: "Learn how to use ClickHouse for vector search, including storing embeddings and searching with distance functions like cosine similarity.",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/vector-search",
      category: "General & FAQs",
      tags: ["Use Cases", "Concepts"]
    },
    {
      id: "monitoring-debugging/send-logs-level",
      title: "Capturing server logs of queries at the client",
      description: "Learn how to capture server logs at the client level, even with different log settings, using the `send_logs_level` client setting.",
      href: "/ko/resources/support-center/knowledge-base/monitoring-debugging/send-logs-level",
      category: "Monitoring & debugging",
      tags: ["Server Admin"]
    },
    {
      id: "configuration-settings/change-the-prompt-in-clickhouse-client",
      title: "Change the prompt in clickhouse-client",
      description: "This article explains how to change the prompt in your Clickhouse client and clickhouse-local terminal window from :) to a prefix followed by :)",
      href: "/ko/resources/support-center/knowledge-base/configuration-settings/change-the-prompt-in-clickhouse-client",
      category: "Configuration & settings",
      tags: ["Settings", "Native Clients and Interfaces"]
    },
    {
      id: "security/common-rbac-queries",
      title: "일반적인 RBAC 쿼리",
      description: "사용자에게 특정 권한을 부여하는 데 도움이 되는 쿼리입니다.",
      href: "/ko/resources/support-center/knowledge-base/security/common-rbac-queries",
      category: "Security & access control",
      tags: ["Security and Authentication", "Managing Cloud"]
    },
    {
      id: "queries-sql/comparing-metrics-between-queries",
      title: "데시벨 단위로 쿼리 간 메트릭 비교",
      description: "ClickHouse에서 두 쿼리 간의 메트릭을 비교하는 쿼리입니다.",
      href: "/ko/resources/support-center/knowledge-base/queries-sql/comparing-metrics-between-queries",
      category: "Queries & SQL",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "configuration-settings/configure-cap-ipc-lock-and-cap-sys-nice-in-docker",
      title: "Docker에서 CAP_IPC_LOCK 및 CAP_SYS_NICE 기능 구성",
      description: "컨테이너에서 ClickHouse를 실행할 때 `CAP_IPC_LOCK` 및 `CAP_SYS_NICE`에 대한 Docker 기능 경고를 해결하는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/configuration-settings/configure-cap-ipc-lock-and-cap-sys-nice-in-docker",
      category: "Configuration & settings",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "troubleshooting/configure-cap-ipc-lock-and-cap-sys-nice-in-docker",
      title: "Docker에서 CAP_IPC_LOCK 및 CAP_SYS_NICE 기능 구성",
      description: "컨테이너에서 ClickHouse를 실행할 때 `CAP_IPC_LOCK` 및 `CAP_SYS_NICE`에 대한 Docker 기능 경고를 해결하는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/troubleshooting/configure-cap-ipc-lock-and-cap-sys-nice-in-docker",
      category: "Troubleshooting & errors",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "cloud-services/confluent-cloud-private-connectivity-for-clickpipes",
      title: "ClickPipes를 위한 Confluent Cloud 프라이빗 연결",
      description: "AWS PrivateLink 또는 GCP Private Service Connect를 통해 ClickPipes를 기존 Confluent Cloud Kafka 클러스터에 연결하는 방법입니다.",
      href: "/ko/resources/support-center/knowledge-base/cloud-services/confluent-cloud-private-connectivity-for-clickpipes",
      category: "Cloud",
      tags: ["Security and Authentication", "Managing Cloud"]
    },
    {
      id: "cloud-services/custom-dns-alias-for-instance",
      title: "리버스 프록시를 설정하여 사용자 지정 DNS 별칭 만들기",
      description: "리버스 프록시를 사용하여 인스턴스에 대한 사용자 지정 DNS 별칭을 설정하는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/cloud-services/custom-dns-alias-for-instance",
      category: "Cloud",
      tags: ["Server Admin", "Security and Authentication"]
    },
    {
      id: "troubleshooting/part-intersects-previous-part",
      title: "DB::Exception: Part XXXXX intersects previous part YYYYY. It is a bug or a result of manual intervention in the ZooKeeper data.",
      description:
        "이 문서에서는 ZooKeeper 데이터의 경쟁 조건 또는 수동 개입으로 인해 발생하는 ClickHouse의 파트 교차 관련 DB::Exception 오류를 해결하는 방법을 설명합니다.",
      href: "/ko/resources/support-center/knowledge-base/troubleshooting/part-intersects-previous-part",
      category: "Troubleshooting & errors",
      tags: ["Errors and Exceptions", "System Tables"]
    },
    {
      id: "setup-installation/difference-between-official-builds-and-3rd-party",
      title: "공식 ClickHouse 빌드와 서드파티 빌드의 차이점",
      description: "업데이트, 호환성 및 보안 고려 사항을 포함하여 공식 ClickHouse 빌드와 서드파티 빌드 간의 주요 차이점을 이해하세요.",
      href: "/ko/resources/support-center/knowledge-base/setup-installation/difference-between-official-builds-and-3rd-party",
      category: "Setup & installation",
      tags: ["Concepts"]
    },
    {
      id: "general-faqs/cost-based",
      title: "ClickHouse에 비용 기반 옵티마이저가 있나요?",
      description: "ClickHouse에는 특정 비용 기반 최적화 메커니즘이 있습니다.",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/cost-based",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/datalake",
      title: "ClickHouse는 데이터 레이크를 지원하나요?",
      description: "ClickHouse는 Iceberg, Delta Lake, Apache Hudi, Apache Paimon, Hive를 포함한 데이터 레이크를 지원합니다.",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/datalake",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/distributed-join",
      title: "ClickHouse는 분산 JOIN을 지원하나요?",
      description: "ClickHouse는 분산 JOIN을 지원합니다.",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/distributed-join",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/federated",
      title: "ClickHouse는 페더레이션 쿼리를 지원하나요?",
      description: "ClickHouse는 페더레이션 및 하이브리드 쿼리를 광범위하게 지원합니다.",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/federated",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/concurrency",
      title: "ClickHouse는 빈번한 동시 쿼리를 지원하나요?",
      description: "ClickHouse는 높은 QPS와 높은 동시성을 지원합니다.",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/concurrency",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "cloud-services/multi-region-replication",
      title: "ClickHouse는 다중 리전 복제를 지원하나요?",
      description: "이 페이지에서는 ClickHouse가 다중 리전 복제를 지원하는지 여부를 답변합니다.",
      href: "/ko/resources/support-center/knowledge-base/cloud-services/multi-region-replication",
      category: "Cloud",
      tags: []
    },
    {
      id: "general-faqs/updates",
      title: "ClickHouse는 실시간 업데이트를 지원하나요?",
      description: "ClickHouse는 경량 실시간 업데이트를 지원합니다.",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/updates",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "security/row-column-policy",
      title: "ClickHouse는 행 수준 및 열 수준 보안을 지원하나요?",
      description: "ClickHouse 및 ClickHouse Cloud의 행 수준 및 열 수준 액세스 제한과 정책을 사용하여 역할 기반 액세스 제어(RBAC)를 구현하는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/security/row-column-policy",
      category: "Security & access control",
      tags: ["Security and Authentication"]
    },
    {
      id: "cloud-services/execute-system-queries-in-cloud",
      title: "ClickHouse Cloud의 모든 노드에서 SYSTEM 문 실행",
      description: "ClickHouse Cloud 서비스의 모든 노드에서 SYSTEM 문과 쿼리를 실행하기 위해 `ON CLUSTER` 및 `clusterAllReplicas`를 사용하는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/cloud-services/execute-system-queries-in-cloud",
      category: "Cloud",
      tags: ["Deployments and Scaling"]
    },
    {
      id: "troubleshooting/count-parts-by-type",
      title: "와이드 또는 컴팩트 파트의 수와 크기 찾기",
      description: "이 지식 베이스 문서에서는 파트 유형(와이드 또는 컴팩트)별로 파트 수를 찾는 방법을 보여줍니다.",
      href: "/ko/resources/support-center/knowledge-base/troubleshooting/count-parts-by-type",
      category: "Troubleshooting & errors",
      tags: ["Troubleshooting"]
    },
    {
      id: "troubleshooting/fix-developer-verification-error-in-macos",
      title: "macOS에서 개발자 확인 오류 수정",
      description: "시스템 설정 또는 터미널을 사용하여 ClickHouse 명령 실행 시 발생하는 macOS 개발자 확인 오류를 해결하는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/troubleshooting/fix-developer-verification-error-in-macos",
      category: "Troubleshooting & errors",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "data-import-export/s3-export-data-year-month-folders",
      title: "S3에서 연도 및 월별로 파티션된 쓰기를 어떻게 할 수 있나요?",
      description: "데이터 구성을 위한 사용자 지정 경로 구조를 사용하여 ClickHouse에서 S3 버킷에 연도 및 월별로 파티션된 데이터를 쓰는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/data-import-export/s3-export-data-year-month-folders",
      category: "Data import & export",
      tags: ["Data Export", "Native Clients and Interfaces"]
    },
    {
      id: "data-import-export/kafka-clickhouse-json",
      title: "Kafka에서 새로운 JSON 데이터 타입을 사용하려면 어떻게 해야 하나요?",
      description: "Learn how to load JSON messages from Apache Kafka directly into a single JSON column in ClickHouse using the Kafka table engine and JSON data type.",
      href: "/ko/resources/support-center/knowledge-base/data-import-export/kafka-clickhouse-json",
      category: "Data import & export",
      tags: ["Data Formats", "Data Ingestion"]
    },
    {
      id: "cloud-services/change-billing-email",
      title: "ClickHouse Cloud에서 청구 담당자를 변경하려면 어떻게 해야 하나요?",
      description: "Let's learn how to change your billing address in ClickHouse Cloud.",
      href: "/ko/resources/support-center/knowledge-base/cloud-services/change-billing-email",
      category: "Cloud",
      tags: ["Managing Cloud"]
    },
    {
      id: "general-faqs/how-do-i-contribute-code-to-clickhouse",
      title: "How do I contribute code to ClickHouse?",
      description: "ClickHouse is an open-source project developed on GitHub. As customary, contribution instructions are published in CONTRIBUTING file in the root of the source code repository.",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/how-do-i-contribute-code-to-clickhouse",
      category: "General & FAQs",
      tags: ["Community"]
    },
    {
      id: "data-import-export/parquet-to-csv-json",
      title: "Parquet 파일을 CSV 또는 JSON으로 변환하는 방법",
      description: "Learn how to use ClickHouse's `clickhouse-local` tool to easily convert Parquet files to CSV or JSON formats.",
      href: "/ko/resources/support-center/knowledge-base/data-import-export/parquet-to-csv-json",
      category: "Data import & export",
      tags: ["Data Sources", "Data Formats"]
    },
    {
      id: "data-import-export/mysql-to-parquet-csv-json",
      title: "ClickHouse를 사용하여 MySQL 데이터를 Parquet, CSV 또는 JSON으로 내보내는 방법",
      description: "Learn how to use the `clickhouse-local` tool to export MySQL data into formats like Parquet, CSV, or JSON quickly and efficiently.",
      href: "/ko/resources/support-center/knowledge-base/data-import-export/mysql-to-parquet-csv-json",
      category: "Data import & export",
      tags: ["Data Formats", "Data Export"]
    },
    {
      id: "data-import-export/postgresql-to-parquet-csv-json",
      title: "How do I export PostgreSQL data to Parquet, CSV or JSON?",
      description: "Learn how to export PostgreSQL data to Parquet, CSV, or JSON formats using `clickhouse-local` with various examples.",
      href: "/ko/resources/support-center/knowledge-base/data-import-export/postgresql-to-parquet-csv-json",
      category: "Data import & export",
      tags: ["Data Export", "Data Formats"]
    },
    {
      id: "setup-installation/install-clickhouse-windows10",
      title: "Windows 10에 ClickHouse를 설치하는 방법",
      description: "Learn how to install and test ClickHouse on Windows 10 using WSL 2. Includes setup, troubleshooting, and running a test environment.",
      href: "/ko/resources/support-center/knowledge-base/setup-installation/install-clickhouse-windows10",
      category: "Setup & installation",
      tags: ["Tools and Utilities"]
    },
    {
      id: "security/remove-default-user",
      title: "How do I remove the default user?",
      description: "Learn how to remove the default user when running ClickHouse Server.",
      href: "/ko/resources/support-center/knowledge-base/security/remove-default-user",
      category: "Security & access control",
      tags: ["Server Admin"]
    },
    {
      id: "cloud-services/ingest-failures-23-9-release",
      title: "ClickHouse 23.9 릴리스 이후 수집 실패를 해결하는 방법",
      description: "Learn how to resolve ingest failures caused by stricter grant checking introduced in ClickHouse 23.9 for tables using `async_inserts`. Update grants to fix errors.",
      href: "/ko/resources/support-center/knowledge-base/cloud-services/ingest-failures-23-9-release",
      category: "Cloud",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "performance-optimization/insert-select-settings-tuning",
      title: "How do I solve TOO MANY PARTS error during an INSERT...SELECT?",
      description: "Resolve the TOO_MANY_PARTS error in ClickHouse during an `INSERT...SELECT` by tuning expert-level settings for larger blocks and increasing partition thresholds.",
      href: "/ko/resources/support-center/knowledge-base/performance-optimization/insert-select-settings-tuning",
      category: "Performance & optimization",
      tags: ["Settings", "Errors and Exceptions"]
    },
    {
      id: "integrations/node-js-example",
      title: "How do I use NodeJS with @clickhouse/client",
      description: "Learn how to use @clickhouse/client in a Node.js application to interact with ClickHouse and perform queries.",
      href: "/ko/resources/support-center/knowledge-base/integrations/node-js-example",
      category: "Integrations & client libraries",
      tags: ["Language Clients"]
    },
    {
      id: "monitoring-debugging/view-number-of-active-mutations",
      title: "How do I view the number of active or queued mutations?",
      description:
        "Monitor the number of active or queued mutations in ClickHouse, especially when performing `ALTER` or `UPDATE` operations. Use the `system.mutations` table for tracking mutations.",
      href: "/ko/resources/support-center/knowledge-base/monitoring-debugging/view-number-of-active-mutations",
      category: "Monitoring & debugging",
      tags: ["System Tables"]
    },
    {
      id: "data-management/read-consistency",
      title: "How to achieve data read consistency in ClickHouse?",
      description: "Learn how to ensure data consistency when reading from ClickHouse, whether you're connected to the same node or a random node.",
      href: "/ko/resources/support-center/knowledge-base/data-management/read-consistency",
      category: "Data management",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "setup-installation/llvm-clang-up-to-date",
      title: "How to build LLVM and clang on Linux",
      description: "Commands to build LLVM and clang on Linux.",
      href: "/ko/resources/support-center/knowledge-base/setup-installation/llvm-clang-up-to-date",
      category: "Setup & installation",
      tags: ["Community", "Tools and Utilities"]
    },
    {
      id: "data-management/calculate-ratio-of-zero-sparse-serialization",
      title: "How to calculate the ratio of empty/zero values in every column in a table",
      description: "Learn how to calculate the ratio of empty or zero values in every column of a ClickHouse table to optimize sparse column serialization.",
      href: "/ko/resources/support-center/knowledge-base/data-management/calculate-ratio-of-zero-sparse-serialization",
      category: "Data management",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "security/check-users-roles",
      title: "How to Check Users Assigned to Roles and Vice Versa",
      description: "Learn how to query ClickHouse's `system.role_grants` to find users assigned to roles and roles assigned to specific users.",
      href: "/ko/resources/support-center/knowledge-base/security/check-users-roles",
      category: "Security & access control",
      tags: ["Server Admin", "System Tables", "Managing Cloud"]
    },
    {
      id: "monitoring-debugging/which-processes-are-currently-running",
      title: "How to check what code is currently running on a server?",
      description:
        "ClickHouse provides introspection tools like `system.stack_trace` for inspecting what code is currently running on each server thread, helping with debugging and performance monitoring.",
      href: "/ko/resources/support-center/knowledge-base/monitoring-debugging/which-processes-are-currently-running",
      category: "Monitoring & debugging",
      tags: ["Server Admin"]
    },
    {
      id: "cloud-services/how-to-check-my-clickhouse-cloud-sevice-state",
      title: "ClickHouse Cloud 서비스 상태를 확인하는 방법",
      description: "Learn how to use the ClickHouse Cloud API to check if your service is stopped, idle, or running without waking it up.",
      href: "/ko/resources/support-center/knowledge-base/cloud-services/how-to-check-my-clickhouse-cloud-sevice-state",
      category: "Cloud",
      tags: ["Managing Cloud"]
    },
    {
      id: "monitoring-debugging/collect-and-draw-traces",
      title: "How to collect and draw a query trace",
      description:
        "이 가이드에서는 내장 방법 또는 Grafana를 사용하여 자체 관리형 ClickHouse에서 쿼리 트레이스를 수집하고 시각화하는 방법을 설명합니다. 복잡한 쿼리를 다루면서 EXPLAIN이 제공하는 정보 이상으로 내부 실행 메커니즘을 이해해야 할 때 특히 유용합니다.",
      href: "/ko/resources/support-center/knowledge-base/monitoring-debugging/collect-and-draw-traces",
      category: "Monitoring & debugging",
      tags: ["Tools and Utilities"]
    },
    {
      id: "configuration-settings/configure-a-user-setting",
      title: "ClickHouse에서 사용자 설정을 구성하는 방법",
      description: "`SET` 및 `ALTER USER` 명령을 사용하여 개별 쿼리, 클라이언트 세션 또는 특정 사용자에 대한 ClickHouse 설정을 정의하는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/configuration-settings/configure-a-user-setting",
      category: "Configuration & settings",
      tags: ["Settings"]
    },
    {
      id: "materialized-views/projection-example",
      title: "쿼리에서 Projection이 사용되는지 확인하는 방법",
      description: "샘플 데이터로 테스트하고 EXPLAIN을 사용하여 ClickHouse 쿼리에서 프로젝션이 사용되는지 확인하는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/materialized-views/projection-example",
      category: "Materialized views & projections",
      tags: ["Data Modelling"]
    },
    {
      id: "cloud-services/how-to-connect-to-ch-cloud-using-ssh-keys",
      title: "SSH 키를 사용하여 ClickHouse에 연결하는 방법",
      description: "SSH 키를 사용하여 ClickHouse 및 ClickHouse Cloud에 연결하는 방법",
      href: "/ko/resources/support-center/knowledge-base/cloud-services/how-to-connect-to-ch-cloud-using-ssh-keys",
      category: "Cloud",
      tags: ["Managing Cloud", "Security and Authentication"]
    },
    {
      id: "data-management/dictionary-using-strings",
      title: "문자열 키와 값을 사용하여 ClickHouse 딕셔너리를 만드는 방법",
      description: "MergeTree 테이블을 소스로 사용하여 문자열 키와 값으로 ClickHouse 딕셔너리를 만드는 방법을 설정 및 사용 예제와 함께 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/data-management/dictionary-using-strings",
      category: "Data management",
      tags: ["Data Modelling"]
    },
    {
      id: "tables-schema/how-to-create-table-to-query-multiple-remote-clusters",
      title: "여러 원격 클러스터를 쿼리할 수 있는 테이블을 만드는 방법",
      description: "여러 원격 클러스터를 쿼리할 수 있는 테이블을 만드는 방법",
      href: "/ko/resources/support-center/knowledge-base/tables-schema/how-to-create-table-to-query-multiple-remote-clusters",
      category: "Tables & schema",
      tags: ["Deployments and Scaling"]
    },
    {
      id: "setup-installation/enabling-ssl-with-lets-encrypt",
      title: "단일 ClickHouse 서버에서 Let's Encrypt로 SSL을 활성화하는 방법",
      description: "인증서 발급, 구성 및 유효성 검사를 포함하여 Let's Encrypt를 사용하여 단일 ClickHouse 서버에 SSL을 설정하는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/setup-installation/enabling-ssl-with-lets-encrypt",
      category: "Setup & installation",
      tags: ["Security and Authentication"]
    },
    {
      id: "data-import-export/file-export",
      title: "ClickHouse에서 파일로 데이터를 내보내는 방법",
      description: "`INTO OUTFILE`, File 테이블 엔진, 명령줄 리디렉션 등 ClickHouse에서 데이터를 내보내는 다양한 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/data-import-export/file-export",
      category: "Data import & export",
      tags: ["Data Export"]
    },
    {
      id: "queries-sql/how-to-filter-a-clickhouse-table-by-an-array-column",
      title: "배열 컬럼으로 ClickHouse 테이블을 필터링하는 방법",
      description: "배열 컬럼으로 ClickHouse 테이블을 필터링하는 방법에 대한 지식 베이스 문서입니다.",
      href: "/ko/resources/support-center/knowledge-base/queries-sql/how-to-filter-a-clickhouse-table-by-an-array-column",
      category: "Queries & SQL",
      tags: ["Data Modelling", "Functions"]
    },
    {
      id: "monitoring-debugging/generate-har-file",
      title: "지원을 위한 HAR 파일 생성 방법",
      description: "HAR(HTTP Archive) 파일은 브라우저의 네트워크 활동을 캡처합니다. 지원 팀이 느린 페이지 로드, 실패한 요청 또는 기타 네트워크 문제를 진단하는 데 도움이 될 수 있습니다.",
      href: "/ko/resources/support-center/knowledge-base/monitoring-debugging/generate-har-file",
      category: "Monitoring & debugging",
      tags: ["Tools and Utilities"]
    },
    {
      id: "materialized-views/how-to-display-queries-using-mv",
      title: "ClickHouse에서 구체화된 뷰를 사용하는 쿼리를 식별하는 방법",
      description: "ClickHouse 로그를 쿼리하여 지정된 시간 범위 내에서 구체화된 뷰와 관련된 모든 쿼리를 식별하는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/materialized-views/how-to-display-queries-using-mv",
      category: "Materialized views & projections",
      tags: ["System Tables"]
    },
    {
      id: "performance-optimization/find-expensive-queries",
      title: "ClickHouse에서 가장 비용이 많이 드는 쿼리를 식별하는 방법",
      description: "ClickHouse의 `query_log` 테이블을 사용하여 분산 노드 전반에서 메모리 및 CPU를 가장 많이 사용하는 쿼리를 식별하는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/performance-optimization/find-expensive-queries",
      category: "Performance & optimization",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "configuration-settings/ignoring-incorrect-settings",
      title: "ClickHouse에서 잘못된 설정을 무시하는 방법",
      description: "`skip_check_for_incorrect_settings` 옵션을 사용하여 사용자 수준 설정이 잘못 지정된 경우에도 ClickHouse가 시작될 수 있도록 하는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/configuration-settings/ignoring-incorrect-settings",
      category: "Configuration & settings",
      tags: ["Settings"]
    },
    {
      id: "data-import-export/json-import",
      title: "ClickHouse에 JSON을 가져오는 방법",
      description: "이 페이지에서는 ClickHouse에 JSON을 가져오는 방법을 설명합니다.",
      href: "/ko/resources/support-center/knowledge-base/data-import-export/json-import",
      category: "Data import & export",
      tags: []
    },
    {
      id: "setup-installation/how-to-increase-thread-pool-size",
      title: "ClickHouse에서 스레드 수를 늘리는 방법",
      description: "`max_thread_pool_size`, `thread_pool_queue_size`, `max_thread_pool_free_size` 등의 설정을 조정하여 ClickHouse의 글로벌 스레드 풀을 구성하는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/setup-installation/how-to-increase-thread-pool-size",
      category: "Setup & installation",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "data-import-export/kafka-to-clickhouse-setup",
      title: "Kafka에서 ClickHouse로 데이터를 수집하는 방법",
      description: "Kafka 테이블 엔진, 구체화된 뷰, MergeTree 테이블을 사용하여 Kafka 토픽에서 ClickHouse로 데이터를 수집하는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/data-import-export/kafka-to-clickhouse-setup",
      category: "Data import & export",
      tags: ["Data Ingestion"]
    },
    {
      id: "data-import-export/ingest-parquet-files-in-s3",
      title: "S3 버킷에서 Parquet 파일을 수집하는 방법",
      description: "설정, 액세스 권한, 데이터 가져오기 예제를 포함하여 ClickHouse의 S3 테이블 엔진을 사용하여 S3 버킷에서 Parquet 파일을 수집하고 쿼리하는 기본 사항을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/data-import-export/ingest-parquet-files-in-s3",
      category: "Data import & export",
      tags: ["Data Ingestion"]
    },
    {
      id: "queries-sql/how-to-insert-all-rows-from-another-table",
      title: "한 테이블의 모든 행을 다른 테이블에 삽입하는 방법",
      description: "한 테이블의 모든 행을 다른 테이블에 삽입하는 방법에 대한 지식 베이스 문서입니다.",
      href: "/ko/resources/support-center/knowledge-base/queries-sql/how-to-insert-all-rows-from-another-table",
      category: "Queries & SQL",
      tags: ["Data Ingestion"]
    },
    {
      id: "performance-optimization/check-query-processing-time-only",
      title: "행을 반환하지 않고 쿼리 처리 시간을 측정하는 방법",
      description: "ClickHouse의 `FORMAT Null` 옵션을 사용하여 클라이언트에 행을 반환하지 않고 쿼리 처리 시간을 측정하는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/performance-optimization/check-query-processing-time-only",
      category: "Performance & optimization",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "cloud-services/opt-out-core-dump-collection",
      title: "충돌 보고서 수집을 거부하는 방법",
      description: "이 문서에서는 ClickHouse Cloud에서 충돌 보고서 수집을 거부하는 방법을 설명합니다.",
      href: "/ko/resources/support-center/knowledge-base/cloud-services/opt-out-core-dump-collection",
      category: "Cloud",
      tags: ["Managing Cloud"]
    },
    {
      id: "monitoring-debugging/outputSendLogsLevelTracesToFile",
      title: "How to output send logs level traces to file using the clickhouse-client",
      description: "How to output send logs level traces to file using the clickhouse-client",
      href: "/ko/resources/support-center/knowledge-base/monitoring-debugging/outputSendLogsLevelTracesToFile",
      category: "Monitoring & debugging",
      tags: ["Data Export"]
    },
    {
      id: "tables-schema/recreate-table-across-terminals",
      title: "How to quickly recreate a small table across different terminals",
      description: "Learn how to quickly recreate a small table and its data across different terminals using copy/paste for development environments.",
      href: "/ko/resources/support-center/knowledge-base/tables-schema/recreate-table-across-terminals",
      category: "Tables & schema",
      tags: ["Tools and Utilities"]
    },
    {
      id: "troubleshooting/recovering-from-corrupt-keeper-snapshot",
      title: "손상된 Keeper 스냅샷에서 복구하는 방법",
      description: "손상된 Keeper 스냅샷에서 복구하는 방법을 설명하는 문서입니다. 문제가 어떻게 나타나는지, 스냅샷이란 무엇이며 어디서 찾을 수 있는지, 그리고 가능한 복구 전략을 다룹니다.",
      href: "/ko/resources/support-center/knowledge-base/troubleshooting/recovering-from-corrupt-keeper-snapshot",
      category: "Troubleshooting & errors",
      tags: ["Troubleshooting"]
    },
    {
      id: "troubleshooting/restore-replica-after-storage-failure",
      title: "스토리지 장애 후 복제본을 복원하는 방법",
      description: "이 문서에서는 ClickHouse의 원자적 데이터베이스에서 복제된 테이블(Replicated Table)을 사용할 때 복제본 중 하나의 디스크/스토리지가 손실되거나 손상된 경우 데이터를 복구하는 방법을 설명합니다.",
      href: "/ko/resources/support-center/knowledge-base/troubleshooting/restore-replica-after-storage-failure",
      category: "Troubleshooting & errors",
      tags: ["Deployments and Scaling"]
    },
    {
      id: "integrations/how-to-set-up-ch-on-docker-odbc-connect-mssql",
      title: "How to set up ClickHouse on Docker with ODBC to connect to a Microsoft SQL Server (MSSQL) database",
      description: "How to set up ClickHouse on Docker with ODBC to connect to a Microsoft SQL Server (MSSQL) database",
      href: "/ko/resources/support-center/knowledge-base/integrations/how-to-set-up-ch-on-docker-odbc-connect-mssql",
      category: "Integrations & client libraries",
      tags: ["Native Clients and Interfaces"]
    },
    {
      id: "queries-sql/using-array-join-to-extract-and-query-attributes",
      title: "How to use array join to extract and query varying attributes using map keys and values",
      description: "Simple example to illustrate how to use array join to extract and query varying attributes using map keys and values",
      href: "/ko/resources/support-center/knowledge-base/queries-sql/using-array-join-to-extract-and-query-attributes",
      category: "Queries & SQL",
      tags: ["Functions"]
    },
    {
      id: "materialized-views/how-to-use-parametrised-views",
      title: "How to Use Parameterized Views in ClickHouse",
      description: "Learn how to create and query parameterized views in ClickHouse for dynamic data slicing based on query-time parameters.",
      href: "/ko/resources/support-center/knowledge-base/materialized-views/how-to-use-parametrised-views",
      category: "Materialized views & projections",
      tags: ["Use Cases"]
    },
    {
      id: "tables-schema/exchangeStatementToSwitchTables",
      title: "How to use the exchange command to switch tables",
      description: "How to use the exchange command to switch tables",
      href: "/ko/resources/support-center/knowledge-base/tables-schema/exchangeStatementToSwitchTables",
      category: "Tables & schema",
      tags: ["Managing Data"]
    },
    {
      id: "queries-sql/compare-resultsets",
      title: "How to Validate if Two Queries Return the Same Result-sets",
      description: "Learn how to validate that two ClickHouse queries produce identical result-sets using hash functions and comparison techniques.",
      href: "/ko/resources/support-center/knowledge-base/queries-sql/compare-resultsets",
      category: "Queries & SQL",
      tags: ["Functions"]
    },
    {
      id: "monitoring-debugging/check-query-cache-in-use",
      title: "How to verify query cache usage in ClickHouse",
      description: "Learn how to check if query cache is being utilized in ClickHouse using `clickhouse-client` trace logs or SQL commands.",
      href: "/ko/resources/support-center/knowledge-base/monitoring-debugging/check-query-cache-in-use",
      category: "Monitoring & debugging",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "cloud-services/unable-to-access-cloud-service",
      title: "I am unable to access a ClickHouse Cloud service",
      description: "Troubleshooting access issues with ClickHouse Cloud services, including IP Access List configuration",
      href: "/ko/resources/support-center/knowledge-base/cloud-services/unable-to-access-cloud-service",
      category: "Cloud",
      tags: ["Errors and Exceptions", "Managing Cloud"]
    },
    {
      id: "performance-optimization/finding-expensive-queries-by-memory-usage",
      title: "Identifying Expensive Queries by Memory Usage in ClickHouse",
      description: "Learn how to use the `system.query_log` table to find the most memory-intensive queries in ClickHouse, with examples for clustered and standalone setups.",
      href: "/ko/resources/support-center/knowledge-base/performance-optimization/finding-expensive-queries-by-memory-usage",
      category: "Performance & optimization",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "data-import-export/importing-and-working-with-json-array-objects",
      title: "Importing and Querying JSON Array Objects in ClickHouse",
      description: "Learn how to import JSON array objects into ClickHouse and perform advanced queries using JSON functions and array operations.",
      href: "/ko/resources/support-center/knowledge-base/data-import-export/importing-and-working-with-json-array-objects",
      category: "Data import & export",
      tags: ["Data Formats"]
    },
    {
      id: "data-import-export/importing-geojason-with-nested-object-array",
      title: "Importing GeoJSON with a deeply nested object array",
      description: "Learn how to import GeoJSON files with deeply nested object arrays into ClickHouse and query the nested feature data.",
      href: "/ko/resources/support-center/knowledge-base/data-import-export/importing-geojason-with-nested-object-array",
      category: "Data import & export",
      tags: ["Data Formats"]
    },
    {
      id: "performance-optimization/improve-map-performance",
      title: "Improving Map lookup performance in ClickHouse",
      description: "Learn how to optimize Map column lookups in ClickHouse for better query performance by materializing specific keys as standalone columns.",
      href: "/ko/resources/support-center/knowledge-base/performance-optimization/improve-map-performance",
      category: "Performance & optimization",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "tables-schema/delete-old-data",
      title: "Is it possible to delete old records from a ClickHouse table?",
      description: "This page answers the question of whether it is possible to delete old records from a ClickHouse table",
      href: "/ko/resources/support-center/knowledge-base/tables-schema/delete-old-data",
      category: "Tables & schema",
      tags: []
    },
    {
      id: "general-faqs/separate-storage",
      title: "Is it possible to deploy ClickHouse with separate storage and compute?",
      description: "This page provides an answer as to whether it is possible to deploy ClickHouse with separate storage and compute",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/separate-storage",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "data-import-export/json-extract-example",
      title: "JSON Extract example",
      description: "A short example on how to extract base types from JSON",
      href: "/ko/resources/support-center/knowledge-base/data-import-export/json-extract-example",
      category: "Data import & export",
      tags: ["Data Formats"]
    },
    {
      id: "queries-sql/calculate-pi-using-sql",
      title: "Let's calculate pi using SQL",
      description: "It's Pi Day! Let's calculate pi using ClickHouse SQL",
      href: "/ko/resources/support-center/knowledge-base/queries-sql/calculate-pi-using-sql",
      category: "Queries & SQL",
      tags: ["Use Cases"]
    },
    {
      id: "cloud-services/clickhouse-cloud-api-usage",
      title: "Managing ClickHouse Cloud Service with API and cURL",
      description: "Learn how to start, stop, and resume a ClickHouse Cloud service using API endpoints and cURL commands.",
      href: "/ko/resources/support-center/knowledge-base/cloud-services/clickhouse-cloud-api-usage",
      category: "Cloud",
      tags: ["Managing Cloud", "Tools and Utilities"]
    },
    {
      id: "monitoring-debugging/mapping-of-system-metrics-to-prometheus-metrics",
      title: "Mapping of metrics used in system.dashboards to Prometheus metrics in `system.custom_metrics`",
      description: "Mapping of metrics used in system.dashboards to Prometheus metrics in system.custom_metrics",
      href: "/ko/resources/support-center/knowledge-base/monitoring-debugging/mapping-of-system-metrics-to-prometheus-metrics",
      category: "Monitoring & debugging",
      tags: ["System Tables"]
    },
    {
      id: "security/windows-active-directory-to-ch-roles",
      title: "Mapping Windows Active Directory security groups to ClickHouse roles",
      description: "Example of mapping Windows Active Directory security groups to ClickHouse roles",
      href: "/ko/resources/support-center/knowledge-base/security/windows-active-directory-to-ch-roles",
      category: "Security & access control",
      tags: ["Tools and Utilities"]
    },
    {
      id: "performance-optimization/memory-limit-exceeded-for-query",
      title: "Memory limit exceeded for query",
      description: "Troubleshooting memory limit exceeded errors for a query",
      href: "/ko/resources/support-center/knowledge-base/performance-optimization/memory-limit-exceeded-for-query",
      category: "Performance & optimization",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "integrations/ODBC-authentication-failed-error-using-PowerBI-CH-connector",
      title: "ODBC authentication failed error when using the Power BI ClickHouse connector",
      description: "ODBC authentication failed error when using the Power BI ClickHouse connector",
      href: "/ko/resources/support-center/knowledge-base/integrations/ODBC-authentication-failed-error-using-PowerBI-CH-connector",
      category: "Integrations & client libraries",
      tags: ["Native Clients and Interfaces", "Errors and Exceptions"]
    },
    {
      id: "monitoring-debugging/profiling-clickhouse-with-llvm-xray",
      title: "Profiling ClickHouse with LLVM's XRay",
      description: "Learn how to profile ClickHouse using LLVM's XRay instrumentation profiler, visualize traces, and analyze performance.",
      href: "/ko/resources/support-center/knowledge-base/monitoring-debugging/profiling-clickhouse-with-llvm-xray",
      category: "Monitoring & debugging",
      tags: ["Performance and Optimizations", "Tools and Utilities"]
    },
    {
      id: "integrations/python-http-requests",
      title: "Python quick example using HTTP requests module",
      description: "An example using Python and requests module to write and read to ClickHouse",
      href: "/ko/resources/support-center/knowledge-base/integrations/python-http-requests",
      category: "Integrations & client libraries",
      tags: ["Native Clients and Interfaces"]
    },
    {
      id: "configuration-settings/maximum-number-of-tables-and-databases",
      title: "Recommended Maximum Databases, Tables, Partitions, and Parts in ClickHouse",
      description: "Learn the recommended maximum limits for databases, tables, partitions, and parts in a ClickHouse cluster to ensure optimal performance.",
      href: "/ko/resources/support-center/knowledge-base/configuration-settings/maximum-number-of-tables-and-databases",
      category: "Configuration & settings",
      tags: ["Performance and Optimizations", "Deployments and Scaling"]
    },
    {
      id: "data-import-export/cannot-append-data-to-parquet-format",
      title: 'ClickHouse에서 "Parquet 포맷으로 데이터를 추가할 수 없음" 오류 해결하기',
      description: 'ClickHouse에서 "Cannot append data in format Parquet to file" 오류가 발생하고 있나요? 해결 방법을 살펴보겠습니다.',
      href: "/ko/resources/support-center/knowledge-base/data-import-export/cannot-append-data-to-parquet-format",
      category: "Data import & export",
      tags: ["Errors and Exceptions", "Data Formats"]
    },
    {
      id: "troubleshooting/exception-too-many-parts",
      title: 'ClickHouse에서 "파트 수 초과(Too Many Parts)" 오류 해결하기',
      description: '삽입 속도 최적화, MergeTree 설정 구성, 파티션 효과적 관리를 통해 ClickHouse의 "Too many parts" 오류를 해결하는 방법을 알아보세요.',
      href: "/ko/resources/support-center/knowledge-base/troubleshooting/exception-too-many-parts",
      category: "Troubleshooting & errors",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "troubleshooting/certificate-verify-failed-error",
      title: "Resolving SSL Certificate Verify Error in ClickHouse",
      description: "Learn how to resolve the SSL Exception CERTIFICATE_VERIFY_FAILED error.",
      href: "/ko/resources/support-center/knowledge-base/troubleshooting/certificate-verify-failed-error",
      category: "Troubleshooting & errors",
      tags: ["Security and Authentication", "Errors and Exceptions"]
    },
    {
      id: "troubleshooting/connection-timeout-remote-remoteSecure",
      title: "Resolving Timeout Errors with `remote` and `remoteSecure` Table Functions",
      description: "Learn how to fix timeout errors when using `remote` or `remoteSecure` table functions in ClickHouse by adjusting the connection timeout settings.",
      href: "/ko/resources/support-center/knowledge-base/troubleshooting/connection-timeout-remote-remoteSecure",
      category: "Troubleshooting & errors",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "tables-schema/runbook-json",
      title: "Runbook: JSON 스키마",
      description: "ClickHouse에서 JSON 데이터에 적합한 스키마 방식을 선택하세요 — 타입이 지정된 컬럼, 하이브리드, 네이티브 JSON, 또는 String 저장 방식",
      href: "/ko/resources/support-center/knowledge-base/tables-schema/runbook-json",
      category: "Tables & schema",
      tags: ["Runbooks", "Data Modelling"]
    },
    {
      id: "tables-schema/search-across-node-for-tables-with-a-wildcard",
      title: "Searching across nodes for tables with a wildcard",
      description: "Learn how to search across nodes for tables with a wildcard.",
      href: "/ko/resources/support-center/knowledge-base/tables-schema/search-across-node-for-tables-with-a-wildcard",
      category: "Tables & schema",
      tags: ["Deployments and Scaling"]
    },
    {
      id: "performance-optimization/query-max-execution-time",
      title: "Setting a limit on query execution time",
      description: "How to enforce limit on max query execution time",
      href: "/ko/resources/support-center/knowledge-base/performance-optimization/query-max-execution-time",
      category: "Performance & optimization",
      tags: ["Managing Cloud", "Settings"]
    },
    {
      id: "data-import-export/json-simple-example",
      title: "Simple example flow for extracting JSON data using a landing table with a Materialized View",
      description: "Simple example flow for extracting JSON data using a landing table with a Materialized View",
      href: "/ko/resources/support-center/knowledge-base/data-import-export/json-simple-example",
      category: "Data import & export",
      tags: ["Data Formats"]
    },
    {
      id: "performance-optimization/async-vs-optimize-read-in-order",
      title: "Synchronous data reading",
      description:
        "The new setting `allow_asynchronous_read_from_io_pool_for_merge_tree` allows the number of reading threads (streams) to be higher than the number of threads in the rest of the query execution pipeline.",
      href: "/ko/resources/support-center/knowledge-base/performance-optimization/async-vs-optimize-read-in-order",
      category: "Performance & optimization",
      tags: ["Settings", "Performance and Optimizations"]
    },
    {
      id: "integrations/terraform-example",
      title: "Terraform example on how to use Cloud API",
      description: "This covers an example of how you can use terraform to create/delete clusters using the API",
      href: "/ko/resources/support-center/knowledge-base/integrations/terraform-example",
      category: "Integrations & client libraries",
      tags: ["Native Clients and Interfaces"]
    },
    {
      id: "performance-optimization/tips-tricks-optimizing-basic-data-types-in-clickhouse",
      title: "Tips and tricks on optimizing basic data types in ClickHouse",
      description: "Tips and tricks on optimizing basic data types in ClickHouse",
      href: "/ko/resources/support-center/knowledge-base/performance-optimization/tips-tricks-optimizing-basic-data-types-in-clickhouse",
      category: "Performance & optimization",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "data-management/understanding-part-types-and-storage-formats",
      title: "파트 유형 및 스토리지 포맷 이해하기",
      description: "ClickHouse의 다양한 파트 유형(Wide vs Compact)과 스토리지 포맷(Full vs Packed), 그리고 이들이 성능에 미치는 영향에 대해 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/data-management/understanding-part-types-and-storage-formats",
      category: "Data management",
      tags: ["Core Data Concepts"]
    },
    {
      id: "queries-sql/useful-queries-for-troubleshooting",
      title: "문제 해결을 위한 유용한 쿼리",
      description: "테이블 크기 모니터링, 장시간 실행 쿼리, 오류 등 ClickHouse 문제 해결을 위한 유용한 쿼리 모음입니다.",
      href: "/ko/resources/support-center/knowledge-base/queries-sql/useful-queries-for-troubleshooting",
      category: "Queries & SQL",
      tags: ["Settings"]
    },
    {
      id: "general-faqs/use-clickhouse-for-log-analytics",
      title: "로그 분석에 ClickHouse 사용하기",
      description: "ClickHouse는 실시간 분석 기능 덕분에 로그 및 메트릭 분석에 널리 사용됩니다. 더 알아볼 준비가 되셨나요?",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/use-clickhouse-for-log-analytics",
      category: "General & FAQs",
      tags: ["Use Cases"]
    },
    {
      id: "queries-sql/filtered-aggregates",
      title: "ClickHouse에서 필터링된 집계 사용하기",
      description: "쿼리 문법을 단순화하고 분석을 향상시키기 위해 `-If` 및 `-Distinct` 집계 결합자를 사용하여 ClickHouse에서 필터링된 집계를 사용하는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/queries-sql/filtered-aggregates",
      category: "Queries & SQL",
      tags: ["Functions"]
    },
    {
      id: "general-faqs/dependencies",
      title: "ClickHouse 실행을 위한 서드파티 의존성은 무엇입니까?",
      description: "ClickHouse는 독립적으로 실행되며 런타임 의존성이 없습니다.",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/dependencies",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/dbms-naming",
      title: '"ClickHouse"는 무슨 의미입니까?',
      description: '"ClickHouse"가 무슨 의미인지 알아보세요.',
      href: "/ko/resources/support-center/knowledge-base/general-faqs/dbms-naming",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/ne-tormozit",
      title: '"не тормозит"는 무슨 의미입니까?',
      description: '"Не тормозит"가 무슨 의미인지 설명하는 페이지입니다.',
      href: "/ko/resources/support-center/knowledge-base/general-faqs/ne-tormozit",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "integrations/oracle-odbc",
      title: "ODBC를 통해 Oracle을 사용할 때 인코딩 문제가 발생하면 어떻게 해야 합니까?",
      description: "ODBC를 통해 Oracle을 사용할 때 인코딩 문제가 발생하는 경우 해결 방법을 안내하는 페이지입니다.",
      href: "/ko/resources/support-center/knowledge-base/integrations/oracle-odbc",
      category: "Integrations & client libraries",
      tags: []
    },
    {
      id: "general-faqs/columnar-database",
      title: "컬럼형 데이터베이스란 무엇입니까?",
      description: "컬럼형 데이터베이스는 각 열의 데이터를 독립적으로 저장합니다. 이를 통해 특정 쿼리에서 사용되는 열의 데이터만 디스크에서 읽을 수 있습니다.",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/columnar-database",
      category: "General & FAQs",
      tags: ["Core Data Concepts"]
    },
    {
      id: "general-faqs/olap",
      title: "OLAP란 무엇입니까?",
      description: "온라인 분석 처리(Online Analytical Processing)에 대한 설명입니다.",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/olap",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "performance-optimization/optimize-final-vs-final",
      title: "OPTIMIZE FINAL과 FINAL의 차이점은 무엇입니까?",
      description: "OPTIMIZE FINAL과 FINAL의 차이점, 그리고 각각을 언제 사용하고 언제 피해야 하는지 설명합니다.",
      href: "/ko/resources/support-center/knowledge-base/performance-optimization/optimize-final-vs-final",
      category: "Performance & optimization",
      tags: ["Core Data Concepts"]
    },
    {
      id: "general-faqs/sql",
      title: "ClickHouse는 어떤 SQL 문법을 지원합니까?",
      description: "ClickHouse는 SQL 문법을 100% 지원합니다.",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/sql",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "data-management/when-is-ttl-applied",
      title: "TTL 규칙은 언제 적용되며, 이를 제어할 수 있습니까?",
      description:
        "ClickHouse의 TTL 규칙은 최종적으로 적용되며, `merge_with_ttl_timeout` 설정을 사용하여 실행 시점을 제어할 수 있습니다. TTL을 강제로 적용하는 방법과 TTL 실행을 위한 백그라운드 스레드를 관리하는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/data-management/when-is-ttl-applied",
      category: "Data management",
      tags: ["Core Data Concepts"]
    },
    {
      id: "setup-installation/production",
      title: "프로덕션 환경에서 어떤 ClickHouse 버전을 사용해야 합니까?",
      description: "프로덕션 환경에서 사용할 ClickHouse 버전 선택에 대한 지침을 제공합니다.",
      href: "/ko/resources/support-center/knowledge-base/setup-installation/production",
      category: "Setup & installation",
      tags: []
    },
    {
      id: "general-faqs/who-is-using-clickhouse",
      title: "ClickHouse를 사용하는 곳은 어디입니까?",
      description: "ClickHouse 사용 사례를 설명합니다.",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/who-is-using-clickhouse",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "data-management/dictionaries-consistent-state",
      title: "ClickHouse Cloud의 딕셔너리에서 데이터가 보이지 않는 이유는 무엇입니까?",
      description: "딕셔너리 생성 직후 데이터가 즉시 표시되지 않는 문제가 있습니다.",
      href: "/ko/resources/support-center/knowledge-base/data-management/dictionaries-consistent-state",
      category: "Data management",
      tags: ["Managing Cloud", "Data Modelling"]
    },
    {
      id: "general-faqs/why-recommend-clickhouse-keeper-over-zookeeper",
      title: "ZooKeeper 대신 ClickHouse Keeper가 권장되는 이유는 무엇입니까?",
      description:
        "ClickHouse Keeper는 디스크 공간 사용량 감소, 빠른 복구, 낮은 메모리 소비 등의 개선 사항을 통해 ClickHouse 클러스터에서 ZooKeeper보다 더 나은 성능을 제공합니다.",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/why-recommend-clickhouse-keeper-over-zookeeper",
      category: "General & FAQs",
      tags: ["Core Data Concepts"]
    },
    {
      id: "monitoring-debugging/why-default-logging-verbose",
      title: "ClickHouse의 기본 로깅이 왜 이렇게 상세합니까?",
      description: "ClickHouse 개발자들이 기본 로깅 수준을 상세하게 설정한 이유를 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/monitoring-debugging/why-default-logging-verbose",
      category: "Monitoring & debugging",
      tags: ["Settings"]
    },
    {
      id: "performance-optimization/why-is-my-primary-key-not-used",
      title: "기본 키가 사용되지 않는 이유는 무엇입니까? 어떻게 확인할 수 있습니까?",
      description: "정렬 시 기본 키가 사용되지 않는 일반적인 원인과 이를 확인하는 방법을 설명합니다.",
      href: "/ko/resources/support-center/knowledge-base/performance-optimization/why-is-my-primary-key-not-used",
      category: "Performance & optimization",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "general-faqs/mapreduce",
      title: "MapReduce 같은 것을 사용하지 않는 이유는 무엇입니까?",
      description: "MapReduce 대신 ClickHouse를 사용해야 하는 이유를 설명합니다.",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/mapreduce",
      category: "General & FAQs",
      tags: []
    }
  ]
}