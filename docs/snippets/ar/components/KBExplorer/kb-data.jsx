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
      href: "/ar/resources/support-center/knowledge-base/integrations/python-clickhouse-connect-example",
      category: "Integrations & client libraries",
      tags: ["Language Clients"]
    },
    {
      id: "configuration-settings/about-quotas-and-query-complexity",
      title: "About quotas and query complexity",
      description:
        "Quotas and Query Complexity are powerful ways to limit and restrict what users can do in ClickHouse. This KB article shows examples on how to apply these two different approaches.",
      href: "/ar/resources/support-center/knowledge-base/configuration-settings/about-quotas-and-query-complexity",
      category: "Configuration & settings",
      tags: ["Managing Cloud"]
    },
    {
      id: "data-import-export/achieving-atomic-inserts",
      title: "Achieving atomic inserts and multi-table consistency in ClickHouse Cloud",
      description: "How to load data atomically and keep multiple tables consistent in ClickHouse Cloud without multi-statement transactions, using staging tables and partition-level operations.",
      href: "/ar/resources/support-center/knowledge-base/data-import-export/achieving-atomic-inserts",
      category: "Data import & export",
      tags: ["Data Ingestion", "Best Practices"]
    },
    {
      id: "tables-schema/add-column",
      title: "Adding a column to a table",
      description: "In this guide, we'll learn how to add a column to an existing table.",
      href: "/ar/resources/support-center/knowledge-base/tables-schema/add-column",
      category: "Tables & schema",
      tags: ["Data Modelling"]
    },
    {
      id: "configuration-settings/alter-user-settings-exception",
      title: "Alter user settings exception",
      description: "Handing the an exception thrown when altering user settings",
      href: "/ar/resources/support-center/knowledge-base/configuration-settings/alter-user-settings-exception",
      category: "Configuration & settings",
      tags: ["Settings", "Errors and Exceptions"]
    },
    {
      id: "materialized-views/are-materialized-views-inserted-asynchronously",
      title: "Are Materialized Views inserted synchronously?",
      description: "This KB article explores whether Materialized Views are inserted synchronously",
      href: "/ar/resources/support-center/knowledge-base/materialized-views/are-materialized-views-inserted-asynchronously",
      category: "Materialized views & projections",
      tags: ["Data Modelling"]
    },
    {
      id: "tables-schema/schema-migration-tools",
      title: "Automatic schema migration tools for ClickHouse",
      description: "Learn about automatic schema migration tools for ClickHouse and how to manage changing database schemas over time.",
      href: "/ar/resources/support-center/knowledge-base/tables-schema/schema-migration-tools",
      category: "Tables & schema",
      tags: ["Tools and Utilities"]
    },
    {
      id: "cloud-services/aws-privatelink-setup-for-msk-clickpipes",
      title: "AWS PrivateLink setup to expose MSK for ClickPipes",
      description: "Setup steps to expose a private MSK via MSK multi-VPC connectivity to ClickPipes.",
      href: "/ar/resources/support-center/knowledge-base/cloud-services/aws-privatelink-setup-for-msk-clickpipes",
      category: "Cloud",
      tags: ["Security and Authentication", "Managing Cloud"]
    },
    {
      id: "cloud-services/aws-privatelink-setup-for-clickpipes",
      title: "AWS PrivateLink setup to expose private RDS for ClickPipes",
      description: "Setup steps to expose a private RDS via AWS PrivateLink to ClickPipes.",
      href: "/ar/resources/support-center/knowledge-base/cloud-services/aws-privatelink-setup-for-clickpipes",
      category: "Cloud",
      tags: ["Security and Authentication", "Managing Cloud"]
    },
    {
      id: "cloud-services/aws-privatelink-vpc-endpoint-service-for-msk-cluster",
      title: "خدمة نقطة نهاية AWS PrivateLink VPC لمجموعة MSK",
      description: "خطوات الإعداد لكشف مجموعة MSK أمام ClickPipes عبر خدمات نقطة نهاية AWS PrivateLink VPC.",
      href: "/ar/resources/support-center/knowledge-base/cloud-services/aws-privatelink-vpc-endpoint-service-for-msk-cluster",
      category: "Cloud",
      tags: ["Security and Authentication", "Managing Cloud"]
    },
    {
      id: "data-management/backing-up-a-specific-partition",
      title: "Backing up a specific partition",
      description: "How can I backup a specific partition in ClickHouse?",
      href: "/ar/resources/support-center/knowledge-base/data-management/backing-up-a-specific-partition",
      category: "Data management",
      tags: ["Managing Data"]
    },
    {
      id: "general-faqs/key-value",
      title: "Can I use ClickHouse as a key-value storage?",
      description: "Answers the frequently asked question of whether or not ClickHouse can be used as a key-value storage?",
      href: "/ar/resources/support-center/knowledge-base/general-faqs/key-value",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/time-series",
      title: "Can I use ClickHouse as a time-series database?",
      description: "Page describing how to use ClickHouse as a time-series database",
      href: "/ar/resources/support-center/knowledge-base/general-faqs/time-series",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "queries-sql/pivot",
      title: "Can you PIVOT in ClickHouse?",
      description:
        "ClickHouse doesn't have a PIVOT clause, but we can get close to this functionality using aggregate function combinators. Let's see how to do this using the UK housing prices dataset.",
      href: "/ar/resources/support-center/knowledge-base/queries-sql/pivot",
      category: "Queries & SQL",
      tags: ["Data Modelling", "Core Data Concepts"]
    },
    {
      id: "general-faqs/vector-search",
      title: "Can you use ClickHouse for vector search?",
      description: "Learn how to use ClickHouse for vector search, including storing embeddings and searching with distance functions like cosine similarity.",
      href: "/ar/resources/support-center/knowledge-base/general-faqs/vector-search",
      category: "General & FAQs",
      tags: ["Use Cases", "Concepts"]
    },
    {
      id: "monitoring-debugging/send-logs-level",
      title: "Capturing server logs of queries at the client",
      description: "Learn how to capture server logs at the client level, even with different log settings, using the `send_logs_level` client setting.",
      href: "/ar/resources/support-center/knowledge-base/monitoring-debugging/send-logs-level",
      category: "Monitoring & debugging",
      tags: ["Server Admin"]
    },
    {
      id: "configuration-settings/change-the-prompt-in-clickhouse-client",
      title: "Change the prompt in clickhouse-client",
      description: "This article explains how to change the prompt in your Clickhouse client and clickhouse-local terminal window from :) to a prefix followed by :)",
      href: "/ar/resources/support-center/knowledge-base/configuration-settings/change-the-prompt-in-clickhouse-client",
      category: "Configuration & settings",
      tags: ["Settings", "Native Clients and Interfaces"]
    },
    {
      id: "security/common-rbac-queries",
      title: "استعلامات RBAC الشائعة",
      description: "استعلامات للمساعدة في منح أذونات محددة للمستخدمين.",
      href: "/ar/resources/support-center/knowledge-base/security/common-rbac-queries",
      category: "الأمان والتحكم في الوصول",
      tags: ["Security and Authentication", "Managing Cloud"]
    },
    {
      id: "queries-sql/comparing-metrics-between-queries",
      title: "مقارنة المقاييس بين الاستعلامات بالديسيبل",
      description: "استعلام لمقارنة المقاييس بين استعلامين في ClickHouse.",
      href: "/ar/resources/support-center/knowledge-base/queries-sql/comparing-metrics-between-queries",
      category: "الاستعلامات وSQL",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "configuration-settings/configure-cap-ipc-lock-and-cap-sys-nice-in-docker",
      title: "تهيئة قدرات CAP_IPC_LOCK وCAP_SYS_NICE في Docker",
      description: "تعرّف على كيفية حل تحذيرات قدرات Docker لـ `CAP_IPC_LOCK` و`CAP_SYS_NICE` عند تشغيل ClickHouse في حاوية.",
      href: "/ar/resources/support-center/knowledge-base/configuration-settings/configure-cap-ipc-lock-and-cap-sys-nice-in-docker",
      category: "الإعدادات والتهيئة",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "troubleshooting/configure-cap-ipc-lock-and-cap-sys-nice-in-docker",
      title: "تهيئة قدرات CAP_IPC_LOCK وCAP_SYS_NICE في Docker",
      description: "تعرّف على كيفية حل تحذيرات قدرات Docker لـ `CAP_IPC_LOCK` و`CAP_SYS_NICE` عند تشغيل ClickHouse في حاوية.",
      href: "/ar/resources/support-center/knowledge-base/troubleshooting/configure-cap-ipc-lock-and-cap-sys-nice-in-docker",
      category: "استكشاف الأخطاء وإصلاحها",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "cloud-services/confluent-cloud-private-connectivity-for-clickpipes",
      title: "الاتصال الخاص بـ Confluent Cloud لـ ClickPipes",
      description: "كيفية توصيل ClickPipes بمجموعة Confluent Cloud Kafka موجودة عبر AWS PrivateLink أو GCP Private Service Connect.",
      href: "/ar/resources/support-center/knowledge-base/cloud-services/confluent-cloud-private-connectivity-for-clickpipes",
      category: "Cloud",
      tags: ["Security and Authentication", "Managing Cloud"]
    },
    {
      id: "cloud-services/custom-dns-alias-for-instance",
      title: "إنشاء اسم مستعار DNS مخصص عن طريق إعداد وكيل عكسي",
      description: "تعرّف على كيفية إعداد اسم مستعار DNS مخصص لمثيلك باستخدام وكيل عكسي",
      href: "/ar/resources/support-center/knowledge-base/cloud-services/custom-dns-alias-for-instance",
      category: "Cloud",
      tags: ["Server Admin", "Security and Authentication"]
    },
    {
      id: "troubleshooting/part-intersects-previous-part",
      title: "DB::Exception: Part XXXXX intersects previous part YYYYY. It is a bug or a result of manual intervention in the ZooKeeper data.",
      description:
        "تشرح هذه المقالة كيفية حل خطأ DB::Exception المتعلق بالأجزاء المتقاطعة في ClickHouse، والذي غالبًا ما يكون ناتجًا عن حالة تسابق أو تدخل يدوي في بيانات ZooKeeper.",
      href: "/ar/resources/support-center/knowledge-base/troubleshooting/part-intersects-previous-part",
      category: "استكشاف الأخطاء وإصلاحها",
      tags: ["Errors and Exceptions", "System Tables"]
    },
    {
      id: "setup-installation/difference-between-official-builds-and-3rd-party",
      title: "الفروق بين إصدارات ClickHouse الرسمية وإصدارات الجهات الخارجية",
      description: "افهم الفروق الرئيسية بين إصدارات ClickHouse الرسمية وإصدارات الجهات الخارجية، بما في ذلك التحديثات والتوافق واعتبارات الأمان.",
      href: "/ar/resources/support-center/knowledge-base/setup-installation/difference-between-official-builds-and-3rd-party",
      category: "الإعداد والتثبيت",
      tags: ["Concepts"]
    },
    {
      id: "general-faqs/cost-based",
      title: "هل يمتلك ClickHouse محسّنًا قائمًا على التكلفة؟",
      description: "يمتلك ClickHouse آليات معينة للتحسين القائم على التكلفة",
      href: "/ar/resources/support-center/knowledge-base/general-faqs/cost-based",
      category: "عام والأسئلة الشائعة",
      tags: []
    },
    {
      id: "general-faqs/datalake",
      title: "هل يدعم ClickHouse بحيرات البيانات؟",
      description: "يدعم ClickHouse بحيرات البيانات، بما في ذلك Iceberg وDelta Lake وApache Hudi وApache Paimon وHive",
      href: "/ar/resources/support-center/knowledge-base/general-faqs/datalake",
      category: "عام والأسئلة الشائعة",
      tags: []
    },
    {
      id: "general-faqs/distributed-join",
      title: "هل يدعم ClickHouse الـ JOIN الموزع؟",
      description: "يدعم ClickHouse الـ JOIN الموزع",
      href: "/ar/resources/support-center/knowledge-base/general-faqs/distributed-join",
      category: "عام والأسئلة الشائعة",
      tags: []
    },
    {
      id: "general-faqs/federated",
      title: "هل يدعم ClickHouse الاستعلامات الموحدة؟",
      description: "يدعم ClickHouse نطاقًا واسعًا من الاستعلامات الموحدة والهجينة",
      href: "/ar/resources/support-center/knowledge-base/general-faqs/federated",
      category: "عام والأسئلة الشائعة",
      tags: []
    },
    {
      id: "general-faqs/concurrency",
      title: "هل يدعم ClickHouse الاستعلامات المتكررة والمتزامنة؟",
      description: "يدعم ClickHouse معدل استعلامات عالٍ وتزامنًا عاليًا",
      href: "/ar/resources/support-center/knowledge-base/general-faqs/concurrency",
      category: "عام والأسئلة الشائعة",
      tags: []
    },
    {
      id: "cloud-services/multi-region-replication",
      title: "هل يدعم ClickHouse النسخ المتماثل متعدد المناطق؟",
      description: "تجيب هذه الصفحة على ما إذا كان ClickHouse يدعم النسخ المتماثل متعدد المناطق",
      href: "/ar/resources/support-center/knowledge-base/cloud-services/multi-region-replication",
      category: "Cloud",
      tags: []
    },
    {
      id: "general-faqs/updates",
      title: "هل يدعم ClickHouse التحديثات في الوقت الفعلي؟",
      description: "يدعم ClickHouse التحديثات الخفيفة في الوقت الفعلي",
      href: "/ar/resources/support-center/knowledge-base/general-faqs/updates",
      category: "عام والأسئلة الشائعة",
      tags: []
    },
    {
      id: "security/row-column-policy",
      title: "هل يدعم ClickHouse الأمان على مستوى الصف والعمود؟",
      description: "تعرّف على قيود الوصول على مستوى الصف والعمود في ClickHouse وClickHouse Cloud، وكيفية تطبيق التحكم في الوصول القائم على الأدوار (RBAC) باستخدام السياسات.",
      href: "/ar/resources/support-center/knowledge-base/security/row-column-policy",
      category: "الأمان والتحكم في الوصول",
      tags: ["Security and Authentication"]
    },
    {
      id: "cloud-services/execute-system-queries-in-cloud",
      title: "تنفيذ عبارات SYSTEM على جميع العقد في ClickHouse Cloud",
      description: "تعرّف على كيفية استخدام `ON CLUSTER` و`clusterAllReplicas` لتنفيذ عبارات SYSTEM والاستعلامات عبر جميع العقد في خدمة ClickHouse Cloud.",
      href: "/ar/resources/support-center/knowledge-base/cloud-services/execute-system-queries-in-cloud",
      category: "Cloud",
      tags: ["Deployments and Scaling"]
    },
    {
      id: "troubleshooting/count-parts-by-type",
      title: "البحث عن أعداد وأحجام الأجزاء الواسعة أو المضغوطة",
      description: "توضح هذه المقالة في قاعدة المعرفة كيفية البحث عن أعداد الأجزاء حسب نوع الجزء - واسع أو مضغوط.",
      href: "/ar/resources/support-center/knowledge-base/troubleshooting/count-parts-by-type",
      category: "استكشاف الأخطاء وإصلاحها",
      tags: ["Troubleshooting"]
    },
    {
      id: "troubleshooting/fix-developer-verification-error-in-macos",
      title: "إصلاح خطأ التحقق من المطور في macOS",
      description: "تعرّف على كيفية حل خطأ التحقق من المطور في MacOS عند تشغيل أوامر ClickHouse، باستخدام إعدادات النظام أو الطرفية.",
      href: "/ar/resources/support-center/knowledge-base/troubleshooting/fix-developer-verification-error-in-macos",
      category: "استكشاف الأخطاء وإصلاحها",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "data-import-export/s3-export-data-year-month-folders",
      title: "كيف يمكنني إجراء كتابات مقسّمة حسب السنة والشهر على S3؟",
      description: "تعرّف على كيفية كتابة البيانات المقسّمة حسب السنة والشهر إلى حاوية S3 في ClickHouse، باستخدام بنية مسار مخصصة لتنظيم البيانات.",
      href: "/ar/resources/support-center/knowledge-base/data-import-export/s3-export-data-year-month-folders",
      category: "استيراد البيانات وتصديرها",
      tags: ["Data Export", "Native Clients and Interfaces"]
    },
    {
      id: "data-import-export/kafka-clickhouse-json",
      title: "كيف يمكنني استخدام نوع بيانات JSON الجديد مع Kafka؟",
      description: "Learn how to load JSON messages from Apache Kafka directly into a single JSON column in ClickHouse using the Kafka table engine and JSON data type.",
      href: "/ar/resources/support-center/knowledge-base/data-import-export/kafka-clickhouse-json",
      category: "Data import & export",
      tags: ["Data Formats", "Data Ingestion"]
    },
    {
      id: "cloud-services/change-billing-email",
      title: "كيف أغيّر جهة اتصال الفوترة في ClickHouse Cloud؟",
      description: "Let's learn how to change your billing address in ClickHouse Cloud.",
      href: "/ar/resources/support-center/knowledge-base/cloud-services/change-billing-email",
      category: "Cloud",
      tags: ["Managing Cloud"]
    },
    {
      id: "general-faqs/how-do-i-contribute-code-to-clickhouse",
      title: "How do I contribute code to ClickHouse?",
      description: "ClickHouse is an open-source project developed on GitHub. As customary, contribution instructions are published in CONTRIBUTING file in the root of the source code repository.",
      href: "/ar/resources/support-center/knowledge-base/general-faqs/how-do-i-contribute-code-to-clickhouse",
      category: "General & FAQs",
      tags: ["Community"]
    },
    {
      id: "data-import-export/parquet-to-csv-json",
      title: "كيف أحوّل الملفات من Parquet إلى CSV أو JSON؟",
      description: "Learn how to use ClickHouse's `clickhouse-local` tool to easily convert Parquet files to CSV or JSON formats.",
      href: "/ar/resources/support-center/knowledge-base/data-import-export/parquet-to-csv-json",
      category: "Data import & export",
      tags: ["Data Sources", "Data Formats"]
    },
    {
      id: "data-import-export/mysql-to-parquet-csv-json",
      title: "كيف أصدّر بيانات MySQL إلى Parquet أو CSV أو JSON باستخدام ClickHouse؟",
      description: "Learn how to use the `clickhouse-local` tool to export MySQL data into formats like Parquet, CSV, or JSON quickly and efficiently.",
      href: "/ar/resources/support-center/knowledge-base/data-import-export/mysql-to-parquet-csv-json",
      category: "Data import & export",
      tags: ["Data Formats", "Data Export"]
    },
    {
      id: "data-import-export/postgresql-to-parquet-csv-json",
      title: "How do I export PostgreSQL data to Parquet, CSV or JSON?",
      description: "Learn how to export PostgreSQL data to Parquet, CSV, or JSON formats using `clickhouse-local` with various examples.",
      href: "/ar/resources/support-center/knowledge-base/data-import-export/postgresql-to-parquet-csv-json",
      category: "Data import & export",
      tags: ["Data Export", "Data Formats"]
    },
    {
      id: "setup-installation/install-clickhouse-windows10",
      title: "كيف أثبّت ClickHouse على Windows 10؟",
      description: "Learn how to install and test ClickHouse on Windows 10 using WSL 2. Includes setup, troubleshooting, and running a test environment.",
      href: "/ar/resources/support-center/knowledge-base/setup-installation/install-clickhouse-windows10",
      category: "Setup & installation",
      tags: ["Tools and Utilities"]
    },
    {
      id: "security/remove-default-user",
      title: "How do I remove the default user?",
      description: "Learn how to remove the default user when running ClickHouse Server.",
      href: "/ar/resources/support-center/knowledge-base/security/remove-default-user",
      category: "Security & access control",
      tags: ["Server Admin"]
    },
    {
      id: "cloud-services/ingest-failures-23-9-release",
      title: "كيف أحلّ أخطاء الاستيعاب بعد إصدار ClickHouse 23.9؟",
      description: "Learn how to resolve ingest failures caused by stricter grant checking introduced in ClickHouse 23.9 for tables using `async_inserts`. Update grants to fix errors.",
      href: "/ar/resources/support-center/knowledge-base/cloud-services/ingest-failures-23-9-release",
      category: "Cloud",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "performance-optimization/insert-select-settings-tuning",
      title: "How do I solve TOO MANY PARTS error during an INSERT...SELECT?",
      description: "Resolve the TOO_MANY_PARTS error in ClickHouse during an `INSERT...SELECT` by tuning expert-level settings for larger blocks and increasing partition thresholds.",
      href: "/ar/resources/support-center/knowledge-base/performance-optimization/insert-select-settings-tuning",
      category: "Performance & optimization",
      tags: ["Settings", "Errors and Exceptions"]
    },
    {
      id: "integrations/node-js-example",
      title: "How do I use NodeJS with @clickhouse/client",
      description: "Learn how to use @clickhouse/client in a Node.js application to interact with ClickHouse and perform queries.",
      href: "/ar/resources/support-center/knowledge-base/integrations/node-js-example",
      category: "Integrations & client libraries",
      tags: ["Language Clients"]
    },
    {
      id: "monitoring-debugging/view-number-of-active-mutations",
      title: "How do I view the number of active or queued mutations?",
      description:
        "Monitor the number of active or queued mutations in ClickHouse, especially when performing `ALTER` or `UPDATE` operations. Use the `system.mutations` table for tracking mutations.",
      href: "/ar/resources/support-center/knowledge-base/monitoring-debugging/view-number-of-active-mutations",
      category: "Monitoring & debugging",
      tags: ["System Tables"]
    },
    {
      id: "data-management/read-consistency",
      title: "How to achieve data read consistency in ClickHouse?",
      description: "Learn how to ensure data consistency when reading from ClickHouse, whether you're connected to the same node or a random node.",
      href: "/ar/resources/support-center/knowledge-base/data-management/read-consistency",
      category: "Data management",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "setup-installation/llvm-clang-up-to-date",
      title: "How to build LLVM and clang on Linux",
      description: "Commands to build LLVM and clang on Linux.",
      href: "/ar/resources/support-center/knowledge-base/setup-installation/llvm-clang-up-to-date",
      category: "Setup & installation",
      tags: ["Community", "Tools and Utilities"]
    },
    {
      id: "data-management/calculate-ratio-of-zero-sparse-serialization",
      title: "How to calculate the ratio of empty/zero values in every column in a table",
      description: "Learn how to calculate the ratio of empty or zero values in every column of a ClickHouse table to optimize sparse column serialization.",
      href: "/ar/resources/support-center/knowledge-base/data-management/calculate-ratio-of-zero-sparse-serialization",
      category: "Data management",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "security/check-users-roles",
      title: "How to Check Users Assigned to Roles and Vice Versa",
      description: "Learn how to query ClickHouse's `system.role_grants` to find users assigned to roles and roles assigned to specific users.",
      href: "/ar/resources/support-center/knowledge-base/security/check-users-roles",
      category: "Security & access control",
      tags: ["Server Admin", "System Tables", "Managing Cloud"]
    },
    {
      id: "monitoring-debugging/which-processes-are-currently-running",
      title: "How to check what code is currently running on a server?",
      description:
        "ClickHouse provides introspection tools like `system.stack_trace` for inspecting what code is currently running on each server thread, helping with debugging and performance monitoring.",
      href: "/ar/resources/support-center/knowledge-base/monitoring-debugging/which-processes-are-currently-running",
      category: "Monitoring & debugging",
      tags: ["Server Admin"]
    },
    {
      id: "cloud-services/how-to-check-my-clickhouse-cloud-sevice-state",
      title: "كيف أتحقق من حالة خدمة ClickHouse Cloud؟",
      description: "Learn how to use the ClickHouse Cloud API to check if your service is stopped, idle, or running without waking it up.",
      href: "/ar/resources/support-center/knowledge-base/cloud-services/how-to-check-my-clickhouse-cloud-sevice-state",
      category: "Cloud",
      tags: ["Managing Cloud"]
    },
    {
      id: "monitoring-debugging/collect-and-draw-traces",
      title: "كيفية جمع أثر الاستعلام ورسمه",
      description:
        "يوضّح هذا الدليل كيفية جمع آثار الاستعلامات ورسمها في ClickHouse المُدار ذاتيًا باستخدام الأساليب المدمجة أو Grafana. يُعدّ هذا مفيدًا بشكل خاص عند العمل مع استعلامات معقدة وتحتاج إلى فهم آليات التنفيذ الداخلية التي تتجاوز ما يوفّره EXPLAIN.",
      href: "/ar/resources/support-center/knowledge-base/monitoring-debugging/collect-and-draw-traces",
      category: "Monitoring & debugging",
      tags: ["Tools and Utilities"]
    },
    {
      id: "configuration-settings/configure-a-user-setting",
      title: "كيفية تكوين الإعدادات لمستخدم في ClickHouse",
      description: "تعرّف على كيفية تعريف الإعدادات في ClickHouse للاستعلامات الفردية أو جلسات العميل أو مستخدمين محددين باستخدام أوامر `SET` و`ALTER USER`.",
      href: "/ar/resources/support-center/knowledge-base/configuration-settings/configure-a-user-setting",
      category: "Configuration & settings",
      tags: ["Settings"]
    },
    {
      id: "materialized-views/projection-example",
      title: "كيفية التحقق مما إذا كانت الإسقاطات مستخدمة في الاستعلام؟",
      description: "تعرّف على كيفية التحقق مما إذا كانت الإسقاطات مستخدمة في استعلامات ClickHouse عن طريق الاختبار ببيانات نموذجية واستخدام EXPLAIN للتحقق من استخدام الإسقاطات.",
      href: "/ar/resources/support-center/knowledge-base/materialized-views/projection-example",
      category: "Materialized views & projections",
      tags: ["Data Modelling"]
    },
    {
      id: "cloud-services/how-to-connect-to-ch-cloud-using-ssh-keys",
      title: "كيفية الاتصال بـ ClickHouse باستخدام مفاتيح SSH",
      description: "كيفية الاتصال بـ ClickHouse وClickHouse Cloud باستخدام مفاتيح SSH",
      href: "/ar/resources/support-center/knowledge-base/cloud-services/how-to-connect-to-ch-cloud-using-ssh-keys",
      category: "Cloud",
      tags: ["Managing Cloud", "Security and Authentication"]
    },
    {
      id: "data-management/dictionary-using-strings",
      title: "كيفية إنشاء قاموس ClickHouse بمفاتيح وقيم نصية",
      description: "تعرّف على كيفية إنشاء قاموس ClickHouse باستخدام مفاتيح وقيم نصية من جدول MergeTree كمصدر، مع أمثلة على الإعداد والاستخدام.",
      href: "/ar/resources/support-center/knowledge-base/data-management/dictionary-using-strings",
      category: "Data management",
      tags: ["Data Modelling"]
    },
    {
      id: "tables-schema/how-to-create-table-to-query-multiple-remote-clusters",
      title: "كيفية إنشاء جدول يمكنه الاستعلام من مجموعات بعيدة متعددة",
      description: "كيفية إنشاء جدول يمكنه الاستعلام من مجموعات بعيدة متعددة",
      href: "/ar/resources/support-center/knowledge-base/tables-schema/how-to-create-table-to-query-multiple-remote-clusters",
      category: "Tables & schema",
      tags: ["Deployments and Scaling"]
    },
    {
      id: "setup-installation/enabling-ssl-with-lets-encrypt",
      title: "كيفية تفعيل SSL مع Let's Encrypt على خادم ClickHouse واحد",
      description: "تعرّف على كيفية إعداد SSL لخادم ClickHouse واحد باستخدام Let's Encrypt، بما في ذلك إصدار الشهادات والتكوين والتحقق.",
      href: "/ar/resources/support-center/knowledge-base/setup-installation/enabling-ssl-with-lets-encrypt",
      category: "Setup & installation",
      tags: ["Security and Authentication"]
    },
    {
      id: "data-import-export/file-export",
      title: "كيفية تصدير البيانات من ClickHouse إلى ملف",
      description: "تعرّف على الطرق المختلفة لتصدير البيانات من ClickHouse، بما في ذلك `INTO OUTFILE` ومحرك جدول الملفات وإعادة توجيه سطر الأوامر.",
      href: "/ar/resources/support-center/knowledge-base/data-import-export/file-export",
      category: "Data import & export",
      tags: ["Data Export"]
    },
    {
      id: "queries-sql/how-to-filter-a-clickhouse-table-by-an-array-column",
      title: "كيفية تصفية جدول ClickHouse حسب عمود مصفوفة؟",
      description: "مقالة قاعدة المعرفة حول كيفية تصفية جدول ClickHouse حسب عمود مصفوفة.",
      href: "/ar/resources/support-center/knowledge-base/queries-sql/how-to-filter-a-clickhouse-table-by-an-array-column",
      category: "Queries & SQL",
      tags: ["Data Modelling", "Functions"]
    },
    {
      id: "monitoring-debugging/generate-har-file",
      title: "كيفية إنشاء ملف HAR للدعم الفني",
      description: "يلتقط ملف HAR (أرشيف HTTP) نشاط الشبكة في متصفحك. يمكنه مساعدة فريق الدعم لدينا في تشخيص بطء تحميل الصفحات أو الطلبات الفاشلة أو مشكلات الشبكة الأخرى.",
      href: "/ar/resources/support-center/knowledge-base/monitoring-debugging/generate-har-file",
      category: "Monitoring & debugging",
      tags: ["Tools and Utilities"]
    },
    {
      id: "materialized-views/how-to-display-queries-using-mv",
      title: "كيفية تحديد الاستعلامات التي تستخدم المشاهدات المادية في ClickHouse",
      description: "تعرّف على كيفية الاستعلام من سجلات ClickHouse لتحديد جميع الاستعلامات التي تتضمن المشاهدات المادية ضمن نطاق زمني محدد.",
      href: "/ar/resources/support-center/knowledge-base/materialized-views/how-to-display-queries-using-mv",
      category: "Materialized views & projections",
      tags: ["System Tables"]
    },
    {
      id: "performance-optimization/find-expensive-queries",
      title: "كيفية تحديد الاستعلامات الأكثر تكلفةً في ClickHouse",
      description: "تعرّف على كيفية استخدام جدول `query_log` في ClickHouse لتحديد الاستعلامات الأكثر استهلاكًا للذاكرة ووحدة المعالجة المركزية عبر العقد الموزعة.",
      href: "/ar/resources/support-center/knowledge-base/performance-optimization/find-expensive-queries",
      category: "Performance & optimization",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "configuration-settings/ignoring-incorrect-settings",
      title: "كيفية تجاهل الإعدادات غير الصحيحة في ClickHouse",
      description: "تعرّف على كيفية استخدام خيار `skip_check_for_incorrect_settings` للسماح لـ ClickHouse بالبدء حتى عند تحديد إعدادات مستوى المستخدم بشكل غير صحيح.",
      href: "/ar/resources/support-center/knowledge-base/configuration-settings/ignoring-incorrect-settings",
      category: "Configuration & settings",
      tags: ["Settings"]
    },
    {
      id: "data-import-export/json-import",
      title: "كيفية استيراد JSON إلى ClickHouse؟",
      description: "تعرض هذه الصفحة كيفية استيراد JSON إلى ClickHouse",
      href: "/ar/resources/support-center/knowledge-base/data-import-export/json-import",
      category: "Data import & export",
      tags: []
    },
    {
      id: "setup-installation/how-to-increase-thread-pool-size",
      title: "كيفية زيادة عدد الخيوط في ClickHouse",
      description: "تعرّف على كيفية تكوين مجموعة الخيوط العامة في ClickHouse عن طريق ضبط إعدادات مثل `max_thread_pool_size` و`thread_pool_queue_size` و`max_thread_pool_free_size`.",
      href: "/ar/resources/support-center/knowledge-base/setup-installation/how-to-increase-thread-pool-size",
      category: "Setup & installation",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "data-import-export/kafka-to-clickhouse-setup",
      title: "كيفية استيعاب البيانات من Kafka إلى ClickHouse",
      description: "تعرّف على كيفية استيعاب البيانات من موضوع Kafka إلى ClickHouse باستخدام محرك جدول Kafka والمشاهدات المادية وجداول MergeTree.",
      href: "/ar/resources/support-center/knowledge-base/data-import-export/kafka-to-clickhouse-setup",
      category: "Data import & export",
      tags: ["Data Ingestion"]
    },
    {
      id: "data-import-export/ingest-parquet-files-in-s3",
      title: "كيفية استيعاب ملفات Parquet من حاوية S3",
      description: "تعرّف على أساسيات استخدام محرك جدول S3 في ClickHouse لاستيعاب ملفات Parquet والاستعلام منها من حاوية S3، بما في ذلك الإعداد وأذونات الوصول وأمثلة استيراد البيانات.",
      href: "/ar/resources/support-center/knowledge-base/data-import-export/ingest-parquet-files-in-s3",
      category: "Data import & export",
      tags: ["Data Ingestion"]
    },
    {
      id: "queries-sql/how-to-insert-all-rows-from-another-table",
      title: "كيفية إدراج جميع الصفوف من جدول إلى آخر؟",
      description: "مقالة قاعدة المعرفة حول كيفية إدراج جميع الصفوف من جدول إلى آخر.",
      href: "/ar/resources/support-center/knowledge-base/queries-sql/how-to-insert-all-rows-from-another-table",
      category: "Queries & SQL",
      tags: ["Data Ingestion"]
    },
    {
      id: "performance-optimization/check-query-processing-time-only",
      title: "كيفية قياس وقت معالجة الاستعلام دون إرجاع صفوف",
      description: "تعرّف على كيفية استخدام خيار `FORMAT Null` في ClickHouse لقياس وقت معالجة الاستعلام دون إرجاع أي صفوف إلى العميل.",
      href: "/ar/resources/support-center/knowledge-base/performance-optimization/check-query-processing-time-only",
      category: "Performance & optimization",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "cloud-services/opt-out-core-dump-collection",
      title: "كيفية إلغاء الاشتراك في جمع تقارير الأعطال",
      description: "يوضّح هذا المقال كيفية إلغاء الاشتراك في جمع تقارير الأعطال على ClickHouse Cloud",
      href: "/ar/resources/support-center/knowledge-base/cloud-services/opt-out-core-dump-collection",
      category: "Cloud",
      tags: ["Managing Cloud"]
    },
    {
      id: "monitoring-debugging/outputSendLogsLevelTracesToFile",
      title: "How to output send logs level traces to file using the clickhouse-client",
      description: "How to output send logs level traces to file using the clickhouse-client",
      href: "/ar/resources/support-center/knowledge-base/monitoring-debugging/outputSendLogsLevelTracesToFile",
      category: "Monitoring & debugging",
      tags: ["Data Export"]
    },
    {
      id: "tables-schema/recreate-table-across-terminals",
      title: "How to quickly recreate a small table across different terminals",
      description: "Learn how to quickly recreate a small table and its data across different terminals using copy/paste for development environments.",
      href: "/ar/resources/support-center/knowledge-base/tables-schema/recreate-table-across-terminals",
      category: "Tables & schema",
      tags: ["Tools and Utilities"]
    },
    {
      id: "troubleshooting/recovering-from-corrupt-keeper-snapshot",
      title: "كيفية الاسترداد من لقطة Keeper تالفة",
      description: "مقال يصف كيفية الاسترداد من لقطة Keeper تالفة: كيف تظهر المشكلة، وما هي اللقطة وأين تجدها، والاستراتيجيات الممكنة للاسترداد.",
      href: "/ar/resources/support-center/knowledge-base/troubleshooting/recovering-from-corrupt-keeper-snapshot",
      category: "Troubleshooting & errors",
      tags: ["Troubleshooting"]
    },
    {
      id: "troubleshooting/restore-replica-after-storage-failure",
      title: "كيفية استعادة نسخة متماثلة بعد فشل التخزين",
      description: "يشرح هذا المقال كيفية استرداد البيانات عند استخدام الجداول المتماثلة في قواعد البيانات الذرية في ClickHouse وفقدان الأقراص/التخزين في إحدى النسخ المتماثلة أو تلفها.",
      href: "/ar/resources/support-center/knowledge-base/troubleshooting/restore-replica-after-storage-failure",
      category: "Troubleshooting & errors",
      tags: ["Deployments and Scaling"]
    },
    {
      id: "integrations/how-to-set-up-ch-on-docker-odbc-connect-mssql",
      title: "How to set up ClickHouse on Docker with ODBC to connect to a Microsoft SQL Server (MSSQL) database",
      description: "How to set up ClickHouse on Docker with ODBC to connect to a Microsoft SQL Server (MSSQL) database",
      href: "/ar/resources/support-center/knowledge-base/integrations/how-to-set-up-ch-on-docker-odbc-connect-mssql",
      category: "Integrations & client libraries",
      tags: ["Native Clients and Interfaces"]
    },
    {
      id: "queries-sql/using-array-join-to-extract-and-query-attributes",
      title: "How to use array join to extract and query varying attributes using map keys and values",
      description: "Simple example to illustrate how to use array join to extract and query varying attributes using map keys and values",
      href: "/ar/resources/support-center/knowledge-base/queries-sql/using-array-join-to-extract-and-query-attributes",
      category: "Queries & SQL",
      tags: ["Functions"]
    },
    {
      id: "materialized-views/how-to-use-parametrised-views",
      title: "How to Use Parameterized Views in ClickHouse",
      description: "Learn how to create and query parameterized views in ClickHouse for dynamic data slicing based on query-time parameters.",
      href: "/ar/resources/support-center/knowledge-base/materialized-views/how-to-use-parametrised-views",
      category: "Materialized views & projections",
      tags: ["Use Cases"]
    },
    {
      id: "tables-schema/exchangeStatementToSwitchTables",
      title: "How to use the exchange command to switch tables",
      description: "How to use the exchange command to switch tables",
      href: "/ar/resources/support-center/knowledge-base/tables-schema/exchangeStatementToSwitchTables",
      category: "Tables & schema",
      tags: ["Managing Data"]
    },
    {
      id: "queries-sql/compare-resultsets",
      title: "How to Validate if Two Queries Return the Same Result-sets",
      description: "Learn how to validate that two ClickHouse queries produce identical result-sets using hash functions and comparison techniques.",
      href: "/ar/resources/support-center/knowledge-base/queries-sql/compare-resultsets",
      category: "Queries & SQL",
      tags: ["Functions"]
    },
    {
      id: "monitoring-debugging/check-query-cache-in-use",
      title: "How to verify query cache usage in ClickHouse",
      description: "Learn how to check if query cache is being utilized in ClickHouse using `clickhouse-client` trace logs or SQL commands.",
      href: "/ar/resources/support-center/knowledge-base/monitoring-debugging/check-query-cache-in-use",
      category: "Monitoring & debugging",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "cloud-services/unable-to-access-cloud-service",
      title: "I am unable to access a ClickHouse Cloud service",
      description: "Troubleshooting access issues with ClickHouse Cloud services, including IP Access List configuration",
      href: "/ar/resources/support-center/knowledge-base/cloud-services/unable-to-access-cloud-service",
      category: "Cloud",
      tags: ["Errors and Exceptions", "Managing Cloud"]
    },
    {
      id: "performance-optimization/finding-expensive-queries-by-memory-usage",
      title: "Identifying Expensive Queries by Memory Usage in ClickHouse",
      description: "Learn how to use the `system.query_log` table to find the most memory-intensive queries in ClickHouse, with examples for clustered and standalone setups.",
      href: "/ar/resources/support-center/knowledge-base/performance-optimization/finding-expensive-queries-by-memory-usage",
      category: "Performance & optimization",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "data-import-export/importing-and-working-with-json-array-objects",
      title: "Importing and Querying JSON Array Objects in ClickHouse",
      description: "Learn how to import JSON array objects into ClickHouse and perform advanced queries using JSON functions and array operations.",
      href: "/ar/resources/support-center/knowledge-base/data-import-export/importing-and-working-with-json-array-objects",
      category: "Data import & export",
      tags: ["Data Formats"]
    },
    {
      id: "data-import-export/importing-geojason-with-nested-object-array",
      title: "Importing GeoJSON with a deeply nested object array",
      description: "Learn how to import GeoJSON files with deeply nested object arrays into ClickHouse and query the nested feature data.",
      href: "/ar/resources/support-center/knowledge-base/data-import-export/importing-geojason-with-nested-object-array",
      category: "Data import & export",
      tags: ["Data Formats"]
    },
    {
      id: "performance-optimization/improve-map-performance",
      title: "Improving Map lookup performance in ClickHouse",
      description: "Learn how to optimize Map column lookups in ClickHouse for better query performance by materializing specific keys as standalone columns.",
      href: "/ar/resources/support-center/knowledge-base/performance-optimization/improve-map-performance",
      category: "Performance & optimization",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "tables-schema/delete-old-data",
      title: "Is it possible to delete old records from a ClickHouse table?",
      description: "This page answers the question of whether it is possible to delete old records from a ClickHouse table",
      href: "/ar/resources/support-center/knowledge-base/tables-schema/delete-old-data",
      category: "Tables & schema",
      tags: []
    },
    {
      id: "general-faqs/separate-storage",
      title: "Is it possible to deploy ClickHouse with separate storage and compute?",
      description: "This page provides an answer as to whether it is possible to deploy ClickHouse with separate storage and compute",
      href: "/ar/resources/support-center/knowledge-base/general-faqs/separate-storage",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "data-import-export/json-extract-example",
      title: "JSON Extract example",
      description: "A short example on how to extract base types from JSON",
      href: "/ar/resources/support-center/knowledge-base/data-import-export/json-extract-example",
      category: "Data import & export",
      tags: ["Data Formats"]
    },
    {
      id: "queries-sql/calculate-pi-using-sql",
      title: "Let's calculate pi using SQL",
      description: "It's Pi Day! Let's calculate pi using ClickHouse SQL",
      href: "/ar/resources/support-center/knowledge-base/queries-sql/calculate-pi-using-sql",
      category: "Queries & SQL",
      tags: ["Use Cases"]
    },
    {
      id: "cloud-services/clickhouse-cloud-api-usage",
      title: "Managing ClickHouse Cloud Service with API and cURL",
      description: "Learn how to start, stop, and resume a ClickHouse Cloud service using API endpoints and cURL commands.",
      href: "/ar/resources/support-center/knowledge-base/cloud-services/clickhouse-cloud-api-usage",
      category: "Cloud",
      tags: ["Managing Cloud", "Tools and Utilities"]
    },
    {
      id: "monitoring-debugging/mapping-of-system-metrics-to-prometheus-metrics",
      title: "Mapping of metrics used in system.dashboards to Prometheus metrics in `system.custom_metrics`",
      description: "Mapping of metrics used in system.dashboards to Prometheus metrics in system.custom_metrics",
      href: "/ar/resources/support-center/knowledge-base/monitoring-debugging/mapping-of-system-metrics-to-prometheus-metrics",
      category: "Monitoring & debugging",
      tags: ["System Tables"]
    },
    {
      id: "security/windows-active-directory-to-ch-roles",
      title: "Mapping Windows Active Directory security groups to ClickHouse roles",
      description: "Example of mapping Windows Active Directory security groups to ClickHouse roles",
      href: "/ar/resources/support-center/knowledge-base/security/windows-active-directory-to-ch-roles",
      category: "Security & access control",
      tags: ["Tools and Utilities"]
    },
    {
      id: "performance-optimization/memory-limit-exceeded-for-query",
      title: "Memory limit exceeded for query",
      description: "Troubleshooting memory limit exceeded errors for a query",
      href: "/ar/resources/support-center/knowledge-base/performance-optimization/memory-limit-exceeded-for-query",
      category: "Performance & optimization",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "integrations/ODBC-authentication-failed-error-using-PowerBI-CH-connector",
      title: "ODBC authentication failed error when using the Power BI ClickHouse connector",
      description: "ODBC authentication failed error when using the Power BI ClickHouse connector",
      href: "/ar/resources/support-center/knowledge-base/integrations/ODBC-authentication-failed-error-using-PowerBI-CH-connector",
      category: "Integrations & client libraries",
      tags: ["Native Clients and Interfaces", "Errors and Exceptions"]
    },
    {
      id: "monitoring-debugging/profiling-clickhouse-with-llvm-xray",
      title: "Profiling ClickHouse with LLVM's XRay",
      description: "Learn how to profile ClickHouse using LLVM's XRay instrumentation profiler, visualize traces, and analyze performance.",
      href: "/ar/resources/support-center/knowledge-base/monitoring-debugging/profiling-clickhouse-with-llvm-xray",
      category: "Monitoring & debugging",
      tags: ["Performance and Optimizations", "Tools and Utilities"]
    },
    {
      id: "integrations/python-http-requests",
      title: "Python quick example using HTTP requests module",
      description: "An example using Python and requests module to write and read to ClickHouse",
      href: "/ar/resources/support-center/knowledge-base/integrations/python-http-requests",
      category: "Integrations & client libraries",
      tags: ["Native Clients and Interfaces"]
    },
    {
      id: "configuration-settings/maximum-number-of-tables-and-databases",
      title: "Recommended Maximum Databases, Tables, Partitions, and Parts in ClickHouse",
      description: "Learn the recommended maximum limits for databases, tables, partitions, and parts in a ClickHouse cluster to ensure optimal performance.",
      href: "/ar/resources/support-center/knowledge-base/configuration-settings/maximum-number-of-tables-and-databases",
      category: "Configuration & settings",
      tags: ["Performance and Optimizations", "Deployments and Scaling"]
    },
    {
      id: "data-import-export/cannot-append-data-to-parquet-format",
      title: 'Resolving "Cannot Append Data in Parquet Format" error in ClickHouse',
      description: 'Are you getting the error "Cannot append data in format Parquet to file" error in ClickHouse? Let\'s take a look at how to resolve it.',
      href: "/ar/resources/support-center/knowledge-base/data-import-export/cannot-append-data-to-parquet-format",
      category: "Data import & export",
      tags: ["Errors and Exceptions", "Data Formats"]
    },
    {
      id: "troubleshooting/exception-too-many-parts",
      title: 'Resolving "Too Many Parts" error in ClickHouse',
      description: 'Learn how to address the "Too many parts" error in ClickHouse by optimizing insert rates, configuring MergeTree settings, and managing partitions effectively.',
      href: "/ar/resources/support-center/knowledge-base/troubleshooting/exception-too-many-parts",
      category: "Troubleshooting & errors",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "troubleshooting/certificate-verify-failed-error",
      title: "Resolving SSL Certificate Verify Error in ClickHouse",
      description: "Learn how to resolve the SSL Exception CERTIFICATE_VERIFY_FAILED error.",
      href: "/ar/resources/support-center/knowledge-base/troubleshooting/certificate-verify-failed-error",
      category: "Troubleshooting & errors",
      tags: ["Security and Authentication", "Errors and Exceptions"]
    },
    {
      id: "troubleshooting/connection-timeout-remote-remoteSecure",
      title: "Resolving Timeout Errors with `remote` and `remoteSecure` Table Functions",
      description: "Learn how to fix timeout errors when using `remote` or `remoteSecure` table functions in ClickHouse by adjusting the connection timeout settings.",
      href: "/ar/resources/support-center/knowledge-base/troubleshooting/connection-timeout-remote-remoteSecure",
      category: "Troubleshooting & errors",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "tables-schema/runbook-json",
      title: "Runbook: JSON schema",
      description: "اختر نهج المخطط المناسب لبيانات JSON في ClickHouse — أعمدة مكتوبة، أو هجين، أو JSON أصلي، أو تخزين String",
      href: "/ar/resources/support-center/knowledge-base/tables-schema/runbook-json",
      category: "Tables & schema",
      tags: ["Runbooks", "Data Modelling"]
    },
    {
      id: "tables-schema/search-across-node-for-tables-with-a-wildcard",
      title: "Searching across nodes for tables with a wildcard",
      description: "Learn how to search across nodes for tables with a wildcard.",
      href: "/ar/resources/support-center/knowledge-base/tables-schema/search-across-node-for-tables-with-a-wildcard",
      category: "Tables & schema",
      tags: ["Deployments and Scaling"]
    },
    {
      id: "performance-optimization/query-max-execution-time",
      title: "Setting a limit on query execution time",
      description: "How to enforce limit on max query execution time",
      href: "/ar/resources/support-center/knowledge-base/performance-optimization/query-max-execution-time",
      category: "Performance & optimization",
      tags: ["Managing Cloud", "Settings"]
    },
    {
      id: "data-import-export/json-simple-example",
      title: "Simple example flow for extracting JSON data using a landing table with a Materialized View",
      description: "Simple example flow for extracting JSON data using a landing table with a Materialized View",
      href: "/ar/resources/support-center/knowledge-base/data-import-export/json-simple-example",
      category: "Data import & export",
      tags: ["Data Formats"]
    },
    {
      id: "performance-optimization/async-vs-optimize-read-in-order",
      title: "Synchronous data reading",
      description:
        "The new setting `allow_asynchronous_read_from_io_pool_for_merge_tree` allows the number of reading threads (streams) to be higher than the number of threads in the rest of the query execution pipeline.",
      href: "/ar/resources/support-center/knowledge-base/performance-optimization/async-vs-optimize-read-in-order",
      category: "Performance & optimization",
      tags: ["Settings", "Performance and Optimizations"]
    },
    {
      id: "integrations/terraform-example",
      title: "Terraform example on how to use Cloud API",
      description: "This covers an example of how you can use terraform to create/delete clusters using the API",
      href: "/ar/resources/support-center/knowledge-base/integrations/terraform-example",
      category: "Integrations & client libraries",
      tags: ["Native Clients and Interfaces"]
    },
    {
      id: "performance-optimization/tips-tricks-optimizing-basic-data-types-in-clickhouse",
      title: "Tips and tricks on optimizing basic data types in ClickHouse",
      description: "Tips and tricks on optimizing basic data types in ClickHouse",
      href: "/ar/resources/support-center/knowledge-base/performance-optimization/tips-tricks-optimizing-basic-data-types-in-clickhouse",
      category: "Performance & optimization",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "data-management/understanding-part-types-and-storage-formats",
      title: "فهم أنواع الأجزاء وتنسيقات التخزين",
      description: "تعرّف على أنواع الأجزاء المختلفة (Wide مقابل Compact) وتنسيقات التخزين (Full مقابل Packed) في ClickHouse، وكيف تؤثر على الأداء.",
      href: "/ar/resources/support-center/knowledge-base/data-management/understanding-part-types-and-storage-formats",
      category: "Data management",
      tags: ["Core Data Concepts"]
    },
    {
      id: "queries-sql/useful-queries-for-troubleshooting",
      title: "Useful queries for troubleshooting",
      description: "A collection of handy queries for troubleshooting ClickHouse, including monitoring table sizes, long-running queries, and errors.",
      href: "/ar/resources/support-center/knowledge-base/queries-sql/useful-queries-for-troubleshooting",
      category: "Queries & SQL",
      tags: ["Settings"]
    },
    {
      id: "general-faqs/use-clickhouse-for-log-analytics",
      title: "Using ClickHouse for log analytics",
      description: "ClickHouse is popular for logs and metrics analysis because of the real-time analytics capabilities provided. Ready to find out more?",
      href: "/ar/resources/support-center/knowledge-base/general-faqs/use-clickhouse-for-log-analytics",
      category: "General & FAQs",
      tags: ["Use Cases"]
    },
    {
      id: "queries-sql/filtered-aggregates",
      title: "Using filtered aggregates in ClickHouse",
      description: "Learn how to use filtered aggregates in ClickHouse with `-If` and `-Distinct` aggregate combinators to simplify query syntax and enhance analytics.",
      href: "/ar/resources/support-center/knowledge-base/queries-sql/filtered-aggregates",
      category: "Queries & SQL",
      tags: ["Functions"]
    },
    {
      id: "general-faqs/dependencies",
      title: "What are the 3rd-party dependencies for running ClickHouse?",
      description: "ClickHouse is self-contained and has no runtime dependencies",
      href: "/ar/resources/support-center/knowledge-base/general-faqs/dependencies",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/dbms-naming",
      title: 'What does "ClickHouse" mean?',
      description: 'Learn about What does "ClickHouse" mean?',
      href: "/ar/resources/support-center/knowledge-base/general-faqs/dbms-naming",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/ne-tormozit",
      title: "What does “не тормозит” mean?",
      description: 'This page explains what "Не тормозит" means',
      href: "/ar/resources/support-center/knowledge-base/general-faqs/ne-tormozit",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "integrations/oracle-odbc",
      title: "What if I have a problem with encodings when using Oracle via ODBC?",
      description: "This page provides guidance on what to do if you have a problem with encodings when using Oracle via ODBC",
      href: "/ar/resources/support-center/knowledge-base/integrations/oracle-odbc",
      category: "Integrations & client libraries",
      tags: []
    },
    {
      id: "general-faqs/columnar-database",
      title: "What is a columnar database?",
      description: "A columnar database stores the data of each column independently. This allows reading data from disk only for those columns that are used in any given query.",
      href: "/ar/resources/support-center/knowledge-base/general-faqs/columnar-database",
      category: "General & FAQs",
      tags: ["Core Data Concepts"]
    },
    {
      id: "general-faqs/olap",
      title: "What is OLAP?",
      description: "An explainer on what Online Analytical Processing is",
      href: "/ar/resources/support-center/knowledge-base/general-faqs/olap",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "performance-optimization/optimize-final-vs-final",
      title: "What is the difference between OPTIMIZE FINAL and FINAL?",
      description: "Discusses the differences between OPTIMIZE FINAL and FINAL, and when to use and avoid them.",
      href: "/ar/resources/support-center/knowledge-base/performance-optimization/optimize-final-vs-final",
      category: "Performance & optimization",
      tags: ["Core Data Concepts"]
    },
    {
      id: "general-faqs/sql",
      title: "What SQL syntax does ClickHouse support?",
      description: "ClickHouse supports 100% of SQL syntax",
      href: "/ar/resources/support-center/knowledge-base/general-faqs/sql",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "data-management/when-is-ttl-applied",
      title: "When are TTL rules applied, and do we have control over it?",
      description:
        "TTL rules in ClickHouse are eventually applied, and you can control when they are executed using the `merge_with_ttl_timeout` setting. Learn how to force TTL application and manage background threads for TTL execution.",
      href: "/ar/resources/support-center/knowledge-base/data-management/when-is-ttl-applied",
      category: "Data management",
      tags: ["Core Data Concepts"]
    },
    {
      id: "setup-installation/production",
      title: "Which ClickHouse version to use in production?",
      description: "This page provides guidance on which ClickHouse version to use in production",
      href: "/ar/resources/support-center/knowledge-base/setup-installation/production",
      category: "Setup & installation",
      tags: []
    },
    {
      id: "general-faqs/who-is-using-clickhouse",
      title: "Who is using ClickHouse?",
      description: "Describes who is using ClickHouse",
      href: "/ar/resources/support-center/knowledge-base/general-faqs/who-is-using-clickhouse",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "data-management/dictionaries-consistent-state",
      title: "Why can't I see my data in a dictionary in ClickHouse Cloud?",
      description: "There is an issue where data in dictionaries may not be visible immediately after creation.",
      href: "/ar/resources/support-center/knowledge-base/data-management/dictionaries-consistent-state",
      category: "Data management",
      tags: ["Managing Cloud", "Data Modelling"]
    },
    {
      id: "general-faqs/why-recommend-clickhouse-keeper-over-zookeeper",
      title: "Why is ClickHouse Keeper recommended over ZooKeeper?",
      description:
        "ClickHouse Keeper improves upon ZooKeeper with features like reduced disk space usage, faster recovery, and less memory consumption, offering better performance for ClickHouse clusters.",
      href: "/ar/resources/support-center/knowledge-base/general-faqs/why-recommend-clickhouse-keeper-over-zookeeper",
      category: "General & FAQs",
      tags: ["Core Data Concepts"]
    },
    {
      id: "monitoring-debugging/why-default-logging-verbose",
      title: "Why is ClickHouse logging so verbose by default?",
      description: "Learn why the ClickHouse developers chose to set a verbose logging level by default.",
      href: "/ar/resources/support-center/knowledge-base/monitoring-debugging/why-default-logging-verbose",
      category: "Monitoring & debugging",
      tags: ["Settings"]
    },
    {
      id: "performance-optimization/why-is-my-primary-key-not-used",
      title: "Why is my primary key not used? How can I check?",
      description: "Covers a common reason why a primary key is not used in ordering and how we can confirm",
      href: "/ar/resources/support-center/knowledge-base/performance-optimization/why-is-my-primary-key-not-used",
      category: "Performance & optimization",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "general-faqs/mapreduce",
      title: "Why not use something like MapReduce?",
      description: "This page explains why you would use ClickHouse over MapReduce",
      href: "/ar/resources/support-center/knowledge-base/general-faqs/mapreduce",
      category: "General & FAQs",
      tags: []
    }
  ]
}