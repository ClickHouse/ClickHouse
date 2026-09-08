export const kbIndex = {
  categories: [
    "Cloud",
    "Configuration et paramètres",
    "Import et export de données",
    "Gestion des données",
    "Général et FAQ",
    "Intégrations et bibliothèques clientes",
    "Vues matérialisées et projections",
    "Surveillance et débogage",
    "Performance et optimisation",
    "Requêtes et SQL",
    "Sécurité et contrôle d'accès",
    "Installation et configuration",
    "Tables et schéma",
    "Dépannage et erreurs"
  ],
  tags: [
    "Bonnes pratiques",
    "Communauté",
    "Concepts",
    "Concepts fondamentaux des données",
    "Export de données",
    "Formats de données",
    "Ingestion de données",
    "Modélisation des données",
    "Sources de données",
    "Déploiements et mise à l'échelle",
    "Erreurs et exceptions",
    "Fonctions",
    "Clients de langage",
    "Gestion du Cloud",
    "Gestion des données",
    "Clients et interfaces natifs",
    "Performance et optimisations",
    "Sécurité et authentification",
    "Administration serveur",
    "Paramètres",
    "Tables système",
    "Outils et utilitaires",
    "Dépannage",
    "Cas d'usage"
  ],
  articles: [
    {
      id: "integrations/python-clickhouse-connect-example",
      title: "Exemple fonctionnel de client Python pour se connecter à ClickHouse Cloud Service",
      description: "Apprenez à vous connecter à ClickHouse Cloud Service en Python grâce à un exemple pas à pas utilisant le pilote clickhouse-connect.",
      href: "/fr/resources/support-center/knowledge-base/integrations/python-clickhouse-connect-example",
      category: "Integrations & client libraries",
      tags: ["Language Clients"]
    },
    {
      id: "configuration-settings/about-quotas-and-query-complexity",
      title: "À propos des quotas et de la complexité des requêtes",
      description:
        "Les quotas et la complexité des requêtes sont des moyens efficaces de limiter et de restreindre ce que les utilisateurs peuvent faire dans ClickHouse. Cet article de la base de connaissances présente des exemples d'application de ces deux approches.",
      href: "/fr/resources/support-center/knowledge-base/configuration-settings/about-quotas-and-query-complexity",
      category: "Configuration & settings",
      tags: ["Managing Cloud"]
    },
    {
      id: "data-import-export/achieving-atomic-inserts",
      title: "Insertions atomiques et cohérence multi-tables dans ClickHouse Cloud",
      description: "Comment charger des données de manière atomique et maintenir la cohérence entre plusieurs tables dans ClickHouse Cloud sans transactions multi-instructions, en utilisant des tables de transit et des opérations au niveau des partitions.",
      href: "/fr/resources/support-center/knowledge-base/data-import-export/achieving-atomic-inserts",
      category: "Data import & export",
      tags: ["Data Ingestion", "Best Practices"]
    },
    {
      id: "tables-schema/add-column",
      title: "Ajouter une colonne à une table",
      description: "Dans ce guide, nous allons apprendre à ajouter une colonne à une table existante.",
      href: "/fr/resources/support-center/knowledge-base/tables-schema/add-column",
      category: "Tables & schema",
      tags: ["Data Modelling"]
    },
    {
      id: "configuration-settings/alter-user-settings-exception",
      title: "Exception lors de la modification des paramètres utilisateur",
      description: "Gestion de l'exception levée lors de la modification des paramètres utilisateur",
      href: "/fr/resources/support-center/knowledge-base/configuration-settings/alter-user-settings-exception",
      category: "Configuration & settings",
      tags: ["Settings", "Errors and Exceptions"]
    },
    {
      id: "materialized-views/are-materialized-views-inserted-asynchronously",
      title: "Les vues matérialisées sont-elles alimentées de manière synchrone ?",
      description: "Cet article de la base de connaissances examine si les vues matérialisées sont alimentées de manière synchrone",
      href: "/fr/resources/support-center/knowledge-base/materialized-views/are-materialized-views-inserted-asynchronously",
      category: "Materialized views & projections",
      tags: ["Data Modelling"]
    },
    {
      id: "tables-schema/schema-migration-tools",
      title: "Outils de migration de schéma automatisée pour ClickHouse",
      description: "Découvrez les outils de migration de schéma automatisée pour ClickHouse et comment gérer l'évolution des schémas de base de données au fil du temps.",
      href: "/fr/resources/support-center/knowledge-base/tables-schema/schema-migration-tools",
      category: "Tables & schema",
      tags: ["Tools and Utilities"]
    },
    {
      id: "cloud-services/aws-privatelink-setup-for-msk-clickpipes",
      title: "Configuration d'AWS PrivateLink pour exposer MSK à ClickPipes",
      description: "Étapes de configuration pour exposer un MSK privé via la connectivité multi-VPC MSK à ClickPipes.",
      href: "/fr/resources/support-center/knowledge-base/cloud-services/aws-privatelink-setup-for-msk-clickpipes",
      category: "Cloud",
      tags: ["Security and Authentication", "Managing Cloud"]
    },
    {
      id: "cloud-services/aws-privatelink-setup-for-clickpipes",
      title: "Configuration d'AWS PrivateLink pour exposer un RDS privé à ClickPipes",
      description: "Étapes de configuration pour exposer un RDS privé via AWS PrivateLink à ClickPipes.",
      href: "/fr/resources/support-center/knowledge-base/cloud-services/aws-privatelink-setup-for-clickpipes",
      category: "Cloud",
      tags: ["Security and Authentication", "Managing Cloud"]
    },
    {
      id: "data-management/backing-up-a-specific-partition",
      title: "Sauvegarder une partition spécifique",
      description: "Comment sauvegarder une partition spécifique dans ClickHouse ?",
      href: "/fr/resources/support-center/knowledge-base/data-management/backing-up-a-specific-partition",
      category: "Data management",
      tags: ["Managing Data"]
    },
    {
      id: "general-faqs/key-value",
      title: "Puis-je utiliser ClickHouse comme stockage clé-valeur ?",
      description: "Répond à la question fréquemment posée de savoir si ClickHouse peut être utilisé comme stockage clé-valeur.",
      href: "/fr/resources/support-center/knowledge-base/general-faqs/key-value",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/time-series",
      title: "Puis-je utiliser ClickHouse comme base de données de séries temporelles ?",
      description: "Page décrivant comment utiliser ClickHouse comme base de données de séries temporelles",
      href: "/fr/resources/support-center/knowledge-base/general-faqs/time-series",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "queries-sql/pivot",
      title: "Peut-on faire un PIVOT dans ClickHouse ?",
      description:
        "ClickHouse ne dispose pas d'une clause PIVOT, mais il est possible d'approcher cette fonctionnalité à l'aide de combinateurs de fonctions d'agrégation. Voyons comment procéder avec le jeu de données des prix de l'immobilier au Royaume-Uni.",
      href: "/fr/resources/support-center/knowledge-base/queries-sql/pivot",
      category: "Queries & SQL",
      tags: ["Data Modelling", "Core Data Concepts"]
    },
    {
      id: "general-faqs/vector-search",
      title: "Peut-on utiliser ClickHouse pour la recherche vectorielle ?",
      description: "Apprenez à utiliser ClickHouse pour la recherche vectorielle, notamment pour stocker des embeddings et effectuer des recherches avec des fonctions de distance telles que la similarité cosinus.",
      href: "/fr/resources/support-center/knowledge-base/general-faqs/vector-search",
      category: "General & FAQs",
      tags: ["Use Cases", "Concepts"]
    },
    {
      id: "monitoring-debugging/send-logs-level",
      title: "Capturer les journaux serveur des requêtes côté client",
      description: "Apprenez à capturer les journaux serveur au niveau du client, même avec des paramètres de journalisation différents, en utilisant le paramètre client `send_logs_level`.",
      href: "/fr/resources/support-center/knowledge-base/monitoring-debugging/send-logs-level",
      category: "Monitoring & debugging",
      tags: ["Server Admin"]
    },
    {
      id: "configuration-settings/change-the-prompt-in-clickhouse-client",
      title: "Modifier l'invite dans clickhouse-client",
      description: "Cet article explique comment modifier l'invite dans votre client ClickHouse et dans la fenêtre de terminal clickhouse-local, en remplaçant :) par un préfixe suivi de :)",
      href: "/fr/resources/support-center/knowledge-base/configuration-settings/change-the-prompt-in-clickhouse-client",
      category: "Configuration & settings",
      tags: ["Settings", "Native Clients and Interfaces"]
    },
    {
      id: "security/common-rbac-queries",
      title: "Requêtes RBAC courantes",
      description: "Requêtes permettant d'accorder des permissions spécifiques aux utilisateurs.",
      href: "/fr/resources/support-center/knowledge-base/security/common-rbac-queries",
      category: "Security & access control",
      tags: ["Security and Authentication", "Managing Cloud"]
    },
    {
      id: "queries-sql/comparing-metrics-between-queries",
      title: "Comparer des métriques entre des requêtes en décibels",
      description: "Une requête pour comparer des métriques entre deux requêtes dans ClickHouse.",
      href: "/fr/resources/support-center/knowledge-base/queries-sql/comparing-metrics-between-queries",
      category: "Queries & SQL",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "configuration-settings/configure-cap-ipc-lock-and-cap-sys-nice-in-docker",
      title: "Configuring CAP_IPC_LOCK and CAP_SYS_NICE Capabilities in Docker",
      description: "Learn how to resolve Docker capability warnings for `CAP_IPC_LOCK` and `CAP_SYS_NICE` when running ClickHouse in a container.",
      href: "/fr/resources/support-center/knowledge-base/configuration-settings/configure-cap-ipc-lock-and-cap-sys-nice-in-docker",
      category: "Configuration & settings",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "troubleshooting/configure-cap-ipc-lock-and-cap-sys-nice-in-docker",
      title: "Configuring CAP_IPC_LOCK and CAP_SYS_NICE Capabilities in Docker",
      description: "Learn how to resolve Docker capability warnings for `CAP_IPC_LOCK` and `CAP_SYS_NICE` when running ClickHouse in a container.",
      href: "/fr/resources/support-center/knowledge-base/troubleshooting/configure-cap-ipc-lock-and-cap-sys-nice-in-docker",
      category: "Troubleshooting & errors",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "cloud-services/custom-dns-alias-for-instance",
      title: "Create a custom DNS alias by setting up a reverse proxy",
      description: "Learn how to set up a custom DNS alias for your instance using a reverse proxy",
      href: "/fr/resources/support-center/knowledge-base/cloud-services/custom-dns-alias-for-instance",
      category: "Cloud",
      tags: ["Server Admin", "Security and Authentication"]
    },
    {
      id: "troubleshooting/part-intersects-previous-part",
      title: "DB::Exception: Part XXXXX intersects previous part YYYYY. It is a bug or a result of manual intervention in the ZooKeeper data.",
      description:
        "Cet article explique comment résoudre l'erreur DB::Exception liée à l'intersection de parts dans ClickHouse, souvent causée par une condition de concurrence ou une intervention manuelle dans les données ZooKeeper.",
      href: "/fr/resources/support-center/knowledge-base/troubleshooting/part-intersects-previous-part",
      category: "Dépannage & erreurs",
      tags: ["Errors and Exceptions", "System Tables"]
    },
    {
      id: "setup-installation/difference-between-official-builds-and-3rd-party",
      title: "Differences Between Official and 3rd-Party ClickHouse Builds",
      description: "Understand the key differences between official ClickHouse builds and 3rd-party builds, including updates, compatibility, and security considerations.",
      href: "/fr/resources/support-center/knowledge-base/setup-installation/difference-between-official-builds-and-3rd-party",
      category: "Setup & installation",
      tags: ["Concepts"]
    },
    {
      id: "general-faqs/cost-based",
      title: "Does ClickHouse have a cost-based optimizer",
      description: "ClickHouse has certain cost-based optimization mechanics",
      href: "/fr/resources/support-center/knowledge-base/general-faqs/cost-based",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/datalake",
      title: "Does ClickHouse support data lakes?",
      description: "ClickHouse supports data lakes, including Iceberg, Delta Lake, Apache Hudi, Apache Paimon, Hive",
      href: "/fr/resources/support-center/knowledge-base/general-faqs/datalake",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/distributed-join",
      title: "Does ClickHouse support distributed JOIN?",
      description: "ClickHouse supports distributed JOIN",
      href: "/fr/resources/support-center/knowledge-base/general-faqs/distributed-join",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/federated",
      title: "Does ClickHouse support federated queries?",
      description: "ClickHouse supports a wide range for federated and hybrid queries",
      href: "/fr/resources/support-center/knowledge-base/general-faqs/federated",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/concurrency",
      title: "Does ClickHouse support frequent, concurrent queries?",
      description: "ClickHouse supports high QPS and high concurrency",
      href: "/fr/resources/support-center/knowledge-base/general-faqs/concurrency",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "cloud-services/multi-region-replication",
      title: "Does ClickHouse support multi-region replication?",
      description: "This page answers whether ClickHouse supports multi-region replication",
      href: "/fr/resources/support-center/knowledge-base/cloud-services/multi-region-replication",
      category: "Cloud",
      tags: []
    },
    {
      id: "general-faqs/updates",
      title: "Does ClickHouse support real-time updates?",
      description: "ClickHouse supports lightweight real-time updates",
      href: "/fr/resources/support-center/knowledge-base/general-faqs/updates",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "security/row-column-policy",
      title: "Does ClickHouse support row-level and column-level security?",
      description: "Learn about row-level and column-level access restrictions in ClickHouse and ClickHouse Cloud, and how to implement role-based access control (RBAC) with policies.",
      href: "/fr/resources/support-center/knowledge-base/security/row-column-policy",
      category: "Security & access control",
      tags: ["Security and Authentication"]
    },
    {
      id: "cloud-services/execute-system-queries-in-cloud",
      title: "Execute SYSTEM Statements on All Nodes in ClickHouse Cloud",
      description: "Learn how to use `ON CLUSTER` and `clusterAllReplicas` to execute SYSTEM statements and queries across all nodes in a ClickHouse Cloud service.",
      href: "/fr/resources/support-center/knowledge-base/cloud-services/execute-system-queries-in-cloud",
      category: "Cloud",
      tags: ["Deployments and Scaling"]
    },
    {
      id: "troubleshooting/count-parts-by-type",
      title: "Find counts and sizes of wide or compact parts",
      description: "This knowledgebase article shows you how to find part counts by the type of part - wide or compact.",
      href: "/fr/resources/support-center/knowledge-base/troubleshooting/count-parts-by-type",
      category: "Troubleshooting & errors",
      tags: ["Troubleshooting"]
    },
    {
      id: "troubleshooting/fix-developer-verification-error-in-macos",
      title: "Fix the Developer Verification Error in MacOS",
      description: "Learn how to resolve the MacOS developer verification error when running ClickHouse commands, using either System Settings or the terminal.",
      href: "/fr/resources/support-center/knowledge-base/troubleshooting/fix-developer-verification-error-in-macos",
      category: "Troubleshooting & errors",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "data-import-export/s3-export-data-year-month-folders",
      title: "How can I do partitioned writes by year and month on S3?",
      description: "Learn how to write partitioned data by year and month to an S3 bucket in ClickHouse, using a custom path structure for organizing the data.",
      href: "/fr/resources/support-center/knowledge-base/data-import-export/s3-export-data-year-month-folders",
      category: "Data import & export",
      tags: ["Data Export", "Native Clients and Interfaces"]
    },
    {
      id: "data-import-export/kafka-clickhouse-json",
      title: "How can I use the new JSON Data Type with Kafka?",
      description: "Learn how to load JSON messages from Apache Kafka directly into a single JSON column in ClickHouse using the Kafka table engine and JSON data type.",
      href: "/fr/resources/support-center/knowledge-base/data-import-export/kafka-clickhouse-json",
      category: "Data import & export",
      tags: ["Data Formats", "Data Ingestion"]
    },
    {
      id: "cloud-services/change-billing-email",
      title: "How do I change my Billing Contact in ClickHouse Cloud?",
      description: "Let's learn how to change your billing address in ClickHouse Cloud.",
      href: "/fr/resources/support-center/knowledge-base/cloud-services/change-billing-email",
      category: "Cloud",
      tags: ["Managing Cloud"]
    },
    {
      id: "general-faqs/how-do-i-contribute-code-to-clickhouse",
      title: "How do I contribute code to ClickHouse?",
      description: "ClickHouse est un projet open source développé sur GitHub. Comme il est d'usage, les instructions de contribution sont publiées dans le fichier CONTRIBUTING à la racine du dépôt de code source.",
      href: "/fr/resources/support-center/knowledge-base/general-faqs/how-do-i-contribute-code-to-clickhouse",
      category: "Général & FAQ",
      tags: ["Community"]
    },
    {
      id: "data-import-export/parquet-to-csv-json",
      title: "Comment convertir des fichiers Parquet en CSV ou JSON ?",
      description: "Apprenez à utiliser l'outil `clickhouse-local` de ClickHouse pour convertir facilement des fichiers Parquet aux formats CSV ou JSON.",
      href: "/fr/resources/support-center/knowledge-base/data-import-export/parquet-to-csv-json",
      category: "Import et export de données",
      tags: ["Data Sources", "Data Formats"]
    },
    {
      id: "data-import-export/mysql-to-parquet-csv-json",
      title: "Comment exporter des données MySQL vers Parquet, CSV ou JSON avec ClickHouse",
      description: "Apprenez à utiliser l'outil `clickhouse-local` pour exporter des données MySQL vers des formats tels que Parquet, CSV ou JSON, rapidement et efficacement.",
      href: "/fr/resources/support-center/knowledge-base/data-import-export/mysql-to-parquet-csv-json",
      category: "Import et export de données",
      tags: ["Data Formats", "Data Export"]
    },
    {
      id: "data-import-export/postgresql-to-parquet-csv-json",
      title: "Comment exporter des données PostgreSQL vers Parquet, CSV ou JSON ?",
      description: "Apprenez à exporter des données PostgreSQL vers les formats Parquet, CSV ou JSON à l'aide de `clickhouse-local`, avec divers exemples.",
      href: "/fr/resources/support-center/knowledge-base/data-import-export/postgresql-to-parquet-csv-json",
      category: "Import et export de données",
      tags: ["Data Export", "Data Formats"]
    },
    {
      id: "setup-installation/install-clickhouse-windows10",
      title: "Comment installer ClickHouse sur Windows 10 ?",
      description: "Apprenez à installer et tester ClickHouse sur Windows 10 avec WSL 2. Inclut la configuration, le dépannage et l'exécution d'un environnement de test.",
      href: "/fr/resources/support-center/knowledge-base/setup-installation/install-clickhouse-windows10",
      category: "Configuration et installation",
      tags: ["Tools and Utilities"]
    },
    {
      id: "security/remove-default-user",
      title: "Comment supprimer l'utilisateur par défaut ?",
      description: "Apprenez à supprimer l'utilisateur par défaut lors de l'exécution du serveur ClickHouse.",
      href: "/fr/resources/support-center/knowledge-base/security/remove-default-user",
      category: "Sécurité et contrôle d'accès",
      tags: ["Server Admin"]
    },
    {
      id: "cloud-services/ingest-failures-23-9-release",
      title: "Comment résoudre les échecs d'ingestion après la version ClickHouse 23.9 ?",
      description: "Apprenez à résoudre les échecs d'ingestion causés par une vérification plus stricte des droits introduite dans ClickHouse 23.9 pour les tables utilisant `async_inserts`. Mettez à jour les droits pour corriger les erreurs.",
      href: "/fr/resources/support-center/knowledge-base/cloud-services/ingest-failures-23-9-release",
      category: "Cloud",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "performance-optimization/insert-select-settings-tuning",
      title: "Comment résoudre l'erreur TOO MANY PARTS lors d'un INSERT...SELECT ?",
      description: "Résolvez l'erreur TOO_MANY_PARTS dans ClickHouse lors d'un `INSERT...SELECT` en ajustant des paramètres avancés pour des blocs plus grands et en augmentant les seuils de partition.",
      href: "/fr/resources/support-center/knowledge-base/performance-optimization/insert-select-settings-tuning",
      category: "Performance et optimisation",
      tags: ["Settings", "Errors and Exceptions"]
    },
    {
      id: "integrations/node-js-example",
      title: "Comment utiliser NodeJS avec @clickhouse/client",
      description: "Apprenez à utiliser @clickhouse/client dans une application Node.js pour interagir avec ClickHouse et exécuter des requêtes.",
      href: "/fr/resources/support-center/knowledge-base/integrations/node-js-example",
      category: "Intégrations et bibliothèques clientes",
      tags: ["Language Clients"]
    },
    {
      id: "monitoring-debugging/view-number-of-active-mutations",
      title: "Comment afficher le nombre de mutations actives ou en file d'attente ?",
      description:
        "Surveillez le nombre de mutations actives ou en file d'attente dans ClickHouse, notamment lors d'opérations `ALTER` ou `UPDATE`. Utilisez la table `system.mutations` pour le suivi des mutations.",
      href: "/fr/resources/support-center/knowledge-base/monitoring-debugging/view-number-of-active-mutations",
      category: "Surveillance et débogage",
      tags: ["System Tables"]
    },
    {
      id: "data-management/read-consistency",
      title: "Comment garantir la cohérence de lecture des données dans ClickHouse ?",
      description: "Apprenez à assurer la cohérence des données lors de la lecture depuis ClickHouse, que vous soyez connecté au même nœud ou à un nœud quelconque.",
      href: "/fr/resources/support-center/knowledge-base/data-management/read-consistency",
      category: "Gestion des données",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "setup-installation/llvm-clang-up-to-date",
      title: "Comment compiler LLVM et clang sur Linux",
      description: "Commandes pour compiler LLVM et clang sur Linux.",
      href: "/fr/resources/support-center/knowledge-base/setup-installation/llvm-clang-up-to-date",
      category: "Configuration et installation",
      tags: ["Community", "Tools and Utilities"]
    },
    {
      id: "data-management/calculate-ratio-of-zero-sparse-serialization",
      title: "Comment calculer le ratio de valeurs vides/nulles dans chaque colonne d'une table",
      description: "Apprenez à calculer le ratio de valeurs vides ou nulles dans chaque colonne d'une table ClickHouse afin d'optimiser la sérialisation des colonnes creuses.",
      href: "/fr/resources/support-center/knowledge-base/data-management/calculate-ratio-of-zero-sparse-serialization",
      category: "Gestion des données",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "security/check-users-roles",
      title: "Comment vérifier les utilisateurs assignés aux rôles et inversement",
      description: "Apprenez à interroger `system.role_grants` de ClickHouse pour identifier les utilisateurs assignés à des rôles et les rôles assignés à des utilisateurs spécifiques.",
      href: "/fr/resources/support-center/knowledge-base/security/check-users-roles",
      category: "Sécurité et contrôle d'accès",
      tags: ["Server Admin", "System Tables", "Managing Cloud"]
    },
    {
      id: "monitoring-debugging/which-processes-are-currently-running",
      title: "Comment vérifier le code en cours d'exécution sur un serveur ?",
      description:
        "ClickHouse fournit des outils d'introspection tels que `system.stack_trace` pour inspecter le code en cours d'exécution sur chaque thread du serveur, facilitant le débogage et la surveillance des performances.",
      href: "/fr/resources/support-center/knowledge-base/monitoring-debugging/which-processes-are-currently-running",
      category: "Surveillance et débogage",
      tags: ["Server Admin"]
    },
    {
      id: "cloud-services/how-to-check-my-clickhouse-cloud-sevice-state",
      title: "Comment vérifier l'état de votre service ClickHouse Cloud",
      description: "Apprenez à utiliser l'API ClickHouse Cloud pour vérifier si votre service est arrêté, inactif ou en cours d'exécution, sans le réveiller.",
      href: "/fr/resources/support-center/knowledge-base/cloud-services/how-to-check-my-clickhouse-cloud-sevice-state",
      category: "Cloud",
      tags: ["Managing Cloud"]
    },
    {
      id: "configuration-settings/configure-a-user-setting",
      title: "Comment configurer les paramètres d'un utilisateur dans ClickHouse",
      description: "Apprenez à définir des paramètres dans ClickHouse pour des requêtes individuelles, des sessions client ou des utilisateurs spécifiques à l'aide des commandes `SET` et `ALTER USER`.",
      href: "/fr/resources/support-center/knowledge-base/configuration-settings/configure-a-user-setting",
      category: "Configuration et paramètres",
      tags: ["Settings"]
    },
    {
      id: "materialized-views/projection-example",
      title: "Comment vérifier si une projection est utilisée par la requête ?",
      description: "Apprenez à vérifier si une projection est utilisée dans les requêtes ClickHouse en testant avec des données d'exemple et en utilisant EXPLAIN pour confirmer l'utilisation de la projection.",
      href: "/fr/resources/support-center/knowledge-base/materialized-views/projection-example",
      category: "Vues matérialisées et projections",
      tags: ["Data Modelling"]
    },
    {
      id: "cloud-services/how-to-connect-to-ch-cloud-using-ssh-keys",
      title: "Comment se connecter à ClickHouse avec des clés SSH",
      description: "Comment se connecter à ClickHouse et ClickHouse Cloud à l'aide de clés SSH",
      href: "/fr/resources/support-center/knowledge-base/cloud-services/how-to-connect-to-ch-cloud-using-ssh-keys",
      category: "Cloud",
      tags: ["Managing Cloud", "Security and Authentication"]
    },
    {
      id: "data-management/dictionary-using-strings",
      title: "Comment créer un dictionnaire ClickHouse avec des clés et des valeurs de type chaîne",
      description: "Apprenez à créer un dictionnaire ClickHouse à partir de clés et de valeurs de type chaîne issues d'une table MergeTree, avec des exemples de configuration et d'utilisation.",
      href: "/fr/resources/support-center/knowledge-base/data-management/dictionary-using-strings",
      category: "Gestion des données",
      tags: ["Data Modelling"]
    },
    {
      id: "tables-schema/how-to-create-table-to-query-multiple-remote-clusters",
      title: "Comment créer une table capable d'interroger plusieurs clusters distants",
      description: "Comment créer une table capable d'interroger plusieurs clusters distants",
      href: "/fr/resources/support-center/knowledge-base/tables-schema/how-to-create-table-to-query-multiple-remote-clusters",
      category: "Tables et schéma",
      tags: ["Deployments and Scaling"]
    },
    {
      id: "setup-installation/enabling-ssl-with-lets-encrypt",
      title: "Comment activer SSL avec Let's Encrypt sur un serveur ClickHouse unique",
      description: "Apprenez à configurer SSL pour un serveur ClickHouse unique à l'aide de Let's Encrypt, notamment l'émission de certificats, la configuration et la validation.",
      href: "/fr/resources/support-center/knowledge-base/setup-installation/enabling-ssl-with-lets-encrypt",
      category: "Configuration et installation",
      tags: ["Security and Authentication"]
    },
    {
      id: "data-import-export/file-export",
      title: "Comment exporter des données de ClickHouse vers un fichier",
      description: "Découvrez différentes méthodes pour exporter des données depuis ClickHouse, notamment `INTO OUTFILE`, le moteur de table File et la redirection en ligne de commande.",
      href: "/fr/resources/support-center/knowledge-base/data-import-export/file-export",
      category: "Import et export de données",
      tags: ["Data Export"]
    },
    {
      id: "queries-sql/how-to-filter-a-clickhouse-table-by-an-array-column",
      title: "Comment filtrer une table ClickHouse par une colonne de type tableau ?",
      description: "Article de la base de connaissances sur le filtrage d'une table ClickHouse par une colonne de type tableau.",
      href: "/fr/resources/support-center/knowledge-base/queries-sql/how-to-filter-a-clickhouse-table-by-an-array-column",
      category: "Requêtes et SQL",
      tags: ["Data Modelling", "Functions"]
    },
    {
      id: "monitoring-debugging/generate-har-file",
      title: "Comment générer un fichier HAR pour le support",
      description: "Un fichier HAR (HTTP Archive) capture l'activité réseau de votre navigateur. Il peut aider notre équipe de support à diagnostiquer les chargements de page lents, les requêtes échouées ou d'autres problèmes réseau.",
      href: "/fr/resources/support-center/knowledge-base/monitoring-debugging/generate-har-file",
      category: "Surveillance et débogage",
      tags: ["Tools and Utilities"]
    },
    {
      id: "materialized-views/how-to-display-queries-using-mv",
      title: "Comment identifier les requêtes utilisant des vues matérialisées dans ClickHouse",
      description: "Apprenez à interroger les journaux ClickHouse pour identifier toutes les requêtes impliquant des vues matérialisées dans un intervalle de temps donné.",
      href: "/fr/resources/support-center/knowledge-base/materialized-views/how-to-display-queries-using-mv",
      category: "Vues matérialisées et projections",
      tags: ["System Tables"]
    },
    {
      id: "performance-optimization/find-expensive-queries",
      title: "Comment identifier les requêtes les plus coûteuses dans ClickHouse",
      description: "Apprenez à utiliser la table `query_log` dans ClickHouse pour identifier les requêtes les plus gourmandes en mémoire et en CPU sur les nœuds distribués.",
      href: "/fr/resources/support-center/knowledge-base/performance-optimization/find-expensive-queries",
      category: "Performance et optimisation",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "configuration-settings/ignoring-incorrect-settings",
      title: "Comment ignorer les paramètres incorrects dans ClickHouse",
      description: "Apprenez à utiliser l'option `skip_check_for_incorrect_settings` pour permettre à ClickHouse de démarrer même lorsque des paramètres au niveau utilisateur sont mal configurés.",
      href: "/fr/resources/support-center/knowledge-base/configuration-settings/ignoring-incorrect-settings",
      category: "Configuration et paramètres",
      tags: ["Settings"]
    },
    {
      id: "data-import-export/json-import",
      title: "Comment importer du JSON dans ClickHouse ?",
      description: "Cette page vous explique comment importer du JSON dans ClickHouse",
      href: "/fr/resources/support-center/knowledge-base/data-import-export/json-import",
      category: "Import et export de données",
      tags: []
    },
    {
      id: "setup-installation/how-to-increase-thread-pool-size",
      title: "Comment augmenter le nombre de threads dans ClickHouse",
      description: "Apprenez à configurer le pool de threads global dans ClickHouse en ajustant des paramètres tels que `max_thread_pool_size`, `thread_pool_queue_size` et `max_thread_pool_free_size`.",
      href: "/fr/resources/support-center/knowledge-base/setup-installation/how-to-increase-thread-pool-size",
      category: "Configuration et installation",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "data-import-export/kafka-to-clickhouse-setup",
      title: "Comment ingérer des données depuis Kafka dans ClickHouse",
      description: "Apprenez à ingérer des données depuis un topic Kafka dans ClickHouse à l'aide du moteur de table Kafka, des vues matérialisées et des tables MergeTree.",
      href: "/fr/resources/support-center/knowledge-base/data-import-export/kafka-to-clickhouse-setup",
      category: "Import et export de données",
      tags: ["Data Ingestion"]
    },
    {
      id: "data-import-export/ingest-parquet-files-in-s3",
      title: "Comment ingérer des fichiers Parquet depuis un bucket S3",
      description: "Découvrez les bases de l'utilisation du moteur de table S3 dans ClickHouse pour ingérer et interroger des fichiers Parquet depuis un bucket S3, notamment la configuration, les permissions d'accès et des exemples d'import de données.",
      href: "/fr/resources/support-center/knowledge-base/data-import-export/ingest-parquet-files-in-s3",
      category: "Import et export de données",
      tags: ["Data Ingestion"]
    },
    {
      id: "queries-sql/how-to-insert-all-rows-from-another-table",
      title: "Comment insérer toutes les lignes d'une table dans une autre ?",
      description: "Article de la base de connaissances sur l'insertion de toutes les lignes d'une table dans une autre.",
      href: "/fr/resources/support-center/knowledge-base/queries-sql/how-to-insert-all-rows-from-another-table",
      category: "Requêtes et SQL",
      tags: ["Data Ingestion"]
    },
    {
      id: "performance-optimization/check-query-processing-time-only",
      title: "Comment mesurer le temps de traitement d'une requête sans retourner de lignes",
      description: "Apprenez à utiliser l'option `FORMAT Null` dans ClickHouse pour mesurer le temps de traitement d'une requête sans renvoyer aucune ligne au client.",
      href: "/fr/resources/support-center/knowledge-base/performance-optimization/check-query-processing-time-only",
      category: "Performance et optimisation",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "monitoring-debugging/outputSendLogsLevelTracesToFile",
      title: "Comment exporter les traces de niveau de journalisation vers un fichier avec le clickhouse-client",
      description: "Comment exporter les traces de niveau de journalisation vers un fichier avec le clickhouse-client",
      href: "/fr/resources/support-center/knowledge-base/monitoring-debugging/outputSendLogsLevelTracesToFile",
      category: "Surveillance et débogage",
      tags: ["Data Export"]
    },
    {
      id: "tables-schema/recreate-table-across-terminals",
      title: "Comment recréer rapidement une petite table dans différents terminaux",
      description: "Apprenez à recréer rapidement une petite table et ses données dans différents terminaux par copier-coller, pour les environnements de développement.",
      href: "/fr/resources/support-center/knowledge-base/tables-schema/recreate-table-across-terminals",
      category: "Tables et schéma",
      tags: ["Tools and Utilities"]
    },
    {
      id: "integrations/how-to-set-up-ch-on-docker-odbc-connect-mssql",
      title: "Comment configurer ClickHouse sur Docker avec ODBC pour se connecter à une base de données Microsoft SQL Server (MSSQL)",
      description: "Comment configurer ClickHouse sur Docker avec ODBC pour se connecter à une base de données Microsoft SQL Server (MSSQL)",
      href: "/fr/resources/support-center/knowledge-base/integrations/how-to-set-up-ch-on-docker-odbc-connect-mssql",
      category: "Intégrations et bibliothèques clientes",
      tags: ["Native Clients and Interfaces"]
    },
    {
      id: "queries-sql/using-array-join-to-extract-and-query-attributes",
      title: "Comment utiliser array join pour extraire et interroger des attributs variables à l'aide de clés et de valeurs de type map",
      description: "Simple example to illustrate how to use array join to extract and query varying attributes using map keys and values",
      href: "/fr/resources/support-center/knowledge-base/queries-sql/using-array-join-to-extract-and-query-attributes",
      category: "Queries & SQL",
      tags: ["Functions"]
    },
    {
      id: "materialized-views/how-to-use-parametrised-views",
      title: "How to Use Parameterized Views in ClickHouse",
      description: "Learn how to create and query parameterized views in ClickHouse for dynamic data slicing based on query-time parameters.",
      href: "/fr/resources/support-center/knowledge-base/materialized-views/how-to-use-parametrised-views",
      category: "Materialized views & projections",
      tags: ["Use Cases"]
    },
    {
      id: "tables-schema/exchangeStatementToSwitchTables",
      title: "How to use the exchange command to switch tables",
      description: "How to use the exchange command to switch tables",
      href: "/fr/resources/support-center/knowledge-base/tables-schema/exchangeStatementToSwitchTables",
      category: "Tables & schema",
      tags: ["Managing Data"]
    },
    {
      id: "queries-sql/compare-resultsets",
      title: "How to Validate if Two Queries Return the Same Result-sets",
      description: "Learn how to validate that two ClickHouse queries produce identical result-sets using hash functions and comparison techniques.",
      href: "/fr/resources/support-center/knowledge-base/queries-sql/compare-resultsets",
      category: "Queries & SQL",
      tags: ["Functions"]
    },
    {
      id: "monitoring-debugging/check-query-cache-in-use",
      title: "How to Verify Query Cache Usage in ClickHouse",
      description: "Learn how to check if query cache is being utilized in ClickHouse using `clickhouse-client` trace logs or SQL commands.",
      href: "/fr/resources/support-center/knowledge-base/monitoring-debugging/check-query-cache-in-use",
      category: "Monitoring & debugging",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "cloud-services/unable-to-access-cloud-service",
      title: "I am unable to access a ClickHouse Cloud service",
      description: "Troubleshooting access issues with ClickHouse Cloud services, including IP Access List configuration",
      href: "/fr/resources/support-center/knowledge-base/cloud-services/unable-to-access-cloud-service",
      category: "Cloud",
      tags: ["Errors and Exceptions", "Managing Cloud"]
    },
    {
      id: "performance-optimization/finding-expensive-queries-by-memory-usage",
      title: "Identification des requêtes coûteuses par consommation mémoire dans ClickHouse",
      description: "Apprenez à utiliser la table `system.query_log` pour identifier les requêtes les plus gourmandes en mémoire dans ClickHouse, avec des exemples pour les configurations en cluster et en mode autonome.",
      href: "/fr/resources/support-center/knowledge-base/performance-optimization/finding-expensive-queries-by-memory-usage",
      category: "Performance & optimization",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "data-import-export/importing-and-working-with-json-array-objects",
      title: "Importing and Querying JSON Array Objects in ClickHouse",
      description: "Learn how to import JSON array objects into ClickHouse and perform advanced queries using JSON functions and array operations.",
      href: "/fr/resources/support-center/knowledge-base/data-import-export/importing-and-working-with-json-array-objects",
      category: "Data import & export",
      tags: ["Data Formats"]
    },
    {
      id: "data-import-export/importing-geojason-with-nested-object-array",
      title: "Importing GeoJSON with a deeply nested object array",
      description: "“Importing GeoJSON with a deeply nested object array“",
      href: "/fr/resources/support-center/knowledge-base/data-import-export/importing-geojason-with-nested-object-array",
      category: "Data import & export",
      tags: ["Data Formats"]
    },
    {
      id: "performance-optimization/improve-map-performance",
      title: "Improving Map Lookup Performance in ClickHouse",
      description: "Learn how to optimize Map column lookups in ClickHouse for better query performance by materializing specific keys as standalone columns.",
      href: "/fr/resources/support-center/knowledge-base/performance-optimization/improve-map-performance",
      category: "Performance & optimization",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "tables-schema/delete-old-data",
      title: "Is it possible to delete old records from a ClickHouse table?",
      description: "This page answers the question of whether it is possible to delete old records from a ClickHouse table",
      href: "/fr/resources/support-center/knowledge-base/tables-schema/delete-old-data",
      category: "Tables & schema",
      tags: []
    },
    {
      id: "general-faqs/separate-storage",
      title: "Is it possible to deploy ClickHouse with separate storage and compute?",
      description: "This page provides an answer as to whether it is possible to deploy ClickHouse with separate storage and compute",
      href: "/fr/resources/support-center/knowledge-base/general-faqs/separate-storage",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "data-import-export/json-extract-example",
      title: "JSON Extract example",
      description: "A short example on how to extract base types from JSON",
      href: "/fr/resources/support-center/knowledge-base/data-import-export/json-extract-example",
      category: "Data import & export",
      tags: ["Data Formats"]
    },
    {
      id: "queries-sql/calculate-pi-using-sql",
      title: "Let's calculate pi using SQL",
      description: "It's Pi Day! Let's calculate pi using ClickHouse SQL",
      href: "/fr/resources/support-center/knowledge-base/queries-sql/calculate-pi-using-sql",
      category: "Queries & SQL",
      tags: ["Use Cases"]
    },
    {
      id: "cloud-services/clickhouse-cloud-api-usage",
      title: "Managing ClickHouse Cloud Service with API and cURL",
      description: "Learn how to start, stop, and resume a ClickHouse Cloud service using API endpoints and cURL commands.",
      href: "/fr/resources/support-center/knowledge-base/cloud-services/clickhouse-cloud-api-usage",
      category: "Cloud",
      tags: ["Managing Cloud", "Tools and Utilities"]
    },
    {
      id: "monitoring-debugging/mapping-of-system-metrics-to-prometheus-metrics",
      title: "Mapping of metrics used in system.dashboards to Prometheus metrics in `system.custom_metrics`",
      description: "Mapping of metrics used in system.dashboards to Prometheus metrics in system.custom_metrics",
      href: "/fr/resources/support-center/knowledge-base/monitoring-debugging/mapping-of-system-metrics-to-prometheus-metrics",
      category: "Monitoring & debugging",
      tags: ["System Tables"]
    },
    {
      id: "security/windows-active-directory-to-ch-roles",
      title: "Mapping Windows Active Directory security groups to ClickHouse roles",
      description: "Example of mapping Windows Active Directory security groups to ClickHouse roles",
      href: "/fr/resources/support-center/knowledge-base/security/windows-active-directory-to-ch-roles",
      category: "Security & access control",
      tags: ["Tools and Utilities"]
    },
    {
      id: "performance-optimization/memory-limit-exceeded-for-query",
      title: "Memory limit exceeded for query",
      description: "Troubleshooting memory limit exceeded errors for a query",
      href: "/fr/resources/support-center/knowledge-base/performance-optimization/memory-limit-exceeded-for-query",
      category: "Performance & optimization",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "integrations/ODBC-authentication-failed-error-using-PowerBI-CH-connector",
      title: "ODBC authentication failed error when using the Power BI ClickHouse connector",
      description: "ODBC authentication failed error when using the Power BI ClickHouse connector",
      href: "/fr/resources/support-center/knowledge-base/integrations/ODBC-authentication-failed-error-using-PowerBI-CH-connector",
      category: "Integrations & client libraries",
      tags: ["Native Clients and Interfaces", "Errors and Exceptions"]
    },
    {
      id: "monitoring-debugging/profiling-clickhouse-with-llvm-xray",
      title: "Profiling ClickHouse with LLVM's XRay",
      description: "Learn how to profile ClickHouse using LLVM's XRay instrumentation profiler, visualize traces, and analyze performance.",
      href: "/fr/resources/support-center/knowledge-base/monitoring-debugging/profiling-clickhouse-with-llvm-xray",
      category: "Monitoring & debugging",
      tags: ["Performance and Optimizations", "Tools and Utilities"]
    },
    {
      id: "integrations/python-http-requests",
      title: "Exemple rapide en Python avec le module requests",
      description: "Un exemple utilisant Python et le module requests pour écrire dans ClickHouse et lire des données",
      href: "/fr/resources/support-center/knowledge-base/integrations/python-http-requests",
      category: "Intégrations et bibliothèques clientes",
      tags: ["Clients natifs et interfaces"]
    },
    {
      id: "configuration-settings/maximum-number-of-tables-and-databases",
      title: "Nombre maximal recommandé de bases de données, de tables, de partitions et de parts dans ClickHouse",
      description: "Découvrez les limites maximales recommandées pour les bases de données, les tables, les partitions et les parts dans un cluster ClickHouse afin de garantir des performances optimales.",
      href: "/fr/resources/support-center/knowledge-base/configuration-settings/maximum-number-of-tables-and-databases",
      category: "Configuration et paramètres",
      tags: ["Performances et optimisations", "Déploiements et mise à l’échelle"]
    },
    {
      id: "data-import-export/cannot-append-data-to-parquet-format",
      title: 'Résolution de l’erreur « Cannot Append Data in Parquet Format » dans ClickHouse',
      description: 'Vous obtenez l’erreur « Cannot append data in format Parquet to file » dans ClickHouse ? Voyons comment la résoudre.',
      href: "/fr/resources/support-center/knowledge-base/data-import-export/cannot-append-data-to-parquet-format",
      category: "Importation et exportation de données",
      tags: ["Erreurs et exceptions", "Formats de données"]
    },
    {
      id: "troubleshooting/exception-too-many-parts",
      title: 'Résolution de l’erreur « Too Many Parts » dans ClickHouse',
      description: 'Apprenez à résoudre l’erreur « Too many parts » dans ClickHouse en optimisant les taux d’insertion, en configurant les paramètres MergeTree et en gérant efficacement les partitions.',
      href: "/fr/resources/support-center/knowledge-base/troubleshooting/exception-too-many-parts",
      category: "Dépannage et erreurs",
      tags: ["Erreurs et exceptions"]
    },
    {
      id: "troubleshooting/certificate-verify-failed-error",
      title: "Résolution de l’erreur de vérification du certificat SSL dans ClickHouse",
      description: "Découvrez comment résoudre l’erreur SSL Exception CERTIFICATE_VERIFY_FAILED.",
      href: "/fr/resources/support-center/knowledge-base/troubleshooting/certificate-verify-failed-error",
      category: "Dépannage et erreurs",
      tags: ["Sécurité et authentification", "Erreurs et exceptions"]
    },
    {
      id: "troubleshooting/connection-timeout-remote-remoteSecure",
      title: "Résolution des erreurs de délai d’attente avec les fonctions de table `remote` et `remoteSecure`",
      description: "Apprenez à corriger les erreurs de délai d’attente lors de l’utilisation des fonctions de table `remote` ou `remoteSecure` dans ClickHouse en ajustant les paramètres de délai d’attente de connexion.",
      href: "/fr/resources/support-center/knowledge-base/troubleshooting/connection-timeout-remote-remoteSecure",
      category: "Dépannage et erreurs",
      tags: ["Erreurs et exceptions"]
    },
    {
      id: "tables-schema/search-across-node-for-tables-with-a-wildcard",
      title: "Recherche de tables sur plusieurs nœuds avec un caractère générique",
      description: "Apprenez à rechercher des tables sur plusieurs nœuds avec un caractère générique.",
      href: "/fr/resources/support-center/knowledge-base/tables-schema/search-across-node-for-tables-with-a-wildcard",
      category: "Tables et schéma",
      tags: ["Déploiements et mise à l’échelle"]
    },
    {
      id: "performance-optimization/query-max-execution-time",
      title: "Définir une limite de temps d’exécution des requêtes",
      description: "Comment imposer une limite au temps d’exécution maximal des requêtes",
      href: "/fr/resources/support-center/knowledge-base/performance-optimization/query-max-execution-time",
      category: "Performances et optimisation",
      tags: ["Gestion du Cloud", "Paramètres"]
    },
    {
      id: "data-import-export/json-simple-example",
      title: "Exemple simple de workflow pour extraire des données JSON à l’aide d’une table d’atterrissage avec une vue matérialisée",
      description: "Exemple simple de workflow pour extraire des données JSON à l’aide d’une table d’atterrissage avec une vue matérialisée",
      href: "/fr/resources/support-center/knowledge-base/data-import-export/json-simple-example",
      category: "Importation et exportation de données",
      tags: ["Formats de données"]
    },
    {
      id: "performance-optimization/async-vs-optimize-read-in-order",
      title: "Lecture synchrone des données",
      description:
        "Le nouveau paramètre `allow_asynchronous_read_from_io_pool_for_merge_tree` permet au nombre de threads de lecture (flux) d’être supérieur à celui des threads dans le reste du pipeline d’exécution de la requête.",
      href: "/fr/resources/support-center/knowledge-base/performance-optimization/async-vs-optimize-read-in-order",
      category: "Performances et optimisation",
      tags: ["Paramètres", "Performances et optimisations"]
    },
    {
      id: "integrations/terraform-example",
      title: "Exemple Terraform montrant comment utiliser l’API Cloud",
      description: "Cet exemple montre comment utiliser Terraform pour créer et supprimer des clusters à l’aide de l’API",
      href: "/fr/resources/support-center/knowledge-base/integrations/terraform-example",
      category: "Intégrations et bibliothèques clientes",
      tags: ["Clients natifs et interfaces"]
    },
    {
      id: "performance-optimization/tips-tricks-optimizing-basic-data-types-in-clickhouse",
      title: "Conseils et astuces pour optimiser les types de données de base dans ClickHouse",
      description: "Conseils et astuces pour optimiser les types de données de base dans ClickHouse",
      href: "/fr/resources/support-center/knowledge-base/performance-optimization/tips-tricks-optimizing-basic-data-types-in-clickhouse",
      category: "Performances et optimisation",
      tags: ["Performances et optimisations"]
    },
    {
      id: "queries-sql/useful-queries-for-troubleshooting",
      title: "Requêtes utiles pour le dépannage",
      description: "Une collection de requêtes pratiques pour dépanner ClickHouse, notamment pour surveiller la taille des tables, les requêtes longues et les erreurs.",
      href: "/fr/resources/support-center/knowledge-base/queries-sql/useful-queries-for-troubleshooting",
      category: "Requêtes et SQL",
      tags: ["Paramètres"]
    },
    {
      id: "general-faqs/use-clickhouse-for-log-analytics",
      title: "Utiliser ClickHouse pour l’analyse de logs",
      description: "ClickHouse est populaire pour l’analyse des logs et des métriques grâce à ses capacités d’analyse en temps réel. Envie d’en savoir plus ?",
      href: "/fr/resources/support-center/knowledge-base/general-faqs/use-clickhouse-for-log-analytics",
      category: "Général et FAQ",
      tags: ["Cas d’utilisation"]
    },
    {
      id: "queries-sql/filtered-aggregates",
      title: "Utiliser les agrégats filtrés dans ClickHouse",
      description: "Apprenez à utiliser les agrégats filtrés dans ClickHouse avec les combinateurs d’agrégats `-If` et `-Distinct` pour simplifier la syntaxe des requêtes et améliorer l’analyse.",
      href: "/fr/resources/support-center/knowledge-base/queries-sql/filtered-aggregates",
      category: "Requêtes et SQL",
      tags: ["Fonctions"]
    },
    {
      id: "general-faqs/dependencies",
      title: "Quelles sont les dépendances tierces nécessaires pour exécuter ClickHouse ?",
      description: "ClickHouse est autonome et n’a aucune dépendance à l’exécution",
      href: "/fr/resources/support-center/knowledge-base/general-faqs/dependencies",
      category: "Général et FAQ",
      tags: []
    },
    {
      id: "general-faqs/dbms-naming",
      title: 'Que signifie « ClickHouse » ?',
      description: 'Découvrez ce que signifie « ClickHouse »',
      href: "/fr/resources/support-center/knowledge-base/general-faqs/dbms-naming",
      category: "Général et FAQ",
      tags: []
    },
    {
      id: "general-faqs/ne-tormozit",
      title: "Que signifie « не тормозит » ?",
      description: 'Cette page explique ce que signifie « Не тормозит »',
      href: "/fr/resources/support-center/knowledge-base/general-faqs/ne-tormozit",
      category: "Général et FAQ",
      tags: []
    },
    {
      id: "integrations/oracle-odbc",
      title: "Que faire si j’ai un problème d’encodage lors de l’utilisation d’Oracle via ODBC ?",
      description: "Cette page explique quoi faire si vous rencontrez un problème d’encodage lors de l’utilisation d’Oracle via ODBC",
      href: "/fr/resources/support-center/knowledge-base/integrations/oracle-odbc",
      category: "Intégrations et bibliothèques clientes",
      tags: []
    },
    {
      id: "general-faqs/columnar-database",
      title: "Qu’est-ce qu’une base de données en colonnes ?",
      description: "Cette page explique ce qu’est une base de données en colonnes",
      href: "/fr/resources/support-center/knowledge-base/general-faqs/columnar-database",
      category: "Général et FAQ",
      tags: []
    },
    {
      id: "general-faqs/olap",
      title: "What is OLAP?",
      description: "An explainer on what Online Analytical Processing is",
      href: "/fr/resources/support-center/knowledge-base/general-faqs/olap",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "performance-optimization/optimize-final-vs-final",
      title: "What is the difference between OPTIMIZE FINAL and FINAL?",
      description: "Discusses the differences between OPTIMIZE FINAL and FINAL, and when to use and avoid them.",
      href: "/fr/resources/support-center/knowledge-base/performance-optimization/optimize-final-vs-final",
      category: "Performance & optimization",
      tags: ["Core Data Concepts"]
    },
    {
      id: "general-faqs/sql",
      title: "What SQL syntax does ClickHouse support?",
      description: "ClickHouse supports 100% of SQL syntax",
      href: "/fr/resources/support-center/knowledge-base/general-faqs/sql",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "data-management/when-is-ttl-applied",
      title: "Quand les règles TTL sont-elles appliquées, et peut-on les contrôler ?",
      description:
        "Les règles TTL dans ClickHouse sont appliquées de façon asynchrone, et vous pouvez contrôler le moment de leur exécution à l'aide du paramètre `merge_with_ttl_timeout`. Découvrez comment forcer l'application des TTL et gérer les threads d'arrière-plan pour leur exécution.",
      href: "/fr/resources/support-center/knowledge-base/data-management/when-is-ttl-applied",
      category: "Data management",
      tags: ["Core Data Concepts"]
    },
    {
      id: "setup-installation/production",
      title: "Which ClickHouse version to use in production?",
      description: "This page provides guidance on which ClickHouse version to use in production",
      href: "/fr/resources/support-center/knowledge-base/setup-installation/production",
      category: "Setup & installation",
      tags: []
    },
    {
      id: "general-faqs/who-is-using-clickhouse",
      title: "Who is using ClickHouse?",
      description: "Describes who is using ClickHouse",
      href: "/fr/resources/support-center/knowledge-base/general-faqs/who-is-using-clickhouse",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "data-management/dictionaries-consistent-state",
      title: "Why can't I see my data in a dictionary in ClickHouse Cloud?",
      description: "There is an issue where data in dictionaries may not be visible immediately after creation.",
      href: "/fr/resources/support-center/knowledge-base/data-management/dictionaries-consistent-state",
      category: "Data management",
      tags: ["Managing Cloud", "Data Modelling"]
    },
    {
      id: "general-faqs/why-recommend-clickhouse-keeper-over-zookeeper",
      title: "Why is ClickHouse Keeper recommended over ZooKeeper?",
      description:
        "ClickHouse Keeper improves upon ZooKeeper with features like reduced disk space usage, faster recovery, and less memory consumption, offering better performance for ClickHouse clusters.",
      href: "/fr/resources/support-center/knowledge-base/general-faqs/why-recommend-clickhouse-keeper-over-zookeeper",
      category: "General & FAQs",
      tags: ["Core Data Concepts"]
    },
    {
      id: "monitoring-debugging/why-default-logging-verbose",
      title: "Why is ClickHouse logging so verbose by default?",
      description: "Learn why the ClickHouse developers chose to set a verbose logging level by default.",
      href: "/fr/resources/support-center/knowledge-base/monitoring-debugging/why-default-logging-verbose",
      category: "Monitoring & debugging",
      tags: ["Settings"]
    },
    {
      id: "performance-optimization/why-is-my-primary-key-not-used",
      title: "Why is my primary key not used? How can I check?",
      description: "Covers a common reason why a primary key is not used in ordering and how we can confirm",
      href: "/fr/resources/support-center/knowledge-base/performance-optimization/why-is-my-primary-key-not-used",
      category: "Performance & optimization",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "general-faqs/mapreduce",
      title: "Why not use something like MapReduce?",
      description: "This page explains why you would use ClickHouse over MapReduce",
      href: "/fr/resources/support-center/knowledge-base/general-faqs/mapreduce",
      category: "General & FAQs",
      tags: []
    }
  ]
}