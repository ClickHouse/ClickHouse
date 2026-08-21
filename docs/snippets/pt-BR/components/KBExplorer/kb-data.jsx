export const kbIndex = {
  categories: [
    "Cloud",
    "Configuração e ajustes",
    "Importação e exportação de dados",
    "Gerenciamento de dados",
    "Geral e perguntas frequentes",
    "Integrações e bibliotecas de cliente",
    "Visões materializadas e projeções",
    "Monitoramento e depuração",
    "Performance & optimization",
    "Consultas e SQL",
    "Segurança e controle de acesso",
    "Setup & installation",
    "Tabelas e esquema",
    "Solução de problemas e erros"
  ],
  tags: [
    "Boas práticas",
    "Comunidade",
    "Conceitos",
    "Conceitos fundamentais de dados",
    "Exportação de dados",
    "Formatos de dados",
    "Ingestão de dados",
    "Modelagem de dados",
    "Fontes de dados",
    "Implantações e escalabilidade",
    "Erros e exceções",
    "Functions",
    "Clientes de linguagem",
    "Gerenciamento do Cloud",
    "Gerenciamento de dados",
    "Clientes e interfaces nativos",
    "Desempenho e otimizações",
    "Segurança e autenticação",
    "Administração do servidor",
    "Settings",
    "Tabelas do sistema",
    "Ferramentas e utilitários",
    "Troubleshooting",
    "Casos de uso"
  ],
  articles: [
    {
      id: "integrations/python-clickhouse-connect-example",
      title: "Exemplo funcional de cliente Python para conexão ao ClickHouse Cloud Service",
      description: "Aprenda a se conectar ao ClickHouse Cloud Service usando Python com um exemplo passo a passo utilizando o driver clickhouse-connect.",
      href: "/pt-BR/resources/support-center/knowledge-base/integrations/python-clickhouse-connect-example",
      category: "Integrações e bibliotecas de cliente",
      tags: ["Language Clients"]
    },
    {
      id: "configuration-settings/about-quotas-and-query-complexity",
      title: "Sobre cotas e complexidade de consultas",
      description:
        "Cotas e complexidade de consultas são formas eficazes de limitar e restringir o que os usuários podem fazer no ClickHouse. Este artigo da base de conhecimento apresenta exemplos de como aplicar essas duas abordagens.",
      href: "/pt-BR/resources/support-center/knowledge-base/configuration-settings/about-quotas-and-query-complexity",
      category: "Configuração e ajustes",
      tags: ["Managing Cloud"]
    },
    {
      id: "data-import-export/achieving-atomic-inserts",
      title: "Realizando inserções atômicas e consistência entre múltiplas tabelas no ClickHouse Cloud",
      description: "Como carregar dados de forma atômica e manter a consistência entre múltiplas tabelas no ClickHouse Cloud sem transações de múltiplos comandos, usando tabelas de staging e operações no nível de partição.",
      href: "/pt-BR/resources/support-center/knowledge-base/data-import-export/achieving-atomic-inserts",
      category: "Importação e exportação de dados",
      tags: ["Ingestão de dados", "Best Practices"]
    },
    {
      id: "tables-schema/add-column",
      title: "Adicionando uma coluna a uma tabela",
      description: "Neste guia, veremos como adicionar uma coluna a uma tabela existente.",
      href: "/pt-BR/resources/support-center/knowledge-base/tables-schema/add-column",
      category: "Tabelas e esquema",
      tags: ["Data Modelling"]
    },
    {
      id: "configuration-settings/alter-user-settings-exception",
      title: "Exceção ao alterar configurações de usuário",
      description: "Como tratar a exceção lançada ao alterar as configurações de usuário",
      href: "/pt-BR/resources/support-center/knowledge-base/configuration-settings/alter-user-settings-exception",
      category: "Configuração e ajustes",
      tags: ["Settings", "Errors and Exceptions"]
    },
    {
      id: "materialized-views/are-materialized-views-inserted-asynchronously",
      title: "As visões materializadas são inseridas de forma síncrona?",
      description: "Este artigo da base de conhecimento explora se as visões materializadas são inseridas de forma síncrona",
      href: "/pt-BR/resources/support-center/knowledge-base/materialized-views/are-materialized-views-inserted-asynchronously",
      category: "Visões materializadas e projeções",
      tags: ["Data Modelling"]
    },
    {
      id: "tables-schema/schema-migration-tools",
      title: "Ferramentas de migração automática de esquema para ClickHouse",
      description: "Conheça as ferramentas de migração automática de esquema para ClickHouse e saiba como gerenciar alterações no esquema do banco de dados ao longo do tempo.",
      href: "/pt-BR/resources/support-center/knowledge-base/tables-schema/schema-migration-tools",
      category: "Tabelas e esquema",
      tags: ["Tools and Utilities"]
    },
    {
      id: "cloud-services/aws-privatelink-setup-for-msk-clickpipes",
      title: "Configuração do AWS PrivateLink para expor o MSK ao ClickPipes",
      description: "Etapas de configuração para expor um MSK privado via conectividade multi-VPC do MSK ao ClickPipes.",
      href: "/pt-BR/resources/support-center/knowledge-base/cloud-services/aws-privatelink-setup-for-msk-clickpipes",
      category: "Cloud",
      tags: ["Segurança e autenticação", "Managing Cloud"]
    },
    {
      id: "cloud-services/aws-privatelink-setup-for-clickpipes",
      title: "Configuração do AWS PrivateLink para expor o RDS privado ao ClickPipes",
      description: "Etapas de configuração para expor um RDS privado via AWS PrivateLink ao ClickPipes.",
      href: "/pt-BR/resources/support-center/knowledge-base/cloud-services/aws-privatelink-setup-for-clickpipes",
      category: "Cloud",
      tags: ["Segurança e autenticação", "Managing Cloud"]
    },
    {
      id: "data-management/backing-up-a-specific-partition",
      title: "Realizando backup de uma partição específica",
      description: "Como fazer backup de uma partição específica no ClickHouse?",
      href: "/pt-BR/resources/support-center/knowledge-base/data-management/backing-up-a-specific-partition",
      category: "Gerenciamento de dados",
      tags: ["Managing Data"]
    },
    {
      id: "general-faqs/key-value",
      title: "Posso usar o ClickHouse como armazenamento chave-valor?",
      description: "Responde à pergunta frequente sobre se o ClickHouse pode ser usado como armazenamento chave-valor.",
      href: "/pt-BR/resources/support-center/knowledge-base/general-faqs/key-value",
      category: "Geral e perguntas frequentes",
      tags: []
    },
    {
      id: "general-faqs/time-series",
      title: "Posso usar o ClickHouse como banco de dados de séries temporais?",
      description: "Página que descreve como usar o ClickHouse como banco de dados de séries temporais",
      href: "/pt-BR/resources/support-center/knowledge-base/general-faqs/time-series",
      category: "Geral e perguntas frequentes",
      tags: []
    },
    {
      id: "queries-sql/pivot",
      title: "É possível usar PIVOT no ClickHouse?",
      description:
        "O ClickHouse não possui uma cláusula PIVOT, mas é possível aproximar essa funcionalidade usando combinadores de funções de agregação. Veja como fazer isso com o conjunto de dados de preços de imóveis do Reino Unido.",
      href: "/pt-BR/resources/support-center/knowledge-base/queries-sql/pivot",
      category: "Consultas e SQL",
      tags: ["Modelagem de dados", "Core Data Concepts"]
    },
    {
      id: "general-faqs/vector-search",
      title: "É possível usar o ClickHouse para busca vetorial?",
      description: "Aprenda a usar o ClickHouse para busca vetorial, incluindo o armazenamento de embeddings e a busca com funções de distância como similaridade de cosseno.",
      href: "/pt-BR/resources/support-center/knowledge-base/general-faqs/vector-search",
      category: "Geral e perguntas frequentes",
      tags: ["Casos de uso", "Concepts"]
    },
    {
      id: "monitoring-debugging/send-logs-level",
      title: "Capturando logs do servidor de consultas no cliente",
      description: "Aprenda a capturar logs do servidor no nível do cliente, mesmo com diferentes configurações de log, usando a configuração de cliente `send_logs_level`.",
      href: "/pt-BR/resources/support-center/knowledge-base/monitoring-debugging/send-logs-level",
      category: "Monitoramento e depuração",
      tags: ["Server Admin"]
    },
    {
      id: "configuration-settings/change-the-prompt-in-clickhouse-client",
      title: "Alterar o prompt no clickhouse-client",
      description: "Este artigo explica como alterar o prompt no seu cliente ClickHouse e na janela de terminal do clickhouse-local de :) para um prefixo seguido de :)",
      href: "/pt-BR/resources/support-center/knowledge-base/configuration-settings/change-the-prompt-in-clickhouse-client",
      category: "Configuração e ajustes",
      tags: ["Settings", "Native Clients and Interfaces"]
    },
    {
      id: "security/common-rbac-queries",
      title: "Consultas RBAC comuns",
      description: "Consultas para auxiliar na concessão de permissões específicas a usuários.",
      href: "/pt-BR/resources/support-center/knowledge-base/security/common-rbac-queries",
      category: "Segurança e controle de acesso",
      tags: ["Segurança e autenticação", "Managing Cloud"]
    },
    {
      id: "queries-sql/comparing-metrics-between-queries",
      title: "Comparando métricas entre consultas em decibéis",
      description: "Uma consulta para comparar métricas entre duas consultas no ClickHouse.",
      href: "/pt-BR/resources/support-center/knowledge-base/queries-sql/comparing-metrics-between-queries",
      category: "Queries & SQL",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "configuration-settings/configure-cap-ipc-lock-and-cap-sys-nice-in-docker",
      title: "Configuring CAP_IPC_LOCK and CAP_SYS_NICE Capabilities in Docker",
      description: "Learn how to resolve Docker capability warnings for `CAP_IPC_LOCK` and `CAP_SYS_NICE` when running ClickHouse in a container.",
      href: "/pt-BR/resources/support-center/knowledge-base/configuration-settings/configure-cap-ipc-lock-and-cap-sys-nice-in-docker",
      category: "Configuration & settings",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "troubleshooting/configure-cap-ipc-lock-and-cap-sys-nice-in-docker",
      title: "Configuring CAP_IPC_LOCK and CAP_SYS_NICE Capabilities in Docker",
      description: "Learn how to resolve Docker capability warnings for `CAP_IPC_LOCK` and `CAP_SYS_NICE` when running ClickHouse in a container.",
      href: "/pt-BR/resources/support-center/knowledge-base/troubleshooting/configure-cap-ipc-lock-and-cap-sys-nice-in-docker",
      category: "Troubleshooting & errors",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "cloud-services/custom-dns-alias-for-instance",
      title: "Create a custom DNS alias by setting up a reverse proxy",
      description: "Learn how to set up a custom DNS alias for your instance using a reverse proxy",
      href: "/pt-BR/resources/support-center/knowledge-base/cloud-services/custom-dns-alias-for-instance",
      category: "Cloud",
      tags: ["Server Admin", "Security and Authentication"]
    },
    {
      id: "troubleshooting/part-intersects-previous-part",
      title: "DB::Exception: Part XXXXX intersects previous part YYYYY. It is a bug or a result of manual intervention in the ZooKeeper data.",
      description:
        "Este artigo explica como resolver o erro DB::Exception relacionado a partes que se intersectam no ClickHouse, geralmente causado por uma condição de corrida ou intervenção manual nos dados do ZooKeeper.",
      href: "/pt-BR/resources/support-center/knowledge-base/troubleshooting/part-intersects-previous-part",
      category: "Solução de problemas & erros",
      tags: ["Errors and Exceptions", "System Tables"]
    },
    {
      id: "setup-installation/difference-between-official-builds-and-3rd-party",
      title: "Differences Between Official and 3rd-Party ClickHouse Builds",
      description: "Understand the key differences between official ClickHouse builds and 3rd-party builds, including updates, compatibility, and security considerations.",
      href: "/pt-BR/resources/support-center/knowledge-base/setup-installation/difference-between-official-builds-and-3rd-party",
      category: "Setup & installation",
      tags: ["Concepts"]
    },
    {
      id: "general-faqs/cost-based",
      title: "Does ClickHouse have a cost-based optimizer",
      description: "ClickHouse has certain cost-based optimization mechanics",
      href: "/pt-BR/resources/support-center/knowledge-base/general-faqs/cost-based",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/datalake",
      title: "Does ClickHouse support data lakes?",
      description: "ClickHouse supports data lakes, including Iceberg, Delta Lake, Apache Hudi, Apache Paimon, Hive",
      href: "/pt-BR/resources/support-center/knowledge-base/general-faqs/datalake",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/distributed-join",
      title: "Does ClickHouse support distributed JOIN?",
      description: "ClickHouse supports distributed JOIN",
      href: "/pt-BR/resources/support-center/knowledge-base/general-faqs/distributed-join",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/federated",
      title: "Does ClickHouse support federated queries?",
      description: "ClickHouse supports a wide range for federated and hybrid queries",
      href: "/pt-BR/resources/support-center/knowledge-base/general-faqs/federated",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/concurrency",
      title: "Does ClickHouse support frequent, concurrent queries?",
      description: "ClickHouse supports high QPS and high concurrency",
      href: "/pt-BR/resources/support-center/knowledge-base/general-faqs/concurrency",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "cloud-services/multi-region-replication",
      title: "Does ClickHouse support multi-region replication?",
      description: "This page answers whether ClickHouse supports multi-region replication",
      href: "/pt-BR/resources/support-center/knowledge-base/cloud-services/multi-region-replication",
      category: "Cloud",
      tags: []
    },
    {
      id: "general-faqs/updates",
      title: "Does ClickHouse support real-time updates?",
      description: "ClickHouse supports lightweight real-time updates",
      href: "/pt-BR/resources/support-center/knowledge-base/general-faqs/updates",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "security/row-column-policy",
      title: "Does ClickHouse support row-level and column-level security?",
      description: "Learn about row-level and column-level access restrictions in ClickHouse and ClickHouse Cloud, and how to implement role-based access control (RBAC) with policies.",
      href: "/pt-BR/resources/support-center/knowledge-base/security/row-column-policy",
      category: "Security & access control",
      tags: ["Security and Authentication"]
    },
    {
      id: "cloud-services/execute-system-queries-in-cloud",
      title: "Execute SYSTEM Statements on All Nodes in ClickHouse Cloud",
      description: "Learn how to use `ON CLUSTER` and `clusterAllReplicas` to execute SYSTEM statements and queries across all nodes in a ClickHouse Cloud service.",
      href: "/pt-BR/resources/support-center/knowledge-base/cloud-services/execute-system-queries-in-cloud",
      category: "Cloud",
      tags: ["Deployments and Scaling"]
    },
    {
      id: "troubleshooting/count-parts-by-type",
      title: "Find counts and sizes of wide or compact parts",
      description: "This knowledgebase article shows you how to find part counts by the type of part - wide or compact.",
      href: "/pt-BR/resources/support-center/knowledge-base/troubleshooting/count-parts-by-type",
      category: "Troubleshooting & errors",
      tags: ["Troubleshooting"]
    },
    {
      id: "troubleshooting/fix-developer-verification-error-in-macos",
      title: "Fix the Developer Verification Error in MacOS",
      description: "Learn how to resolve the MacOS developer verification error when running ClickHouse commands, using either System Settings or the terminal.",
      href: "/pt-BR/resources/support-center/knowledge-base/troubleshooting/fix-developer-verification-error-in-macos",
      category: "Troubleshooting & errors",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "data-import-export/s3-export-data-year-month-folders",
      title: "How can I do partitioned writes by year and month on S3?",
      description: "Learn how to write partitioned data by year and month to an S3 bucket in ClickHouse, using a custom path structure for organizing the data.",
      href: "/pt-BR/resources/support-center/knowledge-base/data-import-export/s3-export-data-year-month-folders",
      category: "Data import & export",
      tags: ["Data Export", "Native Clients and Interfaces"]
    },
    {
      id: "data-import-export/kafka-clickhouse-json",
      title: "How can I use the new JSON Data Type with Kafka?",
      description: "Learn how to load JSON messages from Apache Kafka directly into a single JSON column in ClickHouse using the Kafka table engine and JSON data type.",
      href: "/pt-BR/resources/support-center/knowledge-base/data-import-export/kafka-clickhouse-json",
      category: "Data import & export",
      tags: ["Data Formats", "Data Ingestion"]
    },
    {
      id: "cloud-services/change-billing-email",
      title: "How do I change my Billing Contact in ClickHouse Cloud?",
      description: "Let's learn how to change your billing address in ClickHouse Cloud.",
      href: "/pt-BR/resources/support-center/knowledge-base/cloud-services/change-billing-email",
      category: "Cloud",
      tags: ["Managing Cloud"]
    },
    {
      id: "general-faqs/how-do-i-contribute-code-to-clickhouse",
      title: "How do I contribute code to ClickHouse?",
      description: "ClickHouse é um projeto de código aberto desenvolvido no GitHub. Como de costume, as instruções de contribuição são publicadas no arquivo CONTRIBUTING na raiz do repositório do código-fonte.",
      href: "/pt-BR/resources/support-center/knowledge-base/general-faqs/how-do-i-contribute-code-to-clickhouse",
      category: "Geral e FAQs",
      tags: ["Community"]
    },
    {
      id: "data-import-export/parquet-to-csv-json",
      title: "Como converter arquivos de Parquet para CSV ou JSON?",
      description: "Aprenda a usar a ferramenta `clickhouse-local` do ClickHouse para converter arquivos Parquet para os formatos CSV ou JSON com facilidade.",
      href: "/pt-BR/resources/support-center/knowledge-base/data-import-export/parquet-to-csv-json",
      category: "Importação e exportação de dados",
      tags: ["Data Sources", "Data Formats"]
    },
    {
      id: "data-import-export/mysql-to-parquet-csv-json",
      title: "Como exportar dados do MySQL para Parquet, CSV ou JSON usando o ClickHouse",
      description: "Aprenda a usar a ferramenta `clickhouse-local` para exportar dados do MySQL para formatos como Parquet, CSV ou JSON de forma rápida e eficiente.",
      href: "/pt-BR/resources/support-center/knowledge-base/data-import-export/mysql-to-parquet-csv-json",
      category: "Importação e exportação de dados",
      tags: ["Data Formats", "Data Export"]
    },
    {
      id: "data-import-export/postgresql-to-parquet-csv-json",
      title: "Como exportar dados do PostgreSQL para Parquet, CSV ou JSON?",
      description: "Aprenda a exportar dados do PostgreSQL para os formatos Parquet, CSV ou JSON usando o `clickhouse-local`, com diversos exemplos.",
      href: "/pt-BR/resources/support-center/knowledge-base/data-import-export/postgresql-to-parquet-csv-json",
      category: "Importação e exportação de dados",
      tags: ["Data Export", "Data Formats"]
    },
    {
      id: "setup-installation/install-clickhouse-windows10",
      title: "Como instalar o ClickHouse no Windows 10?",
      description: "Aprenda a instalar e testar o ClickHouse no Windows 10 usando o WSL 2. Inclui configuração, solução de problemas e execução de um ambiente de testes.",
      href: "/pt-BR/resources/support-center/knowledge-base/setup-installation/install-clickhouse-windows10",
      category: "Setup e instalação",
      tags: ["Tools and Utilities"]
    },
    {
      id: "security/remove-default-user",
      title: "Como remover o usuário padrão?",
      description: "Aprenda a remover o usuário padrão ao executar o servidor ClickHouse.",
      href: "/pt-BR/resources/support-center/knowledge-base/security/remove-default-user",
      category: "Segurança e controle de acesso",
      tags: ["Server Admin"]
    },
    {
      id: "cloud-services/ingest-failures-23-9-release",
      title: "Como resolver falhas de ingestão após o lançamento do ClickHouse 23.9?",
      description: "Aprenda a resolver falhas de ingestão causadas pela verificação mais rigorosa de permissões introduzida no ClickHouse 23.9 para tabelas que usam `async_inserts`. Atualize as permissões para corrigir os erros.",
      href: "/pt-BR/resources/support-center/knowledge-base/cloud-services/ingest-failures-23-9-release",
      category: "Cloud",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "performance-optimization/insert-select-settings-tuning",
      title: "Como resolver o erro TOO MANY PARTS durante um INSERT...SELECT?",
      description: "Resolva o erro TOO_MANY_PARTS no ClickHouse durante um `INSERT...SELECT` ajustando configurações avançadas para blocos maiores e aumentando os limites de partição.",
      href: "/pt-BR/resources/support-center/knowledge-base/performance-optimization/insert-select-settings-tuning",
      category: "Desempenho e otimização",
      tags: ["Settings", "Errors and Exceptions"]
    },
    {
      id: "integrations/node-js-example",
      title: "Como usar o NodeJS com @clickhouse/client",
      description: "Aprenda a usar o @clickhouse/client em uma aplicação Node.js para interagir com o ClickHouse e executar consultas.",
      href: "/pt-BR/resources/support-center/knowledge-base/integrations/node-js-example",
      category: "Integrações e bibliotecas de cliente",
      tags: ["Language Clients"]
    },
    {
      id: "monitoring-debugging/view-number-of-active-mutations",
      title: "Como visualizar o número de mutações ativas ou na fila?",
      description:
        "Monitore o número de mutações ativas ou na fila no ClickHouse, especialmente ao executar operações `ALTER` ou `UPDATE`. Use a tabela `system.mutations` para acompanhar as mutações.",
      href: "/pt-BR/resources/support-center/knowledge-base/monitoring-debugging/view-number-of-active-mutations",
      category: "Monitoramento e depuração",
      tags: ["System Tables"]
    },
    {
      id: "data-management/read-consistency",
      title: "Como garantir consistência na leitura de dados no ClickHouse?",
      description: "Aprenda a garantir a consistência dos dados ao ler do ClickHouse, independentemente de estar conectado ao mesmo nó ou a um nó aleatório.",
      href: "/pt-BR/resources/support-center/knowledge-base/data-management/read-consistency",
      category: "Gerenciamento de dados",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "setup-installation/llvm-clang-up-to-date",
      title: "Como compilar LLVM e clang no Linux",
      description: "Comandos para compilar LLVM e clang no Linux.",
      href: "/pt-BR/resources/support-center/knowledge-base/setup-installation/llvm-clang-up-to-date",
      category: "Setup e instalação",
      tags: ["Community", "Tools and Utilities"]
    },
    {
      id: "data-management/calculate-ratio-of-zero-sparse-serialization",
      title: "Como calcular a proporção de valores vazios/zero em cada coluna de uma tabela",
      description: "Aprenda a calcular a proporção de valores vazios ou zero em cada coluna de uma tabela do ClickHouse para otimizar a serialização esparsa de colunas.",
      href: "/pt-BR/resources/support-center/knowledge-base/data-management/calculate-ratio-of-zero-sparse-serialization",
      category: "Gerenciamento de dados",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "security/check-users-roles",
      title: "Como verificar usuários atribuídos a roles e vice-versa",
      description: "Aprenda a consultar o `system.role_grants` do ClickHouse para encontrar usuários atribuídos a roles e roles atribuídas a usuários específicos.",
      href: "/pt-BR/resources/support-center/knowledge-base/security/check-users-roles",
      category: "Segurança e controle de acesso",
      tags: ["Server Admin", "System Tables", "Managing Cloud"]
    },
    {
      id: "monitoring-debugging/which-processes-are-currently-running",
      title: "Como verificar qual código está sendo executado no momento em um servidor?",
      description:
        "O ClickHouse fornece ferramentas de introspecção como `system.stack_trace` para inspecionar qual código está sendo executado em cada thread do servidor, auxiliando na depuração e no monitoramento de desempenho.",
      href: "/pt-BR/resources/support-center/knowledge-base/monitoring-debugging/which-processes-are-currently-running",
      category: "Monitoramento e depuração",
      tags: ["Server Admin"]
    },
    {
      id: "cloud-services/how-to-check-my-clickhouse-cloud-sevice-state",
      title: "Como verificar o estado do seu serviço no ClickHouse Cloud",
      description: "Aprenda a usar a API do ClickHouse Cloud para verificar se o seu serviço está parado, ocioso ou em execução sem ativá-lo.",
      href: "/pt-BR/resources/support-center/knowledge-base/cloud-services/how-to-check-my-clickhouse-cloud-sevice-state",
      category: "Cloud",
      tags: ["Managing Cloud"]
    },
    {
      id: "configuration-settings/configure-a-user-setting",
      title: "Como configurar parâmetros para um usuário no ClickHouse",
      description: "Aprenda a definir configurações no ClickHouse para consultas individuais, sessões de cliente ou usuários específicos usando os comandos `SET` e `ALTER USER`.",
      href: "/pt-BR/resources/support-center/knowledge-base/configuration-settings/configure-a-user-setting",
      category: "Configuração e definições",
      tags: ["Settings"]
    },
    {
      id: "materialized-views/projection-example",
      title: "Como confirmar se uma projeção está sendo usada pela consulta?",
      description: "Aprenda a verificar se uma projeção é utilizada em consultas do ClickHouse testando com dados de exemplo e usando EXPLAIN para confirmar o uso da projeção.",
      href: "/pt-BR/resources/support-center/knowledge-base/materialized-views/projection-example",
      category: "Visões materializadas e projeções",
      tags: ["Data Modelling"]
    },
    {
      id: "cloud-services/how-to-connect-to-ch-cloud-using-ssh-keys",
      title: "Como conectar ao ClickHouse usando chaves SSH",
      description: "Como conectar ao ClickHouse e ao ClickHouse Cloud usando chaves SSH",
      href: "/pt-BR/resources/support-center/knowledge-base/cloud-services/how-to-connect-to-ch-cloud-using-ssh-keys",
      category: "Cloud",
      tags: ["Managing Cloud", "Security and Authentication"]
    },
    {
      id: "data-management/dictionary-using-strings",
      title: "Como Criar um Dicionário ClickHouse com Chaves e Valores do Tipo String",
      description: "Aprenda a criar um dicionário ClickHouse usando chaves e valores do tipo string de uma tabela MergeTree como fonte, com exemplos de configuração e uso.",
      href: "/pt-BR/resources/support-center/knowledge-base/data-management/dictionary-using-strings",
      category: "Gerenciamento de dados",
      tags: ["Data Modelling"]
    },
    {
      id: "tables-schema/how-to-create-table-to-query-multiple-remote-clusters",
      title: "Como criar uma tabela que consulte múltiplos clusters remotos",
      description: "Como criar uma tabela que consulte múltiplos clusters remotos",
      href: "/pt-BR/resources/support-center/knowledge-base/tables-schema/how-to-create-table-to-query-multiple-remote-clusters",
      category: "Tabelas e esquema",
      tags: ["Deployments and Scaling"]
    },
    {
      id: "setup-installation/enabling-ssl-with-lets-encrypt",
      title: "Como Habilitar SSL com Let's Encrypt em um Único Servidor ClickHouse",
      description: "Aprenda a configurar SSL para um único servidor ClickHouse usando Let's Encrypt, incluindo emissão de certificado, configuração e validação.",
      href: "/pt-BR/resources/support-center/knowledge-base/setup-installation/enabling-ssl-with-lets-encrypt",
      category: "Configuração e instalação",
      tags: ["Security and Authentication"]
    },
    {
      id: "data-import-export/file-export",
      title: "Como Exportar Dados do ClickHouse para um Arquivo",
      description: "Conheça os diferentes métodos para exportar dados do ClickHouse, incluindo `INTO OUTFILE`, o mecanismo de tabela File e redirecionamento por linha de comando.",
      href: "/pt-BR/resources/support-center/knowledge-base/data-import-export/file-export",
      category: "Importação e exportação de dados",
      tags: ["Data Export"]
    },
    {
      id: "queries-sql/how-to-filter-a-clickhouse-table-by-an-array-column",
      title: "Como filtrar uma tabela ClickHouse por uma coluna do tipo array?",
      description: "Artigo da base de conhecimento sobre como filtrar uma tabela ClickHouse por uma coluna do tipo array.",
      href: "/pt-BR/resources/support-center/knowledge-base/queries-sql/how-to-filter-a-clickhouse-table-by-an-array-column",
      category: "Consultas e SQL",
      tags: ["Data Modelling", "Functions"]
    },
    {
      id: "monitoring-debugging/generate-har-file",
      title: "Como Gerar um Arquivo HAR para Suporte",
      description: "Um arquivo HAR (HTTP Archive) registra a atividade de rede do seu navegador. Ele pode ajudar nossa equipe de suporte a diagnosticar carregamentos lentos de página, requisições com falha ou outros problemas de rede.",
      href: "/pt-BR/resources/support-center/knowledge-base/monitoring-debugging/generate-har-file",
      category: "Monitoramento e depuração",
      tags: ["Tools and Utilities"]
    },
    {
      id: "materialized-views/how-to-display-queries-using-mv",
      title: "Como Identificar Consultas que Utilizam Visões Materializadas no ClickHouse",
      description: "Aprenda a consultar os logs do ClickHouse para identificar todas as consultas que envolvem visões materializadas em um intervalo de tempo determinado.",
      href: "/pt-BR/resources/support-center/knowledge-base/materialized-views/how-to-display-queries-using-mv",
      category: "Visões materializadas e projeções",
      tags: ["System Tables"]
    },
    {
      id: "performance-optimization/find-expensive-queries",
      title: "Como Identificar as Consultas Mais Custosas no ClickHouse",
      description: "Aprenda a usar a tabela `query_log` no ClickHouse para identificar as consultas com maior consumo de memória e CPU em nós distribuídos.",
      href: "/pt-BR/resources/support-center/knowledge-base/performance-optimization/find-expensive-queries",
      category: "Desempenho e otimização",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "configuration-settings/ignoring-incorrect-settings",
      title: "Como Ignorar Configurações Incorretas no ClickHouse",
      description: "Aprenda a usar a opção `skip_check_for_incorrect_settings` para permitir que o ClickHouse inicie mesmo quando configurações no nível do usuário estejam definidas incorretamente.",
      href: "/pt-BR/resources/support-center/knowledge-base/configuration-settings/ignoring-incorrect-settings",
      category: "Configuração e ajustes",
      tags: ["Settings"]
    },
    {
      id: "data-import-export/json-import",
      title: "Como importar JSON no ClickHouse?",
      description: "Esta página mostra como importar JSON no ClickHouse",
      href: "/pt-BR/resources/support-center/knowledge-base/data-import-export/json-import",
      category: "Importação e exportação de dados",
      tags: []
    },
    {
      id: "setup-installation/how-to-increase-thread-pool-size",
      title: "Como Aumentar o Número de Threads no ClickHouse",
      description: "Aprenda a configurar o pool de threads global no ClickHouse ajustando parâmetros como `max_thread_pool_size`, `thread_pool_queue_size` e `max_thread_pool_free_size`.",
      href: "/pt-BR/resources/support-center/knowledge-base/setup-installation/how-to-increase-thread-pool-size",
      category: "Configuração e instalação",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "data-import-export/kafka-to-clickhouse-setup",
      title: "Como Realizar a Ingestão de Dados do Kafka no ClickHouse",
      description: "Aprenda a realizar a ingestão de dados de um tópico Kafka no ClickHouse usando o mecanismo de tabela Kafka, visões materializadas e tabelas MergeTree.",
      href: "/pt-BR/resources/support-center/knowledge-base/data-import-export/kafka-to-clickhouse-setup",
      category: "Importação e exportação de dados",
      tags: ["Data Ingestion"]
    },
    {
      id: "data-import-export/ingest-parquet-files-in-s3",
      title: "Como realizar a ingestão de arquivos Parquet de um bucket S3",
      description: "Aprenda os fundamentos do uso do mecanismo de tabela S3 no ClickHouse para realizar a ingestão e consulta de arquivos Parquet de um bucket S3, incluindo configuração, permissões de acesso e exemplos de importação de dados.",
      href: "/pt-BR/resources/support-center/knowledge-base/data-import-export/ingest-parquet-files-in-s3",
      category: "Importação e exportação de dados",
      tags: ["Data Ingestion"]
    },
    {
      id: "queries-sql/how-to-insert-all-rows-from-another-table",
      title: "Como inserir todas as linhas de uma tabela em outra?",
      description: "Artigo da base de conhecimento sobre como inserir todas as linhas de uma tabela em outra.",
      href: "/pt-BR/resources/support-center/knowledge-base/queries-sql/how-to-insert-all-rows-from-another-table",
      category: "Consultas e SQL",
      tags: ["Data Ingestion"]
    },
    {
      id: "performance-optimization/check-query-processing-time-only",
      title: "Como Medir o Tempo de Processamento de Consultas Sem Retornar Linhas",
      description: "Aprenda a usar a opção `FORMAT Null` no ClickHouse para medir o tempo de processamento de consultas sem retornar nenhuma linha ao cliente.",
      href: "/pt-BR/resources/support-center/knowledge-base/performance-optimization/check-query-processing-time-only",
      category: "Desempenho e otimização",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "monitoring-debugging/outputSendLogsLevelTracesToFile",
      title: "Como redirecionar rastreamentos de nível de log para arquivo usando o clickhouse-client",
      description: "Como redirecionar rastreamentos de nível de log para arquivo usando o clickhouse-client",
      href: "/pt-BR/resources/support-center/knowledge-base/monitoring-debugging/outputSendLogsLevelTracesToFile",
      category: "Monitoramento e depuração",
      tags: ["Data Export"]
    },
    {
      id: "tables-schema/recreate-table-across-terminals",
      title: "Como recriar rapidamente uma tabela pequena em diferentes terminais",
      description: "Aprenda a recriar rapidamente uma tabela pequena e seus dados em diferentes terminais usando copiar/colar em ambientes de desenvolvimento.",
      href: "/pt-BR/resources/support-center/knowledge-base/tables-schema/recreate-table-across-terminals",
      category: "Tabelas e esquema",
      tags: ["Tools and Utilities"]
    },
    {
      id: "integrations/how-to-set-up-ch-on-docker-odbc-connect-mssql",
      title: "Como configurar o ClickHouse no Docker com ODBC para conectar a um banco de dados Microsoft SQL Server (MSSQL)",
      description: "Como configurar o ClickHouse no Docker com ODBC para conectar a um banco de dados Microsoft SQL Server (MSSQL)",
      href: "/pt-BR/resources/support-center/knowledge-base/integrations/how-to-set-up-ch-on-docker-odbc-connect-mssql",
      category: "Integrações e bibliotecas de cliente",
      tags: ["Native Clients and Interfaces"]
    },
    {
      id: "queries-sql/using-array-join-to-extract-and-query-attributes",
      title: "Como usar array join para extrair e consultar atributos variáveis usando chaves e valores de map",
      description: "Exemplo simples para ilustrar como usar array join para extrair e consultar atributos variáveis usando chaves e valores de map",
      href: "/pt-BR/resources/support-center/knowledge-base/queries-sql/using-array-join-to-extract-and-query-attributes",
      category: "Consultas & SQL",
      tags: ["Functions"]
    },
    {
      id: "materialized-views/how-to-use-parametrised-views",
      title: "Como usar visões parametrizadas no ClickHouse",
      description: "Aprenda a criar e consultar visões parametrizadas no ClickHouse para fatiamento dinâmico de dados com base em parâmetros definidos no momento da consulta.",
      href: "/pt-BR/resources/support-center/knowledge-base/materialized-views/how-to-use-parametrised-views",
      category: "Visões materializadas & projeções",
      tags: ["Use Cases"]
    },
    {
      id: "tables-schema/exchangeStatementToSwitchTables",
      title: "Como usar o comando exchange para trocar tabelas",
      description: "Como usar o comando exchange para trocar tabelas",
      href: "/pt-BR/resources/support-center/knowledge-base/tables-schema/exchangeStatementToSwitchTables",
      category: "Tabelas & esquema",
      tags: ["Managing Data"]
    },
    {
      id: "queries-sql/compare-resultsets",
      title: "Como validar se duas consultas retornam os mesmos conjuntos de resultados",
      description: "Aprenda a validar se duas consultas do ClickHouse produzem conjuntos de resultados idênticos usando funções de hash e técnicas de comparação.",
      href: "/pt-BR/resources/support-center/knowledge-base/queries-sql/compare-resultsets",
      category: "Consultas & SQL",
      tags: ["Functions"]
    },
    {
      id: "monitoring-debugging/check-query-cache-in-use",
      title: "Como verificar o uso do cache de consultas no ClickHouse",
      description: "Aprenda a verificar se o cache de consultas está sendo utilizado no ClickHouse usando logs de rastreamento do `clickhouse-client` ou comandos SQL.",
      href: "/pt-BR/resources/support-center/knowledge-base/monitoring-debugging/check-query-cache-in-use",
      category: "Monitoramento & depuração",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "cloud-services/unable-to-access-cloud-service",
      title: "Não consigo acessar um serviço do ClickHouse Cloud",
      description: "Solução de problemas de acesso a serviços do ClickHouse Cloud, incluindo configuração da lista de acesso por IP",
      href: "/pt-BR/resources/support-center/knowledge-base/cloud-services/unable-to-access-cloud-service",
      category: "Cloud",
      tags: ["Erros e Exceções", "Managing Cloud"]
    },
    {
      id: "performance-optimization/finding-expensive-queries-by-memory-usage",
      title: "Identificando consultas custosas por uso de memória no ClickHouse",
      description: "Aprenda a usar a tabela `system.query_log` para encontrar as consultas com maior consumo de memória no ClickHouse, com exemplos para configurações em cluster e standalone.",
      href: "/pt-BR/resources/support-center/knowledge-base/performance-optimization/finding-expensive-queries-by-memory-usage",
      category: "Desempenho & otimização",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "data-import-export/importing-and-working-with-json-array-objects",
      title: "Importando e consultando objetos de array JSON no ClickHouse",
      description: "Aprenda a importar objetos de array JSON no ClickHouse e realizar consultas avançadas usando funções JSON e operações de array.",
      href: "/pt-BR/resources/support-center/knowledge-base/data-import-export/importing-and-working-with-json-array-objects",
      category: "Importação & exportação de dados",
      tags: ["Data Formats"]
    },
    {
      id: "data-import-export/importing-geojason-with-nested-object-array",
      title: "Importando GeoJSON com um array de objetos profundamente aninhado",
      description: "Importando GeoJSON com um array de objetos profundamente aninhado",
      href: "/pt-BR/resources/support-center/knowledge-base/data-import-export/importing-geojason-with-nested-object-array",
      category: "Importação & exportação de dados",
      tags: ["Data Formats"]
    },
    {
      id: "performance-optimization/improve-map-performance",
      title: "Melhorando o desempenho de buscas em Map no ClickHouse",
      description: "Aprenda a otimizar buscas em colunas do tipo Map no ClickHouse para melhor desempenho de consultas, materializando chaves específicas como colunas independentes.",
      href: "/pt-BR/resources/support-center/knowledge-base/performance-optimization/improve-map-performance",
      category: "Desempenho & otimização",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "tables-schema/delete-old-data",
      title: "É possível excluir registros antigos de uma tabela do ClickHouse?",
      description: "Esta página responde se é possível excluir registros antigos de uma tabela do ClickHouse",
      href: "/pt-BR/resources/support-center/knowledge-base/tables-schema/delete-old-data",
      category: "Tabelas & esquema",
      tags: []
    },
    {
      id: "general-faqs/separate-storage",
      title: "É possível implantar o ClickHouse com armazenamento e computação separados?",
      description: "Esta página responde se é possível implantar o ClickHouse com armazenamento e computação separados",
      href: "/pt-BR/resources/support-center/knowledge-base/general-faqs/separate-storage",
      category: "Geral & FAQs",
      tags: []
    },
    {
      id: "data-import-export/json-extract-example",
      title: "Exemplo de extração de JSON",
      description: "Um exemplo breve de como extrair tipos primitivos de JSON",
      href: "/pt-BR/resources/support-center/knowledge-base/data-import-export/json-extract-example",
      category: "Importação & exportação de dados",
      tags: ["Data Formats"]
    },
    {
      id: "queries-sql/calculate-pi-using-sql",
      title: "Vamos calcular pi usando SQL",
      description: "É o Dia do Pi! Vamos calcular pi usando o SQL do ClickHouse",
      href: "/pt-BR/resources/support-center/knowledge-base/queries-sql/calculate-pi-using-sql",
      category: "Consultas & SQL",
      tags: ["Use Cases"]
    },
    {
      id: "cloud-services/clickhouse-cloud-api-usage",
      title: "Gerenciando o serviço do ClickHouse Cloud com API e cURL",
      description: "Aprenda a iniciar, parar e retomar um serviço do ClickHouse Cloud usando endpoints de API e comandos cURL.",
      href: "/pt-BR/resources/support-center/knowledge-base/cloud-services/clickhouse-cloud-api-usage",
      category: "Cloud",
      tags: ["Managing Cloud", "Tools and Utilities"]
    },
    {
      id: "monitoring-debugging/mapping-of-system-metrics-to-prometheus-metrics",
      title: "Mapeamento de métricas usadas em system.dashboards para métricas do Prometheus em `system.custom_metrics`",
      description: "Mapeamento de métricas usadas em system.dashboards para métricas do Prometheus em system.custom_metrics",
      href: "/pt-BR/resources/support-center/knowledge-base/monitoring-debugging/mapping-of-system-metrics-to-prometheus-metrics",
      category: "Monitoramento & depuração",
      tags: ["System Tables"]
    },
    {
      id: "security/windows-active-directory-to-ch-roles",
      title: "Mapeando grupos de segurança do Windows Active Directory para roles do ClickHouse",
      description: "Exemplo de mapeamento de grupos de segurança do Windows Active Directory para roles do ClickHouse",
      href: "/pt-BR/resources/support-center/knowledge-base/security/windows-active-directory-to-ch-roles",
      category: "Segurança & controle de acesso",
      tags: ["Tools and Utilities"]
    },
    {
      id: "performance-optimization/memory-limit-exceeded-for-query",
      title: "Limite de memória excedido para a consulta",
      description: "Solução de problemas de erros de limite de memória excedido em consultas",
      href: "/pt-BR/resources/support-center/knowledge-base/performance-optimization/memory-limit-exceeded-for-query",
      category: "Desempenho & otimização",
      tags: ["Erros e Exceções"]
    },
    {
      id: "integrations/ODBC-authentication-failed-error-using-PowerBI-CH-connector",
      title: "Erro de falha na autenticação ODBC ao usar o conector Power BI do ClickHouse",
      description: "Erro de falha na autenticação ODBC ao usar o conector Power BI do ClickHouse",
      href: "/pt-BR/resources/support-center/knowledge-base/integrations/ODBC-authentication-failed-error-using-PowerBI-CH-connector",
      category: "Integrações & bibliotecas de cliente",
      tags: ["Native Clients and Interfaces", "Erros e Exceções"]
    },
    {
      id: "monitoring-debugging/profiling-clickhouse-with-llvm-xray",
      title: "Perfilando o ClickHouse com o XRay do LLVM",
      description: "Aprenda a perfilar o ClickHouse usando o profiler de instrumentação XRay do LLVM, visualizar rastreamentos e analisar o desempenho.",
      href: "/pt-BR/resources/support-center/knowledge-base/monitoring-debugging/profiling-clickhouse-with-llvm-xray",
      category: "Monitoramento & depuração",
      tags: ["Performance and Optimizations", "Tools and Utilities"]
    },
    {
      id: "integrations/python-http-requests",
      title: "Exemplo rápido em Python usando o módulo HTTP requests",
      description: "Um exemplo usando Python e o módulo requests para gravar e ler dados no ClickHouse",
      href: "/pt-BR/resources/support-center/knowledge-base/integrations/python-http-requests",
      category: "Integrações e bibliotecas de cliente",
      tags: ["Native Clients and Interfaces"]
    },
    {
      id: "configuration-settings/maximum-number-of-tables-and-databases",
      title: "Limites máximos recomendados de bancos de dados, tabelas, partições e partes no ClickHouse",
      description: "Conheça os limites máximos recomendados para bancos de dados, tabelas, partições e partes em um cluster ClickHouse para garantir desempenho ideal.",
      href: "/pt-BR/resources/support-center/knowledge-base/configuration-settings/maximum-number-of-tables-and-databases",
      category: "Configuração e definições",
      tags: ["Performance and Optimizations", "Deployments and Scaling"]
    },
    {
      id: "data-import-export/cannot-append-data-to-parquet-format",
      title: 'Resolvendo o erro "Cannot Append Data in Parquet Format" no ClickHouse',
      description: 'Está recebendo o erro "Cannot append data in format Parquet to file" no ClickHouse? Veja como resolvê-lo.',
      href: "/pt-BR/resources/support-center/knowledge-base/data-import-export/cannot-append-data-to-parquet-format",
      category: "Importação e exportação de dados",
      tags: ["Errors and Exceptions", "Data Formats"]
    },
    {
      id: "troubleshooting/exception-too-many-parts",
      title: 'Resolvendo o erro "Too Many Parts" no ClickHouse',
      description: 'Saiba como resolver o erro "Too many parts" no ClickHouse otimizando as taxas de inserção, configurando as definições do MergeTree e gerenciando partições de forma eficaz.',
      href: "/pt-BR/resources/support-center/knowledge-base/troubleshooting/exception-too-many-parts",
      category: "Solução de problemas e erros",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "troubleshooting/certificate-verify-failed-error",
      title: "Resolvendo o erro de verificação de certificado SSL no ClickHouse",
      description: "Saiba como resolver o erro SSL Exception CERTIFICATE_VERIFY_FAILED.",
      href: "/pt-BR/resources/support-center/knowledge-base/troubleshooting/certificate-verify-failed-error",
      category: "Solução de problemas e erros",
      tags: ["Security and Authentication", "Errors and Exceptions"]
    },
    {
      id: "troubleshooting/connection-timeout-remote-remoteSecure",
      title: "Resolvendo erros de timeout com as funções de tabela `remote` e `remoteSecure`",
      description: "Saiba como corrigir erros de timeout ao usar as funções de tabela `remote` ou `remoteSecure` no ClickHouse ajustando as configurações de timeout de conexão.",
      href: "/pt-BR/resources/support-center/knowledge-base/troubleshooting/connection-timeout-remote-remoteSecure",
      category: "Solução de problemas e erros",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "tables-schema/search-across-node-for-tables-with-a-wildcard",
      title: "Pesquisando tabelas com curinga em múltiplos nós",
      description: "Saiba como pesquisar tabelas com curinga em múltiplos nós.",
      href: "/pt-BR/resources/support-center/knowledge-base/tables-schema/search-across-node-for-tables-with-a-wildcard",
      category: "Tabelas e esquema",
      tags: ["Deployments and Scaling"]
    },
    {
      id: "performance-optimization/query-max-execution-time",
      title: "Definindo um limite no tempo de execução de consultas",
      description: "Como impor um limite no tempo máximo de execução de consultas",
      href: "/pt-BR/resources/support-center/knowledge-base/performance-optimization/query-max-execution-time",
      category: "Desempenho e otimização",
      tags: ["Managing Cloud", "Settings"]
    },
    {
      id: "data-import-export/json-simple-example",
      title: "Exemplo simples de fluxo para extração de dados JSON usando uma tabela de entrada com uma visão materializada",
      description: "Exemplo simples de fluxo para extração de dados JSON usando uma tabela de entrada com uma visão materializada",
      href: "/pt-BR/resources/support-center/knowledge-base/data-import-export/json-simple-example",
      category: "Importação e exportação de dados",
      tags: ["Data Formats"]
    },
    {
      id: "performance-optimization/async-vs-optimize-read-in-order",
      title: "Leitura síncrona de dados",
      description:
        "A nova configuração `allow_asynchronous_read_from_io_pool_for_merge_tree` permite que o número de threads de leitura (streams) seja maior do que o número de threads no restante do pipeline de execução de consultas.",
      href: "/pt-BR/resources/support-center/knowledge-base/performance-optimization/async-vs-optimize-read-in-order",
      category: "Desempenho e otimização",
      tags: ["Settings", "Performance and Optimizations"]
    },
    {
      id: "integrations/terraform-example",
      title: "Exemplo com Terraform de como usar a API do Cloud",
      description: "Este guia apresenta um exemplo de como usar o Terraform para criar/excluir clusters usando a API",
      href: "/pt-BR/resources/support-center/knowledge-base/integrations/terraform-example",
      category: "Integrações e bibliotecas de cliente",
      tags: ["Native Clients and Interfaces"]
    },
    {
      id: "performance-optimization/tips-tricks-optimizing-basic-data-types-in-clickhouse",
      title: "Dicas e truques para otimizar tipos de dados básicos no ClickHouse",
      description: "Dicas e truques para otimizar tipos de dados básicos no ClickHouse",
      href: "/pt-BR/resources/support-center/knowledge-base/performance-optimization/tips-tricks-optimizing-basic-data-types-in-clickhouse",
      category: "Desempenho e otimização",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "queries-sql/useful-queries-for-troubleshooting",
      title: "Consultas úteis para solução de problemas",
      description: "Uma coleção de consultas práticas para solução de problemas no ClickHouse, incluindo monitoramento de tamanhos de tabelas, consultas de longa duração e erros.",
      href: "/pt-BR/resources/support-center/knowledge-base/queries-sql/useful-queries-for-troubleshooting",
      category: "Consultas e SQL",
      tags: ["Settings"]
    },
    {
      id: "general-faqs/use-clickhouse-for-log-analytics",
      title: "Usando o ClickHouse para análise de logs",
      description: "O ClickHouse é popular para análise de logs e métricas graças às suas capacidades de análise em tempo real. Quer saber mais?",
      href: "/pt-BR/resources/support-center/knowledge-base/general-faqs/use-clickhouse-for-log-analytics",
      category: "Geral e FAQs",
      tags: ["Use Cases"]
    },
    {
      id: "queries-sql/filtered-aggregates",
      title: "Usando agregações filtradas no ClickHouse",
      description: "Saiba como usar agregações filtradas no ClickHouse com os combinadores de agregação `-If` e `-Distinct` para simplificar a sintaxe de consultas e aprimorar análises.",
      href: "/pt-BR/resources/support-center/knowledge-base/queries-sql/filtered-aggregates",
      category: "Consultas e SQL",
      tags: ["Functions"]
    },
    {
      id: "general-faqs/dependencies",
      title: "Quais são as dependências de terceiros para executar o ClickHouse?",
      description: "O ClickHouse é autossuficiente e não possui dependências em tempo de execução",
      href: "/pt-BR/resources/support-center/knowledge-base/general-faqs/dependencies",
      category: "Geral e FAQs",
      tags: []
    },
    {
      id: "general-faqs/dbms-naming",
      title: 'O que significa "ClickHouse"?',
      description: 'Saiba o que significa "ClickHouse".',
      href: "/pt-BR/resources/support-center/knowledge-base/general-faqs/dbms-naming",
      category: "Geral e FAQs",
      tags: []
    },
    {
      id: "general-faqs/ne-tormozit",
      title: 'O que significa "не тормозит"?',
      description: 'Esta página explica o que significa "Не тормозит"',
      href: "/pt-BR/resources/support-center/knowledge-base/general-faqs/ne-tormozit",
      category: "Geral e FAQs",
      tags: []
    },
    {
      id: "integrations/oracle-odbc",
      title: "O que fazer se eu tiver um problema com codificações ao usar o Oracle via ODBC?",
      description: "Esta página fornece orientações sobre o que fazer se você tiver um problema com codificações ao usar o Oracle via ODBC",
      href: "/pt-BR/resources/support-center/knowledge-base/integrations/oracle-odbc",
      category: "Integrações e bibliotecas de cliente",
      tags: []
    },
    {
      id: "general-faqs/columnar-database",
      title: "O que é um banco de dados colunar?",
      description: "Esta página descreve o que é um banco de dados colunar",
      href: "/pt-BR/resources/support-center/knowledge-base/general-faqs/columnar-database",
      category: "Geral e FAQs",
      tags: []
    },
    {
      id: "general-faqs/olap",
      title: "O que é OLAP?",
      description: "Uma explicação sobre o que é Processamento Analítico Online (Online Analytical Processing)",
      href: "/pt-BR/resources/support-center/knowledge-base/general-faqs/olap",
      category: "Geral e FAQs",
      tags: []
    },
    {
      id: "performance-optimization/optimize-final-vs-final",
      title: "Qual é a diferença entre OPTIMIZE FINAL e FINAL?",
      description: "Discute as diferenças entre OPTIMIZE FINAL e FINAL, e quando usá-los ou evitá-los.",
      href: "/pt-BR/resources/support-center/knowledge-base/performance-optimization/optimize-final-vs-final",
      category: "Desempenho e otimização",
      tags: ["Conceitos Principais de Dados"]
    },
    {
      id: "general-faqs/sql",
      title: "Qual sintaxe SQL o ClickHouse suporta?",
      description: "O ClickHouse suporta 100% da sintaxe SQL",
      href: "/pt-BR/resources/support-center/knowledge-base/general-faqs/sql",
      category: "Geral e FAQs",
      tags: []
    },
    {
      id: "data-management/when-is-ttl-applied",
      title: "Quando as regras de TTL são aplicadas e temos controle sobre isso?",
      description:
        "As regras de TTL no ClickHouse acabam sendo aplicadas em algum momento, e você pode controlar quando elas são executadas usando a configuração `merge_with_ttl_timeout`. Aprenda a forçar a aplicação de TTL e a gerenciar threads em segundo plano para a execução de TTL.",
      href: "/pt-BR/resources/support-center/knowledge-base/data-management/when-is-ttl-applied",
      category: "Gerenciamento de dados",
      tags: ["Conceitos Principais de Dados"]
    },
    {
      id: "setup-installation/production",
      title: "Qual versão do ClickHouse usar em produção?",
      description: "Esta página fornece orientações sobre qual versão do ClickHouse usar em produção",
      href: "/pt-BR/resources/support-center/knowledge-base/setup-installation/production",
      category: "Configuração e instalação",
      tags: []
    },
    {
      id: "general-faqs/who-is-using-clickhouse",
      title: "Quem está usando o ClickHouse?",
      description: "Descreve quem está usando o ClickHouse",
      href: "/pt-BR/resources/support-center/knowledge-base/general-faqs/who-is-using-clickhouse",
      category: "Geral e FAQs",
      tags: []
    },
    {
      id: "data-management/dictionaries-consistent-state",
      title: "Por que não consigo ver meus dados em um dicionário no ClickHouse Cloud?",
      description: "Existe um problema em que os dados em dicionários podem não ficar visíveis imediatamente após a criação.",
      href: "/pt-BR/resources/support-center/knowledge-base/data-management/dictionaries-consistent-state",
      category: "Gerenciamento de dados",
      tags: ["Gerenciamento do Cloud", "Modelagem de Dados"]
    },
    {
      id: "general-faqs/why-recommend-clickhouse-keeper-over-zookeeper",
      title: "Por que o ClickHouse Keeper é recomendado em vez do ZooKeeper?",
      description:
        "O ClickHouse Keeper aprimora o ZooKeeper com recursos como uso reduzido de espaço em disco, recuperação mais rápida e menor consumo de memória, oferecendo melhor desempenho para clusters do ClickHouse.",
      href: "/pt-BR/resources/support-center/knowledge-base/general-faqs/why-recommend-clickhouse-keeper-over-zookeeper",
      category: "Geral e FAQs",
      tags: ["Conceitos Principais de Dados"]
    },
    {
      id: "monitoring-debugging/why-default-logging-verbose",
      title: "Por que os logs do ClickHouse são tão detalhados por padrão?",
      description: "Entenda por que os desenvolvedores do ClickHouse optaram por definir um nível de log detalhado por padrão.",
      href: "/pt-BR/resources/support-center/knowledge-base/monitoring-debugging/why-default-logging-verbose",
      category: "Monitoramento e depuração",
      tags: ["Configurações"]
    },
    {
      id: "performance-optimization/why-is-my-primary-key-not-used",
      title: "Por que minha chave primária não é usada? Como posso verificar?",
      description: "Aborda um motivo comum pelo qual uma chave primária não é usada na ordenação e como podemos confirmar isso",
      href: "/pt-BR/resources/support-center/knowledge-base/performance-optimization/why-is-my-primary-key-not-used",
      category: "Desempenho e otimização",
      tags: ["Desempenho e Otimizações"]
    },
    {
      id: "general-faqs/mapreduce",
      title: "Por que não usar algo como o MapReduce?",
      description: "Esta página explica por que você usaria o ClickHouse em vez do MapReduce",
      href: "/pt-BR/resources/support-center/knowledge-base/general-faqs/mapreduce",
      category: "Geral e FAQs",
      tags: []
    }
  ]
}