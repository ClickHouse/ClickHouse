export const quickStartsData = [
  {
    id: "connect-your-iceberg-catalog",
    title: "Como conectar seu catálogo Iceberg no ClickHouse Cloud",
    description: "Aprenda como conectar o ClickHouse Cloud ao seu Data Catalog e consultar tabelas Iceberg.",
    href: "/get-started/quickstarts/connect-your-iceberg-catalog",
    useCases: ["data-warehousing"],
    products: ["cloud"]
  },
  {
    id: "create-your-first-materialized-view",
    title: "Crie sua primeira visão materializada",
    description:
      "Aprenda como usar visões materializadas no ClickHouse para pré-computar e armazenar resultados de consultas com uma ordem de classificação diferente, permitindo buscas rápidas em colunas não cobertas pela sua chave primária.",
    href: "/get-started/quickstarts/create-your-first-materialized-view",
    useCases: ["real-time-analytics", "data-warehousing"],
    products: ["cloud"]
  },
  {
    id: "create-your-first-mergetree-table",
    title: "Crie sua primeira tabela MergeTree",
    description:
      "Aprenda como funciona o principal table engine do ClickHouse criando uma tabela MergeTree, carregando dados de preços de imóveis do Reino Unido e observando como partes e merges afetam o armazenamento e o desempenho das consultas.",
    href: "/get-started/quickstarts/create-your-first-mergetree-table",
    useCases: ["all"],
    products: ["cloud"]
  },
  {
    id: "create-your-first-projection",
    title: "Crie sua primeira projeção",
    description: "Aprenda como usar projeções no ClickHouse para armazenar uma cópia adicional e ordenada dos seus dados na mesma tabela, permitindo buscas rápidas em colunas não cobertas pela sua chave primária.",
    href: "/get-started/quickstarts/create-your-first-projection",
    useCases: ["real-time-analytics", "data-warehousing"],
    products: ["cloud"]
  },
  {
    id: "create-your-first-service-on-cloud",
    title: "Crie seu primeiro serviço no Cloud e carregue dados de exemplo",
    description: "Crie um serviço no ClickHouse Cloud, explore o SQL Console e carregue um dataset de exemplo para começar a consultar dados reais em minutos.",
    href: "/get-started/quickstarts/create-your-first-service-on-cloud",
    useCases: ["all"],
    products: ["cloud"]
  },
  {
    id: "creating-tables",
    title: "Criando tabelas no ClickHouse",
    description: "Aprenda a criar tabelas no ClickHouse",
    href: "/get-started/quickstarts/creating-tables",
    useCases: ["all"],
    products: ["self-managed"]
  },
  {
    id: "insert-data-using-clickhouse-client",
    title: "Insira dados no ClickHouse Cloud usando o clickhouse-client",
    description: "Aprenda como usar o clickhouse-client para inserir dados de arquivos CSV e Parquet locais em um serviço do ClickHouse Cloud pela linha de comando.",
    href: "/get-started/quickstarts/insert-data-using-clickhouse-client",
    useCases: ["all"],
    products: ["cloud"]
  },
  {
    id: "mutations",
    title: "Atualizando e excluindo dados no ClickHouse",
    description: "Descreve como realizar operações de atualização e exclusão no ClickHouse",
    href: "/get-started/quickstarts/mutations",
    useCases: ["data-warehousing"],
    products: ["self-managed"]
  },
  {
    id: "obtain-your-cloud-connection-details",
    title: "Obtenha os detalhes de conexão do seu Cloud",
    description: "Aprenda como encontrar o hostname, a porta e as credenciais do seu serviço no ClickHouse Cloud para se conectar a partir de clientes externos, CLIs e aplicações.",
    href: "/get-started/quickstarts/obtain-your-cloud-connection-details",
    useCases: ["all"],
    products: ["cloud"]
  },
  {
    id: "tutorial",
    title: "Tutorial avançado",
    description: "Aprenda como ingerir e consultar dados no ClickHouse usando um dataset de exemplo de táxis de Nova York.",
    href: "/get-started/quickstarts/tutorial",
    useCases: ["real-time-analytics", "data-warehousing"],
    products: ["cloud", "self-managed"]
  },
  {
    id: "working-with-the-map-type",
    title: "Trabalhando com o tipo Map no ClickHouse",
    description: "Aprenda como usar o tipo Map no ClickHouse para armazenar, consultar e agregar dados dinâmicos de chave-valor usando atributos de recursos OTel como exemplo prático.",
    href: "/get-started/quickstarts/working-with-the-map-type",
    useCases: ["observability"],
    products: ["self-managed"]
  },
  {
    id: "writing-queries",
    title: "Selecionando dados no ClickHouse",
    description: "Aprenda a selecionar dados no ClickHouse",
    href: "/get-started/quickstarts/writing-queries",
    useCases: ["all"],
    products: ["self-managed"]
  }
]