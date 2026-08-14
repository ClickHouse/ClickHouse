export const quickStartsData = [
  {
    id: "connect-your-iceberg-catalog",
    title: "如何在 ClickHouse Cloud 中连接您的 Iceberg catalog",
    description: "了解如何将 ClickHouse Cloud 连接到您的 Data Catalog 并查询 Iceberg 表。",
    href: "/get-started/quickstarts/connect-your-iceberg-catalog",
    useCases: ["data-warehousing"],
    products: ["cloud"]
  },
  {
    id: "create-your-first-materialized-view",
    title: "创建您的第一个 materialized view",
    description:
      "了解如何在 ClickHouse 中使用 materialized views，以不同的排序顺序预计算并存储查询结果，从而对主键未覆盖的列实现快速查找。",
    href: "/get-started/quickstarts/create-your-first-materialized-view",
    useCases: ["real-time-analytics", "data-warehousing"],
    products: ["cloud"]
  },
  {
    id: "create-your-first-mergetree-table",
    title: "创建您的第一个 MergeTree 表",
    description:
      "通过创建 MergeTree 表、加载英国房产价格数据，并观察数据片段与合并操作如何影响存储和查询性能，深入了解 ClickHouse 主表引擎的工作原理。",
    href: "/get-started/quickstarts/create-your-first-mergetree-table",
    useCases: ["all"],
    products: ["cloud"]
  },
  {
    id: "create-your-first-projection",
    title: "创建您的第一个 projection",
    description: "了解如何在 ClickHouse 中使用 projections，在同一张表内存储额外的有序数据副本，从而对主键未覆盖的列实现快速查找。",
    href: "/get-started/quickstarts/create-your-first-projection",
    useCases: ["real-time-analytics", "data-warehousing"],
    products: ["cloud"]
  },
  {
    id: "create-your-first-service-on-cloud",
    title: "创建您的第一个 Cloud 服务并加载示例数据",
    description: "创建 ClickHouse Cloud 服务，探索 SQL 控制台，并加载示例数据集，几分钟内即可开始查询真实数据。",
    href: "/get-started/quickstarts/create-your-first-service-on-cloud",
    useCases: ["all"],
    products: ["cloud"]
  },
  {
    id: "creating-tables",
    title: "在 ClickHouse 中创建表",
    description: "了解如何在 ClickHouse 中创建表",
    href: "/get-started/quickstarts/creating-tables",
    useCases: ["all"],
    products: ["self-managed"]
  },
  {
    id: "insert-data-using-clickhouse-client",
    title: "使用 ClickHouse 客户端向 ClickHouse Cloud 插入数据",
    description: "了解如何使用 ClickHouse 客户端通过命令行将本地 CSV 和 Parquet 文件中的数据插入 ClickHouse Cloud 服务。",
    href: "/get-started/quickstarts/insert-data-using-clickhouse-client",
    useCases: ["all"],
    products: ["cloud"]
  },
  {
    id: "mutations",
    title: "更新和删除 ClickHouse 数据",
    description: "介绍如何在 ClickHouse 中执行更新和删除操作",
    href: "/get-started/quickstarts/mutations",
    useCases: ["data-warehousing"],
    products: ["self-managed"]
  },
  {
    id: "obtain-your-cloud-connection-details",
    title: "获取您的 Cloud 连接详情",
    description: "了解如何查找 ClickHouse Cloud 服务的主机名、端口和凭据，以便从外部客户端、命令行客户端和应用程序进行连接。",
    href: "/get-started/quickstarts/obtain-your-cloud-connection-details",
    useCases: ["all"],
    products: ["cloud"]
  },
  {
    id: "tutorial",
    title: "进阶教程",
    description: "了解如何使用纽约市出租车示例数据集在 ClickHouse 中摄取和查询数据。",
    href: "/get-started/quickstarts/tutorial",
    useCases: ["real-time-analytics", "data-warehousing"],
    products: ["cloud", "self-managed"]
  },
  {
    id: "working-with-the-map-type",
    title: "在 ClickHouse 中使用 Map 类型",
    description: "了解如何在 ClickHouse 中使用 Map 类型存储、查询和聚合动态键值数据，并以 OTel resource attributes 为实际示例加以说明。",
    href: "/get-started/quickstarts/working-with-the-map-type",
    useCases: ["observability"],
    products: ["self-managed"]
  },
  {
    id: "writing-queries",
    title: "查询 ClickHouse 数据",
    description: "了解如何查询 ClickHouse 数据",
    href: "/get-started/quickstarts/writing-queries",
    useCases: ["all"],
    products: ["self-managed"]
  }
]