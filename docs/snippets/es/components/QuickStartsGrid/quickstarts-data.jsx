export const quickStartsData = [
  {
    id: "connect-your-iceberg-catalog",
    title: "Cómo conectar tu catálogo Iceberg en ClickHouse Cloud",
    description: "Aprende a conectar ClickHouse Cloud a tu Data Catalog y a consultar tablas Iceberg.",
    href: "/get-started/quickstarts/connect-your-iceberg-catalog",
    useCases: ["data-warehousing"],
    products: ["cloud"]
  },
  {
    id: "create-your-first-materialized-view",
    title: "Crea tu primera vista materializada",
    description:
      "Aprende a usar vistas materializadas en ClickHouse para precomputar y almacenar resultados de consultas con un orden de clasificación diferente, lo que permite búsquedas rápidas en columnas no cubiertas por tu clave primaria.",
    href: "/get-started/quickstarts/create-your-first-materialized-view",
    useCases: ["real-time-analytics", "data-warehousing"],
    products: ["cloud"]
  },
  {
    id: "create-your-first-mergetree-table",
    title: "Crea tu primera tabla MergeTree",
    description:
      "Aprende cómo funciona el motor de tablas principal de ClickHouse creando una tabla MergeTree, cargando datos de precios de propiedades del Reino Unido y observando cómo las partes y las fusiones afectan al almacenamiento y al rendimiento de las consultas.",
    href: "/get-started/quickstarts/create-your-first-mergetree-table",
    useCases: ["all"],
    products: ["cloud"]
  },
  {
    id: "create-your-first-projection",
    title: "Crea tu primera proyección",
    description: "Aprende a usar proyecciones en ClickHouse para almacenar una copia ordenada adicional de tus datos dentro de la misma tabla, lo que permite búsquedas rápidas en columnas no cubiertas por tu clave primaria.",
    href: "/get-started/quickstarts/create-your-first-projection",
    useCases: ["real-time-analytics", "data-warehousing"],
    products: ["cloud"]
  },
  {
    id: "create-your-first-service-on-cloud",
    title: "Crea tu primer servicio en Cloud y carga datos de ejemplo",
    description: "Crea un servicio de ClickHouse Cloud, explora la SQL Console y carga un conjunto de datos de ejemplo para empezar a consultar datos reales en minutos.",
    href: "/get-started/quickstarts/create-your-first-service-on-cloud",
    useCases: ["all"],
    products: ["cloud"]
  },
  {
    id: "creating-tables",
    title: "Creación de tablas en ClickHouse",
    description: "Aprende a crear tablas en ClickHouse",
    href: "/get-started/quickstarts/creating-tables",
    useCases: ["all"],
    products: ["self-managed"]
  },
  {
    id: "insert-data-using-clickhouse-client",
    title: "Insertar datos en ClickHouse Cloud con clickhouse-client",
    description: "Aprende a usar clickhouse-client para insertar datos desde archivos CSV y Parquet locales en un servicio de ClickHouse Cloud desde la línea de comandos.",
    href: "/get-started/quickstarts/insert-data-using-clickhouse-client",
    useCases: ["all"],
    products: ["cloud"]
  },
  {
    id: "mutations",
    title: "Actualización y eliminación de datos en ClickHouse",
    description: "Describe cómo realizar operaciones de actualización y eliminación en ClickHouse",
    href: "/get-started/quickstarts/mutations",
    useCases: ["data-warehousing"],
    products: ["self-managed"]
  },
  {
    id: "obtain-your-cloud-connection-details",
    title: "Obtén los detalles de conexión de tu Cloud",
    description: "Aprende a localizar el hostname, el puerto y las credenciales de tu servicio de ClickHouse Cloud para conectarte desde clientes externos, CLIs y aplicaciones.",
    href: "/get-started/quickstarts/obtain-your-cloud-connection-details",
    useCases: ["all"],
    products: ["cloud"]
  },
  {
    id: "tutorial",
    title: "Tutorial avanzado",
    description: "Aprende a ingestar y consultar datos en ClickHouse con un conjunto de datos de ejemplo de taxis de Nueva York.",
    href: "/get-started/quickstarts/tutorial",
    useCases: ["real-time-analytics", "data-warehousing"],
    products: ["cloud", "self-managed"]
  },
  {
    id: "working-with-the-map-type",
    title: "Trabajo con el tipo Map en ClickHouse",
    description: "Aprende a usar el tipo Map en ClickHouse para almacenar, consultar y agregar datos dinámicos de tipo clave-valor, con los atributos de recurso de OTel como ejemplo práctico.",
    href: "/get-started/quickstarts/working-with-the-map-type",
    useCases: ["observability"],
    products: ["self-managed"]
  },
  {
    id: "writing-queries",
    title: "Selección de datos en ClickHouse",
    description: "Aprende sobre la selección de datos en ClickHouse",
    href: "/get-started/quickstarts/writing-queries",
    useCases: ["all"],
    products: ["self-managed"]
  }
]