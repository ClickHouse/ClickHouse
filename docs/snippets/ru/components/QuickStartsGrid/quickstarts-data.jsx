export const quickStartsData = [
  {
    id: "connect-your-iceberg-catalog",
    title: "Как подключить каталог Iceberg в ClickHouse Cloud",
    description: "Узнайте, как подключить ClickHouse Cloud к вашему Data Catalog и выполнять запросы к таблицам Iceberg.",
    href: "/get-started/quickstarts/connect-your-iceberg-catalog",
    useCases: ["data-warehousing"],
    products: ["cloud"]
  },
  {
    id: "create-your-first-materialized-view",
    title: "Создайте свой первый materialized view",
    description:
      "Узнайте, как использовать materialized views в ClickHouse для предварительного вычисления и хранения результатов запросов с другим порядком сортировки, что позволяет быстро выполнять поиск по столбцам, не входящим в первичный ключ.",
    href: "/get-started/quickstarts/create-your-first-materialized-view",
    useCases: ["real-time-analytics", "data-warehousing"],
    products: ["cloud"]
  },
  {
    id: "create-your-first-mergetree-table",
    title: "Создайте свою первую таблицу семейства MergeTree",
    description:
      "Узнайте, как работает основной движок таблицы ClickHouse: создайте таблицу семейства MergeTree, загрузите данные о ценах на недвижимость в Великобритании и посмотрите, как части и слияния влияют на хранение данных и производительность запросов.",
    href: "/get-started/quickstarts/create-your-first-mergetree-table",
    useCases: ["all"],
    products: ["cloud"]
  },
  {
    id: "create-your-first-projection",
    title: "Создайте свою первую проекцию",
    description: "Узнайте, как использовать проекции в ClickHouse для хранения дополнительной отсортированной копии данных в той же таблице, что позволяет быстро выполнять поиск по столбцам, не входящим в первичный ключ.",
    href: "/get-started/quickstarts/create-your-first-projection",
    useCases: ["real-time-analytics", "data-warehousing"],
    products: ["cloud"]
  },
  {
    id: "create-your-first-service-on-cloud",
    title: "Создайте свой первый сервис ClickHouse Cloud и загрузите демонстрационные данные",
    description: "Создайте сервис ClickHouse Cloud, изучите SQL Console и загрузите демонстрационный набор данных, чтобы начать выполнять запросы к реальным данным за считанные минуты.",
    href: "/get-started/quickstarts/create-your-first-service-on-cloud",
    useCases: ["all"],
    products: ["cloud"]
  },
  {
    id: "creating-tables",
    title: "Создание таблиц в ClickHouse",
    description: "Узнайте о создании таблиц в ClickHouse",
    href: "/get-started/quickstarts/creating-tables",
    useCases: ["all"],
    products: ["self-managed"]
  },
  {
    id: "insert-data-using-clickhouse-client",
    title: "Вставка данных в ClickHouse Cloud с помощью clickhouse-client",
    description: "Узнайте, как использовать clickhouse-client для вставки данных из локальных файлов CSV и Parquet в сервис ClickHouse Cloud из командной строки.",
    href: "/get-started/quickstarts/insert-data-using-clickhouse-client",
    useCases: ["all"],
    products: ["cloud"]
  },
  {
    id: "mutations",
    title: "Обновление и удаление данных в ClickHouse",
    description: "Описание операций обновления и удаления данных в ClickHouse",
    href: "/get-started/quickstarts/mutations",
    useCases: ["data-warehousing"],
    products: ["self-managed"]
  },
  {
    id: "obtain-your-cloud-connection-details",
    title: "Получите сведения о подключении к Cloud",
    description: "Узнайте, как найти имя хоста, порт и учётные данные для вашего сервиса ClickHouse Cloud, чтобы подключаться из внешних клиентов, CLI и приложений.",
    href: "/get-started/quickstarts/obtain-your-cloud-connection-details",
    useCases: ["all"],
    products: ["cloud"]
  },
  {
    id: "tutorial",
    title: "Расширенное руководство",
    description: "Узнайте, как загружать данные и выполнять запросы в ClickHouse на примере демонстрационного набора данных о такси Нью-Йорка.",
    href: "/get-started/quickstarts/tutorial",
    useCases: ["real-time-analytics", "data-warehousing"],
    products: ["cloud", "self-managed"]
  },
  {
    id: "working-with-the-map-type",
    title: "Работа с типом Map в ClickHouse",
    description: "Узнайте, как использовать тип Map в ClickHouse для хранения, запросов и агрегации динамических данных в формате ключ-значение на практическом примере атрибутов ресурсов OTel.",
    href: "/get-started/quickstarts/working-with-the-map-type",
    useCases: ["observability"],
    products: ["self-managed"]
  },
  {
    id: "writing-queries",
    title: "Выборка данных из ClickHouse",
    description: "Узнайте о выборке данных из ClickHouse",
    href: "/get-started/quickstarts/writing-queries",
    useCases: ["all"],
    products: ["self-managed"]
  }
]