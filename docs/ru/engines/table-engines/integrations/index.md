---
sidebar_label: "Движки таблиц для интеграции"
sidebar_position: 30
---

# Движки таблиц для интеграции {#table-engines-for-integrations}

Для интеграции с внешними системами ClickHouse предоставляет различные средства, включая движки таблиц. Конфигурирование интеграционных движков осуществляется с помощью запросов `CREATE TABLE` или `ALTER TABLE`, как и для других табличных движков. С точки зрения пользователя, настроенная интеграция выглядит как обычная таблица, но запросы к ней передаются через прокси во внешнюю систему. Этот прозрачный запрос является одним из ключевых преимуществ этого подхода по сравнению с альтернативными методами интеграции, такими как внешние словари или табличные функции, которые требуют использования пользовательских методов запроса при каждом использовании.

Список поддерживаемых интеграций:

-   [ODBC](../../../engines/table-engines/integrations/odbc.md)
-   [JDBC](../../../engines/table-engines/integrations/jdbc.md)
-   [MySQL](../../../engines/table-engines/integrations/mysql.md)
-   [MongoDB](../../../engines/table-engines/integrations/mongodb.md)
-   [HDFS](../../../engines/table-engines/integrations/hdfs.md)
-   [S3](../../../engines/table-engines/integrations/s3.md)
-   [Kafka](../../../engines/table-engines/integrations/kafka.md)
-   [EmbeddedRocksDB](../../../engines/table-engines/integrations/embedded-rocksdb.md)
-   [RabbitMQ](../../../engines/table-engines/integrations/rabbitmq.md)
-   [PostgreSQL](../../../engines/table-engines/integrations/postgresql.md)

