---
toc_folder_title: "\u0414\u0432\u0438\u0436\u043a\u0438\u0020\u0442\u0430\u0431\u043b\u0438\u0446\u0020\u0434\u043b\u044f\u0020\u0438\u043d\u0442\u0435\u0433\u0440\u0430\u0446\u0438\u0438"
toc_priority: 30
---

# Движки таблиц для интеграции {#table-engines-for-integrations}

Для интеграции с внешними системами ClickHouse предоставляет различные средства, включая движки таблиц. Конфигурирование интеграционных движков осуществляется с помощью запросов `CREATE TABLE` или `ALTER TABLE`, как и для других табличных движков. С точки зрения пользователя, настроенная интеграция выглядит как обычная таблица, но запросы к ней передаются через прокси во внешнюю систему. Этот прозрачный запрос является одним из ключевых преимуществ этого подхода по сравнению с альтернативными методами интеграции, такими как внешние словари или табличные функции, которые требуют использования пользовательских методов запроса при каждом использовании.

Список поддерживаемых интеграций:

-   [ODBC](../../../engines/table-engines/integrations/odbc.md)
-   [JDBC](../../../engines/table-engines/integrations/jdbc.md)
-   [MySQL](../../../engines/table-engines/integrations/mysql.md)
-   [HDFS](../../../engines/table-engines/integrations/hdfs.md)
-   [Kafka](../../../engines/table-engines/integrations/kafka.md)

[Оригинальная статья](https://clickhouse.tech/docs/ru/engines/table-engines/integrations/) <!--hide-->
