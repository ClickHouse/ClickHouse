# system.graphite_retentions {#system-graphite-retentions}

Содержит информацию о том, какие параметры [graphite_rollup](../server-configuration-parameters/settings.md#server_configuration_parameters-graphite) используются в таблицах с движками [\*GraphiteMergeTree](../../engines/table-engines/mergetree-family/graphitemergetree.md).

Столбцы:

-   `config_name` (String) - Имя параметра, используемого для `graphite_rollup`.
-   `regexp` (String) - Шаблон имени метрики.
-   `function` (String) - Имя агрегирующей функции.
-   `age` (UInt64) - Минимальный возраст данных в секундах.
-   `precision` (UInt64) - Точность определения возраста данных в секундах.
-   `priority` (UInt16) - Приоритет раздела pattern.
-   `is_default` (UInt8) - Является ли раздел pattern дефолтным.
-   `Tables.database` (Array(String)) - Массив имён баз данных таблиц, использующих параметр `config_name`.
-   `Tables.table` (Array(String)) - Массив имён таблиц, использующих параметр `config_name`.

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/system_tables/graphite_retentions) <!--hide-->
