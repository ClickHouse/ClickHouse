# system.graphite_retentions {#system-graphite-retentions}

Contains information about parameters [graphite_rollup](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-graphite) which are used in tables with [\*GraphiteMergeTree](../../engines/table-engines/mergetree-family/graphitemergetree.md) engines.

Columns:

-   `config_name` (String) - `graphite_rollup` parameter name.
-   `regexp` (String) - A pattern for the metric name.
-   `function` (String) - The name of the aggregating function.
-   `age` (UInt64) - The minimum age of the data in seconds.
-   `precision` (UInt64) - How precisely to define the age of the data in seconds.
-   `priority` (UInt16) - Pattern priority.
-   `is_default` (UInt8) - Whether the pattern is the default.
-   `Tables.database` (Array(String)) - Array of names of database tables that use the `config_name` parameter.
-   `Tables.table` (Array(String)) - Array of table names that use the `config_name` parameter.

[Original article](https://clickhouse.tech/docs/en/operations/system_tables/graphite_retentions) <!--hide-->
