# system.graphite_retentions {#system-graphite-retentions}

包含有关参数 [graphite_rollup](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-graphite)的信息， 该参数在[\*GraphiteMergeTree](../../engines/table-engines/mergetree-family/graphitemergetree.md) 引擎表中使用。

列:

-   `config_name` (String) - `graphite_rollup` 参数名称。
-   `regexp` (String)- 指标名称的正则表达式。
-   `function` (String)- 聚合函数的名称。
-   `age` (UInt64)- 数据的最小年龄（以秒为单位）。
-   `precision` （UInt64）- 如何精确地定义数据的年龄（以秒为单位）。
-   `priority` (UInt16)- 模式优先级。
-   `is_default` (UInt8)- 是否为默认模式。
-   `Tables.database` (Array(String))- 使用该 `config_name` 参数的数据库名的数组 。
-   `Tables.table` (Array(String))- 使用该 `config_name` 参数的表名的数组。

[原始文章](https://clickhouse.tech/docs/en/operations/system_tables/graphite_retentions) <!--hide-->
