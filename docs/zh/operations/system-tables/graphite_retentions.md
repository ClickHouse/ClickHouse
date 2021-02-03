---
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
---

# 系统。graphite_retentions {#system-graphite-retentions}

包含有关参数的信息 [graphite_rollup](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-graphite) 这是在表中使用 [\*GraphiteMergeTree](../../engines/table-engines/mergetree-family/graphitemergetree.md) 引擎

列:

-   `config_name` (字符串) - `graphite_rollup` 参数名称。
-   `regexp` (String)-指标名称的模式。
-   `function` (String)-聚合函数的名称。
-   `age` (UInt64)-以秒为单位的数据的最小期限。
-   `precision` （UInt64）-如何精确地定义以秒为单位的数据的年龄。
-   `priority` (UInt16)-模式优先级。
-   `is_default` (UInt8)-模式是否为默认值。
-   `Tables.database` (Array(String))-使用数据库表名称的数组 `config_name` 参数。
-   `Tables.table` (Array(String))-使用表名称的数组 `config_name` 参数。
