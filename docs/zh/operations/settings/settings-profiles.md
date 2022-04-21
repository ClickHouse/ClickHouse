---
sidebar_position: 61
sidebar_label: "\u8BBE\u7F6E\u914D\u7F6E"
---

# 设置配置 {#settings-profiles}

设置配置是设置的集合，并按照相同的名称进行分组。

!!! note "信息"
    ClickHouse 还支持用 [SQL驱动的工作流](../../operations/access-rights.md#access-control) 管理设置配置。我们建议使用它。

设置配置可以任意命名。你可以为不同的用户指定相同的设置配置。您可以在设置配置中写入的最重要的内容是 `readonly=1`，这将确保只读访问。

设置配置可以彼此继承。要使用继承，请在文件中列举的其他设置之前，指定一个或多个 `profile` 设置。如果在不同的设置配置中定义了同一个设置，则使用最新的定义。

要应用设置配置中的所有设置，请设定 `profile` 设置。

示例:

添加 `web` 配置。

``` sql
SET profile = 'web'
```

设置配置在用户配置文件中声明。这通常是指 `users.xml`.

示例:

``` xml
<!-- Settings profiles -->
<profiles>
    <!-- Default settings -->
    <default>
        <!-- The maximum number of threads when running a single query. -->
        <max_threads>8</max_threads>
    </default>

    <!-- Settings for quries from the user interface -->
    <web>
        <max_rows_to_read>1000000000</max_rows_to_read>
        <max_bytes_to_read>100000000000</max_bytes_to_read>

        <max_rows_to_group_by>1000000</max_rows_to_group_by>
        <group_by_overflow_mode>any</group_by_overflow_mode>

        <max_rows_to_sort>1000000</max_rows_to_sort>
        <max_bytes_to_sort>1000000000</max_bytes_to_sort>

        <max_result_rows>100000</max_result_rows>
        <max_result_bytes>100000000</max_result_bytes>
        <result_overflow_mode>break</result_overflow_mode>

        <max_execution_time>600</max_execution_time>
        <min_execution_speed>1000000</min_execution_speed>
        <timeout_before_checking_execution_speed>15</timeout_before_checking_execution_speed>

        <max_columns_to_read>25</max_columns_to_read>
        <max_temporary_columns>100</max_temporary_columns>
        <max_temporary_non_const_columns>50</max_temporary_non_const_columns>

        <max_subquery_depth>2</max_subquery_depth>
        <max_pipeline_depth>25</max_pipeline_depth>
        <max_ast_depth>50</max_ast_depth>
        <max_ast_elements>100</max_ast_elements>

        <readonly>1</readonly>
    </web>
</profiles>
```

这个示例指定了两个配置： `default` 和 `web` 。

这个 `default` 配置有一个特殊用途：它必须始终存在并在启动服务时应用。换句话说， `default` 配置包含默认设置。

`web` 配置是一个常规的配置，它可以通过 `SET` 查询进行设定，也可以通过在HTTP查询中使用URL参数进行设定。

[原始文章](https://clickhouse.com/docs/en/operations/settings/settings_profiles/) <!--hide-->
