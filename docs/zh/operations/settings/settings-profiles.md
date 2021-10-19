---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 61
toc_title: "\u8BBE\u7F6E\u914D\u7F6E\u6587\u4EF6"
---

# 设置配置文件 {#settings-profiles}

设置配置文件是以相同名称分组的设置的集合。

!!! note "信息"
    ClickHouse还支持 [SQL驱动的工作流](../access-rights.md#access-control) 用于管理设置配置文件。 我们建议使用它。

配置文件可以有任何名称。 配置文件可以有任何名称。 您可以为不同的用户指定相同的配置文件。 您可以在设置配置文件中编写的最重要的事情是 `readonly=1`，这确保只读访问。

设置配置文件可以彼此继承。 要使用继承，请指示一个或多个 `profile` 配置文件中列出的其他设置之前的设置。 如果在不同的配置文件中定义了一个设置，则使用最新定义。

要应用配置文件中的所有设置，请设置 `profile` 设置。

示例:

安装 `web` 侧写

``` sql
SET profile = 'web'
```

设置配置文件在用户配置文件中声明。 这通常是 `users.xml`.

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

该示例指定了两个配置文件: `default` 和 `web`.

该 `default` 配置文件有一个特殊用途：它必须始终存在并在启动服务器时应用。 换句话说， `default` 配置文件包含默认设置。

该 `web` 配置文件是一个常规的配置文件，可以使用设置 `SET` 查询或在HTTP查询中使用URL参数。

[原始文章](https://clickhouse.com/docs/en/operations/settings/settings_profiles/) <!--hide-->
