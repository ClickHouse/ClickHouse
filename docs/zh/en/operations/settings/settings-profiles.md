---
description: '归在同一名称下的一组设置。'
sidebar_label: '设置 profile'
sidebar_position: 61
slug: /operations/settings/settings-profiles
title: '设置 profile'
doc_type: 'reference'
---

设置 profile 是归在同一名称下的一组设置。

:::note
ClickHouse 也支持通过 [SQL 驱动的工作流](/zh/operations/access-rights#access-control-usage) 管理设置 profile。建议使用这种方式。
:::

profile 可以使用任意名称。你可以为不同用户指定同一个 profile。在设置 profile 中，最重要的一项是 `readonly=1`，它可确保只读访问。

设置 profile 之间可以相互继承。要使用继承，请先指定一个或多个 `profile` 设置，再指定该 profile 中列出的其他设置。如果同一设置在不同的 profile 中都有定义，则以最后定义的值为准。

要应用某个 profile 中的全部设置，请设置 `profile`。

示例：

安装 `web` profile。

```sql
SET profile = 'web'
```

profile 在用户配置文件中定义，通常是 `users.xml`。

示例：

```xml
<!-- Settings profiles -->
<profiles>
    <!-- Default settings -->
    <default>
        <!-- The maximum number of threads when running a single query. -->
        <max_threads>8</max_threads>
    </default>

    <!-- Background operations settings -->
    <background>
        <!-- Re-defining maximum number of threads for background operations -->
        <max_threads>12</max_threads>
    </background>

    <!-- Settings for queries from the user interface -->
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

        <max_sessions_for_user>4</max_sessions_for_user>

        <readonly>1</readonly>
    </web>
</profiles>
```

该示例指定了两个 profile：`default` 和 `web`。

`default` profile 有特殊用途：它必须始终存在，并在 server 启动时应用。换句话说，`default` profile 包含默认设置。默认 profile 的名称可以通过 `default_profile` server setting 更改。

`background` profile 也有特殊用途：它可以存在，用于覆盖后台操作的设置。该 parameter 是可选的，其名称可以通过 `background_profile` server setting 更改。

`web` profile 是一个常规 profile，可以使用 `SET` 查询来设置，也可以在 HTTP 查询中通过 URL parameter 设置。