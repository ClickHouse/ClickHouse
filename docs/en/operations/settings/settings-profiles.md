---
sidebar_position: 61
sidebar_label: Settings Profiles
---

# Settings Profiles

A settings profile is a collection of settings grouped under the same name.

:::note
ClickHouse also supports [SQL-driven workflow](../../operations/access-rights.md#access-control) for managing settings profiles. We recommend using it.
:::

The profile can have any name. You can specify the same profile for different users. The most important thing you can write in the settings profile is `readonly=1`, which ensures read-only access.

Settings profiles can inherit from each other. To use inheritance, indicate one or multiple `profile` settings before the other settings that are listed in the profile. In case when one setting is defined in different profiles, the latest defined is used.

To apply all the settings in a profile, set the `profile` setting.

Example:

Install the `web` profile.

``` sql
SET profile = 'web'
```

Settings profiles are declared in the user config file. This is usually `users.xml`.

Example:

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

The example specifies two profiles: `default` and `web`.

The `default` profile has a special purpose: it must always be present and is applied when starting the server. In other words, the `default` profile contains default settings.

The `web` profile is a regular profile that can be set using the `SET` query or using a URL parameter in an HTTP query.

[Original article](https://clickhouse.com/docs/en/operations/settings/settings_profiles/) <!--hide-->
