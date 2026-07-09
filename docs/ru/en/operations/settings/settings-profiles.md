---
description: 'Набор настроек, сгруппированных под одним именем.'
sidebar_label: 'Профили настроек'
sidebar_position: 61
slug: /operations/settings/settings-profiles
title: 'Профили настроек'
doc_type: 'reference'
---

Профиль настроек — это набор настроек, сгруппированных под одним именем.

:::note
ClickHouse также поддерживает [workflow, управляемый SQL](/ru/operations/access-rights#access-control-usage) для управления профилями настроек. Мы рекомендуем использовать именно его.
:::

Профиль может иметь любое имя. Один и тот же профиль можно назначить разным пользователям. Самое важное, что можно задать в профиле настроек, — это `readonly=1`, что обеспечивает доступ только для чтения.

Профили настроек могут наследоваться друг от друга. Чтобы использовать наследование, укажите одну или несколько настроек `profile` перед остальными настройками, перечисленными в профиле. Если одна и та же настройка определена в разных профилях, используется последнее заданное значение.

Чтобы применить все настройки профиля, задайте настройку `profile`.

Пример:

Установите профиль `web`.

```sql
SET profile = 'web'
```

Профили настроек задаются в пользовательском файле конфигурации. Обычно это `users.xml`.

Пример:

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

В примере указаны два профиля: `default` и `web`.

Профиль `default` имеет особое назначение: он должен присутствовать всегда и применяется при запуске сервера. Иными словами, профиль `default` содержит настройки по умолчанию. Имя профиля по умолчанию можно изменить с помощью настройки сервера `default_profile`.

Профиль `background` имеет особое назначение: он может использоваться для переопределения настроек фоновых операций. Этот параметр необязателен, и его имя можно изменить с помощью настройки сервера `background_profile`.

Профиль `web` — это обычный профиль, который можно задать с помощью запроса `SET` или параметра URL в HTTP-запросе.