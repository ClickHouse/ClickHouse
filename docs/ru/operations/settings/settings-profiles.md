---
toc_priority: 61
toc_title: "\u041f\u0440\u043e\u0444\u0438\u043b\u0438\u0020\u043d\u0430\u0441\u0442\u0440\u043e\u0435\u043a"
---

# Профили настроек {#settings-profiles}

Профиль настроек — это набор настроек, сгруппированных под одним именем.

!!! note "Информация"
    Для управления профилями настроек рекомендуется использовать [SQL-ориентированный воркфлоу](../access-rights.md#access-control), который также поддерживается в ClickHouse.


Название профиля может быть любым. Вы можете указать один и тот же профиль для разных пользователей. Самое важное, что можно прописать в профиле — `readonly=1`, это обеспечит доступ только на чтение.

Профили настроек поддерживают наследование. Это реализуется указанием одной или нескольких настроек `profile` перед остальными настройками, перечисленными в профиле. Если одна настройка указана в нескольких профилях, используется последнее из значений.

Все настройки профиля можно применить, установив настройку `profile`.

Пример:

Установить профиль `web`.

``` sql
SET profile = 'web'
```

Профили настроек объявляются в конфигурационном файле пользователей. Обычно это `users.xml`.

Пример:

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

В примере задано два профиля: `default` и `web`. 

Профиль `default` имеет специальное значение — он обязателен и применяется при запуске сервера. Профиль `default` содержит настройки по умолчанию. 

Профиль `web` — обычный профиль, который может быть установлен с помощью запроса `SET` или параметра URL при запросе по HTTP.

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/settings/settings_profiles/) <!--hide-->
