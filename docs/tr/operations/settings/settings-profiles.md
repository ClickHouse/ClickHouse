---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 61
toc_title: Ayarlar Profilleri
---

# Ayarlar Profilleri {#settings-profiles}

Ayarlar profili, aynı ad altında gruplandırılmış ayarlar topluluğudur.

!!! note "Bilgi"
    ClickHouse da destekler [SQL tabanlı iş akışı](../access-rights.md#access-control) ayarları profilleri yönetmek için. Bunu kullanmanızı öneririz.

Bir profilin herhangi bir adı olabilir. Profilin herhangi bir adı olabilir. Farklı kullanıcılar için aynı profili belirtebilirsiniz. Ayarlar profilinde yazabileceğiniz en önemli şey `readonly=1` sağlar okumak-sadece erişim.

Ayarlar profilleri birbirinden miras alabilir. Kalıtım kullanmak için, bir veya birden fazla belirtiniz `profile` ayarlar profilde listelenen diğer ayarlardan önce. Farklı profillerde bir ayar tanımlandığında, en son tanımlı kullanılır.

Bir profildeki tüm ayarları uygulamak için `profile` ayar.

Örnek:

Yüklemek `web` profilli.

``` sql
SET profile = 'web'
```

Ayarlar profilleri kullanıcı yapılandırma dosyasında bildirilir. Bu genellikle `users.xml`.

Örnek:

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

Örnek iki profili belirtir: `default` ve `web`.

Bu `default` profilin özel bir amacı vardır: her zaman mevcut olmalı ve sunucuyu başlatırken uygulanır. Diğer bir deyişle, `default` profil varsayılan ayarları içerir.

Bu `web` profil kullanılarak ayarlanabilir düzenli bir profil `SET` sorgu veya bir HTTP sorgusunda bir URL parametresi kullanma.

[Orijinal makale](https://clickhouse.tech/docs/en/operations/settings/settings_profiles/) <!--hide-->
