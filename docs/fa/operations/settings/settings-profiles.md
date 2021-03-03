---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 61
toc_title: "\u067E\u0631\u0648\u0641\u0627\u06CC\u0644 \u062A\u0646\u0638\u06CC\u0645\
  \u0627\u062A"
---

# پروفایل تنظیمات {#settings-profiles}

مشخصات تنظیمات مجموعه ای از تنظیمات گروه بندی شده تحت همین نام است.

!!! note "اطلاعات"
    فاحشه خانه نیز پشتیبانی می کند [گردش کار مبتنی بر مربع](../access-rights.md#access-control) برای مدیریت پروفایل تنظیمات. ما توصیه می کنیم از این استفاده کنید.

مشخصات می توانید هر نام دارند. مشخصات می توانید هر نام دارند. شما می توانید مشخصات مشابه برای کاربران مختلف را مشخص کنید. مهم ترین چیز شما می توانید در مشخصات تنظیمات ارسال شده است `readonly=1`, که تضمین می کند فقط خواندنی دسترسی.

پروفایل تنظیمات می توانید از یکدیگر به ارث می برند. برای استفاده از ارث, نشان می دهد یک یا چند `profile` تنظیمات قبل از تنظیمات دیگر که در مشخصات ذکر شده. در صورتی که یک تنظیم در پروفایل های مختلف تعریف شده, از تعریف استفاده شده است.

برای اعمال تمام تنظیمات در یک پروفایل, تنظیم `profile` تنظیمات.

مثال:

نصب `web` پرونده.

``` sql
SET profile = 'web'
```

پروفایل تنظیمات در فایل پیکربندی کاربر اعلام کرد. این است که معمولا `users.xml`.

مثال:

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

به عنوان مثال دو پروفایل مشخص: `default` و `web`.

این `default` مشخصات دارای یک هدف خاص: همیشه باید وجود داشته باشد و در هنگام شروع سرور اعمال می شود. به عبارت دیگر `default` مشخصات شامل تنظیمات پیش فرض.

این `web` مشخصات یک پروفایل به طور منظم است که می تواند با استفاده از مجموعه است `SET` پرسوجو یا استفاده از یک پارامتر نشانی وب در پرسوجو اچتیتیپی.

[مقاله اصلی](https://clickhouse.tech/docs/en/operations/settings/settings_profiles/) <!--hide-->
