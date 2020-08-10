---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 39
toc_title: "\u062A\u0648\u0636\u06CC\u062D\u0627\u062A \u06A9\u0644\u06CC"
---

# واژهنامهها خارجی {#dicts-external-dicts}

شما می توانید لغت نامه خود را از منابع داده های مختلف اضافه کنید. منبع داده برای یک فرهنگ لغت می تواند یک متن محلی و یا فایل اجرایی, یک منبع اچتیتیپی(بازدید کنندگان), یا سندرم داون دیگر. برای کسب اطلاعات بیشتر, دیدن “[منابع لغت نامه های خارجی](external_dicts_dict_sources.md)”.

فاحشه خانه:

-   به طور کامل و یا تا حدی فروشگاه لغت نامه در رم.
-   دوره به روز رسانی لغت نامه ها و به صورت پویا بارهای ارزش از دست رفته. به عبارت دیگر, لغت نامه را می توان به صورت پویا لود.
-   اجازه می دهد تا برای ایجاد لغت نامه های خارجی با فایل های میلی لیتر و یا [نمایش داده شد](../../statements/create.md#create-dictionary-query).

پیکربندی لغت نامه های خارجی را می توان در یک یا چند میلی لیتر فایل واقع شده است. مسیر پیکربندی در مشخص [دیکشنامهای](../../../operations/server_configuration_parameters/settings.md#server_configuration_parameters-dictionaries_config) پارامتر.

واژهنامهها را می توان در هنگام راه اندازی سرور و یا در اولین استفاده لود, بسته به [\_بارگیری کامل](../../../operations/server_configuration_parameters/settings.md#server_configuration_parameters-dictionaries_lazy_load) تنظیمات.

فایل پیکربندی فرهنگ لغت دارای فرمت زیر است:

``` xml
<yandex>
    <comment>An optional element with any content. Ignored by the ClickHouse server.</comment>

    <!--Optional element. File name with substitutions-->
    <include_from>/etc/metrika.xml</include_from>


    <dictionary>
        <!-- Dictionary configuration. -->
        <!-- There can be any number of <dictionary> sections in the configuration file. -->
    </dictionary>

</yandex>
```

شما می توانید [پیکربندی](external_dicts_dict.md) هر تعداد از لغت نامه ها در همان فایل.

[نمایش داده شد دی ال برای لغت نامه](../../statements/create.md#create-dictionary-query) هیچ پرونده اضافی در پیکربندی سرور نیاز ندارد. اجازه می دهد برای کار با لغت نامه به عنوان نهادهای طبقه اول, مانند جداول و یا دیدگاه.

!!! attention "توجه"
    شما می توانید مقادیر را برای یک فرهنگ لغت کوچک با توصیف در یک تبدیل کنید `SELECT` پرسوجو (نگاه کنید به [تبدیل](../../../sql_reference/functions/other_functions.md) تابع). این قابلیت به لغت نامه های خارجی مربوط نیست.

## همچنین نگاه کنید به {#ext-dicts-see-also}

-   [پیکربندی یک فرهنگ لغت خارجی](external_dicts_dict.md)
-   [ذخیره واژهنامهها در حافظه](external_dicts_dict_layout.md)
-   [به روز رسانی فرهنگ لغت](external_dicts_dict_lifetime.md)
-   [منابع لغت نامه های خارجی](external_dicts_dict_sources.md)
-   [کلید فرهنگ لغت و زمینه های](external_dicts_dict_structure.md)
-   [توابع برای کار با لغت نامه های خارجی](../../../sql_reference/functions/ext_dict_functions.md)

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts/) <!--hide-->
