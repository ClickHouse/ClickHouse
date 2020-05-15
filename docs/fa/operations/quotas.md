---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 51
toc_title: "\u0633\u0647\u0645\u06CC\u0647"
---

# سهمیه {#quotas}

سهمیه به شما اجازه محدود کردن استفاده از منابع بیش از یک دوره از زمان و یا پیگیری استفاده از منابع.
سهمیه در پیکربندی کاربر راه اندازی, که معمولا ‘users.xml’.

این سیستم همچنین دارای یک ویژگی برای محدود کردن پیچیدگی یک پرس و جو واحد. بخش را ببینید “Restrictions on query complexity”).

در مقابل به پرس و جو محدودیت پیچیدگی, سهمیه:

-   محل محدودیت در مجموعه ای از نمایش داده شد که می تواند بیش از یک دوره از زمان اجرا, به جای محدود کردن یک پرس و جو.
-   حساب برای منابع صرف شده در تمام سرور از راه دور برای پردازش پرس و جو توزیع شده است.

بیایید به بخش ‘users.xml’ فایل که سهمیه را تعریف می کند.

``` xml
<!-- Quotas -->
<quotas>
    <!-- Quota name. -->
    <default>
        <!-- Restrictions for a time period. You can set many intervals with different restrictions. -->
        <interval>
            <!-- Length of the interval. -->
            <duration>3600</duration>

            <!-- Unlimited. Just collect data for the specified time interval. -->
            <queries>0</queries>
            <errors>0</errors>
            <result_rows>0</result_rows>
            <read_rows>0</read_rows>
            <execution_time>0</execution_time>
        </interval>
    </default>
```

به طور پیش فرض, سهمیه ردیابی مصرف منابع برای هر ساعت, بدون محدود کردن استفاده.
مصرف منابع محاسبه شده برای هر فاصله خروجی به ورود به سیستم سرور بعد از هر درخواست است.

``` xml
<statbox>
    <!-- Restrictions for a time period. You can set many intervals with different restrictions. -->
    <interval>
        <!-- Length of the interval. -->
        <duration>3600</duration>

        <queries>1000</queries>
        <errors>100</errors>
        <result_rows>1000000000</result_rows>
        <read_rows>100000000000</read_rows>
        <execution_time>900</execution_time>
    </interval>

    <interval>
        <duration>86400</duration>

        <queries>10000</queries>
        <errors>1000</errors>
        <result_rows>5000000000</result_rows>
        <read_rows>500000000000</read_rows>
        <execution_time>7200</execution_time>
    </interval>
</statbox>
```

برای ‘statbox’ سهمیه, محدودیت برای هر ساعت و برای هر مجموعه 24 ساعت ها (86,400 ثانیه). فاصله زمانی شمارش شده است, با شروع از یک لحظه ثابت پیاده سازی تعریف شده در زمان. به عبارت دیگر فاصله 24 ساعته لزوما در نیمه شب شروع نمی شود.

هنگامی که فاصله به پایان می رسد تمام مقادیر جمع شده پاک می شوند. برای ساعت بعد محاسبه سهمیه بیش از شروع می شود.

در اینجا مقدار است که می تواند محدود می شود:

`queries` – The total number of requests.

`errors` – The number of queries that threw an exception.

`result_rows` – The total number of rows given as a result.

`read_rows` – The total number of source rows read from tables for running the query on all remote servers.

`execution_time` – The total query execution time, in seconds (wall time).

اگر حد برای حداقل یک فاصله زمانی بیش از, یک استثنا با یک متن که در مورد محدودیت بیش از حد شد پرتاب, که فاصله, و هنگامی که فاصله جدید شروع می شود (هنگامی که نمایش داده شد را می توان دوباره ارسال).

سهمیه می توانید استفاده کنید “quota key” ویژگی به گزارش منابع برای کلید های متعدد به طور مستقل. در اینجا یک مثال از این است:

``` xml
<!-- For the global reports designer. -->
<web_global>
    <!-- keyed – The quota_key "key" is passed in the query parameter,
            and the quota is tracked separately for each key value.
        For example, you can pass a Yandex.Metrica username as the key,
            so the quota will be counted separately for each username.
        Using keys makes sense only if quota_key is transmitted by the program, not by a user.

        You can also write <keyed_by_ip />, so the IP address is used as the quota key.
        (But keep in mind that users can change the IPv6 address fairly easily.)
    -->
    <keyed />
```

سهمیه به کاربران در اختصاص داده ‘users’ بخش پیکربندی. بخش را ببینید “Access rights”.

برای پردازش پرس و جو توزیع, مقدار انباشته شده بر روی سرور درخواست ذخیره می شود. بنابراین اگر کاربر می رود به سرور دیگر, سهمیه وجود خواهد داشت “start over”.

هنگامی که سرور دوباره راه اندازی شده است, سهمیه تنظیم مجدد.

[مقاله اصلی](https://clickhouse.tech/docs/en/operations/quotas/) <!--hide-->
