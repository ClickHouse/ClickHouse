---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 63
toc_title: "\u062A\u0646\u0638\u06CC\u0645\u0627\u062A \u06A9\u0627\u0631\u0628\u0631"
---

# تنظیمات کاربر {#user-settings}

این `users` بخش از `user.xml` فایل پیکربندی شامل تنظیمات کاربر.

!!! note "اطلاعات"
    فاحشه خانه نیز پشتیبانی می کند [گردش کار مبتنی بر مربع](../access-rights.md#access-control) برای مدیریت کاربران. ما توصیه می کنیم از این استفاده کنید.

ساختار `users` بخش:

``` xml
<users>
    <!-- If user name was not specified, 'default' user is used. -->
    <user_name>
        <password></password>
        <!-- Or -->
        <password_sha256_hex></password_sha256_hex>

        <access_management>0|1</access_management>

        <networks incl="networks" replace="replace">
        </networks>

        <profile>profile_name</profile>

        <quota>default</quota>

        <databases>
            <database_name>
                <table_name>
                    <filter>expression</filter>
                <table_name>
            </database_name>
        </databases>
    </user_name>
    <!-- Other users settings -->
</users>
```

### نام / رمز عبور {#user-namepassword}

رمز عبور را می توان در متن یا در شی256 (فرمت سحر و جادو) مشخص شده است.

-   برای اختصاص دادن رمز عبور به متن (**توصیه نمی شود**), جای خود را در یک `password` عنصر.

    به عنوان مثال, `<password>qwerty</password>`. رمز عبور را می توان خالی گذاشت.

<a id="password_sha256_hex"></a>

-   برای اختصاص دادن رمز عبور با استفاده از هش ش256 در یک `password_sha256_hex` عنصر.

    به عنوان مثال, `<password_sha256_hex>65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5</password_sha256_hex>`.

    نمونه ای از نحوه تولید رمز عبور از پوسته:

          PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha256sum | tr -d '-'

    خط اول نتیجه رمز عبور است. خط دوم مربوط به هش ش256 است.

<a id="password_double_sha1_hex"></a>

-   برای سازگاری با مشتریان خروجی زیر, رمز عبور را می توان در دو شی1 هش مشخص. محل را در `password_double_sha1_hex` عنصر.

    به عنوان مثال, `<password_double_sha1_hex>08b4a0f1de6ad37da17359e592c8d74788a83eb0</password_double_sha1_hex>`.

    نمونه ای از نحوه تولید رمز عبور از پوسته:

          PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha1sum | tr -d '-' | xxd -r -p | sha1sum | tr -d '-'

    خط اول نتیجه رمز عبور است. خط دوم مربوط به هش دو شی1 است.

### مدیریت دسترسی {#access_management-user-setting}

این تنظیم را قادر می سازد از غیر فعال با استفاده از گذاشتن محور [کنترل دسترسی و مدیریت حساب](../access-rights.md#access-control) برای کاربر.

مقادیر ممکن:

-   0 — Disabled.
-   1 — Enabled.

مقدار پیش فرض: 0.

### نام / شبکه {#user-namenetworks}

لیست شبکه هایی که کاربر می تواند به سرور کلیک متصل شود.

هر عنصر از لیست می توانید یکی از اشکال زیر را داشته باشد:

-   `<ip>` — IP address or network mask.

    مثالها: `213.180.204.3`, `10.0.0.1/8`, `10.0.0.1/255.255.255.0`, `2a02:6b8::3`, `2a02:6b8::3/64`, `2a02:6b8::3/ffff:ffff:ffff:ffff::`.

-   `<host>` — Hostname.

    مثال: `example01.host.ru`.

    برای بررسی دسترسی یک پرسوجوی دی ان اس انجام میشود و تمامی نشانیهای اینترنتی برگشتی با نشانی همکار مقایسه میشوند.

-   `<host_regexp>` — Regular expression for hostnames.

    مثال, `^example\d\d-\d\d-\d\.host\.ru$`

    برای بررسی دسترسی [جستجو](https://en.wikipedia.org/wiki/Reverse_DNS_lookup) برای نشانی همکار انجام می شود و سپس عبارت منظم مشخص شده اعمال می شود. سپس پرس و جو دی ان اس دیگری برای نتایج پرس و جو انجام می شود و تمامی نشانیهای دریافتی با نشانی همکار مقایسه می شوند. ما قویا توصیه می کنیم که عبارت منظم به پایان می رسد با$.

تمام نتایج درخواست دی ان اس ذخیره سازی تا زمانی که سرور راه اندازی مجدد.

**مثالها**

برای باز کردن دسترسی برای کاربر از هر شبکه مشخص کنید:

``` xml
<ip>::/0</ip>
```

!!! warning "اخطار"
    این نا امن برای باز کردن دسترسی از هر شبکه مگر اینکه شما یک فایروال به درستی پیکربندی و یا سرور به طور مستقیم به اینترنت متصل نیست.

برای باز کردن دسترسی فقط از جایل هاست مشخص کنید:

``` xml
<ip>::1</ip>
<ip>127.0.0.1</ip>
```

### نام / پروفایل {#user-nameprofile}

شما می توانید یک پروفایل تنظیمات برای کاربر اختصاص دهید. پروفایل های تنظیمات در یک بخش جداگانه از پیکربندی `users.xml` پرونده. برای کسب اطلاعات بیشتر, دیدن [پروفایل تنظیمات](settings-profiles.md).

### نام / سهمیه {#user-namequota}

سهمیه اجازه می دهد شما را به پیگیری و یا محدود کردن استفاده از منابع بیش از یک دوره از زمان. سهمیه در پیکربندی `quotas`
بخش از `users.xml` فایل پیکربندی.

شما می توانید یک سهمیه تعیین شده برای کاربر اختصاص. برای شرح مفصلی از پیکربندی سهمیه, دیدن [سهمیه](../quotas.md#quotas).

### نام/پایگاه های داده {#user-namedatabases}

در این بخش می توانید ردیف هایی را که توسط کلیک برای بازگشت هستند محدود کنید `SELECT` نمایش داده شد ساخته شده توسط کاربر فعلی, در نتیجه اجرای امنیت سطح ردیف پایه.

**مثال**

پیکربندی زیر نیروهای که کاربر `user1` فقط می توانید ردیف ها را ببینید `table1` به عنوان نتیجه `SELECT` نمایش داده شد که ارزش `id` درست است 1000.

``` xml
<user1>
    <databases>
        <database_name>
            <table1>
                <filter>id = 1000</filter>
            </table1>
        </database_name>
    </databases>
</user1>
```

این `filter` می تواند هر بیان و در نتیجه [UInt8](../../sql-reference/data-types/int-uint.md)- نوع ارزش. این حالت معمولا شامل مقایسه و اپراتورهای منطقی. سطرها از `database_name.table1` از کجا نتایج فیلتر به 0 برای این کاربر بازگشت نیست. فیلتر کردن با ناسازگار است `PREWHERE` عملیات و معلولین `WHERE→PREWHERE` بهینهسازی.

[مقاله اصلی](https://clickhouse.tech/docs/en/operations/settings/settings_users/) <!--hide-->
