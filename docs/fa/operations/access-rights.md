---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 48
toc_title: "\u062D\u0642\u0648\u0642 \u062F\u0633\u062A\u0631\u0633\u06CC"
---

# حقوق دسترسی {#access-rights}

کاربران و حقوق دسترسی هستند تا در پیکربندی کاربر تنظیم شده است. این است که معمولا `users.xml`.

کاربران در ثبت `users` بخش. در اینجا یک قطعه از است `users.xml` پرونده:

``` xml
<!-- Users and ACL. -->
<users>
    <!-- If the user name is not specified, the 'default' user is used. -->
    <default>
        <!-- Password could be specified in plaintext or in SHA256 (in hex format).

             If you want to specify password in plaintext (not recommended), place it in 'password' element.
             Example: <password>qwerty</password>.
             Password could be empty.

             If you want to specify SHA256, place it in 'password_sha256_hex' element.
             Example: <password_sha256_hex>65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5</password_sha256_hex>

             How to generate decent password:
             Execute: PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha256sum | tr -d '-'
             In first line will be password and in second - corresponding SHA256.
        -->
        <password></password>

        <!-- A list of networks that access is allowed from.
            Each list item has one of the following forms:
            <ip> The IP address or subnet mask. For example: 198.51.100.0/24 or 2001:DB8::/32.
            <host> Host name. For example: example01. A DNS query is made for verification, and all addresses obtained are compared with the address of the customer.
            <host_regexp> Regular expression for host names. For example, ^example\d\d-\d\d-\d\.host\.ru$
                To check it, a DNS PTR request is made for the client's address and a regular expression is applied to the result.
                Then another DNS query is made for the result of the PTR query, and all received address are compared to the client address.
                We strongly recommend that the regex ends with \.host\.ru$.

            If you are installing ClickHouse yourself, specify here:
                <networks>
                        <ip>::/0</ip>
                </networks>
        -->
        <networks incl="networks" />

        <!-- Settings profile for the user. -->
        <profile>default</profile>

        <!-- Quota for the user. -->
        <quota>default</quota>
    </default>

    <!-- For requests from the Yandex.Metrica user interface via the API for data on specific counters. -->
    <web>
        <password></password>
        <networks incl="networks" />
        <profile>web</profile>
        <quota>default</quota>
        <allow_databases>
           <database>test</database>
        </allow_databases>
        <allow_dictionaries>
           <dictionary>test</dictionary>
        </allow_dictionaries>
    </web>
</users>
```

شما می توانید اعلامیه ای از دو کاربر را ببینید: `default`و`web`. ما اضافه کردیم `web` کاربر به طور جداگانه.

این `default` کاربر در مواردی که نام کاربری تصویب نشده است انتخاب شده است. این `default` کاربر همچنین برای پردازش پرس و جو توزیع شده استفاده می شود, اگر پیکربندی سرور یا خوشه می کند مشخص نیست `user` و `password` (نگاه کنید به بخش در [توزیع شده](../engines/table-engines/special/distributed.md) موتور).

The user that is used for exchanging information between servers combined in a cluster must not have substantial restrictions or quotas – otherwise, distributed queries will fail.

رمز عبور در متن روشن مشخص (توصیه نمی شود) و یا در شا 256. هش شور نیست. در این راستا نباید این رمزهای عبور را به عنوان امنیت در برابر حملات مخرب بالقوه در نظر بگیرید. بلکه لازم است برای حفاظت از کارکنان.

یک لیست از شبکه مشخص شده است که دسترسی از اجازه. در این مثال لیستی از شبکه ها برای هر دو کاربران لود شده از یک فایل جداگانه (`/etc/metrika.xml`) حاوی `networks` جایگزینی. در اینجا یک قطعه است:

``` xml
<yandex>
    ...
    <networks>
        <ip>::/64</ip>
        <ip>203.0.113.0/24</ip>
        <ip>2001:DB8::/32</ip>
        ...
    </networks>
</yandex>
```

شما می توانید این لیست از شبکه به طور مستقیم در تعریف `users.xml` یا در یک فایل در `users.d` فهرست راهنما (برای اطلاعات بیشتر, بخش را ببینید “[پروندههای پیکربندی](configuration-files.md#configuration_files)”).

پیکربندی شامل نظرات توضیح میدهد که چگونه برای باز کردن دسترسی از همه جا.

برای استفاده در تولید فقط مشخص کنید `ip` عناصر (نشانی اینترنتی و ماسک خود را), از زمان استفاده از `host` و `hoost_regexp` ممکن است تاخیر اضافی شود.

بعد مشخصات تنظیمات کاربر مشخص شده است (بخش را ببینید “[پروفایل تنظیمات](settings/settings-profiles.md)”. شما می توانید مشخصات پیش فرض را مشخص کنید, `default'`. مشخصات می توانید هر نام دارند. شما می توانید مشخصات مشابه برای کاربران مختلف را مشخص کنید. مهم ترین چیز شما می توانید در مشخصات تنظیمات ارسال شده است `readonly=1`, که تضمین می کند فقط خواندنی دسترسی. سپس سهمیه مشخص مورد استفاده قرار گیرد (بخش را ببینید “[سهمیه](quotas.md#quotas)”). شما می توانید سهمیه پیش فرض را مشخص کنید: `default`. It is set in the config by default to only count resource usage, without restricting it. The quota can have any name. You can specify the same quota for different users – in this case, resource usage is calculated for each user individually.

در اختیاری `<allow_databases>` بخش, شما همچنین می توانید یک لیست از پایگاه داده که کاربر می تواند دسترسی مشخص. به طور پیش فرض تمام پایگاه های داده در دسترس کاربر هستند. شما می توانید مشخص کنید `default` پایگاه داده است. در این مورد, کاربر دسترسی به پایگاه داده به طور پیش فرض دریافت.

در اختیاری `<allow_dictionaries>` بخش, شما همچنین می توانید یک لیست از لغت نامه که کاربر می تواند دسترسی مشخص. به طور پیش فرض تمام لغت نامه ها برای کاربر در دسترس هستند.

دسترسی به `system` پایگاه داده همیشه مجاز (از این پایگاه داده برای پردازش نمایش داده شد استفاده می شود).

کاربر می تواند لیستی از تمام پایگاه های داده و جداول را با استفاده از `SHOW` نمایش داده شد و یا جداول سیستم, حتی اگر دسترسی به پایگاه داده های فردی مجاز نیست.

دسترسی به پایگاه داده به [فقط خواندنی](settings/permissions-for-queries.md#settings_readonly) تنظیمات. شما نمی توانید دسترسی کامل به یک پایگاه داده و `readonly` دسترسی به یکی دیگر.

[مقاله اصلی](https://clickhouse.tech/docs/en/operations/access_rights/) <!--hide-->
