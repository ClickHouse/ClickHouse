---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 48
toc_title: "\u06A9\u0646\u062A\u0631\u0644 \u062F\u0633\u062A\u0631\u0633\u06CC \u0648\
  \ \u0645\u062F\u06CC\u0631\u06CC\u062A \u062D\u0633\u0627\u0628"
---

# کنترل دسترسی و مدیریت حساب {#access-control}

تاتر از مدیریت کنترل دسترسی بر اساس [RBAC](https://en.wikipedia.org/wiki/Role-based_access_control) نزدیک شو

نهادهای دسترسی کلیک کنید:
- [حساب کاربری](#user-account-management)
- [نقش](#role-management)
- [سیاست سطر](#row-policy-management)
- [تنظیمات](#settings-profiles-management)
- [سهمیه](#quotas-management)

شما می توانید اشخاص دسترسی با استفاده از پیکربندی کنید:

-   گردش کار گذاشتن رانده.

    شما نیاز به [فعالسازی](#enabling-access-control) این قابلیت.

-   کارگزار [پروندههای پیکربندی](configuration-files.md) `users.xml` و `config.xml`.

ما توصیه می کنیم با استفاده از گردش کار گذاشتن محور. هر دو روش پیکربندی به طور همزمان کار, بنابراین اگر شما با استفاده از فایل های پیکربندی سرور برای مدیریت حساب و حقوق دسترسی, شما به نرمی می توانید به گردش کار گذاشتن محور حرکت.

!!! note "اخطار"
    شما می توانید نهاد دسترسی مشابه توسط هر دو روش پیکربندی به طور همزمان مدیریت نیست.

## استفاده {#access-control-usage}

به طور پیش فرض سرور کلیک حساب کاربر را فراهم می کند `default` که مجاز نیست با استفاده از کنترل دسترسی گذاشتن محور و مدیریت حساب اما تمام حقوق و مجوز. این `default` حساب کاربری است که در هر مورد استفاده می شود زمانی که نام کاربری تعریف نشده است, مثلا, در ورود از مشتری و یا در نمایش داده شد توزیع. در پرس و جو توزیع پردازش یک حساب کاربری پیش فرض استفاده شده است, اگر پیکربندی سرور یا خوشه مشخص نیست [کاربر و رمز عبور](../engines/table-engines/special/distributed.md) خواص.

اگر شما فقط شروع به استفاده از تاتر, شما می توانید سناریوی زیر استفاده کنید:

1.  [فعالسازی](#enabling-access-control) کنترل دسترسی مبتنی بر مربع و مدیریت حساب برای `default` کاربر.
2.  ورود زیر `default` حساب کاربری و ایجاد تمام کاربران مورد نیاز است. فراموش نکنید که برای ایجاد یک حساب کاربری مدیر (`GRANT ALL ON *.* WITH GRANT OPTION TO admin_user_account`).
3.  [محدود کردن مجوزها](settings/permissions-for-queries.md#permissions_for_queries) برای `default` کاربر و غیر فعال کردن کنترل دسترسی مبتنی بر مربع و مدیریت حساب.

### خواص راه حل فعلی {#access-control-properties}

-   شما می توانید مجوز برای پایگاه داده ها و جداول اعطای حتی در صورتی که وجود ندارد.
-   اگر یک جدول حذف شد, تمام امتیازات که به این جدول مطابقت لغو نمی. بنابراین, اگر یک جدول جدید بعد با همین نام ایجاد شده است تمام امتیازات تبدیل دوباره واقعی. برای لغو امتیازات مربوط به جدول حذف شده, شما نیاز به انجام, مثلا, `REVOKE ALL PRIVILEGES ON db.table FROM ALL` پرس و جو.
-   هیچ تنظیمات طول عمر برای امتیازات وجود دارد.

## حساب کاربری {#user-account-management}

یک حساب کاربری یک نهاد دسترسی است که اجازه می دهد تا به اجازه کسی در خانه کلیک است. یک حساب کاربری شامل:

-   اطلاعات شناسایی.
-   [امتیازات](../sql-reference/statements/grant.md#grant-privileges) که تعریف دامنه نمایش داده شد کاربر می تواند انجام دهد.
-   میزبان که از اتصال به سرور کلیک مجاز است.
-   نقش اعطا شده و به طور پیش فرض.
-   تنظیمات با محدودیت های خود را که به طور پیش فرض در ورود کاربر اعمال می شود.
-   اختصاص داده پروفایل تنظیمات.

امتیازات به یک حساب کاربری را می توان با اعطا [GRANT](../sql-reference/statements/grant.md) پرس و جو و یا با اختصاص [نقش ها](#role-management). برای لغو امتیازات از یک کاربر, تاتر فراهم می کند [REVOKE](../sql-reference/statements/revoke.md) پرس و جو. به لیست امتیازات برای یک کاربر, استفاده از - [SHOW GRANTS](../sql-reference/statements/show.md#show-grants-statement) بیانیه.

نمایش داده شد مدیریت:

-   [CREATE USER](../sql-reference/statements/create.md#create-user-statement)
-   [ALTER USER](../sql-reference/statements/alter.md#alter-user-statement)
-   [DROP USER](../sql-reference/statements/misc.md#drop-user-statement)
-   [SHOW CREATE USER](../sql-reference/statements/show.md#show-create-user-statement)

### تنظیمات استفاده {#access-control-settings-applying}

تنظیمات را می توان با روش های مختلف تنظیم: برای یک حساب کاربری, در نقش اعطا و تنظیمات پروفایل خود را. در ورود کاربر, اگر یک محیط در اشخاص دسترسی های مختلف مجموعه, ارزش و محدودیتهای این تنظیم توسط اولویت های زیر اعمال می شود (از بالاتر به پایین تر):

1.  تنظیمات حساب کاربری.
2.  تنظیمات نقش های پیش فرض حساب کاربری. اگر یک محیط در برخی از نقش ها تنظیم شده است, سپس سفارش از تنظیم استفاده تعریف نشده است.
3.  تنظیمات در پروفایل تنظیمات اختصاص داده شده به یک کاربر و یا به نقش پیش فرض خود را. اگر یک محیط در برخی از پروفیل های مجموعه, سپس منظور از تنظیم استفاده از تعریف نشده است.
4.  تنظیمات به طور پیش فرض به تمام سرور و یا از اعمال [نمایه پیشفرض](server-configuration-parameters/settings.md#default-profile).

## نقش {#role-management}

نقش یک ظرف برای اشخاص دسترسی است که می تواند به یک حساب کاربری اعطا شده است.

نقش شامل:

-   [امتیازات](../sql-reference/statements/grant.md#grant-privileges)
-   تنظیمات و محدودیت ها
-   فهرست نقش های اعطا شده

نمایش داده شد مدیریت:

-   [CREATE ROLE](../sql-reference/statements/create.md#create-role-statement)
-   [ALTER ROLE](../sql-reference/statements/alter.md#alter-role-statement)
-   [DROP ROLE](../sql-reference/statements/misc.md#drop-role-statement)
-   [SET ROLE](../sql-reference/statements/misc.md#set-role-statement)
-   [SET DEFAULT ROLE](../sql-reference/statements/misc.md#set-default-role-statement)
-   [SHOW CREATE ROLE](../sql-reference/statements/show.md#show-create-role-statement)

امتیازات به نقش را می توان با اعطا [GRANT](../sql-reference/statements/grant.md) پرس و جو. برای لغو امتیازات از یک فاحشه خانه نقش فراهم می کند [REVOKE](../sql-reference/statements/revoke.md) پرس و جو.

## سیاست سطر {#row-policy-management}

سیاست ردیف یک فیلتر است که تعریف می کند که یا ردیف برای یک کاربر و یا برای نقش در دسترس است. سیاست ردیف شامل فیلتر برای یک جدول خاص و لیستی از نقش ها و/یا کاربران که باید این سیاست ردیف استفاده کنید.

نمایش داده شد مدیریت:

-   [CREATE ROW POLICY](../sql-reference/statements/create.md#create-row-policy-statement)
-   [ALTER ROW POLICY](../sql-reference/statements/alter.md#alter-row-policy-statement)
-   [DROP ROW POLICY](../sql-reference/statements/misc.md#drop-row-policy-statement)
-   [SHOW CREATE ROW POLICY](../sql-reference/statements/show.md#show-create-row-policy-statement)

## تنظیمات {#settings-profiles-management}

مشخصات تنظیمات مجموعه ای از [تنظیمات](settings/index.md). مشخصات تنظیمات شامل تنظیمات و محدودیت, و لیستی از نقش ها و/یا کاربران که این سهمیه اعمال می شود.

نمایش داده شد مدیریت:

-   [CREATE SETTINGS PROFILE](../sql-reference/statements/create.md#create-settings-profile-statement)
-   [ALTER SETTINGS PROFILE](../sql-reference/statements/alter.md#alter-settings-profile-statement)
-   [DROP SETTINGS PROFILE](../sql-reference/statements/misc.md#drop-settings-profile-statement)
-   [SHOW CREATE SETTINGS PROFILE](../sql-reference/statements/show.md#show-create-settings-profile-statement)

## سهمیه {#quotas-management}

سهمیه محدودیت استفاده از منابع. ببینید [سهمیه](quotas.md).

سهمیه شامل مجموعه ای از محدودیت برای برخی از مدت زمان, و لیستی از نقش ها و/و یا کاربران که باید این سهمیه استفاده.

نمایش داده شد مدیریت:

-   [CREATE QUOTA](../sql-reference/statements/create.md#create-quota-statement)
-   [ALTER QUOTA](../sql-reference/statements/alter.md#alter-quota-statement)
-   [DROP QUOTA](../sql-reference/statements/misc.md#drop-quota-statement)
-   [SHOW CREATE QUOTA](../sql-reference/statements/show.md#show-create-quota-statement)

## فعال کردن کنترل دسترسی مبتنی بر مربع و مدیریت حساب {#enabling-access-control}

-   راه اندازی یک دایرکتوری برای ذخیره سازی تنظیمات.

    فروشگاه های کلیک دسترسی به تنظیمات نهاد در مجموعه پوشه در [_پوشه دستیابی](server-configuration-parameters/settings.md#access_control_path) پارامتر پیکربندی سرور.

-   فعال کردن گذاشتن محور کنترل دسترسی و مدیریت حساب برای حداقل یک حساب کاربری.

    به طور پیش فرض کنترل دسترسی مبتنی بر مربع و مدیریت حساب برای همه کاربران تبدیل شده است. شما نیاز به پیکربندی حداقل یک کاربر در `users.xml` فایل پیکربندی و اختصاص 1 به [مدیریت دسترسی](settings/settings-users.md#access_management-user-setting) تنظیمات.

[مقاله اصلی](https://clickhouse.tech/docs/en/operations/access_rights/) <!--hide-->
