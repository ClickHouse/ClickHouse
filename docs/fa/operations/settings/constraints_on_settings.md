---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 62
toc_title: "\u0645\u062D\u062F\u0648\u062F\u06CC\u062A \u062F\u0631 \u062A\u0646\u0638\
  \u06CC\u0645\u0627\u062A"
---

# محدودیت در تنظیمات {#constraints-on-settings}

محدودیت در تنظیمات را می توان در تعریف `profiles` بخش از `user.xml` فایل پیکربندی و منع کاربران از تغییر برخی از تنظیمات با `SET` پرس و جو.
محدودیت ها به صورت زیر تعریف می شوند:

``` xml
<profiles>
  <user_name>
    <constraints>
      <setting_name_1>
        <min>lower_boundary</min>
      </setting_name_1>
      <setting_name_2>
        <max>upper_boundary</max>
      </setting_name_2>
      <setting_name_3>
        <min>lower_boundary</min>
        <max>upper_boundary</max>
      </setting_name_3>
      <setting_name_4>
        <readonly/>
      </setting_name_4>
    </constraints>
  </user_name>
</profiles>
```

اگر کاربر تلاش می کند به نقض محدودیت یک استثنا پرتاب می شود و تنظیم تغییر نکرده است.
سه نوع محدودیت پشتیبانی می شوند: `min`, `max`, `readonly`. این `min` و `max` محدودیت مشخص مرزهای بالا و پایین برای یک محیط عددی و می تواند در ترکیب استفاده می شود. این `readonly` محدودیت مشخص می کند که کاربر می تواند تنظیمات مربوطه را تغییر دهید و در همه.

**مثال:** اجازه بدهید `users.xml` شامل خطوط:

``` xml
<profiles>
  <default>
    <max_memory_usage>10000000000</max_memory_usage>
    <force_index_by_date>0</force_index_by_date>
    ...
    <constraints>
      <max_memory_usage>
        <min>5000000000</min>
        <max>20000000000</max>
      </max_memory_usage>
      <force_index_by_date>
        <readonly/>
      </force_index_by_date>
    </constraints>
  </default>
</profiles>
```

نمایش داده شد زیر همه استثنا پرتاب:

``` sql
SET max_memory_usage=20000000001;
SET max_memory_usage=4999999999;
SET force_index_by_date=1;
```

``` text
Code: 452, e.displayText() = DB::Exception: Setting max_memory_usage should not be greater than 20000000000.
Code: 452, e.displayText() = DB::Exception: Setting max_memory_usage should not be less than 5000000000.
Code: 452, e.displayText() = DB::Exception: Setting force_index_by_date should not be changed.
```

**یادداشت:** این `default` مشخصات است دست زدن به ویژه: همه محدودیت های تعریف شده برای `default` مشخصات تبدیل به محدودیت های پیش فرض, بنابراین محدود کردن تمام کاربران تا زمانی که به صراحت برای این کاربران باطل.

[مقاله اصلی](https://clickhouse.tech/docs/en/operations/settings/constraints_on_settings/) <!--hide-->
