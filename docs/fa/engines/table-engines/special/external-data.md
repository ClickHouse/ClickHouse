---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 34
toc_title: "\u062F\u0627\u062F\u0647\u0647\u0627\u06CC \u062E\u0627\u0631\u062C\u06CC"
---

# داده های خارجی برای پردازش پرس و جو {#external-data-for-query-processing}

تاتر اجازه می دهد تا ارسال یک سرور داده ها که برای پردازش یک پرس و جو مورد نیاز است, همراه با پرس و جو را انتخاب کنید. این داده ها در یک جدول موقت قرار داده (نگاه کنید به بخش “Temporary tables”) و می تواند مورد استفاده قرار گیرد در پرس و جو (برای مثال در اپراتورها).

مثلا, اگر شما یک فایل متنی با شناسه کاربر مهم, شما می توانید به سرور همراه پرس و جو است که با استفاده از فیلتراسیون توسط این لیست ارسال.

اگر شما نیاز به اجرای بیش از یک پرس و جو با حجم زیادی از داده های خارجی از این ویژگی استفاده نکنید. بهتر است برای بارگذاری داده ها به دسی بل جلوتر از زمان.

داده های خارجی را می توان با استفاده از مشتری خط فرمان (در حالت غیر تعاملی) و یا با استفاده از رابط قام ارسال می شود.

در خط فرمان مشتری شما می توانید مشخص پارامترهای بخش در قالب

``` bash
--external --file=... [--name=...] [--format=...] [--types=...|--structure=...]
```

شما ممکن است بخش های متعدد مثل این, برای تعدادی از جداول در حال انتقال.

**–external** – Marks the beginning of a clause.
**–file** – Path to the file with the table dump, or -, which refers to stdin.
فقط یک جدول را می توان از استدین بازیابی.

پارامترهای زیر اختیاری هستند: **–name**– Name of the table. If omitted, \_data is used.
**–format** – Data format in the file. If omitted, TabSeparated is used.

یکی از پارامترهای زیر مورد نیاز است:**–types** – A list of comma-separated column types. For example: `UInt64,String`. The columns will be named \_1, \_2, …
**–structure**– The table structure in the format`UserID UInt64`, `URL String`. تعریف نام ستون و انواع.

فایل های مشخص شده در ‘file’ خواهد شد با فرمت مشخص شده در تجزیه ‘format’ با استفاده از انواع داده های مشخص شده در ‘types’ یا ‘structure’. جدول خواهد شد به سرور ارسال شده و در دسترس وجود دارد به عنوان یک جدول موقت با نام در ‘name’.

مثالها:

``` bash
$ echo -ne "1\n2\n3\n" | clickhouse-client --query="SELECT count() FROM test.visits WHERE TraficSourceID IN _data" --external --file=- --types=Int8
849897
$ cat /etc/passwd | sed 's/:/\t/g' | clickhouse-client --query="SELECT shell, count() AS c FROM passwd GROUP BY shell ORDER BY c DESC" --external --file=- --name=passwd --structure='login String, unused String, uid UInt16, gid UInt16, comment String, home String, shell String'
/bin/sh 20
/bin/false      5
/bin/bash       4
/usr/sbin/nologin       1
/bin/sync       1
```

هنگام استفاده از رابط اچ تی پی, داده های خارجی در قالب چند/فرم داده به تصویب رسید. هر جدول به عنوان یک فایل جداگانه منتقل می شود. نام جدول از نام فایل گرفته شده است. این ‘query\_string’ پارامترهای منتقل می شود ‘name\_format’, ‘name\_types’ و ‘name\_structure’ کجا ‘name’ نام جدول که این پارامترها به مطابقت است. معنای پارامترهای همان است که در هنگام استفاده از مشتری خط فرمان است.

مثال:

``` bash
$ cat /etc/passwd | sed 's/:/\t/g' > passwd.tsv

$ curl -F 'passwd=@passwd.tsv;' 'http://localhost:8123/?query=SELECT+shell,+count()+AS+c+FROM+passwd+GROUP+BY+shell+ORDER+BY+c+DESC&passwd_structure=login+String,+unused+String,+uid+UInt16,+gid+UInt16,+comment+String,+home+String,+shell+String'
/bin/sh 20
/bin/false      5
/bin/bash       4
/usr/sbin/nologin       1
/bin/sync       1
```

برای پردازش پرس و جو توزیع, جداول موقت به تمام سرور از راه دور ارسال.

[مقاله اصلی](https://clickhouse.tech/docs/en/operations/table_engines/external_data/) <!--hide-->
