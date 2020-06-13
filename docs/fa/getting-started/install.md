---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 11
toc_title: "\u0646\u0635\u0628 \u0648 \u0631\u0627\u0647 \u0627\u0646\u062F\u0627\u0632\
  \u06CC"
---

# نصب و راه اندازی {#installation}

## سیستم مورد نیاز {#system-requirements}

ClickHouse می تواند اجرا بر روی هر Linux, FreeBSD یا سیستم عامل Mac OS X با x86\_64, AArch64 یا PowerPC64LE معماری CPU.

رسمی از پیش ساخته شده باینری به طور معمول وارد شده برای ایکس86\_64 و اهرم بورس تحصیلی 4.2 مجموعه دستورالعمل, بنابراین مگر اینکه در غیر این صورت اعلام کرد استفاده از پردازنده است که پشتیبانی می شود یک سیستم اضافی مورد نیاز. در اینجا دستور برای بررسی اگر پردازنده فعلی دارای پشتیبانی برای اس اس 4.2:

``` bash
$ grep -q sse4_2 /proc/cpuinfo && echo "SSE 4.2 supported" || echo "SSE 4.2 not supported"
```

برای اجرای ClickHouse در پردازنده هایی که پشتیبانی نمی SSE 4.2 یا AArch64 یا PowerPC64LE معماری شما باید [ساخت کلیک از منابع](#from-sources) با تنظیمات پیکربندی مناسب.

## گزینه های نصب موجود {#available-installation-options}

### از بسته های دب {#install-from-deb-packages}

توصیه می شود به استفاده از رسمی از پیش وارد شده `deb` بسته برای دبیان یا اوبونتو. اجرای این دستورات برای نصب بسته:

``` bash
{% include 'install/deb.sh' %}
```

اگر شما می خواهید به استفاده از نسخه های اخیر, جایگزین کردن `stable` با `testing` (این است که برای محیط های تست خود را توصیه می شود).

شما همچنین می توانید بسته ها را به صورت دستی دانلود و نصب کنید [اینجا](https://repo.clickhouse.tech/deb/stable/main/).

#### بستهها {#packages}

-   `clickhouse-common-static` — Installs ClickHouse compiled binary files.
-   `clickhouse-server` — Creates a symbolic link for `clickhouse-server` و نصب پیکربندی سرور به طور پیش فرض.
-   `clickhouse-client` — Creates a symbolic link for `clickhouse-client` و دیگر ابزار مربوط به مشتری. و نصب فایل های پیکربندی مشتری.
-   `clickhouse-common-static-dbg` — Installs ClickHouse compiled binary files with debug info.

### از بسته های دور در دقیقه {#from-rpm-packages}

توصیه می شود به استفاده از رسمی از پیش وارد شده `rpm` بسته برای لینوکس لینوکس, کلاه قرمز, و همه توزیع های لینوکس مبتنی بر دور در دقیقه دیگر.

اولین, شما نیاز به اضافه کردن مخزن رسمی:

``` bash
sudo yum install yum-utils
sudo rpm --import https://repo.clickhouse.tech/CLICKHOUSE-KEY.GPG
sudo yum-config-manager --add-repo https://repo.clickhouse.tech/rpm/stable/x86_64
```

اگر شما می خواهید به استفاده از نسخه های اخیر, جایگزین کردن `stable` با `testing` (این است که برای محیط های تست خود را توصیه می شود). این `prestable` برچسب است که گاهی اوقات در دسترس بیش از حد.

سپس این دستورات را برای نصب بسته ها اجرا کنید:

``` bash
sudo yum install clickhouse-server clickhouse-client
```

شما همچنین می توانید بسته ها را به صورت دستی دانلود و نصب کنید [اینجا](https://repo.clickhouse.tech/rpm/stable/x86_64).

### از بایگانی {#from-tgz-archives}

توصیه می شود به استفاده از رسمی از پیش وارد شده `tgz` بایگانی برای همه توزیع های لینوکس, که نصب و راه اندازی `deb` یا `rpm` بسته امکان پذیر نیست.

نسخه مورد نیاز را می توان با دانلود `curl` یا `wget` از مخزن https://repo.clickhouse.tech/tgz/.
پس از که دانلود بایگانی باید غیر بستهای و نصب شده با اسکریپت نصب و راه اندازی. به عنوان مثال برای جدیدترین نسخه:

``` bash
export LATEST_VERSION=`curl https://api.github.com/repos/ClickHouse/ClickHouse/tags 2>/dev/null | grep -Eo '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' | head -n 1`
curl -O https://repo.clickhouse.tech/tgz/clickhouse-common-static-$LATEST_VERSION.tgz
curl -O https://repo.clickhouse.tech/tgz/clickhouse-common-static-dbg-$LATEST_VERSION.tgz
curl -O https://repo.clickhouse.tech/tgz/clickhouse-server-$LATEST_VERSION.tgz
curl -O https://repo.clickhouse.tech/tgz/clickhouse-client-$LATEST_VERSION.tgz

tar -xzvf clickhouse-common-static-$LATEST_VERSION.tgz
sudo clickhouse-common-static-$LATEST_VERSION/install/doinst.sh

tar -xzvf clickhouse-common-static-dbg-$LATEST_VERSION.tgz
sudo clickhouse-common-static-dbg-$LATEST_VERSION/install/doinst.sh

tar -xzvf clickhouse-server-$LATEST_VERSION.tgz
sudo clickhouse-server-$LATEST_VERSION/install/doinst.sh
sudo /etc/init.d/clickhouse-server start

tar -xzvf clickhouse-client-$LATEST_VERSION.tgz
sudo clickhouse-client-$LATEST_VERSION/install/doinst.sh
```

برای محیط های تولید توصیه می شود از جدیدترین استفاده کنید `stable`- نسخه شما می توانید شماره خود را در صفحه گیتهاب پیدا https://github.com/ClickHouse/ClickHouse/tags با پسوند `-stable`.

### از تصویر کارگر بارانداز {#from-docker-image}

برای اجرای کلیک در داخل کارگر بارانداز راهنمای دنبال کنید [داکر توپی](https://hub.docker.com/r/yandex/clickhouse-server/). این تصاویر استفاده رسمی `deb` بسته در داخل.

### از منابع {#from-sources}

به صورت دستی کامپایل فاحشه خانه, دستورالعمل برای دنبال [لینوکس](../development/build.md) یا [سیستم عامل مک ایکس](../development/build-osx.md).

شما می توانید بسته های کامپایل و نصب و یا استفاده از برنامه های بدون نصب بسته. همچنین با ساخت دستی شما می توانید ثانیه 4.2 مورد نیاز غیر فعال کردن و یا ساخت برای ایالت64 پردازنده.

      Client: programs/clickhouse-client
      Server: programs/clickhouse-server

شما نیاز به ایجاد یک داده ها و پوشه ابرداده و `chown` برای کاربر مورد نظر. مسیر خود را می توان در پیکربندی سرور تغییر (سری سی/برنامه/سرور/پیکربندی.به طور پیش فرض:

      /opt/clickhouse/data/default/
      /opt/clickhouse/metadata/default/

در جنتو, شما فقط می توانید استفاده کنید `emerge clickhouse` برای نصب کلیک از منابع.

## راهاندازی {#launch}

برای شروع سرور به عنوان یک شبح, اجرا:

``` bash
$ sudo service clickhouse-server start
```

اگر شما لازم نیست `service` فرمان, اجرا به عنوان

``` bash
$ sudo /etc/init.d/clickhouse-server start
```

سیاهههای مربوط در `/var/log/clickhouse-server/` فهرست راهنما.

اگر سرور شروع نمی کند, بررسی تنظیمات در فایل `/etc/clickhouse-server/config.xml`.

شما همچنین می توانید سرور را از کنسول به صورت دستی راه اندازی کنید:

``` bash
$ clickhouse-server --config-file=/etc/clickhouse-server/config.xml
```

در این مورد, ورود به سیستم خواهد شد به کنسول چاپ, که مناسب است در طول توسعه.
اگر فایل پیکربندی در دایرکتوری فعلی است, شما لازم نیست برای مشخص کردن `--config-file` پارامتر. به طور پیش فرض استفاده می کند `./config.xml`.

تاتر پشتیبانی از تنظیمات محدودیت دسترسی. این در واقع `users.xml` پرونده) در کنار ( `config.xml`).
به طور پیش فرض, دسترسی از هر نقطه برای اجازه `default` کاربر, بدون رمز عبور. ببینید `user/default/networks`.
برای کسب اطلاعات بیشتر به بخش مراجعه کنید [“Configuration Files”](../operations/configuration-files.md).

پس از راه اندازی سرور, شما می توانید مشتری خط فرمان برای اتصال به استفاده:

``` bash
$ clickhouse-client
```

به طور پیش فرض به `localhost:9000` از طرف کاربر `default` بدون رمز عبور. همچنین می تواند مورد استفاده قرار گیرد برای اتصال به یک سرور از راه دور با استفاده از `--host` استدلال کردن.

ترمینال باید از کدگذاری جی تی اف 8 استفاده کند.
برای کسب اطلاعات بیشتر به بخش مراجعه کنید [“Command-line client”](../interfaces/cli.md).

مثال:

``` bash
$ ./clickhouse-client
ClickHouse client version 0.0.18749.
Connecting to localhost:9000.
Connected to ClickHouse server version 0.0.18749.

:) SELECT 1

SELECT 1

┌─1─┐
│ 1 │
└───┘

1 rows in set. Elapsed: 0.003 sec.

:)
```

**تبریک, سیستم کار می کند!**

برای ادامه تجربه, شما می توانید یکی از مجموعه داده های تست دانلود و یا رفتن را از طریق [اموزش](https://clickhouse.tech/tutorial.html).

[مقاله اصلی](https://clickhouse.tech/docs/en/getting_started/install/) <!--hide-->
