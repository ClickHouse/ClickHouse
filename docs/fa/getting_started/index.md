<div dir="rtl" markdown="1">

# شروع به کار

## نیازمندی های سیستم

این یک سیستم چند سکویی (Cross-Platform) نمی باشد. این ابزار نیاز به Linux Ubuntu Precise (12.04) یا جدیدتر، با معماری x86\_64 و پشتیبانی از SSE 4.2 می باشد. برای چک کردن SSE 4.2 خروجی دستور زیر را بررسی کنید:

</div>

```bash
grep -q sse4_2 /proc/cpuinfo && echo "SSE 4.2 supported" || echo "SSE 4.2 not supported"
```

<div dir="rtl" markdown="1">

پیشنهاد می کنیم از Ubuntu TrustyT، Ubuntu Xenial یا Ubuntu Precise استفاده کنید. ترمینال باید از UTF-8 پشتیبانی کند. (به صورت پیش فرض در Ubuntu پشتیبانی می شود).

## نصب

### نصب از طریق پکیج های Debian/Ubuntu

در فایل `/etc/apt/sources.list` (یا در یک فایل جدا `/etc/apt/sources.list.d/clickhouse.list`)، Repo زیر را اضافه کنید:

</div>

```
deb http://repo.yandex.ru/clickhouse/deb/stable/ main/
```

<div dir="rtl" markdown="1">

اگر شما میخوایید جدیدترین نسخه ی تست را استفاده کنید، 'stable' رو به 'testing' تغییر بدید.

سپس دستورات زیر را اجرا کنید:

</div>

```bash
sudo apt-get install dirmngr    # optional
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv E0C56BD4    # optional
sudo apt-get update
sudo apt-get install clickhouse-client clickhouse-server
```

<div dir="rtl" markdown="1">

شما همچنین می توانید از طریق لینک زیر پکیج ClickHouse را به صورت دستی دانلود و نصب کنید: <https://repo.yandex.ru/clickhouse/deb/stable/main/>.

ClickHouse دارای تنظیمات محدودیت دسترسی می باشد. این تنظیمات در فایل 'users.xml' (کنار 'config.xml') می باشد. به صورت پیش فرض دسترسی برای کاربر 'default' از همه جا بدون نیاز به پسورد وجود دارد. 'user/default/networks' را مشاهده کنید. برای اطلاعات بیشتر قسمت "تنظیمات فایل ها" را مشاهده کنید.

### نصب از طریق Source

برای Compile، دستورالعمل های فایل build.md را دنبال کنید:

شما میتوانید پکیج را compile و نصب کنید. شما همچنین می توانید بدون نصب پکیج از برنامه ها استفاده کنید.

</div>

```
Client: dbms/programs/clickhouse-client
Server: dbms/programs/clickhouse-server
```

<div dir="rtl" markdown="1">

برای سرور، یک کاتالوگ با دیتا بسازید، مانند

</div>

```
/opt/clickhouse/data/default/
/opt/clickhouse/metadata/default/
```

<div dir="rtl" markdown="1">

(قابل تنظیم در تنظیمات سرور). 'chown' را برای کاربر دلخواه اجرا کنید.

به مسیر لاگ ها در تنظیمات سرور توجه کنید (src/dbms/programs/config.xml).

### روش های دیگر نصب

Docker image: <https://hub.docker.com/r/yandex/clickhouse-server/>

پکیج RPM برای CentOS یا RHEL: <https://github.com/Altinity/clickhouse-rpm-install>

Gentoo: `emerge clickhouse`

## راه اندازی

برای استارت سرور (به صورت daemon)، دستور زیر را اجرا کنید:

</div>

```bash
sudo service clickhouse-server start
```

<div dir="rtl" markdown="1">

لاگ های دایرکتوری `/var/log/clickhouse-server/` directory. را مشاهده کنید.

اگر سرور استارت نشد، فایل تنظیمات را بررسی کنید `/etc/clickhouse-server/config.xml.`

شما همچنین می توانید سرور را از طریق کنسول راه اندازی کنید:

</div>

```bash
clickhouse-server --config-file=/etc/clickhouse-server/config.xml
```

<div dir="rtl" markdown="1">

در این مورد که مناسب زمان توسعه می باشد، لاگ ها در کنسول پرینت می شوند. اگر فایل تنظیمات در دایرکتوری جاری باشد، نیازی به مشخص کردن '--config-file' نمی باشد. به صورت پیش فرض از './config.xml' استفاده می شود.

شما می توانید از کلاینت command-line برای اتصال به سرور استفاده کنید:

</div>

```bash
clickhouse-client
```

<div dir="rtl" markdown="1">

پارامترهای پیش فرض، نشان از اتصال به localhost:9000 از طرف کاربر 'default' بدون پسورد را می دهد. از کلاینت میتوان برای اتصال به یک سرور remote استفاده کرد. مثال:

</div>

```bash
clickhouse-client --host=example.com
```

<div dir="rtl" markdown="1">

برای اطلاعات بیشتر، بخش "کلاینت Command-line" را مشاهده کنید.

چک کردن سیستم:

</div>

```bash
milovidov@hostname:~/work/metrica/src/dbms/src/Client$ ./clickhouse-client
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

<div dir="rtl" markdown="1">

**تبریک میگم، سیستم کار می کنه!**

برای ادامه آزمایشات، شما میتوانید دیتاست های تستی را دریافت و امتحان کنید.

</div>
[مقاله اصلی](https://clickhouse.yandex/docs/fa/getting_started/) <!--hide-->
