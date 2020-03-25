<div markdown="1" markdown="1" dir="rtl">

# ﯼﺯﺍﺪﻧﺍ ﻩﺍﺭ ﻭ ﺐﺼﻧ

## نیازمندی های سیستم {#nyzmndy-hy-systm}

ClickHouse ﺲﮐﻮﻨﯿﻟ ﻉﻮﻧ ﺮﻫ ﯼﻭﺭ ﺮﺑ ﺪﻧﺍﻮﺗ ﯽﻣ ، FreeBSD ﺎﯾ Mac OS X ﯼﺭﺎﻤﻌﻣ ﺎﺑ CPU x

:ﺖﺳﺍ ﻩﺪﻣﺁ ، ﺪﻨﮐ ﯽﻣ ﯽﻧﺎﺒﯿﺘﺸﭘ SSE 4.2 ﺯﺍ ﯽﻠﻌﻓ CPU ﺎﯾﺁ ﻪﮑﻨﯾﺍ ﯽﺳﺭﺮﺑ ﯼﺍﺮﺑ ﺭﻮﺘﺳﺩ ﻦﯾﺍ

</div>

``` bash
grep -q sse4_2 /proc/cpuinfo && echo "SSE 4.2 supported" || echo "SSE 4.2 not supported"
```

<div markdown="1" markdown="1" dir="rtl">

ﺪﯾﺎﺑ ، ﺪﻧﺭﺍﺪﻧ PowerPC64LE ﺎﯾ AArch64 ﯼﺭﺎﻤﻌﻣ ﺎﯾ ﺪﻨﻨﮐ ﯽﻤﻧ ﯽﻧﺎﺒﯿﺘﺸﭘ SSE 4.2 ﺯﺍ ﻪﮐ[ClickHouse ﺪﯿﻨﮐ ﺩﺎﺠﯾﺍ ﻊﺑﺎﻨﻣ ﺯﺍ ﺍﺭ](#from-sources) ﺐﺳﺎﻨﻣ ﺕﺎﻤﯿﻈﻨﺗ ﺎﺑ

## ﺩﻮﺟﻮﻣ ﺐﺼﻧ ﯼﺎﻫ ﻪﻨﯾﺰﮔ

<a name="install-from-deb-packages"></a>
\#\#\# نصب از طریق پکیج های Debian/Ubuntu {\#from-deb-packages}

در فایل `/etc/apt/sources.list` (یا در یک فایل جدا `/etc/apt/sources.list.d/clickhouse.list`)، Repo زیر را اضافه کنید:

</div>

    deb http://repo.yandex.ru/clickhouse/deb/stable/ main/

<div markdown="1" markdown="1" dir="rtl">

اگر شما میخوایید جدیدترین نسخه ی تست را استفاده کنید، ‘stable’ رو به ‘testing’ تغییر بدید.

سپس دستورات زیر را اجرا کنید:

</div>

``` bash
sudo apt-get install dirmngr    # optional
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv E0C56BD4    # optional
sudo apt-get update
sudo apt-get install clickhouse-client clickhouse-server
```

<div markdown="1" markdown="1" dir="rtl">

شما همچنین می توانید از طریق لینک زیر پکیج ClickHouse را به صورت دستی دانلود و نصب کنید: https://repo.yandex.ru/clickhouse/deb/stable/main/.

ClickHouse دارای تنظیمات محدودیت دسترسی می باشد. این تنظیمات در فایل ‘users.xml’ (کنار ‘config.xml’) می باشد. به صورت پیش فرض دسترسی برای کاربر ‘default’ از همه جا بدون نیاز به پسورد وجود دارد. ‘user/default/networks’ را مشاهده کنید. برای اطلاعات بیشتر قسمت «تنظیمات فایل ها» را مشاهده کنید.

### RPM ﯼﺎﻫ ﻪﺘﺴﺑ ﺯﺍ {#from-rpm-packages}

.ﺪﻨﮐ ﯽﻣ ﻪﯿﺻﻮﺗ ﺲﮐﻮﻨﯿﻟ ﺮﺑ ﯽﻨﺘﺒﻣ rpm ﺮﺑ ﯽﻨﺘﺒﻣ ﯼﺎﻫ ﻊﯾﺯﻮﺗ ﺮﯾﺎﺳ ﻭ CentOS ، RedHat ﯼﺍ

                                             :ﺪﯿﻨﮐ ﻪﻓﺎﺿﺍ ﺍﺭ ﯽﻤﺳﺭ ﻥﺰﺨﻣ ﺪﯾﺎﺑ ﺍﺪﺘﺑﺍ

``` bash
sudo yum install yum-utils
sudo rpm --import https://repo.yandex.ru/clickhouse/CLICKHOUSE-KEY.GPG
sudo yum-config-manager --add-repo https://repo.yandex.ru/clickhouse/rpm/stable/x86_64
```

.(ﺩﻮﺷ ﯽﻣ ﻪﯿﺻﻮﺗ ﺎﻤﺷ ﺶﯾﺎﻣﺯﺁ ﯼﺎﻫ ﻂﯿﺤﻣ ﯼﺍﺮﺑ ﻦﯾﺍ) ﺪﯿﻨﮐ ﻦﯾﺰﮕﯾﺎﺟ «ﺖﺴﺗ» ﺎﺑ ﺍﺭ «ﺭﺍﺪﯾﺎﭘ»

                    :ﺪﯿﻨﮐ ﺐﺼﻧ ﺍﺭ ﺎﻫ ﻪﺘﺴﺑ ﻊﻗﺍﻭ ﺭﺩ ﺎﺗ ﺪﯿﻨﮐ ﺍﺮﺟﺍ ﺍﺭ ﺕﺍﺭﻮﺘﺳﺩ ﻦﯾﺍ ﺲﭙﺳ

``` bash
sudo yum install clickhouse-server clickhouse-client
```

.https://repo.yandex.ru/clickhouse/rpm/stable/x86\_64 :ﺪﯿﻨﮐ ﺐﺼﻧ ﻭ ﯼﺮﯿﮔﺭﺎﺑ ﺎﺠﻨ

                                                             Docker Image ﺯﺍ ###

.ﺪﻨﻨﮐ ﯽﻣ ﻩﺩﺎﻔﺘﺳﺍ ﻞﺧﺍﺩ ﺭﺩ «deb» ﯽﻤﺳﺭ ﯼﺎﻫ ﻪﺘﺴﺑ ﺯﺍ ﺮﯾﻭﺎﺼﺗ ﻦﯾﺍ .ﺪﯿﻨﮐ ﻝﺎﺒﻧﺩ ﺍﺭ (/ht

### نصب از طریق Source {#from-sources}

برای Compile، دستورالعمل های فایل build.md را دنبال کنید:

شما میتوانید پکیج را compile و نصب کنید. شما همچنین می توانید بدون نصب پکیج از برنامه ها استفاده کنید.

</div>

    Client: dbms/programs/clickhouse-client
    Server: dbms/programs/clickhouse-server

<div markdown="1" markdown="1" dir="rtl">

برای سرور، یک کاتالوگ با دیتا بسازید، مانند

</div>

    /opt/clickhouse/data/default/
    /opt/clickhouse/metadata/default/

<div markdown="1" markdown="1" dir="rtl">

(قابل تنظیم در تنظیمات سرور). ‘chown’ را برای کاربر دلخواه اجرا کنید.

به مسیر لاگ ها در تنظیمات سرور توجه کنید (src/dbms/programs/config.xml).

### روش های دیگر نصب {#from-docker-image}

Docker image: https://hub.docker.com/r/yandex/clickhouse-server/

پکیج RPM برای CentOS یا RHEL: https://github.com/Altinity/clickhouse-rpm-install

Gentoo: `emerge clickhouse`

## راه اندازی {#rh-ndzy}

برای استارت سرور (به صورت daemon)، دستور زیر را اجرا کنید:

</div>

``` bash
sudo service clickhouse-server start
```

<div markdown="1" markdown="1" dir="rtl">

لاگ های دایرکتوری `/var/log/clickhouse-server/` directory. را مشاهده کنید.

اگر سرور استارت نشد، فایل تنظیمات را بررسی کنید `/etc/clickhouse-server/config.xml.`

شما همچنین می توانید سرور را از طریق کنسول راه اندازی کنید:

</div>

``` bash
clickhouse-server --config-file=/etc/clickhouse-server/config.xml
```

<div markdown="1" markdown="1" dir="rtl">

در این مورد که مناسب زمان توسعه می باشد، لاگ ها در کنسول پرینت می شوند. اگر فایل تنظیمات در دایرکتوری جاری باشد، نیازی به مشخص کردن ‘–config-file’ نمی باشد. به صورت پیش فرض از ‘./config.xml’ استفاده می شود.

شما می توانید از کلاینت command-line برای اتصال به سرور استفاده کنید:

</div>

``` bash
clickhouse-client
```

<div markdown="1" markdown="1" dir="rtl">

پارامترهای پیش فرض، نشان از اتصال به localhost:9000 از طرف کاربر ‘default’ بدون پسورد را می دهد. از کلاینت میتوان برای اتصال به یک سرور remote استفاده کرد. مثال:

</div>

``` bash
clickhouse-client --host=example.com
```

<div markdown="1" markdown="1" dir="rtl">

برای اطلاعات بیشتر، بخش «کلاینت Command-line» را مشاهده کنید.

چک کردن سیستم:

</div>

``` bash
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

<div markdown="1" markdown="1" dir="rtl">

**تبریک میگم، سیستم کار می کنه!**

برای ادامه آزمایشات، شما میتوانید دیتاست های تستی را دریافت و امتحان کنید.

</div>

[مقاله اصلی](https://clickhouse.tech/docs/fa/getting_started/install/) <!--hide-->
