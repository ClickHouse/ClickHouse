---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 27
toc_title: "\u06CC\u06A9\u067E\u0627\u0631\u0686\u06AF\u06CC"
---

# کتابخانه ادغام از توسعه دهندگان شخص ثالث {#integration-libraries-from-third-party-developers}

!!! warning "تکذیبنامهها"
    یاندکس می کند **نه** حفظ ابزار و کتابخانه های ذکر شده در زیر و هر تست گسترده برای اطمینان از کیفیت خود را انجام نداده اند.

## محصولات زیربنایی {#infrastructure-products}

-   سیستم های مدیریت پایگاه داده رابطه ای
    -   [MySQL](https://www.mysql.com)
        -   [mysql2ch](https://github.com/long2ice/mysql2ch)
        -   [در حال بارگذاری](https://github.com/sysown/proxysql/wiki/ClickHouse-Support)
        -   [تاتر-خروجی زیر-داده خوان](https://github.com/Altinity/clickhouse-mysql-data-reader)
        -   [horgh-replicator](https://github.com/larsnovikov/horgh-replicator)
    -   [PostgreSQL](https://www.postgresql.org)
        -   [_پاک کردن تصویر](https://github.com/Percona-Lab/clickhousedb_fdw)
        -   [اطالعات._پاک کردن](https://github.com/Infinidat/infi.clickhouse_fdw) (استفاده [اطالعات.کلیک _شورم](https://github.com/Infinidat/infi.clickhouse_orm))
        -   [پی جی 2چ](https://github.com/mkabilov/pg2ch)
        -   [_پاک کردن](https://github.com/adjust/clickhouse_fdw)
    -   [MSSQL](https://en.wikipedia.org/wiki/Microsoft_SQL_Server)
        -   [کلیک کنیدهاجر](https://github.com/zlzforever/ClickHouseMigrator)
-   صف پیام
    -   [کافکا](https://kafka.apache.org)
        -   [در حال بارگذاری](https://github.com/housepower/clickhouse_sinker) (استفاده [برو کارگیر](https://github.com/ClickHouse/clickhouse-go/))
        -   [stream-loader-clickhouse](https://github.com/adform/stream-loader)
-   پردازش جریان
    -   [لرزش](https://flink.apache.org)
        -   [سینک فلینک-کلیک](https://github.com/ivi-ru/flink-clickhouse-sink)
-   ذخیره سازی شی
    -   [S3](https://en.wikipedia.org/wiki/Amazon_S3)
        -   [کلیک-پشتیبان گیری](https://github.com/AlexAkulov/clickhouse-backup)
-   ارکستراسیون کانتینر
    -   [کوبرنتس](https://kubernetes.io)
        -   [کلیک اپراتور](https://github.com/Altinity/clickhouse-operator)
-   مدیریت پیکربندی
    -   [عروسک](https://puppet.com)
        -   [نام و نام خانوادگی / خانه کلیک](https://forge.puppet.com/innogames/clickhouse)
        -   [مفدوتوف / خانه کلیک](https://forge.puppet.com/mfedotov/clickhouse)
-   نظارت
    -   [گرافیت](https://graphiteapp.org)
        -   [نمودار](https://github.com/yandex/graphouse)
        -   [کربن کلیک](https://github.com/lomik/carbon-clickhouse) +
        -   [گرافیت-تاتر](https://github.com/lomik/graphite-clickhouse)
        -   [گرافیت-چ بهینه ساز](https://github.com/innogames/graphite-ch-optimizer) - بهینه سازی پارتیشن های ریخته شده در [اطلاعات دقیق](../../engines/table-engines/mergetree-family/graphitemergetree.md#graphitemergetree) اگر قوانین از [پیکربندی رولپ](../../engines/table-engines/mergetree-family/graphitemergetree.md#rollup-configuration) می تواند اعمال شود
    -   [گرافانا](https://grafana.com/)
        -   [فاحشه خانه-گرافانا](https://github.com/Vertamedia/clickhouse-grafana)
    -   [پرومتیوس](https://prometheus.io/)
        -   [کلیک _گزارشسپر](https://github.com/f1yegor/clickhouse_exporter)
        -   [مجلس رقص رسمی دبیرستان](https://github.com/Percona-Lab/PromHouse)
        -   [کلیک _گزارشسپر](https://github.com/hot-wifi/clickhouse_exporter) (استفاده [برو کارگیر](https://github.com/kshvakov/clickhouse/))
    -   [Nagios](https://www.nagios.org/)
        -   [_تخچه نشانزنی](https://github.com/exogroup/check_clickhouse/)
        -   [check_clickhouse.py](https://github.com/innogames/igmonplugins/blob/master/src/check_clickhouse.py)
    -   [زاببیکس](https://www.zabbix.com)
        -   [هوکاوس-زاببیکس-قالب](https://github.com/Altinity/clickhouse-zabbix-template)
    -   [کلیپ برد چند منظوره](https://sematext.com/)
        -   [ادغام کلیک](https://github.com/sematext/sematext-agent-integrations/tree/master/clickhouse)
-   ثبت
    -   [rsyslog](https://www.rsyslog.com/)
        -   [املتخانه](https://www.rsyslog.com/doc/master/configuration/modules/omclickhouse.html)
    -   [روان کننده](https://www.fluentd.org)
        -   [خانه ثبت](https://github.com/flant/loghouse) (برای [کوبرنتس](https://kubernetes.io))
    -   [ثبت](https://www.sematext.com/logagent)
        -   [خروجی ورود-پلاگین-خانه کلیک](https://sematext.com/docs/logagent/output-plugin-clickhouse/)
-   نق
    -   [نیروی برتر](https://dev.maxmind.com/geoip/)
        -   [خانه هوشمند](https://github.com/AlexeyKupershtokh/clickhouse-maxmind-geoip)

## اکوسیستم های زبان برنامه نویسی {#programming-language-ecosystems}

-   پایتون
    -   [SQLAlchemy](https://www.sqlalchemy.org)
        -   [sqlalchemy-clickhouse](https://github.com/cloudflare/sqlalchemy-clickhouse) (استفاده [اطالعات.کلیک _شورم](https://github.com/Infinidat/infi.clickhouse_orm))
    -   [پانداها](https://pandas.pydata.org)
        -   [پانداهاوس](https://github.com/kszucs/pandahouse)
-   PHP
    -   [دکترین](https://www.doctrine-project.org/)
        -   [خانه هوشمند](https://packagist.org/packages/friendsofdoctrine/dbal-clickhouse)
-   R
    -   [هواپیمای دوباله](https://db.rstudio.com/dplyr/)
        -   [خانه روستایی](https://github.com/IMSMWU/RClickHouse) (استفاده [صفحه اصلی](https://github.com/artpaul/clickhouse-cpp))
-   جاوا
    -   [هادوپ](http://hadoop.apache.org)
        -   [سرویس پرداخت درونبرنامهای پلی](https://github.com/jaykelin/clickhouse-hdfs-loader) (استفاده [JDBC](../../sql-reference/table-functions/jdbc.md))
-   اسکالا
    -   [اککا](https://akka.io)
        -   [تاتر-اسکالا-کارفرما](https://github.com/crobox/clickhouse-scala-client)
-   C#
    -   [ADO.NET](https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/ado-net-overview)
        -   [فاحشه خانه.ادو](https://github.com/killwort/ClickHouse-Net)
        -   [فاحشه خانه.کارگیر](https://github.com/DarkWanderer/ClickHouse.Client)
        -   [ClickHouse.Net](https://github.com/ilyabreev/ClickHouse.Net)
        -   [مهاجرت.](https://github.com/ilyabreev/ClickHouse.Net.Migrations)
-   اکسیر
    -   [Ecto](https://github.com/elixir-ecto/ecto)
        -   [حذف جستجو](https://github.com/appodeal/clickhouse_ecto)
-   Ruby
    -   [Ruby on Rails](https://rubyonrails.org/)
        -   [activecube](https://github.com/bitquery/activecube)
        -   [ActiveRecord](https://github.com/PNixx/clickhouse-activerecord)        
    -   [GraphQL](https://github.com/graphql)
        -   [activecube-graphql](https://github.com/bitquery/activecube-graphql)
        
[مقاله اصلی](https://clickhouse.tech/docs/en/interfaces/third-party/integrations/) <!--hide-->
