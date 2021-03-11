---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 76
toc_title: "\u062A\u063A\u06CC\u06CC\u0631\u0627\u062A \u0627\u0645\u0646\u06CC\u062A\
  \u06CC"
---

## ثابت در ClickHouse انتشار 19.14.3.3, 2019-09-10 {#fixed-in-clickhouse-release-19-14-3-3-2019-09-10}

### CVE-2019-15024 {#cve-2019-15024}

Аn attacker that has write access to ZooKeeper and who ican run a custom server available from the network where ClickHouse runs, can create a custom-built malicious server that will act as a ClickHouse replica and register it in ZooKeeper. When another replica will fetch data part from the malicious replica, it can force clickhouse-server to write to arbitrary path on filesystem.

اعتبار: الدار زیتوف تیم امنیت اطلاعات یاندکس

### CVE-2019-16535 {#cve-2019-16535}

Аn OOB read, OOB write and integer underflow in decompression algorithms can be used to achieve RCE or DoS via native protocol.

اعتبار: الدار زیتوف تیم امنیت اطلاعات یاندکس

### CVE-2019-16536 {#cve-2019-16536}

سرریز پشته منجر به داس را می توان با یک مشتری تصدیق مخرب باعث.

اعتبار: الدار زیتوف تیم امنیت اطلاعات یاندکس

## ثابت در ClickHouse انتشار 19.13.6.1, 2019-09-20 {#fixed-in-clickhouse-release-19-13-6-1-2019-09-20}

### CVE-2019-18657 {#cve-2019-18657}

تابع جدول `url` در صورت امکان پذیری اجازه مهاجم برای تزریق هدر قام دلخواه در درخواست.

اعتبار: [نیکیتا تیکومیرو](https://github.com/NSTikhomirov)

## ثابت در ClickHouse انتشار 18.12.13, 2018-09-10 {#fixed-in-clickhouse-release-18-12-13-2018-09-10}

### CVE-2018-14672 {#cve-2018-14672}

توابع برای بارگذاری مدل های ادم کودن و احمق اجازه عبور مسیر و خواندن فایل های دلخواه از طریق پیام های خطا.

اعتبار: اندری کراسیچکوف از تیم امنیت اطلاعات یاندکس

## ثابت در ClickHouse انتشار 18.10.3, 2018-08-13 {#fixed-in-clickhouse-release-18-10-3-2018-08-13}

### CVE-2018-14671 {#cve-2018-14671}

unixODBC اجازه بارگذاری دلخواه اشیاء مشترک از فایل سیستم است که منجر به اجرای کد از راه دور آسیب پذیری.

اعتبار: اندری کراسیچکوف و اوگنی سیدوروف از تیم امنیت اطلاعات یاندکس

## ثابت در ClickHouse انتشار 1.1.54388, 2018-06-28 {#fixed-in-clickhouse-release-1-1-54388-2018-06-28}

### CVE-2018-14668 {#cve-2018-14668}

“remote” تابع جدول اجازه نمادهای دلخواه در “user”, “password” و “default\_database” زمینه های که منجر به عبور از پروتکل درخواست حملات جعل.

اعتبار: اندری کراسیچکوف از تیم امنیت اطلاعات یاندکس

## ثابت در ClickHouse انتشار 1.1.54390, 2018-07-06 {#fixed-in-clickhouse-release-1-1-54390-2018-07-06}

### CVE-2018-14669 {#cve-2018-14669}

کلاینت خروجی زیر کلیک بود “LOAD DATA LOCAL INFILE” قابلیت را فعال کنید که اجازه یک پایگاه داده خروجی زیر مخرب خواندن فایل های دلخواه از سرور کلیک متصل می شود.

اعتبار: اندری کراسیچکوف و اوگنی سیدوروف از تیم امنیت اطلاعات یاندکس

## ثابت در ClickHouse انتشار 1.1.54131, 2017-01-10 {#fixed-in-clickhouse-release-1-1-54131-2017-01-10}

### CVE-2018-14670 {#cve-2018-14670}

پیکربندی نادرست در بسته دب می تواند به استفاده غیر مجاز از پایگاه داده منجر شود.

اعتبار: انگلستان National Cyber Security Centre (NCSC)

{## [مقاله اصلی](https://clickhouse.tech/docs/en/security_changelog/) ##}
