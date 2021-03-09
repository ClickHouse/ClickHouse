---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# به OUTFILE بند {#into-outfile-clause}

افزودن `INTO OUTFILE filename` بند (جایی که نام فایل یک رشته تحت اللفظی است) به `SELECT query` برای تغییر مسیر خروجی خود را به فایل مشخص شده در سمت سرویس گیرنده.

## پیاده سازی اطلاعات {#implementation-details}

-   این قابلیت در دسترس است [مشتری خط فرمان](../../../interfaces/cli.md) و [کلیک-محلی](../../../operations/utilities/clickhouse-local.md). بنابراین پرس و جو از طریق ارسال [رابط قام](../../../interfaces/http.md) شکست مواجه خواهد شد.
-   پرس و جو شکست مواجه خواهد شد اگر یک فایل با نام فایل مشابه در حال حاضر وجود دارد.
-   پیشفرض [فرمت خروجی](../../../interfaces/formats.md) هست `TabSeparated` (مانند در خط فرمان حالت دسته ای مشتری).
