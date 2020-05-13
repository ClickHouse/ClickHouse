---
machine_translated: true
machine_translated_rev: e74b62fcb409cdf9bb9bb7c5681f8ef07337dd74
---

# به OUTFILE بند {#into-outfile-clause}

افزودن `INTO OUTFILE filename` بند (جایی که نام فایل یک رشته تحت اللفظی است) به `SELECT query` برای تغییر مسیر خروجی خود را به فایل مشخص شده در سمت سرویس گیرنده.

## پیاده سازی اطلاعات {#implementation-details}

-   این قابلیت در دسترس است [مشتری خط فرمان](../../../interfaces/cli.md) و [کلیک-محلی](../../../operations/utilities/clickhouse-local.md). بنابراین پرس و جو از طریق ارسال [رابط قام](../../../interfaces/http.md) شکست مواجه خواهد شد.
-   پرس و جو شکست مواجه خواهد شد اگر یک فایل با نام فایل مشابه در حال حاضر وجود دارد.
-   پیشفرض [فرمت خروجی](../../../interfaces/formats.md) هست `TabSeparated` (مانند در خط فرمان حالت دسته ای مشتری).
