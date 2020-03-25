<div markdown="1" markdown="1" dir="rtl">

# سرورهای پروکسی از توسعه دهندگان شخص ثالث {#srwrhy-prwkhsy-z-tws-h-dhndgn-shkhs-thlth}

[chproxy](https://github.com/Vertamedia/chproxy)، یک پراکسی HTTP و تعادل بار برای پایگاه داده ClickHouse است.

امکانات

-   مسیریابی و پاسخ دهی کاربر به کاربر.
-   محدودیت انعطاف پذیر
-   تمدید SSL cerificate به صورت خودکار.

اجرا شده در برو

## KittenHouse {#kittenhouse}

[KittenHouse](https://github.com/VKCOM/kittenhouse) طراحی شده است که یک پروکسی محلی بین ClickHouse و سرور برنامه باشد در صورتی که غیر ممکن است یا ناخوشایند بافر کردن اطلاعات INSERT در قسمت درخواست شما.

امکانات:

-   بافر حافظه در حافظه و درایو.
-   مسیریابی در جدول
-   تعادل بار و بررسی سلامت.

اجرا شده در برو

## ClickHouse-Bulk {#clickhouse-bulk}

[ClickHouse-Bulk](https://github.com/nikepan/clickhouse-bulk) یک ClickHouse جمع کننده ساده است.

امکانات:

-   درخواست گروهی و ارسال توسط آستانه یا فاصله.
-   چند سرور از راه دور
-   احراز هویت پایه

اجرا شده در برو

</div>

[مقاله اصلی](https://clickhouse.tech/docs/fa/interfaces/third-party/proxy/) <!--hide-->
