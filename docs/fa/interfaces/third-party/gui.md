<div markdown="1" markdown="1" dir="rtl">

# interface های visual توسعه دهندگان third-party {#interface-hy-visual-tws-h-dhndgn-third-party}

## متن باز {#mtn-bz}

### Tabix {#tabix}

interface تحت وب برای ClickHouse در پروژه [Tabix](https://github.com/tabixio/tabix).

ویژگی ها:

-   کار با ClickHouse به صورت مستقیم و از طریق مرورگر، بدون نیاز به نرم افزار اضافی.
-   ادیتور query به همراه syntax highlighting.
-   ویژگی Auto-completion برای دستورات.
-   ابزارهایی برای آنالیز گرافیکی اجرای query.
-   گزینه های Color scheme.

[مستندات Tabix](https://tabix.io/doc/).

### HouseOps {#houseops}

[HouseOps](https://github.com/HouseOps/HouseOps) نرم افزار Desktop برای سیستم عامل های Linux و OSX و Windows می باشد..

ویژگی ها:

-   ابزار Query builder به همراه syntax highlighting. نمایش نتایج به صورت جدول و JSON Object.
-   خروجی نتایج به صورت csv و JSON Object.
-   Pنمایش Processes List ها به همراه توضیحات، ویژگی حالت record و kill کردن process ها.
-   نمودار دیتابیس به همراه تمام جداول و ستون ها به همراه اطلاعات اضافی.
-   نماش آسان سایز ستون ها.
-   تنظیمات سرور.
-   مدیریت دیتابیس (بزودی);
-   مدیریت کاربران (بزودی);
-   آنالیز داده ها به صورت Real-Time (بزودی);
-   مانیتورینگ کلاستر/زیرساخت (بزودی);
-   مدیریت کلاستر (بزودی);
-   مانیتورینگ کافکا و جداول replicate (بزودی);
-   و بسیاری از ویژگی های دیگر برای شما.

### LightHouse {#lighthouse}

[LightHouse](https://github.com/VKCOM/lighthouse) رابط کاربری سبک وزن برای ClickHouse است.

امکانات:

-   لیست جدول با فیلتر کردن و ابرداده.
-   پیش نمایش جدول با فیلتر کردن و مرتب سازی.
-   اعداد نمایش داده شده فقط خواندنی

### DBeaver {#dbeaver}

[DBeaver](https://dbeaver.io/) - مشتری دسکتاپ دسکتاپ دسکتاپ با پشتیبانی ClickHouse.

امکانات:

-   توسعه پرس و جو با برجسته نحو
-   پیش نمایش جدول
-   تکمیل خودکار

### clickhouse-cli {#clickhouse-cli}

[clickhouse-cli](https://github.com/hatarist/clickhouse-cli) یک مشتری خط فرمان برای ClickHouse است که در پایتون 3 نوشته شده است.

امکانات:
- تکمیل خودکار
- نحو برجسته برای نمایش داده ها و خروجی داده ها.
- پشتیبانی از Pager برای خروجی داده.
- دستورات پست سفارشی مانند PostgreSQL.

### clickhouse-flamegraph {#clickhouse-flamegraph}

[clickhouse-flamegraph](https://github.com/Slach/clickhouse-flamegraph) یک ابزار تخصصی برای تجسم است `system.trace_log`مانند[flamegraph](http://www.brendangregg.com/flamegraphs.html).

## تجاری {#tjry}

### DataGrip {#datagrip}

[DataGrip](https://www.jetbrains.com/datagrip/) IDE پایگاه داده از JetBrains با پشتیبانی اختصاصی برای ClickHouse است. این ابزار همچنین به سایر ابزارهای مبتنی بر IntelliJ تعبیه شده است: PyCharm، IntelliJ IDEA، GoLand، PhpStorm و دیگران.

امکانات:

-   تکمیل کد بسیار سریع
-   نحو برجسته ClickHouse.
-   پشتیبانی از ویژگی های خاص برای ClickHouse، برای مثال ستون های توپی، موتورهای جدول.
-   ویرایشگر داده.
-   Refactorings.
-   جستجو و ناوبری

</div>

[مقاله اصلی](https://clickhouse.tech/docs/fa/interfaces/third-party/gui/) <!--hide-->
