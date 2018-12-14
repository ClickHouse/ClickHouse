<div dir="rtl" markdown="1">

# ویژگی های از ClickHouse که می تواند معایبی باشد.

1. بدون پشتیبانی کامل از تراکنش
2. عدم توانایی برای تغییر و یا حذف داده های در حال حاضر وارد شده با سرعت بالا و تاخیر کم. برای پاک کردن و یا اصلاح داده ها، به عنوان مثال برای پیروی از [GDPR](https://gdpr-info.eu)، دسته ای پاک و به روزرسانی وجود دارد.حال توسعه می باشد.
3. Sparse index باعث می شود ClickHouse چندان مناسب اجرای پرسمان های point query برای دریافت یک ردیف از داده ها با استفاده از کلید آنها نباشد.

</div>

[مقاله اصلی](https://clickhouse.yandex/docs/fa/introduction/features_considered_disadvantages/) <!--hide-->
