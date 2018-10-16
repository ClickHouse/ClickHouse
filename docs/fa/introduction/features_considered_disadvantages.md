<div dir="rtl" markdown="1">

# ویژگی های از ClickHouse که می تواند معایبی باشد.

1. بدون پشتیبانی کامل از تراکنش
2. عدم توانایی در تغییر و یا حذف داده های insert شده با rate بالا و latency کم. روشی برای پاک کردن دسته ای داده ها و یا مطابق با قوانین [GDPR](https://gdpr-info.eu) وجود دارد. بروزرسانی دسته ای از July 2018 در حال توسعه می باشد. 
3. Sparse index باعث می شود ClickHouse چندان مناسب اجرای پرسمان های point query برای دریافت یک ردیف از داده ها با استفاده از کلید آنها نباشد.

</div>

[مقاله اصلی](https://clickhouse.yandex/docs/fa/introduction/features_considered_disadvantages/) <!--hide-->
