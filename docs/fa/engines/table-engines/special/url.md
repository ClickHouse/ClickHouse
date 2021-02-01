---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 41
toc_title: URL
---

# آدرس(URL, قالب) {#table_engines-url}

مدیریت داده ها بر روی یک سرور کنترل از راه دور قام/قام. این موتور مشابه است
به [پرونده](file.md) موتور

## با استفاده از موتور در سرور کلیک {#using-the-engine-in-the-clickhouse-server}

این `format` باید یکی باشد که کلیک خانه می تواند در استفاده از
`SELECT` نمایش داده شد و, در صورت لزوم, به `INSERTs`. برای لیست کامل از فرمت های پشتیبانی شده, دیدن
[فرشها](../../../interfaces/formats.md#formats).

این `URL` باید به ساختار یاب منابع یکنواخت مطابقت داشته باشد. نشانی وب مشخصشده باید به کارگزار اشاره کند
که با استفاده از قام یا قام. این هیچ نیاز ندارد
هدر اضافی برای گرفتن پاسخ از سرور.

`INSERT` و `SELECT` نمایش داده شد به تبدیل `POST` و `GET` درخواست ها,
به ترتیب. برای پردازش `POST` درخواست, سرور از راه دور باید پشتیبانی
[کدگذاری انتقال داده شده](https://en.wikipedia.org/wiki/Chunked_transfer_encoding).

شما می توانید حداکثر تعداد قام را محدود کنید تغییر مسیر هاپ به کواس با استفاده از [عناصر](../../../operations/settings/settings.md#setting-max_http_get_redirects) تنظیمات.

**مثال:**

**1.** ایجاد یک `url_engine_table` جدول روی کارگزار :

``` sql
CREATE TABLE url_engine_table (word String, value UInt64)
ENGINE=URL('http://127.0.0.1:12345/', CSV)
```

**2.** ایجاد یک سرور اساسی قام با استفاده از پایتون استاندارد 3 ابزار و
شروع کن:

``` python3
from http.server import BaseHTTPRequestHandler, HTTPServer

class CSVHTTPServer(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/csv')
        self.end_headers()

        self.wfile.write(bytes('Hello,1\nWorld,2\n', "utf-8"))

if __name__ == "__main__":
    server_address = ('127.0.0.1', 12345)
    HTTPServer(server_address, CSVHTTPServer).serve_forever()
```

``` bash
$ python3 server.py
```

**3.** درخواست اطلاعات:

``` sql
SELECT * FROM url_engine_table
```

``` text
┌─word──┬─value─┐
│ Hello │     1 │
│ World │     2 │
└───────┴───────┘
```

## اطلاعات پیاده سازی {#details-of-implementation}

-   می خواند و می نویسد می تواند موازی
-   پشتیبانی نمیشود:
    -   `ALTER` و `SELECT...SAMPLE` عملیات.
    -   شاخص.
    -   تکرار.

[مقاله اصلی](https://clickhouse.tech/docs/en/operations/table_engines/url/) <!--hide-->
