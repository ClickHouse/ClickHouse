---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 14
toc_title: "\u0632\u0645\u06CC\u0646 \u0628\u0627\u0632\u06CC"
---

# تاتر زمین بازی {#clickhouse-playground}

[تاتر زمین بازی](https://play.clickhouse.tech?file=welcome) اجازه می دهد تا مردم را به تجربه با تاتر در حال اجرا نمایش داده شد فورا, بدون راه اندازی سرور و یا خوشه خود را.
چند مجموعه داده به عنوان مثال در زمین بازی و همچنین نمونه نمایش داده شد که نشان می دهد ویژگی های تاتر در دسترس هستند.

نمایش داده شد به عنوان یک کاربر فقط خواندنی اجرا شده است. این نشان میدهد برخی از محدودیت:

-   پرسشهای دادل مجاز نیستند
-   درج نمایش داده شد امکان پذیر نیست

تنظیمات زیر نیز اجرا می شوند:
- [`max_result_bytes=10485760`](../operations/settings/query_complexity/#max-result-bytes)
- [`max_result_rows=2000`](../operations/settings/query_complexity/#setting-max_result_rows)
- [`result_overflow_mode=break`](../operations/settings/query_complexity/#result-overflow-mode)
- [`max_execution_time=60000`](../operations/settings/query_complexity/#max-execution-time)

زمین بازی کلیک می دهد تجربه متر2.کوچک
[خدمات مدیریت شده برای کلیک](https://cloud.yandex.com/services/managed-clickhouse)
به عنوان مثال میزبانی شده در [یاندکسابر](https://cloud.yandex.com/).
اطلاعات بیشتر در مورد [ابر دهندگان](../commercial/cloud.md).

ClickHouse زمین بازی و رابط کاربری وب سایت باعث می شود درخواست از طریق ClickHouse [HTTP API](../interfaces/http.md).
باطن زمین بازی فقط یک خوشه محل کلیک بدون هیچ گونه نرم افزار سمت سرور اضافی است.
نقطه پایانی کلیک اچتیتیپس نیز به عنوان بخشی از زمین بازی در دسترس است.

شما می توانید نمایش داده شد به زمین بازی با استفاده از هر مشتری قام را, مثلا [حلقه](https://curl.haxx.se) یا [عناصر](https://www.gnu.org/software/wget/), و یا راه اندازی یک اتصال با استفاده از [JDBC](../interfaces/jdbc.md) یا [ODBC](../interfaces/odbc.md) رانندگان.
اطلاعات بیشتر در مورد محصولات نرم افزاری است که پشتیبانی از تاتر در دسترس است [اینجا](../interfaces/index.md).

| پارامتر     | مقدار                                    |
|:------------|:-----------------------------------------|
| نقطه پایانی | https://play-api.فاحشه خانه.فناوری: 8443 |
| کاربر       | `playground`                             |
| اسم رمز     | `clickhouse`                             |

توجه داشته باشید که این نقطه پایانی نیاز به یک اتصال امن.

مثال:

``` bash
curl "https://play-api.clickhouse.tech:8443/?query=SELECT+'Play+ClickHouse!';&user=playground&password=clickhouse&database=datasets"
```
