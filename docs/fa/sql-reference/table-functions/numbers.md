---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 39
toc_title: "\u0627\u0639\u062F\u0627\u062F"
---

# اعداد {#numbers}

`numbers(N)` – Returns a table with the single ‘number’ ستون (اوینت64)که شامل اعداد صحیح از 0 تا 1.
`numbers(N, M)` - بازگرداندن یک جدول با تک ‘number’ ستون (اوینت64) که شامل اعداد صحیح از نفر به (نفر + متر - 1).

شبیه به `system.numbers` جدول را می توان برای تست و تولید مقادیر پی در پی استفاده کرد, `numbers(N, M)` کارایی بیشتر از `system.numbers`.

نمایش داده شد زیر معادل هستند:

``` sql
SELECT * FROM numbers(10);
SELECT * FROM numbers(0, 10);
SELECT * FROM system.numbers LIMIT 10;
```

مثالها:

``` sql
-- Generate a sequence of dates from 2010-01-01 to 2010-12-31
select toDate('2010-01-01') + number as d FROM numbers(365);
```

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/table_functions/numbers/) <!--hide-->
