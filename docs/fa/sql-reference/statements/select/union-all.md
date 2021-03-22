---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# اتحادیه همه بند {#union-all-clause}

شما می توانید استفاده کنید `UNION ALL` برای ترکیب هر تعداد از `SELECT` نمایش داده شد با گسترش نتایج خود را. مثال:

``` sql
SELECT CounterID, 1 AS table, toInt64(count()) AS c
    FROM test.hits
    GROUP BY CounterID

UNION ALL

SELECT CounterID, 2 AS table, sum(Sign) AS c
    FROM test.visits
    GROUP BY CounterID
    HAVING c > 0
```

ستون نتیجه با شاخص خود را همسان (سفارش در داخل `SELECT`). اگر نام ستون مطابقت ندارند, نام برای نتیجه نهایی از پرس و جو برای اولین بار گرفته شده.

نوع ریخته گری برای اتحادیه انجام می شود. برای مثال اگر دو نمایش داده شد در حال ترکیب باید همین زمینه را با غیر-`Nullable` و `Nullable` انواع از یک نوع سازگار, در نتیجه `UNION ALL` دارای یک `Nullable` نوع درست.

نمایش داده شد که بخش هایی از `UNION ALL` نمی توان در براکت های گرد محصور کرد. [ORDER BY](order-by.md) و [LIMIT](limit.md) برای نمایش داده شد جداگانه به نتیجه نهایی اعمال می شود. اگر شما نیاز به اعمال تبدیل به نتیجه نهایی شما می توانید تمام نمایش داده شد با قرار دادن `UNION ALL` در یک خرده فروشی در [FROM](from.md) بند بند.

## محدودیت ها {#limitations}

فقط `UNION ALL` پشتیبانی می شود. منظم `UNION` (`UNION DISTINCT`) پشتیبانی نمی شود . اگر شما نیاز دارید `UNION DISTINCT`, شما می توانید ارسال `SELECT DISTINCT` از زیرخاکری حاوی `UNION ALL`.

## پیاده سازی اطلاعات {#implementation-details}

نمایش داده شد که بخش هایی از `UNION ALL` می توان به طور همزمان اجرا, و نتایج خود را می توان با هم مخلوط.
