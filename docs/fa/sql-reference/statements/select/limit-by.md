---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# محدود کردن بند {#limit-by-clause}

پرس و جو با `LIMIT n BY expressions` بند اول را انتخاب می کند `n` ردیف برای هر مقدار مجزا از `expressions`. کلید برای `LIMIT BY` می تواند شامل هر تعداد از [عبارتها](../../syntax.md#syntax-expressions).

تاتر از انواع نحو زیر:

-   `LIMIT [offset_value, ]n BY expressions`
-   `LIMIT n OFFSET offset_value BY expressions`

در طول پردازش پرس و جو, خانه را انتخاب داده دستور داد با مرتب سازی کلید. کلید مرتب سازی به صراحت با استفاده از یک مجموعه [ORDER BY](order-by.md) بند یا به طور ضمنی به عنوان یک ویژگی از موتور جدول. سپس کلیک کنیداوس اعمال می شود `LIMIT n BY expressions` و اولین را برمی گرداند `n` ردیف برای هر ترکیب مجزا از `expressions`. اگر `OFFSET` مشخص شده است, سپس برای هر بلوک داده که متعلق به یک ترکیب متمایز از `expressions`. `offset_value` تعداد ردیف از ابتدای بلوک و حداکثر می گرداند `n` ردیف به عنوان یک نتیجه. اگر `offset_value` بزرگتر از تعدادی از ردیف در بلوک داده است, کلیک بازگشت صفر ردیف از بلوک.

!!! note "یادداشت"
    `LIMIT BY` مربوط به [LIMIT](limit.md). هر دو را می توان در همان پرس و جو استفاده کرد.

## مثالها {#examples}

جدول نمونه:

``` sql
CREATE TABLE limit_by(id Int, val Int) ENGINE = Memory;
INSERT INTO limit_by VALUES (1, 10), (1, 11), (1, 12), (2, 20), (2, 21);
```

نمایش داده شد:

``` sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 2 BY id
```

``` text
┌─id─┬─val─┐
│  1 │  10 │
│  1 │  11 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

``` sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 1, 2 BY id
```

``` text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

این `SELECT * FROM limit_by ORDER BY id, val LIMIT 2 OFFSET 1 BY id` پرس و جو همان نتیجه را برمی گرداند.

پرس و جو زیر را برمی گرداند بالا 5 ارجاع برای هر `domain, device_type` جفت با حداکثر 100 ردیف در مجموع (`LIMIT n BY + LIMIT`).

``` sql
SELECT
    domainWithoutWWW(URL) AS domain,
    domainWithoutWWW(REFERRER_URL) AS referrer,
    device_type,
    count() cnt
FROM hits
GROUP BY domain, referrer, device_type
ORDER BY cnt DESC
LIMIT 5 BY domain, device_type
LIMIT 100
```
