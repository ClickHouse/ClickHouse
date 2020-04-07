---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 37
toc_title: "\u0627\u067E\u0631\u0627\u062A\u0648\u0631\u0647\u0627"
---

# اپراتورها {#operators}

همه اپراتورها در حال تبدیل به توابع مربوط به خود را در پرس و جو و تجزیه مرحله مطابق با اولویت و associativity.
گروه اپراتورهای به ترتیب اولویت ذکر شده (بالاتر در لیست است, زودتر اپراتور به استدلال خود متصل).

## اپراتورهای دسترسی {#access-operators}

`a[N]` – Access to an element of an array. The `arrayElement(a, N)` تابع.

`a.N` – Access to a tuple element. The `tupleElement(a, N)` تابع.

## اپراتور نفی عددی {#numeric-negation-operator}

`-a` – The `negate (a)` تابع.

## اپراتورهای ضرب و تقسیم {#multiplication-and-division-operators}

`a * b` – The `multiply (a, b)` تابع.

`a / b` – The `divide(a, b)` تابع.

`a % b` – The `modulo(a, b)` تابع.

## اپراتورهای جمع و تفریق {#addition-and-subtraction-operators}

`a + b` – The `plus(a, b)` تابع.

`a - b` – The `minus(a, b)` تابع.

## مقایسه اپراتورها {#comparison-operators}

`a = b` – The `equals(a, b)` تابع.

`a == b` – The `equals(a, b)` تابع.

`a != b` – The `notEquals(a, b)` تابع.

`a <> b` – The `notEquals(a, b)` تابع.

`a <= b` – The `lessOrEquals(a, b)` تابع.

`a >= b` – The `greaterOrEquals(a, b)` تابع.

`a < b` – The `less(a, b)` تابع.

`a > b` – The `greater(a, b)` تابع.

`a LIKE s` – The `like(a, b)` تابع.

`a NOT LIKE s` – The `notLike(a, b)` تابع.

`a BETWEEN b AND c` – The same as `a >= b AND a <= c`.

`a NOT BETWEEN b AND c` – The same as `a < b OR a > c`.

## اپراتورها برای کار با مجموعه داده ها {#operators-for-working-with-data-sets}

*ببینید [در اپراتورها](statements/select.md#select-in-operators).*

`a IN ...` – The `in(a, b)` تابع.

`a NOT IN ...` – The `notIn(a, b)` تابع.

`a GLOBAL IN ...` – The `globalIn(a, b)` تابع.

`a GLOBAL NOT IN ...` – The `globalNotIn(a, b)` تابع.

## اپراتورها برای کار با تاریخ و زمان {#operators-datetime}

### EXTRACT {#operator-extract}

``` sql
EXTRACT(part FROM date);
```

عصاره بخشی از یک تاریخ معین. مثلا, شما می توانید یک ماه از یک تاریخ معین بازیابی, یا یک ثانیه از یک زمان.

این `part` پارامتر مشخص می کند که بخشی از تاریخ برای بازیابی. مقادیر زیر در دسترس هستند:

-   `DAY` — The day of the month. Possible values: 1–31.
-   `MONTH` — The number of a month. Possible values: 1–12.
-   `YEAR` — The year.
-   `SECOND` — The second. Possible values: 0–59.
-   `MINUTE` — The minute. Possible values: 0–59.
-   `HOUR` — The hour. Possible values: 0–23.

این `part` پارامتر غیر حساس به حروف است.

این `date` پارامتر تاریخ یا زمان پردازش را مشخص می کند. هر دو [تاریخ](../sql_reference/data_types/date.md) یا [DateTime](../sql_reference/data_types/datetime.md) نوع پشتیبانی می شود.

مثالها:

``` sql
SELECT EXTRACT(DAY FROM toDate('2017-06-15'));
SELECT EXTRACT(MONTH FROM toDate('2017-06-15'));
SELECT EXTRACT(YEAR FROM toDate('2017-06-15'));
```

در مثال زیر ما ایجاد یک جدول و قرار دادن به آن یک مقدار با `DateTime` نوع.

``` sql
CREATE TABLE test.Orders
(
    OrderId UInt64,
    OrderName String,
    OrderDate DateTime
)
ENGINE = Log;
```

``` sql
INSERT INTO test.Orders VALUES (1, 'Jarlsberg Cheese', toDateTime('2008-10-11 13:23:44'));
```

``` sql
SELECT
    toYear(OrderDate) AS OrderYear,
    toMonth(OrderDate) AS OrderMonth,
    toDayOfMonth(OrderDate) AS OrderDay,
    toHour(OrderDate) AS OrderHour,
    toMinute(OrderDate) AS OrderMinute,
    toSecond(OrderDate) AS OrderSecond
FROM test.Orders;
```

``` text
┌─OrderYear─┬─OrderMonth─┬─OrderDay─┬─OrderHour─┬─OrderMinute─┬─OrderSecond─┐
│      2008 │         10 │       11 │        13 │          23 │          44 │
└───────────┴────────────┴──────────┴───────────┴─────────────┴─────────────┘
```

شما می توانید نمونه های بیشتری را در [تستها](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00619_extract.sql).

### INTERVAL {#operator-interval}

ایجاد یک [فاصله](../sql_reference/data_types/special_data_types/interval.md)- ارزش نوع است که باید در عملیات ریاضی با استفاده [تاریخ](../sql_reference/data_types/date.md) و [DateTime](../sql_reference/data_types/datetime.md)- ارزش نوع .

انواع فواصل:
- `SECOND`
- `MINUTE`
- `HOUR`
- `DAY`
- `WEEK`
- `MONTH`
- `QUARTER`
- `YEAR`

!!! warning "اخطار"
    فواصل با انواع مختلف نمی تواند ترکیب شود. شما می توانید عبارات مانند استفاده کنید `INTERVAL 4 DAY 1 HOUR`. بیان فواصل در واحد است که کوچکتر یا مساوی کوچکترین واحد فاصله برای مثال `INTERVAL 25 HOUR`. شما می توانید عملیات تبعی مانند مثال زیر استفاده کنید.

مثال:

``` sql
SELECT now() AS current_date_time, current_date_time + INTERVAL 4 DAY + INTERVAL 3 HOUR
```

``` text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay(4)), toIntervalHour(3))─┐
│ 2019-10-23 11:16:28 │                                    2019-10-27 14:16:28 │
└─────────────────────┴────────────────────────────────────────────────────────┘
```

**همچنین نگاه کنید**

-   [فاصله](../sql_reference/data_types/special_data_types/interval.md) نوع داده
-   [توینتروال](../sql_reference/functions/type_conversion_functions.md#function-tointerval) توابع تبدیل نوع

## اپراتور نفی منطقی {#logical-negation-operator}

`NOT a` – The `not(a)` تابع.

## منطقی و اپراتور {#logical-and-operator}

`a AND b` – The`and(a, b)` تابع.

## منطقی یا اپراتور {#logical-or-operator}

`a OR b` – The `or(a, b)` تابع.

## اپراتور شرطی {#conditional-operator}

`a ? b : c` – The `if(a, b, c)` تابع.

یادداشت:

اپراتور مشروط محاسبه ارزش ب و ج, سپس چک چه شرایط ملاقات کرده است, و سپس مقدار مربوطه را برمی گرداند. اگر `b` یا `C` یک [ارریجین()](../sql_reference/functions/array_join.md#functions_arrayjoin) عملکرد هر سطر خواهد بود تکرار صرف نظر از “a” شرط.

## عبارت شرطی {#operator_case}

``` sql
CASE [x]
    WHEN a THEN b
    [WHEN ... THEN ...]
    [ELSE c]
END
```

اگر `x` مشخص شده است, سپس `transform(x, [a, ...], [b, ...], c)` function is used. Otherwise – `multiIf(a, b, ..., c)`.

اگر وجود ندارد `ELSE c` بند در بیان, مقدار پیش فرض است `NULL`.

این `transform` تابع با کار نمی کند `NULL`.

## اپراتور الحاق {#concatenation-operator}

`s1 || s2` – The `concat(s1, s2) function.`

## لامبدا اپراتور ایجاد {#lambda-creation-operator}

`x -> expr` – The `lambda(x, expr) function.`

اپراتورهای زیر یک اولویت ندارد, از براکت هستند:

## اپراتور ایجاد مجموعه {#array-creation-operator}

`[x1, ...]` – The `array(x1, ...) function.`

## اپراتور ایجاد تاپل {#tuple-creation-operator}

`(x1, x2, ...)` – The `tuple(x2, x2, ...) function.`

## Associativity {#associativity}

همه اپراتورهای دودویی انجمن را ترک کرده اند. به عنوان مثال, `1 + 2 + 3` تبدیل به `plus(plus(1, 2), 3)`.
گاهی اوقات این راه شما انتظار می رود کار نمی کند. به عنوان مثال, `SELECT 4 > 2 > 3` در نتیجه 0.

برای بهره وری `and` و `or` توابع قبول هر تعداد از استدلال. زنجیره های مربوطه از `AND` و `OR` اپراتورها به یک تماس از این توابع تبدیل شده است.

## در حال بررسی برای `NULL` {#checking-for-null}

تاتر از `IS NULL` و `IS NOT NULL` اپراتورها.

### IS NULL {#operator-is-null}

-   برای [Nullable](../sql_reference/data_types/nullable.md) مقادیر نوع `IS NULL` بازگشت اپراتور:
    -   `1` اگر مقدار باشد `NULL`.
    -   `0` وگرنه
-   برای ارزش های دیگر `IS NULL` اپراتور همیشه باز می گردد `0`.

<!-- -->

``` sql
SELECT x+100 FROM t_null WHERE y IS NULL
```

``` text
┌─plus(x, 100)─┐
│          101 │
└──────────────┘
```

### IS NOT NULL {#is-not-null}

-   برای [Nullable](../sql_reference/data_types/nullable.md) مقادیر نوع `IS NOT NULL` بازگشت اپراتور:
    -   `0` اگر مقدار باشد `NULL`.
    -   `1` وگرنه
-   برای ارزش های دیگر `IS NOT NULL` اپراتور همیشه باز می گردد `1`.

<!-- -->

``` sql
SELECT * FROM t_null WHERE y IS NOT NULL
```

``` text
┌─x─┬─y─┐
│ 2 │ 3 │
└───┴───┘
```

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/operators/) <!--hide-->
