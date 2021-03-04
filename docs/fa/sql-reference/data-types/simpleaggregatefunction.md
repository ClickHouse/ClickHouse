---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# عملکرد پلاکتی {#data-type-simpleaggregatefunction}

`SimpleAggregateFunction(name, types_of_arguments…)` نوع داده ها ارزش فعلی عملکرد کل را ذخیره می کند و حالت کامل خود را ذخیره نمی کند [`AggregateFunction`](aggregatefunction.md) چرا این بهینه سازی را می توان به توابع که اموال زیر را نگه می دارد اعمال می شود: در نتیجه استفاده از یک تابع `f` به مجموعه ردیف `S1 UNION ALL S2` می توان با استفاده از `f` به بخش هایی از مجموعه ردیف به طور جداگانه, و سپس دوباره استفاده `f` به نتایج: `f(S1 UNION ALL S2) = f(f(S1) UNION ALL f(S2))`. این ویژگی تضمین می کند که نتایج تجمع بخشی به اندازه کافی برای محاسبه یک ترکیب هستند, بنابراین ما لازم نیست برای ذخیره و پردازش هر گونه اطلاعات اضافی.

توابع زیر مجموع پشتیبانی می شوند:

-   [`any`](../../sql-reference/aggregate-functions/reference.md#agg_function-any)
-   [`anyLast`](../../sql-reference/aggregate-functions/reference.md#anylastx)
-   [`min`](../../sql-reference/aggregate-functions/reference.md#agg_function-min)
-   [`max`](../../sql-reference/aggregate-functions/reference.md#agg_function-max)
-   [`sum`](../../sql-reference/aggregate-functions/reference.md#agg_function-sum)
-   [`groupBitAnd`](../../sql-reference/aggregate-functions/reference.md#groupbitand)
-   [`groupBitOr`](../../sql-reference/aggregate-functions/reference.md#groupbitor)
-   [`groupBitXor`](../../sql-reference/aggregate-functions/reference.md#groupbitxor)

مقادیر `SimpleAggregateFunction(func, Type)` نگاه و ذخیره شده به همان شیوه به عنوان `Type` بنابراین شما نیازی به اعمال توابع با `-Merge`/`-State` پسوندها. `SimpleAggregateFunction` عملکرد بهتر از `AggregateFunction` با همان تابع تجمع.

**پارامترها**

-   نام تابع جمع.
-   انواع استدلال تابع جمع.

**مثال**

``` sql
CREATE TABLE t
(
    column1 SimpleAggregateFunction(sum, UInt64),
    column2 SimpleAggregateFunction(any, String)
) ENGINE = ...
```

[مقاله اصلی](https://clickhouse.tech/docs/en/data_types/simpleaggregatefunction/) <!--hide-->
