---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 37
toc_title: "\u062A\u0631\u06A9\u06CC\u0628 \u06A9\u0646\u0646\u062F\u0647\u0647\u0627\
  \u06CC \u062A\u0627\u0628\u0639 \u062C\u0645\u0639"
---

# ترکیب کنندههای تابع جمع {#aggregate_functions_combinators}

نام یک تابع جمع می تواند یک پسوند اضافه شده است. این تغییر راه تابع کلی کار می کند.

## - اگر {#agg-functions-combinator-if}

The suffix -If can be appended to the name of any aggregate function. In this case, the aggregate function accepts an extra argument – a condition (Uint8 type). The aggregate function processes only the rows that trigger the condition. If the condition was not triggered even once, it returns a default value (usually zeros or empty strings).

مثالها: `sumIf(column, cond)`, `countIf(cond)`, `avgIf(x, cond)`, `quantilesTimingIf(level1, level2)(x, cond)`, `argMinIf(arg, val, cond)` و به همین ترتیب.

با توابع مجموع شرطی, شما می توانید مصالح برای چندین شرایط در یک بار محاسبه, بدون استفاده از کارخانه های فرعی و `JOIN`برای مثال در یاندکس.متریکا, توابع مجموع مشروط استفاده می شود برای پیاده سازی قابلیت مقایسه بخش.

## حداقل صفحه نمایش: {#agg-functions-combinator-array}

پسوند مجموعه را می توان به هر تابع جمع اضافه شده است. در این مورد, تابع کل استدلال از طول می کشد ‘Array(T)’ نوع (ارریس) به جای ‘T’ استدلال نوع. اگر تابع جمع استدلال های متعدد را می پذیرد, این باید مجموعه ای از طول های برابر شود. هنگامی که پردازش ارریس, تابع کل کار می کند مانند تابع کل اصلی در تمام عناصر مجموعه.

مثال 1: `sumArray(arr)` - مجموع تمام عناصر از همه ‘arr’ اراریس در این مثال می توانست بیشتر به سادگی نوشته شده است: `sum(arraySum(arr))`.

مثال 2: `uniqArray(arr)` – Counts the number of unique elements in all ‘arr’ اراریس این می تواند انجام شود یک راه ساده تر: `uniq(arrayJoin(arr))` اما همیشه امکان اضافه کردن وجود ندارد ‘arrayJoin’ به پرس و جو.

\- اگر و مجموعه ای می تواند ترکیب شود. هرچند, ‘Array’ پس اول باید بیای ‘If’. مثالها: `uniqArrayIf(arr, cond)`, `quantilesTimingArrayIf(level1, level2)(arr, cond)`. با توجه به این سفارش ‘cond’ برهان صف نیست.

## - وضعیت {#agg-functions-combinator-state}

اگر شما درخواست این ترکیب, تابع کل می کند مقدار حاصل بازگشت نیست (مانند تعدادی از ارزش های منحصر به فرد برای [uniq](reference.md#agg_function-uniq) تابع) , اما یک دولت متوسط از تجمع (برای `uniq`, این جدول هش برای محاسبه تعداد ارزش های منحصر به فرد است). این یک `AggregateFunction(...)` که می تواند برای پردازش بیشتر استفاده می شود و یا ذخیره شده در یک جدول را به پایان برساند جمع بعد.

برای کار با این کشورها, استفاده:

-   [ریزدانه](../../engines/table-engines/mergetree-family/aggregatingmergetree.md) موتور جدول.
-   [پلاکتی](../../sql-reference/functions/other-functions.md#function-finalizeaggregation) تابع.
-   [خرابی اجرا](../../sql-reference/functions/other-functions.md#function-runningaccumulate) تابع.
-   [- ادغام](#aggregate_functions_combinators-merge) ترکیب کننده.
-   [اطلاعات دقیق](#aggregate_functions_combinators-mergestate) ترکیب کننده.

## - ادغام {#aggregate_functions_combinators-merge}

اگر شما درخواست این ترکیب, تابع کل طول می کشد حالت تجمع متوسط به عنوان یک استدلال, ترکیبی از کشورهای به پایان تجمع, و ارزش حاصل می گرداند.

## اطلاعات دقیق {#aggregate_functions_combinators-mergestate}

ادغام کشورهای تجمع متوسط در همان راه به عنوان ترکیب-ادغام. با این حال, این مقدار حاصل بازگشت نیست, اما یک دولت تجمع متوسط, شبیه به ترکیب دولت.

## - فورچ {#agg-functions-combinator-foreach}

تبدیل یک تابع جمع برای جداول به یک تابع کلی برای ارریس که جمع اقلام مجموعه مربوطه و مجموعه ای از نتایج را برمی گرداند. به عنوان مثال, `sumForEach` برای ارریس `[1, 2]`, `[3, 4, 5]`و`[6, 7]`نتیجه را برمی گرداند `[10, 13, 5]` پس از اضافه کردن با هم موارد مجموعه مربوطه.

## شناسه بسته: {#agg-functions-combinator-ordefault}

پر مقدار پیش فرض از نوع بازگشت تابع جمع است اگر چیزی برای جمع وجود دارد.

``` sql
SELECT avg(number), avgOrDefault(number) FROM numbers(0)
```

``` text
┌─avg(number)─┬─avgOrDefault(number)─┐
│         nan │                    0 │
└─────────────┴──────────────────────┘
```

## اطلاعات دقیق {#agg-functions-combinator-ornull}

پر `null` در صورتی که هیچ چیز به جمع وجود دارد. ستون بازگشت قابل ابطال خواهد بود.

``` sql
SELECT avg(number), avgOrNull(number) FROM numbers(0)
```

``` text
┌─avg(number)─┬─avgOrNull(number)─┐
│         nan │              ᴺᵁᴸᴸ │
└─────────────┴───────────────────┘
```

-OrDefault و OrNull می تواند در ترکیب با دیگر combinators. این زمانی مفید است که تابع جمع می کند ورودی خالی را قبول نمی کند.

``` sql
SELECT avgOrNullIf(x, x > 10)
FROM
(
    SELECT toDecimal32(1.23, 2) AS x
)
```

``` text
┌─avgOrNullIf(x, greater(x, 10))─┐
│                           ᴺᵁᴸᴸ │
└────────────────────────────────┘
```

## - نمونه {#agg-functions-combinator-resample}

به شما امکان می دهد داده ها را به گروه تقسیم کنید و سپس به طور جداگانه داده ها را در این گروه ها جمع کنید. گروه ها با تقسیم مقادیر از یک ستون به فواصل ایجاد شده است.

``` sql
<aggFunction>Resample(start, end, step)(<aggFunction_params>, resampling_key)
```

**پارامترها**

-   `start` — Starting value of the whole required interval for `resampling_key` ارزشهای خبری عبارتند از:
-   `stop` — Ending value of the whole required interval for `resampling_key` ارزشهای خبری عبارتند از: کل فاصله شامل نمی شود `stop` مقدار `[start, stop)`.
-   `step` — Step for separating the whole interval into subintervals. The `aggFunction` بیش از هر یک از این زیرگروه اعدام به طور مستقل.
-   `resampling_key` — Column whose values are used for separating data into intervals.
-   `aggFunction_params` — `aggFunction` پارامترها

**مقادیر بازگشتی**

-   مجموعه ای از `aggFunction` نتایج جستجو برای هر subinterval.

**مثال**

در نظر بگیرید که `people` جدول با داده های زیر:

``` text
┌─name───┬─age─┬─wage─┐
│ John   │  16 │   10 │
│ Alice  │  30 │   15 │
│ Mary   │  35 │    8 │
│ Evelyn │  48 │ 11.5 │
│ David  │  62 │  9.9 │
│ Brian  │  60 │   16 │
└────────┴─────┴──────┘
```

بیایید نام افرادی که سن نهفته در فواصل `[30,60)` و `[60,75)`. پس ما با استفاده از نمایندگی عدد صحیح برای سن, ما سنین در `[30, 59]` و `[60,74]` فواصل زمانی.

به نام کلی در مجموعه, ما با استفاده از [گرامری](reference.md#agg_function-grouparray) تابع جمع. طول می کشد تا یک استدلال. در مورد ما این است `name` ستون. این `groupArrayResample` تابع باید از `age` ستون به نام دانه های سن. برای تعریف فواصل مورد نیاز ما `30, 75, 30` نشانوندها به `groupArrayResample` تابع.

``` sql
SELECT groupArrayResample(30, 75, 30)(name, age) FROM people
```

``` text
┌─groupArrayResample(30, 75, 30)(name, age)─────┐
│ [['Alice','Mary','Evelyn'],['David','Brian']] │
└───────────────────────────────────────────────┘
```

در نظر گرفتن نتایج.

`Jonh` خارج از نمونه است چرا که او بیش از حد جوان است. افراد دیگر با توجه به فواصل زمانی مشخص شده توزیع می شوند.

حالا اجازه دهید تعداد کل مردم و متوسط دستمزد خود را در فواصل سنی مشخص شده است.

``` sql
SELECT
    countResample(30, 75, 30)(name, age) AS amount,
    avgResample(30, 75, 30)(wage, age) AS avg_wage
FROM people
```

``` text
┌─amount─┬─avg_wage──────────────────┐
│ [3,2]  │ [11.5,12.949999809265137] │
└────────┴───────────────────────────┘
```

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/agg_functions/combinators/) <!--hide-->
