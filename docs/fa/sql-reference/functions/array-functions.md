---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 46
toc_title: "\u06A9\u0627\u0631 \u0628\u0627 \u0627\u0631\u0631\u06CC\u0633"
---

# توابع برای کار با ارریس {#functions-for-working-with-arrays}

## خالی {#function-empty}

بازده 1 برای یک مجموعه خالی, یا 0 برای یک مجموعه غیر خالی.
نتیجه این نوع UInt8.
این تابع نیز برای رشته کار می کند.

## notEmpty {#function-notempty}

بازده 0 برای یک مجموعه خالی, یا 1 برای یک مجموعه غیر خالی.
نتیجه این نوع UInt8.
این تابع نیز برای رشته کار می کند.

## طول {#array_functions-length}

بازگرداندن تعداد اقلام در مجموعه.
نتیجه این نوع UInt64.
این تابع نیز برای رشته کار می کند.

## emptyArrayUInt8, emptyArrayUInt16, emptyArrayUInt32, emptyArrayUInt64 {#emptyarrayuint8-emptyarrayuint16-emptyarrayuint32-emptyarrayuint64}

## emptyArrayInt8, emptyArrayInt16, emptyArrayInt32, emptyArrayInt64 {#emptyarrayint8-emptyarrayint16-emptyarrayint32-emptyarrayint64}

## emptyArrayFloat32, emptyArrayFloat64 {#emptyarrayfloat32-emptyarrayfloat64}

## emptyArrayDate, emptyArrayDateTime {#emptyarraydate-emptyarraydatetime}

## تخت خواب {#emptyarraystring}

قبول صفر استدلال و مجموعه ای خالی از نوع مناسب را برمی گرداند.

## خالی {#emptyarraytosingle}

یک مجموعه خالی را می پذیرد و یک مجموعه یک عنصر را که برابر با مقدار پیش فرض است باز می گرداند.

## محدوده( پایان), دامنه (شروع, پایان \[, گام\]) {#rangeend-rangestart-end-step}

بازگرداندن مجموعه ای از اعداد از ابتدا تا انتها-1 به گام.
اگر استدلال `start` مشخص نشده است, به طور پیش فرض به 0.
اگر استدلال `step` مشخص نشده است, به طور پیش فرض به 1.
این رفتار تقریبا مانند پیتون `range`. اما تفاوت این است که همه نوع استدلال باید باشد `UInt` اعداد.
فقط در مورد, یک استثنا پرتاب می شود اگر ارریس با طول کل بیش از 100,000,000 عناصر در یک بلوک داده ها ایجاد.

## array(x1, …), operator \[x1, …\] {#arrayx1-operator-x1}

ایجاد مجموعه ای از استدلال تابع.
استدلال باید ثابت باشد و انواع که کوچکترین نوع رایج. حداقل یک استدلال باید تصویب شود, چرا که در غیر این صورت مشخص نیست که چه نوع از مجموعه ای برای ایجاد. به این معنا که شما نمی توانید از این تابع برای ایجاد یک مجموعه خالی استفاده کنید (برای انجام این کار از ‘emptyArray\*’ تابع در بالا توضیح داده شد).
بازگشت یک ‘Array(T)’ نوع نتیجه, جایی که ‘T’ کوچکترین نوع رایج از استدلال گذشت.

## موافقم {#arrayconcat}

ترکیبی از ارریس به عنوان استدلال گذشت.

``` sql
arrayConcat(arrays)
```

**پارامترها**

-   `arrays` – Arbitrary number of arguments of [& حذف](../../sql-reference/data-types/array.md) نوع.
    **مثال**

<!-- -->

``` sql
SELECT arrayConcat([1, 2], [3, 4], [5, 6]) AS res
```

``` text
┌─res───────────┐
│ [1,2,3,4,5,6] │
└───────────────┘
```

## هشدار داده می شود\] {#arrayelementarr-n-operator-arrn}

عنصر را با شاخص دریافت کنید `n` از مجموعه `arr`. `n` باید هر نوع عدد صحیح باشد.
شاخص ها در مجموعه ای از یک شروع می شوند.
شاخص های منفی پشتیبانی می شوند. در این مورد, این انتخاب عنصر مربوطه شماره از پایان. به عنوان مثال, `arr[-1]` اخرین وسیله ست

اگر شاخص می افتد در خارج از مرزهای مجموعه, این گرداند برخی از مقدار پیش فرض (0 برای اعداد, یک رشته خالی برای رشته, و غیره.), به جز برای مورد با یک مجموعه غیر ثابت و یک شاخص ثابت 0 (در این مورد وجود خواهد داشت یک خطا `Array indices are 1-based`).

## است (ورود, علم) {#hasarr-elem}

بررسی اینکه ‘arr’ اری ‘elem’ عنصر.
بازده 0 اگر عنصر در مجموعه نیست, یا 1 اگر.

`NULL` به عنوان یک ارزش پردازش شده است.

``` sql
SELECT has([1, 2, NULL], NULL)
```

``` text
┌─has([1, 2, NULL], NULL)─┐
│                       1 │
└─────────────────────────┘
```

## حصال {#hasall}

بررسی اینکه یک مجموعه زیر مجموعه دیگری باشد.

``` sql
hasAll(set, subset)
```

**پارامترها**

-   `set` – Array of any type with a set of elements.
-   `subset` – Array of any type with elements that should be tested to be a subset of `set`.

**مقادیر بازگشتی**

-   `1` اگر `set` شامل تمام عناصر از `subset`.
-   `0` وگرنه

**خواص عجیب و غریب**

-   مجموعه خالی زیر مجموعه ای از هر است.
-   `Null` پردازش به عنوان یک ارزش.
-   منظور از ارزش ها در هر دو ارریس مهم نیست.

**مثالها**

`SELECT hasAll([], [])` بازده 1.

`SELECT hasAll([1, Null], [Null])` بازده 1.

`SELECT hasAll([1.0, 2, 3, 4], [1, 3])` بازده 1.

`SELECT hasAll(['a', 'b'], ['a'])` بازده 1.

`SELECT hasAll([1], ['a'])` بازده 0.

`SELECT hasAll([[1, 2], [3, 4]], [[1, 2], [3, 5]])` بازده 0.

## hasAny {#hasany}

بررسی اینکه دو بند چهار راه توسط برخی از عناصر.

``` sql
hasAny(array1, array2)
```

**پارامترها**

-   `array1` – Array of any type with a set of elements.
-   `array2` – Array of any type with a set of elements.

**مقادیر بازگشتی**

-   `1` اگر `array1` و `array2` حداقل یک عنصر مشابه داشته باشید.
-   `0` وگرنه

**خواص عجیب و غریب**

-   `Null` پردازش به عنوان یک ارزش.
-   منظور از ارزش ها در هر دو ارریس مهم نیست.

**مثالها**

`SELECT hasAny([1], [])` بازگشت `0`.

`SELECT hasAny([Null], [Null, 1])` بازگشت `1`.

`SELECT hasAny([-128, 1., 512], [1])` بازگشت `1`.

`SELECT hasAny([[1, 2], [3, 4]], ['a', 'c'])` بازگشت `0`.

`SELECT hasAll([[1, 2], [3, 4]], [[1, 2], [1, 2]])` بازگشت `1`.

## هشدار داده می شود) {#indexofarr-x}

بازگرداندن شاخص از اولین ‘x’ عنصر (با شروع از 1) اگر در مجموعه ای است, یا 0 اگر نیست.

مثال:

``` sql
SELECT indexOf([1, 3, NULL, NULL], NULL)
```

``` text
┌─indexOf([1, 3, NULL, NULL], NULL)─┐
│                                 3 │
└───────────────────────────────────┘
```

عناصر را به `NULL` به عنوان مقادیر طبیعی انجام می شود.

## هشدار داده می شود) {#countequalarr-x}

بازده تعداد عناصر موجود در آرایه برابر با x. معادل arrayCount (elem -\> elem = x arr).

`NULL` عناصر به عنوان مقادیر جداگانه به کار گرفته.

مثال:

``` sql
SELECT countEqual([1, 2, NULL, NULL], NULL)
```

``` text
┌─countEqual([1, 2, NULL, NULL], NULL)─┐
│                                    2 │
└──────────────────────────────────────┘
```

## هشدار داده می شود) {#array_functions-arrayenumerate}

Returns the array \[1, 2, 3, …, length (arr) \]

این تابع به طور معمول با مجموعه ای استفاده می شود. این اجازه می دهد شمارش چیزی فقط یک بار برای هر مجموعه پس از استفاده از مجموعه پیوستن. مثال:

``` sql
SELECT
    count() AS Reaches,
    countIf(num = 1) AS Hits
FROM test.hits
ARRAY JOIN
    GoalsReached,
    arrayEnumerate(GoalsReached) AS num
WHERE CounterID = 160656
LIMIT 10
```

``` text
┌─Reaches─┬──Hits─┐
│   95606 │ 31406 │
└─────────┴───────┘
```

در این مثال, می رسد تعداد تبدیل است (رشته دریافت پس از استفاده از مجموعه ملحق), و بازدید تعداد بازدید صفحات (رشته قبل از مجموعه ملحق). در این مورد خاص شما می توانید همان نتیجه را در یک راه ساده تر:

``` sql
SELECT
    sum(length(GoalsReached)) AS Reaches,
    count() AS Hits
FROM test.hits
WHERE (CounterID = 160656) AND notEmpty(GoalsReached)
```

``` text
┌─Reaches─┬──Hits─┐
│   95606 │ 31406 │
└─────────┴───────┘
```

این تابع همچنین می توانید در توابع مرتبه بالاتر استفاده می شود. برای مثال می توانید از شاخص های مجموعه ای برای عناصری که با شرایط مطابقت دارند استفاده کنید.

## arrayEnumerateUniq(arr, …) {#arrayenumerateuniqarr}

بازگرداندن مجموعه ای به همان اندازه به عنوان مجموعه منبع, نشان می دهد برای هر عنصر چه موقعیت خود را در میان عناصر با همان مقدار.
به عنوان مثال: ارریینومراتونیک(\[10, 20, 10, 30\]) = \[1, 1, 2, 1\].

این تابع در هنگام استفاده از مجموعه ای پیوستن و تجمع عناصر مجموعه ای مفید است.
مثال:

``` sql
SELECT
    Goals.ID AS GoalID,
    sum(Sign) AS Reaches,
    sumIf(Sign, num = 1) AS Visits
FROM test.visits
ARRAY JOIN
    Goals,
    arrayEnumerateUniq(Goals.ID) AS num
WHERE CounterID = 160656
GROUP BY GoalID
ORDER BY Reaches DESC
LIMIT 10
```

``` text
┌──GoalID─┬─Reaches─┬─Visits─┐
│   53225 │    3214 │   1097 │
│ 2825062 │    3188 │   1097 │
│   56600 │    2803 │    488 │
│ 1989037 │    2401 │    365 │
│ 2830064 │    2396 │    910 │
│ 1113562 │    2372 │    373 │
│ 3270895 │    2262 │    812 │
│ 1084657 │    2262 │    345 │
│   56599 │    2260 │    799 │
│ 3271094 │    2256 │    812 │
└─────────┴─────────┴────────┘
```

در این مثال هر هدف شناسه محاسبه تعداد تبدیل (هر عنصر در اهداف تو در تو ساختار داده ها یک هدف است که رسیده بود که ما اشاره به عنوان یک تبدیل) و تعداد جلسات. بدون مجموعه ملحق, ما می خواهیم تعداد جلسات به عنوان مجموع شمارش (امضا کردن). اما در این مورد خاص ردیف شد ضرب در تو در تو در اهداف و ساختار آن در سفارش به تعداد هر جلسه یک بار بعد از این ما اعمال یک شرط به ارزش arrayEnumerateUniq(اهداف است.ID) تابع.

تابع ارریینومراتونیک می تواند چندین بار از همان اندازه به عنوان استدلال استفاده کند. در این مورد, منحصر به فرد است برای تاپل از عناصر در موقعیت های مشابه در تمام ارریس در نظر گرفته.

``` sql
SELECT arrayEnumerateUniq([1, 1, 1, 2, 2, 2], [1, 1, 2, 1, 1, 2]) AS res
```

``` text
┌─res───────────┐
│ [1,2,1,1,2,1] │
└───────────────┘
```

این در هنگام استفاده از مجموعه با یک ساختار داده های تو در تو و تجمع بیشتر در سراسر عناصر متعدد در این ساختار ملحق لازم است.

## عقبگرد {#arraypopback}

حذف مورد گذشته از مجموعه.

``` sql
arrayPopBack(array)
```

**پارامترها**

-   `array` – Array.

**مثال**

``` sql
SELECT arrayPopBack([1, 2, 3]) AS res
```

``` text
┌─res───┐
│ [1,2] │
└───────┘
```

## ساحل {#arraypopfront}

اولین مورد را از مجموعه حذف می کند.

``` sql
arrayPopFront(array)
```

**پارامترها**

-   `array` – Array.

**مثال**

``` sql
SELECT arrayPopFront([1, 2, 3]) AS res
```

``` text
┌─res───┐
│ [2,3] │
└───────┘
```

## عقب نشینی {#arraypushback}

یک مورد را به انتهای مجموعه اضافه می کند.

``` sql
arrayPushBack(array, single_value)
```

**پارامترها**

-   `array` – Array.
-   `single_value` – A single value. Only numbers can be added to an array with numbers, and only strings can be added to an array of strings. When adding numbers, ClickHouse automatically sets the `single_value` نوع داده مجموعه را تایپ کنید. برای کسب اطلاعات بیشتر در مورد انواع داده ها در خانه کلیک کنید “[انواع داده ها](../../sql-reference/data-types/index.md#data_types)”. می تواند باشد `NULL`. تابع می افزاید: `NULL` عنصر به مجموعه ای, و نوع عناصر مجموعه ای تبدیل به `Nullable`.

**مثال**

``` sql
SELECT arrayPushBack(['a'], 'b') AS res
```

``` text
┌─res───────┐
│ ['a','b'] │
└───────────┘
```

## ساحلی {#arraypushfront}

یک عنصر را به ابتدای مجموعه اضافه می کند.

``` sql
arrayPushFront(array, single_value)
```

**پارامترها**

-   `array` – Array.
-   `single_value` – A single value. Only numbers can be added to an array with numbers, and only strings can be added to an array of strings. When adding numbers, ClickHouse automatically sets the `single_value` نوع داده مجموعه را تایپ کنید. برای کسب اطلاعات بیشتر در مورد انواع داده ها در خانه کلیک کنید “[انواع داده ها](../../sql-reference/data-types/index.md#data_types)”. می تواند باشد `NULL`. تابع می افزاید: `NULL` عنصر به مجموعه ای, و نوع عناصر مجموعه ای تبدیل به `Nullable`.

**مثال**

``` sql
SELECT arrayPushFront(['b'], 'a') AS res
```

``` text
┌─res───────┐
│ ['a','b'] │
└───────────┘
```

## نمایش سایت {#arrayresize}

طول مجموعه را تغییر می دهد.

``` sql
arrayResize(array, size[, extender])
```

**پارامترها:**

-   `array` — Array.
-   `size` — Required length of the array.
    -   اگر `size` کمتر از اندازه اصلی مجموعه است, مجموعه ای از سمت راست کوتاه.
-   اگر `size` مجموعه بزرگتر از اندازه اولیه مجموعه است که به سمت راست گسترش می یابد `extender` مقادیر یا مقادیر پیش فرض برای نوع داده از موارد مجموعه.
-   `extender` — Value for extending an array. Can be `NULL`.

**مقدار بازگشتی:**

مجموعه ای از طول `size`.

**نمونه هایی از تماس**

``` sql
SELECT arrayResize([1], 3)
```

``` text
┌─arrayResize([1], 3)─┐
│ [1,0,0]             │
└─────────────────────┘
```

``` sql
SELECT arrayResize([1], 3, NULL)
```

``` text
┌─arrayResize([1], 3, NULL)─┐
│ [1,NULL,NULL]             │
└───────────────────────────┘
```

## بند {#arrayslice}

یک تکه از مجموعه را برمی گرداند.

``` sql
arraySlice(array, offset[, length])
```

**پارامترها**

-   `array` – Array of data.
-   `offset` – Indent from the edge of the array. A positive value indicates an offset on the left, and a negative value is an indent on the right. Numbering of the array items begins with 1.
-   `length` - طول قطعه مورد نیاز . اگر شما یک مقدار منفی مشخص, تابع یک تکه باز می گرداند `[offset, array_length - length)`. اگر شما حذف ارزش, تابع برش می گرداند `[offset, the_end_of_array]`.

**مثال**

``` sql
SELECT arraySlice([1, 2, NULL, 4, 5], 2, 3) AS res
```

``` text
┌─res────────┐
│ [2,NULL,4] │
└────────────┘
```

عناصر مجموعه ای به `NULL` به عنوان مقادیر طبیعی انجام می شود.

## arraySort(\[func,\] arr, …) {#array_functions-sort}

عناصر را مرتب می کند `arr` صف در صعودی. اگر `func` تابع مشخص شده است, مرتب سازی سفارش توسط نتیجه تعیین `func` تابع اعمال شده به عناصر مجموعه. اگر `func` قبول استدلال های متعدد `arraySort` تابع به تصویب می رسد چند بند که استدلال `func` خواهد به مطابقت. نمونه های دقیق در پایان نشان داده شده است `arraySort` توصیف.

نمونه ای از مقادیر صحیح مرتب سازی:

``` sql
SELECT arraySort([1, 3, 3, 0]);
```

``` text
┌─arraySort([1, 3, 3, 0])─┐
│ [0,1,3,3]               │
└─────────────────────────┘
```

نمونه ای از مقادیر رشته مرتب سازی:

``` sql
SELECT arraySort(['hello', 'world', '!']);
```

``` text
┌─arraySort(['hello', 'world', '!'])─┐
│ ['!','hello','world']              │
└────────────────────────────────────┘
```

ترتیب مرتب سازی زیر را برای `NULL`, `NaN` و `Inf` مقادیر:

``` sql
SELECT arraySort([1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf]);
```

``` text
┌─arraySort([1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf])─┐
│ [-inf,-4,1,2,3,inf,nan,nan,NULL,NULL]                     │
└───────────────────────────────────────────────────────────┘
```

-   `-Inf` مقادیر برای اولین بار در مجموعه هستند.
-   `NULL` ارزشهای خبری عبارتند از:
-   `NaN` مقادیر درست قبل هستند `NULL`.
-   `Inf` مقادیر درست قبل هستند `NaN`.

توجه داشته باشید که `arraySort` یک [عملکرد عالی مرتبه](higher-order-functions.md). شما می توانید یک تابع لامبدا را به عنوان اولین استدلال منتقل کنید. در این مورد ترتیب مرتب سازی بر اساس نتیجه تابع لامبدا اعمال شده به عناصر مجموعه تعیین می شود.

بیایید مثال زیر را در نظر بگیریم:

``` sql
SELECT arraySort((x) -> -x, [1, 2, 3]) as res;
```

``` text
┌─res─────┐
│ [3,2,1] │
└─────────┘
```

For each element of the source array, the lambda function returns the sorting key, that is, \[1 –\> -1, 2 –\> -2, 3 –\> -3\]. Since the `arraySort` تابع انواع کلید به ترتیب صعودی, نتیجه این است \[3, 2, 1\]. بنابراین `(x) –> -x` عملکرد لامبدا مجموعه [ترتیب نزولی](#array_functions-reverse-sort) در یک مرتب سازی.

تابع لامبدا می تواند استدلال های متعدد را قبول کند. در این مورد, شما نیاز به تصویب `arraySort` تابع چند بند از طول یکسان است که استدلال تابع لامبدا به مطابقت. مجموعه حاصل از عناصر از اولین مجموعه ورودی تشکیل شده است. به عنوان مثال:

``` sql
SELECT arraySort((x, y) -> y, ['hello', 'world'], [2, 1]) as res;
```

``` text
┌─res────────────────┐
│ ['world', 'hello'] │
└────────────────────┘
```

در اینجا عناصر موجود در مجموعه دوم (\[2, 1\]) تعریف یک کلید مرتب سازی برای عنصر مربوطه از مجموعه منبع (\[‘hello’, ‘world’\]), به این معنا که, \[‘hello’ –\> 2, ‘world’ –\> 1\]. Since the lambda function doesn't use `x` مقادیر واقعی مجموعه منبع بر نظم در نتیجه تاثیر نمی گذارد. پس, ‘hello’ خواهد بود که عنصر دوم در نتیجه, و ‘world’ خواهد بود که برای اولین بار.

نمونه های دیگر در زیر نشان داده شده.

``` sql
SELECT arraySort((x, y) -> y, [0, 1, 2], ['c', 'b', 'a']) as res;
```

``` text
┌─res─────┐
│ [2,1,0] │
└─────────┘
```

``` sql
SELECT arraySort((x, y) -> -y, [0, 1, 2], [1, 2, 3]) as res;
```

``` text
┌─res─────┐
│ [2,1,0] │
└─────────┘
```

!!! note "یادداشت"
    برای بهبود کارایی مرتب سازی [تبدیل شوارتز](https://en.wikipedia.org/wiki/Schwartzian_transform) استفاده شده است.

## arrayReverseSort(\[func,\] arr, …) {#array_functions-reverse-sort}

عناصر را مرتب می کند `arr` صف در نزولی. اگر `func` تابع مشخص شده است, `arr` بر اساس نتیجه طبقه بندی شده اند `func` عملکرد به عناصر مجموعه اعمال می شود و سپس مجموعه مرتب شده معکوس می شود. اگر `func` قبول استدلال های متعدد `arrayReverseSort` تابع به تصویب می رسد چند بند که استدلال `func` خواهد به مطابقت. نمونه های دقیق در پایان نشان داده شده است `arrayReverseSort` توصیف.

نمونه ای از مقادیر صحیح مرتب سازی:

``` sql
SELECT arrayReverseSort([1, 3, 3, 0]);
```

``` text
┌─arrayReverseSort([1, 3, 3, 0])─┐
│ [3,3,1,0]                      │
└────────────────────────────────┘
```

نمونه ای از مقادیر رشته مرتب سازی:

``` sql
SELECT arrayReverseSort(['hello', 'world', '!']);
```

``` text
┌─arrayReverseSort(['hello', 'world', '!'])─┐
│ ['world','hello','!']                     │
└───────────────────────────────────────────┘
```

ترتیب مرتب سازی زیر را برای `NULL`, `NaN` و `Inf` مقادیر:

``` sql
SELECT arrayReverseSort([1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf]) as res;
```

``` text
┌─res───────────────────────────────────┐
│ [inf,3,2,1,-4,-inf,nan,nan,NULL,NULL] │
└───────────────────────────────────────┘
```

-   `Inf` مقادیر برای اولین بار در مجموعه هستند.
-   `NULL` ارزشهای خبری عبارتند از:
-   `NaN` مقادیر درست قبل هستند `NULL`.
-   `-Inf` مقادیر درست قبل هستند `NaN`.

توجه داشته باشید که `arrayReverseSort` یک [عملکرد عالی مرتبه](higher-order-functions.md). شما می توانید یک تابع لامبدا را به عنوان اولین استدلال منتقل کنید. مثال زیر نشان داده شده.

``` sql
SELECT arrayReverseSort((x) -> -x, [1, 2, 3]) as res;
```

``` text
┌─res─────┐
│ [1,2,3] │
└─────────┘
```

این مجموعه به روش زیر مرتب شده است:

1.  ابتدا مجموعه منبع (\[1, 2, 3\]) با توجه به نتیجه تابع لامبدا اعمال شده به عناصر مجموعه طبقه بندی شده اند. نتیجه یک مجموعه است \[3, 2, 1\].
2.  مجموعه ای است که در مرحله قبل به دست, معکوس شده است. بنابراین, نتیجه نهایی است \[1, 2, 3\].

تابع لامبدا می تواند استدلال های متعدد را قبول کند. در این مورد, شما نیاز به تصویب `arrayReverseSort` تابع چند بند از طول یکسان است که استدلال تابع لامبدا به مطابقت. مجموعه حاصل از عناصر از اولین مجموعه ورودی تشکیل شده است. به عنوان مثال:

``` sql
SELECT arrayReverseSort((x, y) -> y, ['hello', 'world'], [2, 1]) as res;
```

``` text
┌─res───────────────┐
│ ['hello','world'] │
└───────────────────┘
```

در این مثال مجموعه به روش زیر مرتب شده است:

1.  در ابتدا مجموعه منبع (\[‘hello’, ‘world’\]) با توجه به نتیجه تابع لامبدا اعمال شده به عناصر از ارریس طبقه بندی شده اند. عناصر که در مجموعه دوم به تصویب رسید (\[2, 1\]), تعریف کلید مرتب سازی برای عناصر مربوطه را از مجموعه منبع. نتیجه یک مجموعه است \[‘world’, ‘hello’\].
2.  مجموعه ای که در مرحله قبل طبقه بندی شده اند, معکوس شده است. بنابراین نتیجه نهایی این است \[‘hello’, ‘world’\].

نمونه های دیگر در زیر نشان داده شده.

``` sql
SELECT arrayReverseSort((x, y) -> y, [4, 3, 5], ['a', 'b', 'c']) AS res;
```

``` text
┌─res─────┐
│ [5,3,4] │
└─────────┘
```

``` sql
SELECT arrayReverseSort((x, y) -> -y, [4, 3, 5], [1, 2, 3]) AS res;
```

``` text
┌─res─────┐
│ [4,3,5] │
└─────────┘
```

## arrayUniq(arr, …) {#arrayuniqarr}

اگر یک استدلال به تصویب می رسد, تعداد عناصر مختلف در مجموعه شمارش.
اگر استدلال های متعدد به تصویب می رسد, شمارش تعداد تاپل های مختلف از عناصر در موقعیت های مربوطه در مجموعه های متعدد.

اگر شما می خواهید برای دریافت یک لیست از اقلام منحصر به فرد در مجموعه, شما می توانید از ارری راهاهن استفاده(‘groupUniqArray’, arr).

## هشدار داده می شود) {#array-functions-join}

یک تابع خاص. بخش را ببینید [“ArrayJoin function”](array-join.md#functions_arrayjoin).

## کلیدواژه {#arraydifference}

محاسبه تفاوت بین عناصر مجموعه مجاور. بازگرداندن مجموعه ای که عنصر اول خواهد بود 0, دوم تفاوت بین است `a[1] - a[0]`, etc. The type of elements in the resulting array is determined by the type inference rules for subtraction (e.g. `UInt8` - `UInt8` = `Int16`).

**نحو**

``` sql
arrayDifference(array)
```

**پارامترها**

-   `array` – [& حذف](https://clickhouse.tech/docs/en/data_types/array/).

**مقادیر بازگشتی**

بازگرداندن مجموعه ای از تفاوت بین عناصر مجاور.

نوع: [اینترنت\*](https://clickhouse.tech/docs/en/data_types/int_uint/#uint-ranges), [Int\*](https://clickhouse.tech/docs/en/data_types/int_uint/#int-ranges), [شناور\*](https://clickhouse.tech/docs/en/data_types/float/).

**مثال**

پرسوجو:

``` sql
SELECT arrayDifference([1, 2, 3, 4])
```

نتیجه:

``` text
┌─arrayDifference([1, 2, 3, 4])─┐
│ [0,1,1,1]                     │
└───────────────────────────────┘
```

مثال سرریز به علت نوع نتیجه اینترن64:

پرسوجو:

``` sql
SELECT arrayDifference([0, 10000000000000000000])
```

نتیجه:

``` text
┌─arrayDifference([0, 10000000000000000000])─┐
│ [0,-8446744073709551616]                   │
└────────────────────────────────────────────┘
```

## حوزه ارریددیست {#arraydistinct}

مجموعه ای را می گیرد و تنها شامل عناصر مجزا می شود.

**نحو**

``` sql
arrayDistinct(array)
```

**پارامترها**

-   `array` – [& حذف](https://clickhouse.tech/docs/en/data_types/array/).

**مقادیر بازگشتی**

بازگرداندن مجموعه ای حاوی عناصر متمایز.

**مثال**

پرسوجو:

``` sql
SELECT arrayDistinct([1, 2, 2, 3, 1])
```

نتیجه:

``` text
┌─arrayDistinct([1, 2, 2, 3, 1])─┐
│ [1,2,3]                        │
└────────────────────────────────┘
```

## هشدار داده می شود) {#array_functions-arrayenumeratedense}

بازگرداندن مجموعه ای از همان اندازه به عنوان مجموعه منبع, نشان می دهد که هر عنصر برای اولین بار در مجموعه منبع به نظر می رسد.

مثال:

``` sql
SELECT arrayEnumerateDense([10, 20, 10, 30])
```

``` text
┌─arrayEnumerateDense([10, 20, 10, 30])─┐
│ [1,2,1,3]                             │
└───────────────────────────────────────┘
```

## هشدار داده می شود) {#array-functions-arrayintersect}

طول می کشد مجموعه ای با عناصر که در تمام مجموعه منبع در حال حاضر می گرداند. عناصر سفارش در مجموعه حاصل همان است که در مجموعه اول است.

مثال:

``` sql
SELECT
    arrayIntersect([1, 2], [1, 3], [2, 3]) AS no_intersect,
    arrayIntersect([1, 2], [1, 3], [1, 4]) AS intersect
```

``` text
┌─no_intersect─┬─intersect─┐
│ []           │ [1]       │
└──────────────┴───────────┘
```

## نمایش سایت {#arrayreduce}

یک تابع کلی برای عناصر مجموعه ای اعمال می شود و نتیجه خود را باز می گرداند. نام تابع تجمع به عنوان یک رشته در نقل قول های تک منتقل می شود `'max'`, `'sum'`. هنگام استفاده از توابع دانه پارامتری پارامتر پس از نام تابع در پرانتز نشان داده شده است `'uniqUpTo(6)'`.

**نحو**

``` sql
arrayReduce(agg_func, arr1, arr2, ..., arrN)
```

**پارامترها**

-   `agg_func` — The name of an aggregate function which should be a constant [رشته](../../sql-reference/data-types/string.md).
-   `arr` — Any number of [& حذف](../../sql-reference/data-types/array.md) نوع ستون به عنوان پارامترهای تابع تجمع.

**مقدار بازگشتی**

**مثال**

``` sql
SELECT arrayReduce('max', [1, 2, 3])
```

``` text
┌─arrayReduce('max', [1, 2, 3])─┐
│                             3 │
└───────────────────────────────┘
```

اگر یک تابع جمع استدلال های متعدد طول می کشد, سپس این تابع باید به مجموعه های متعدد از همان اندازه اعمال.

``` sql
SELECT arrayReduce('maxIf', [3, 5], [1, 0])
```

``` text
┌─arrayReduce('maxIf', [3, 5], [1, 0])─┐
│                                    3 │
└──────────────────────────────────────┘
```

به عنوان مثال با یک تابع جمع پارامتری:

``` sql
SELECT arrayReduce('uniqUpTo(3)', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
```

``` text
┌─arrayReduce('uniqUpTo(3)', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])─┐
│                                                           4 │
└─────────────────────────────────────────────────────────────┘
```

## تغییرات {#arrayreduceinranges}

یک تابع کلی برای عناصر مجموعه ای در محدوده های داده شده اعمال می شود و مجموعه ای حاوی نتیجه مربوط به هر محدوده را باز می گرداند. تابع همان نتیجه به عنوان چند بازگشت `arrayReduce(agg_func, arraySlice(arr1, index, length), ...)`.

**نحو**

``` sql
arrayReduceInRanges(agg_func, ranges, arr1, arr2, ..., arrN)
```

**پارامترها**

-   `agg_func` — The name of an aggregate function which should be a constant [رشته](../../sql-reference/data-types/string.md).
-   `ranges` — The ranges to aggretate which should be an [& حذف](../../sql-reference/data-types/array.md) از [توپلس](../../sql-reference/data-types/tuple.md) که شامل شاخص و طول هر محدوده.
-   `arr` — Any number of [& حذف](../../sql-reference/data-types/array.md) نوع ستون به عنوان پارامترهای تابع تجمع.

**مقدار بازگشتی**

**مثال**

``` sql
SELECT arrayReduceInRanges(
    'sum',
    [(1, 5), (2, 3), (3, 4), (4, 4)],
    [1000000, 200000, 30000, 4000, 500, 60, 7]
) AS res
```

``` text
┌─res─────────────────────────┐
│ [1234500,234000,34560,4567] │
└─────────────────────────────┘
```

## هشدار داده می شود) {#arrayreverse}

بازگرداندن مجموعه ای از همان اندازه به عنوان مجموعه اصلی حاوی عناصر در جهت معکوس.

مثال:

``` sql
SELECT arrayReverse([1, 2, 3])
```

``` text
┌─arrayReverse([1, 2, 3])─┐
│ [3,2,1]                 │
└─────────────────────────┘
```

## معکوس) {#array-functions-reverse}

مترادف برای [“arrayReverse”](#arrayreverse)

## ارریفلاتتن {#arrayflatten}

مجموعه ای از ارریس ها را به یک مجموعه صاف تبدیل می کند.

تابع:

-   امر به هر عمق مجموعه های تو در تو.
-   طعم هایی را که در حال حاضر مسطح هستند تغییر نمی دهد.

مجموعه مسطح شامل تمام عناصر از تمام منابع است.

**نحو**

``` sql
flatten(array_of_arrays)
```

نام مستعار: `flatten`.

**پارامترها**

-   `array_of_arrays` — [& حذف](../../sql-reference/data-types/array.md) ارریس به عنوان مثال, `[[1,2,3], [4,5]]`.

**مثالها**

``` sql
SELECT flatten([[[1]], [[2], [3]]])
```

``` text
┌─flatten(array(array([1]), array([2], [3])))─┐
│ [1,2,3]                                     │
└─────────────────────────────────────────────┘
```

## اررایکمپکت {#arraycompact}

عناصر تکراری متوالی را از یک مجموعه حذف می کند. ترتیب مقادیر نتیجه به ترتیب در مجموعه منبع تعیین می شود.

**نحو**

``` sql
arrayCompact(arr)
```

**پارامترها**

`arr` — The [& حذف](../../sql-reference/data-types/array.md) برای بازرسی.

**مقدار بازگشتی**

مجموعه ای بدون تکراری.

نوع: `Array`.

**مثال**

پرسوجو:

``` sql
SELECT arrayCompact([1, 1, nan, nan, 2, 3, 3, 3])
```

نتیجه:

``` text
┌─arrayCompact([1, 1, nan, nan, 2, 3, 3, 3])─┐
│ [1,nan,nan,2,3]                            │
└────────────────────────────────────────────┘
```

## ارریزیپ {#arrayzip}

ترکیبی از چندین ردیف به یک مجموعه واحد. مجموعه حاصل شامل عناصر مربوطه را از ارریس منبع به تاپل در جهت ذکر شده از استدلال گروه بندی می شوند.

**نحو**

``` sql
arrayZip(arr1, arr2, ..., arrN)
```

**پارامترها**

-   `arrN` — [& حذف](../data-types/array.md).

این تابع می تواند هر تعداد از مجموعه ای از انواع مختلف را. تمام ورودی های ورودی باید با اندازه یکسان باشند.

**مقدار بازگشتی**

-   مجموعه ای با عناصر از ارریس منبع به گروه بندی می شوند [توپلس](../data-types/tuple.md). انواع داده ها در تاپل همان نوع از بند ورودی هستند و در همان جهت به عنوان ارریس به تصویب می رسد.

نوع: [& حذف](../data-types/array.md).

**مثال**

پرسوجو:

``` sql
SELECT arrayZip(['a', 'b', 'c'], [5, 2, 1])
```

نتیجه:

``` text
┌─arrayZip(['a', 'b', 'c'], [5, 2, 1])─┐
│ [('a',5),('b',2),('c',1)]            │
└──────────────────────────────────────┘
```

## ارریایکو {#arrayauc}

محاسبه حراج (منطقه تحت منحنی, که یک مفهوم در یادگیری ماشین است, مشاهده اطلاعات بیشتر: https://en.wikipedia.org/wiki/Receiver_operating_characteristic#Area_under_the_curve).

**نحو**

``` sql
arrayAUC(arr_scores, arr_labels)
```

**پارامترها**
- `arr_scores` — scores prediction model gives.
- `arr_labels` — labels of samples, usually 1 for positive sample and 0 for negtive sample.

**مقدار بازگشتی**
را برمی گرداند ارزش حراج با نوع شناور64.

**مثال**
پرسوجو:

``` sql
select arrayAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1])
```

نتیجه:

``` text
┌─arrayAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1])─┐
│                                          0.75 │
└────────────────────────────────────────---──┘
```

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/functions/array_functions/) <!--hide-->
