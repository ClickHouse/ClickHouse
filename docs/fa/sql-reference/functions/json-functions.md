---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 56
toc_title: "\u06A9\u0627\u0631 \u0628\u0627 \u062C\u0627\u0646\u0633\u0648\u0646"
---

# توابع برای کار با جانسون {#functions-for-working-with-json}

در یاندکسمتریکا جیسون توسط کاربران به عنوان پارامترهای جلسه منتقل می شود. برخی از توابع خاص برای کار با این جانسون وجود دارد. (اگر چه در بسیاری از موارد JSONs هستند علاوه بر این قبل از پردازش و در نتیجه ارزش ها قرار داده و در ستون جداگانه در خود پردازش فرمت.) همه این توابع در فرضیات قوی در مورد چه جانسون می تواند بر اساس, اما سعی می کنند به عنوان کوچک که ممکن است به کار انجام می شود.

مفروضات زیر ساخته شده است:

1.  نام فیلد (استدلال تابع) باید ثابت باشد.
2.  نام فیلد به نحوی می تواند در جیسون کد گذاری شود. به عنوان مثال: `visitParamHas('{"abc":"def"}', 'abc') = 1` اما `visitParamHas('{"\\u0061\\u0062\\u0063":"def"}', 'abc') = 0`
3.  زمینه ها برای در هر سطح تودرتو جستجو, یکسره. اگر زمینه های تطبیق های متعدد وجود دارد, وقوع اول استفاده شده است.
4.  JSON ندارد کاراکتر فضای خارج از string literals.

## ویسیتپراماس (پارامز, نام) {#visitparamhasparams-name}

بررسی اینکه یک میدان با وجود ‘name’ اسم.

## ویسیتپرامستراکتینت (پارامز, نام) {#visitparamextractuintparams-name}

تجزیه ظاهری64 از ارزش این زمینه به نام ‘name’. اگر این یک رشته رشته زمینه, تلاش می کند به تجزیه یک عدد از ابتدای رشته. اگر میدان وجود ندارد, و یا وجود دارد اما حاوی یک عدد نیست, باز می گردد 0.

## ویزیتپرامستراکتینت (پارامز, نام) {#visitparamextractintparams-name}

همان Int64.

## اطلاعات دقیق) {#visitparamextractfloatparams-name}

همان است که برای شناور64.

## ویسیتپرامسترکتبولبولول (پارامز, نام) {#visitparamextractboolparams-name}

تجزیه واقعی / ارزش کاذب. نتیجه این است UInt8.

## ویسیتپرمککتراو (پارامز, نام) {#visitparamextractrawparams-name}

بازگرداندن ارزش یک میدان, از جمله جدا.

مثالها:

``` sql
visitParamExtractRaw('{"abc":"\\n\\u0000"}', 'abc') = '"\\n\\u0000"'
visitParamExtractRaw('{"abc":{"def":[1,2,3]}}', 'abc') = '{"def":[1,2,3]}'
```

## نام و نام خانوادگی) {#visitparamextractstringparams-name}

تجزیه رشته در نقل قول دو. ارزش بی نتیجه است. اگر بیم شکست خورده, این یک رشته خالی می گرداند.

مثالها:

``` sql
visitParamExtractString('{"abc":"\\n\\u0000"}', 'abc') = '\n\0'
visitParamExtractString('{"abc":"\\u263a"}', 'abc') = '☺'
visitParamExtractString('{"abc":"\\u263"}', 'abc') = ''
visitParamExtractString('{"abc":"hello}', 'abc') = ''
```

در حال حاضر هیچ پشتیبانی برای نقاط کد در قالب وجود دارد `\uXXXX\uYYYY` این از هواپیما چند زبانه پایه نیست (به جای اوتو-8 تبدیل می شود).

توابع زیر بر اساس [سیمدجسون](https://github.com/lemire/simdjson) طراحی شده برای نیازهای پیچیده تر جسون تجزیه. فرض 2 ذکر شده در بالا هنوز هم صدق.

## هشدار داده می شود) {#isvalidjsonjson}

چک که رشته گذشت جانسون معتبر است.

مثالها:

``` sql
SELECT isValidJSON('{"a": "hello", "b": [-100, 200.0, 300]}') = 1
SELECT isValidJSON('not a json') = 0
```

## JSONHas(json\[, indices_or_keys\]…) {#jsonhasjson-indices-or-keys}

اگر مقدار در سند جسون وجود داشته باشد, `1` برگردانده خواهد شد.

اگر مقدار وجود ندارد, `0` برگردانده خواهد شد.

مثالها:

``` sql
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 1
SELECT JSONHas('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 4) = 0
```

`indices_or_keys` لیستی از استدلال های صفر یا بیشتر است که هر کدام می توانند رشته یا عدد صحیح باشند.

-   رشته = عضو شی دسترسی های کلیدی.
-   عدد صحیح مثبت = از ابتدا به عضو / کلید نفر دسترسی پیدا کنید.
-   عدد صحیح منفی = دسترسی به عضو / کلید نفر از پایان.

حداقل شاخص عنصر 1 است. بنابراین عنصر 0 وجود ندارد.

شما می توانید از اعداد صحیح برای دسترسی به هر دو اشیای جسون ارریس و جسون استفاده کنید.

بنابراین, مثلا:

``` sql
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', 1) = 'a'
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', 2) = 'b'
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', -1) = 'b'
SELECT JSONExtractKey('{"a": "hello", "b": [-100, 200.0, 300]}', -2) = 'a'
SELECT JSONExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 1) = 'hello'
```

## JSONLength(json\[, indices_or_keys\]…) {#jsonlengthjson-indices-or-keys}

بازگشت طول یک مجموعه جانسون یا یک شی جانسون.

اگر مقدار وجود ندارد و یا دارای یک نوع اشتباه, `0` برگردانده خواهد شد.

مثالها:

``` sql
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 3
SELECT JSONLength('{"a": "hello", "b": [-100, 200.0, 300]}') = 2
```

## JSONType(json\[, indices_or_keys\]…) {#jsontypejson-indices-or-keys}

بازگشت به نوع یک مقدار جانسون.

اگر مقدار وجود ندارد, `Null` برگردانده خواهد شد.

مثالها:

``` sql
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}') = 'Object'
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}', 'a') = 'String'
SELECT JSONType('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = 'Array'
```

## JSONExtractUInt(json\[, indices_or_keys\]…) {#jsonextractuintjson-indices-or-keys}

## JSONExtractInt(json\[, indices_or_keys\]…) {#jsonextractintjson-indices-or-keys}

## JSONExtractFloat(json\[, indices_or_keys\]…) {#jsonextractfloatjson-indices-or-keys}

## JSONExtractBool(json\[, indices_or_keys\]…) {#jsonextractbooljson-indices-or-keys}

تجزیه جانسون و استخراج ارزش. این توابع شبیه به `visitParam` توابع.

اگر مقدار وجود ندارد و یا دارای یک نوع اشتباه, `0` برگردانده خواهد شد.

مثالها:

``` sql
SELECT JSONExtractInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1) = -100
SELECT JSONExtractFloat('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 2) = 200.0
SELECT JSONExtractUInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', -1) = 300
```

## JSONExtractString(json\[, indices_or_keys\]…) {#jsonextractstringjson-indices-or-keys}

تجزیه جانسون و استخراج یک رشته. این تابع شبیه به `visitParamExtractString` توابع.

اگر مقدار وجود ندارد و یا دارای یک نوع اشتباه, یک رشته خالی بازگردانده خواهد شد.

ارزش بی نتیجه است. اگر بیم شکست خورده, این یک رشته خالی می گرداند.

مثالها:

``` sql
SELECT JSONExtractString('{"a": "hello", "b": [-100, 200.0, 300]}', 'a') = 'hello'
SELECT JSONExtractString('{"abc":"\\n\\u0000"}', 'abc') = '\n\0'
SELECT JSONExtractString('{"abc":"\\u263a"}', 'abc') = '☺'
SELECT JSONExtractString('{"abc":"\\u263"}', 'abc') = ''
SELECT JSONExtractString('{"abc":"hello}', 'abc') = ''
```

## JSONExtract(json\[, indices_or_keys…\], Return_type) {#jsonextractjson-indices-or-keys-return-type}

تجزیه یک جسون و استخراج یک مقدار از نوع داده داده داده کلیک.

این یک تعمیم قبلی است `JSONExtract<type>` توابع.
این به این معنی است
`JSONExtract(..., 'String')` بازده دقیقا همان `JSONExtractString()`,
`JSONExtract(..., 'Float64')` بازده دقیقا همان `JSONExtractFloat()`.

مثالها:

``` sql
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'Tuple(String, Array(Float64))') = ('hello',[-100,200,300])
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'Tuple(b Array(Float64), a String)') = ([-100,200,300],'hello')
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 'Array(Nullable(Int8))') = [-100, NULL, NULL]
SELECT JSONExtract('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 4, 'Nullable(Int64)') = NULL
SELECT JSONExtract('{"passed": true}', 'passed', 'UInt8') = 1
SELECT JSONExtract('{"day": "Thursday"}', 'day', 'Enum8(\'Sunday\' = 0, \'Monday\' = 1, \'Tuesday\' = 2, \'Wednesday\' = 3, \'Thursday\' = 4, \'Friday\' = 5, \'Saturday\' = 6)') = 'Thursday'
SELECT JSONExtract('{"day": 5}', 'day', 'Enum8(\'Sunday\' = 0, \'Monday\' = 1, \'Tuesday\' = 2, \'Wednesday\' = 3, \'Thursday\' = 4, \'Friday\' = 5, \'Saturday\' = 6)') = 'Friday'
```

## JSONExtractKeysAndValues(json\[, indices_or_keys…\], Value_type) {#jsonextractkeysandvaluesjson-indices-or-keys-value-type}

تجزیه جفت کلید ارزش از یک جانسون که ارزش از نوع داده داده داده خانه عروسکی هستند.

مثال:

``` sql
SELECT JSONExtractKeysAndValues('{"x": {"a": 5, "b": 7, "c": 11}}', 'x', 'Int8') = [('a',5),('b',7),('c',11)]
```

## JSONExtractRaw(json\[, indices_or_keys\]…) {#jsonextractrawjson-indices-or-keys}

بازگرداندن بخشی از جانسون به عنوان رشته نامحدود.

اگر بخش وجود ندارد و یا دارای یک نوع اشتباه, یک رشته خالی بازگردانده خواهد شد.

مثال:

``` sql
SELECT JSONExtractRaw('{"a": "hello", "b": [-100, 200.0, 300]}', 'b') = '[-100, 200.0, 300]'
```

## JSONExtractArrayRaw(json\[, indices_or_keys…\]) {#jsonextractarrayrawjson-indices-or-keys}

بازگرداندن مجموعه ای با عناصر از مجموعه جانسون,هر یک به عنوان رشته نامحدود نشان.

اگر بخش وجود ندارد و یا مجموعه ای نیست, مجموعه ای خالی بازگردانده خواهد شد.

مثال:

``` sql
SELECT JSONExtractArrayRaw('{"a": "hello", "b": [-100, 200.0, "hello"]}', 'b') = ['-100', '200.0', '"hello"']'
```

## در حال بارگذاری {#json-extract-keys-and-values-raw}

عصاره داده های خام از یک شی جانسون.

**نحو**

``` sql
JSONExtractKeysAndValuesRaw(json[, p, a, t, h])
```

**پارامترها**

-   `json` — [رشته](../data-types/string.md) با جانسون معتبر.
-   `p, a, t, h` — Comma-separated indices or keys that specify the path to the inner field in a nested JSON object. Each argument can be either a [رشته](../data-types/string.md) برای دریافت این زمینه توسط کلید یا یک [عدد صحیح](../data-types/int-uint.md) برای دریافت میدان ازت هفتم (نمایه شده از 1 عدد صحیح منفی از پایان تعداد). اگر تنظیم نشده, طیف جانسون به عنوان شی سطح بالا تجزیه. پارامتر اختیاری.

**مقادیر بازگشتی**

-   & حذف با `('key', 'value')` توپلس هر دو عضو تاپل رشته ها.
-   مجموعه خالی اگر جسم درخواست شده وجود ندارد, یا جانسون ورودی نامعتبر است.

نوع: [& حذف](../data-types/array.md)([تاپل](../data-types/tuple.md)([رشته](../data-types/string.md), [رشته](../data-types/string.md)).

**مثالها**

پرسوجو:

``` sql
SELECT JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}')
```

نتیجه:

``` text
┌─JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}')─┐
│ [('a','[-100,200]'),('b','{"c":{"d":"hello","f":"world"}}')]                                 │
└──────────────────────────────────────────────────────────────────────────────────────────────┘
```

پرسوجو:

``` sql
SELECT JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}', 'b')
```

نتیجه:

``` text
┌─JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}', 'b')─┐
│ [('c','{"d":"hello","f":"world"}')]                                                               │
└───────────────────────────────────────────────────────────────────────────────────────────────────┘
```

پرسوجو:

``` sql
SELECT JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}', -1, 'c')
```

نتیجه:

``` text
┌─JSONExtractKeysAndValuesRaw('{"a": [-100, 200.0], "b":{"c": {"d": "hello", "f": "world"}}}', -1, 'c')─┐
│ [('d','"hello"'),('f','"world"')]                                                                     │
└───────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/functions/json_functions/) <!--hide-->
