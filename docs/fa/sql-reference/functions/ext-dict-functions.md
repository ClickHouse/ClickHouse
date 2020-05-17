---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 58
toc_title: "\u06A9\u0627\u0631 \u0628\u0627 \u0648\u0627\u0698\u0647\u0646\u0627\u0645\
  \u0647\u0647\u0627 \u062E\u0627\u0631\u062C\u06CC"
---

# توابع برای کار با لغت نامه های خارجی {#ext_dict_functions}

برای اطلاعات در مورد اتصال و پیکربندی لغت نامه های خارجی, دیدن [واژهنامهها خارجی](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md).

## دیکته کردن {#dictget}

بازیابی یک مقدار از یک فرهنگ لغت خارجی.

``` sql
dictGet('dict_name', 'attr_name', id_expr)
dictGetOrDefault('dict_name', 'attr_name', id_expr, default_value_expr)
```

**پارامترها**

-   `dict_name` — Name of the dictionary. [رشته تحت اللفظی](../syntax.md#syntax-string-literal).
-   `attr_name` — Name of the column of the dictionary. [رشته تحت اللفظی](../syntax.md#syntax-string-literal).
-   `id_expr` — Key value. [عبارت](../syntax.md#syntax-expressions) بازگشت یک [UInt64](../../sql-reference/data-types/int-uint.md) یا [تاپل](../../sql-reference/data-types/tuple.md)- نوع ارزش بسته به پیکربندی فرهنگ لغت .
-   `default_value_expr` — Value returned if the dictionary doesn't contain a row with the `id_expr` کلید [عبارت](../syntax.md#syntax-expressions) بازگشت ارزش در نوع داده پیکربندی شده برای `attr_name` صفت کردن.

**مقدار بازگشتی**

-   اگر تاتر تجزیه ویژگی موفقیت در [نوع داده خصیصه](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes), توابع بازگشت ارزش ویژگی فرهنگ لغت که مربوط به `id_expr`.

-   اگر هیچ کلید وجود دارد, مربوط به `id_expr`, در فرهنگ لغت, سپس:

        - `dictGet` returns the content of the `<null_value>` element specified for the attribute in the dictionary configuration.
        - `dictGetOrDefault` returns the value passed as the `default_value_expr` parameter.

کلیک هاوس می اندازد یک استثنا اگر می تواند ارزش ویژگی تجزیه و یا ارزش می کند نوع داده ویژگی مطابقت ندارد.

**مثال**

ایجاد یک فایل متنی `ext-dict-text.csv` حاوی موارد زیر است:

``` text
1,1
2,2
```

ستون اول است `id` ستون دوم `c1`.

پیکربندی واژهنامه خارجی:

``` xml
<yandex>
    <dictionary>
        <name>ext-dict-test</name>
        <source>
            <file>
                <path>/path-to/ext-dict-test.csv</path>
                <format>CSV</format>
            </file>
        </source>
        <layout>
            <flat />
        </layout>
        <structure>
            <id>
                <name>id</name>
            </id>
            <attribute>
                <name>c1</name>
                <type>UInt32</type>
                <null_value></null_value>
            </attribute>
        </structure>
        <lifetime>0</lifetime>
    </dictionary>
</yandex>
```

انجام پرس و جو:

``` sql
SELECT
    dictGetOrDefault('ext-dict-test', 'c1', number + 1, toUInt32(number * 10)) AS val,
    toTypeName(val) AS type
FROM system.numbers
LIMIT 3
```

``` text
┌─val─┬─type───┐
│   1 │ UInt32 │
│   2 │ UInt32 │
│  20 │ UInt32 │
└─────┴────────┘
```

**همچنین نگاه کنید به**

-   [واژهنامهها خارجی](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md)

## دیکتس {#dicthas}

بررسی اینکه یک کلید در حال حاضر در یک فرهنگ لغت است.

``` sql
dictHas('dict_name', id_expr)
```

**پارامترها**

-   `dict_name` — Name of the dictionary. [رشته تحت اللفظی](../syntax.md#syntax-string-literal).
-   `id_expr` — Key value. [عبارت](../syntax.md#syntax-expressions) بازگشت یک [UInt64](../../sql-reference/data-types/int-uint.md)- نوع ارزش.

**مقدار بازگشتی**

-   0, اگر هیچ کلید وجود دارد.
-   1, اگر یک کلید وجود دارد.

نوع: `UInt8`.

## حکومت دیکتاتوری {#dictgethierarchy}

یک مجموعه ای ایجاد می کند که شامل همه والدین یک کلید در [فرهنگ لغت سلسله مراتبی](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-hierarchical.md).

**نحو**

``` sql
dictGetHierarchy('dict_name', key)
```

**پارامترها**

-   `dict_name` — Name of the dictionary. [رشته تحت اللفظی](../syntax.md#syntax-string-literal).
-   `key` — Key value. [عبارت](../syntax.md#syntax-expressions) بازگشت یک [UInt64](../../sql-reference/data-types/int-uint.md)- نوع ارزش.

**مقدار بازگشتی**

-   پدر و مادر برای کلید.

نوع: [Array(UInt64)](../../sql-reference/data-types/array.md).

## دیکتاتوری {#dictisin}

جد یک کلید را از طریق کل زنجیره سلسله مراتبی در فرهنگ لغت بررسی می کند.

``` sql
dictIsIn('dict_name', child_id_expr, ancestor_id_expr)
```

**پارامترها**

-   `dict_name` — Name of the dictionary. [رشته تحت اللفظی](../syntax.md#syntax-string-literal).
-   `child_id_expr` — Key to be checked. [عبارت](../syntax.md#syntax-expressions) بازگشت یک [UInt64](../../sql-reference/data-types/int-uint.md)- نوع ارزش.
-   `ancestor_id_expr` — Alleged ancestor of the `child_id_expr` کلید [عبارت](../syntax.md#syntax-expressions) بازگشت یک [UInt64](../../sql-reference/data-types/int-uint.md)- نوع ارزش.

**مقدار بازگشتی**

-   0 اگر `child_id_expr` یک کودک نیست `ancestor_id_expr`.
-   1 اگر `child_id_expr` یک کودک است `ancestor_id_expr` یا اگر `child_id_expr` یک `ancestor_id_expr`.

نوع: `UInt8`.

## توابع دیگر {#ext_dict_functions-other}

تاتر پشتیبانی از توابع تخصصی است که تبدیل ارزش فرهنگ لغت ویژگی به یک نوع داده خاص بدون در نظر گرفتن پیکربندی فرهنگ لغت.

توابع:

-   `dictGetInt8`, `dictGetInt16`, `dictGetInt32`, `dictGetInt64`
-   `dictGetUInt8`, `dictGetUInt16`, `dictGetUInt32`, `dictGetUInt64`
-   `dictGetFloat32`, `dictGetFloat64`
-   `dictGetDate`
-   `dictGetDateTime`
-   `dictGetUUID`
-   `dictGetString`

همه این توابع `OrDefault` اصلاح. به عنوان مثال, `dictGetDateOrDefault`.

نحو:

``` sql
dictGet[Type]('dict_name', 'attr_name', id_expr)
dictGet[Type]OrDefault('dict_name', 'attr_name', id_expr, default_value_expr)
```

**پارامترها**

-   `dict_name` — Name of the dictionary. [رشته تحت اللفظی](../syntax.md#syntax-string-literal).
-   `attr_name` — Name of the column of the dictionary. [رشته تحت اللفظی](../syntax.md#syntax-string-literal).
-   `id_expr` — Key value. [عبارت](../syntax.md#syntax-expressions) بازگشت یک [UInt64](../../sql-reference/data-types/int-uint.md)- نوع ارزش.
-   `default_value_expr` — Value which is returned if the dictionary doesn't contain a row with the `id_expr` کلید [عبارت](../syntax.md#syntax-expressions) بازگشت یک مقدار در نوع داده پیکربندی شده برای `attr_name` صفت کردن.

**مقدار بازگشتی**

-   اگر تاتر تجزیه ویژگی موفقیت در [نوع داده خصیصه](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes), توابع بازگشت ارزش ویژگی فرهنگ لغت که مربوط به `id_expr`.

-   در صورتی که هیچ درخواست وجود دارد `id_expr` در فرهنگ لغت و سپس:

        - `dictGet[Type]` returns the content of the `<null_value>` element specified for the attribute in the dictionary configuration.
        - `dictGet[Type]OrDefault` returns the value passed as the `default_value_expr` parameter.

کلیک هاوس می اندازد یک استثنا اگر می تواند ارزش ویژگی تجزیه و یا ارزش می کند نوع داده ویژگی مطابقت ندارد.

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/functions/ext_dict_functions/) <!--hide-->
