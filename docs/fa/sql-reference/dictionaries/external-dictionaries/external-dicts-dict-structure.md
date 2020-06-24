---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 44
toc_title: "\u06A9\u0644\u06CC\u062F \u0641\u0631\u0647\u0646\u06AF \u0644\u063A\u062A\
  \ \u0648 \u0632\u0645\u06CC\u0646\u0647 \u0647\u0627\u06CC"
---

# کلید فرهنگ لغت و زمینه های {#dictionary-key-and-fields}

این `<structure>` بند توصیف کلید فرهنگ لغت و زمینه های موجود برای نمایش داده شد.

توصیف:

``` xml
<dictionary>
    <structure>
        <id>
            <name>Id</name>
        </id>

        <attribute>
            <!-- Attribute parameters -->
        </attribute>

        ...

    </structure>
</dictionary>
```

صفات در عناصر شرح داده شده است:

-   `<id>` — [ستون کلید](external-dicts-dict-structure.md#ext_dict_structure-key).
-   `<attribute>` — [ستون داده](external-dicts-dict-structure.md#ext_dict_structure-attributes). می تواند تعدادی از ویژگی های وجود دارد.

پرسوجو:

``` sql
CREATE DICTIONARY dict_name (
    Id UInt64,
    -- attributes
)
PRIMARY KEY Id
...
```

صفات در بدن پرس و جو توصیف:

-   `PRIMARY KEY` — [ستون کلید](external-dicts-dict-structure.md#ext_dict_structure-key)
-   `AttrName AttrType` — [ستون داده](external-dicts-dict-structure.md#ext_dict_structure-attributes). می تواند تعدادی از ویژگی های وجود دارد.

## کلید {#ext_dict_structure-key}

تاتر از انواع زیر از کلید:

-   کلید عددی. `UInt64`. تعریف شده در `<id>` برچسب یا استفاده `PRIMARY KEY` کلمه کلیدی.
-   کلید کامپوزیت. مجموعه ای از مقادیر از انواع مختلف. تعریف شده در برچسب `<key>` یا `PRIMARY KEY` کلمه کلیدی.

یک ساختار میلی لیتر می تواند شامل موارد زیر باشد `<id>` یا `<key>`. دی ال پرس و جو باید شامل تک `PRIMARY KEY`.

!!! warning "اخطار"
    شما باید کلید به عنوان یک ویژگی توصیف نیست.

### کلید عددی {#ext_dict-numeric-key}

نوع: `UInt64`.

مثال پیکربندی:

``` xml
<id>
    <name>Id</name>
</id>
```

حوزههای پیکربندی:

-   `name` – The name of the column with keys.

برای & پرسوجو:

``` sql
CREATE DICTIONARY (
    Id UInt64,
    ...
)
PRIMARY KEY Id
...
```

-   `PRIMARY KEY` – The name of the column with keys.

### کلید کامپوزیت {#composite-key}

کلید می تواند یک `tuple` از هر نوع زمینه. این [طرحبندی](external-dicts-dict-layout.md) در این مورد باید باشد `complex_key_hashed` یا `complex_key_cache`.

!!! tip "نکته"
    کلید کامپوزیت می تواند از یک عنصر واحد تشکیل شده است. این امکان استفاده از یک رشته به عنوان کلید, برای مثال.

ساختار کلیدی در عنصر تنظیم شده است `<key>`. زمینه های کلیدی در قالب همان فرهنگ لغت مشخص شده است [خصیصهها](external-dicts-dict-structure.md). مثال:

``` xml
<structure>
    <key>
        <attribute>
            <name>field1</name>
            <type>String</type>
        </attribute>
        <attribute>
            <name>field2</name>
            <type>UInt32</type>
        </attribute>
        ...
    </key>
...
```

یا

``` sql
CREATE DICTIONARY (
    field1 String,
    field2 String
    ...
)
PRIMARY KEY field1, field2
...
```

برای پرس و جو به `dictGet*` تابع, یک تاپل به عنوان کلید به تصویب رسید. مثال: `dictGetString('dict_name', 'attr_name', tuple('string for field1', num_for_field2))`.

## خصیصهها {#ext_dict_structure-attributes}

مثال پیکربندی:

``` xml
<structure>
    ...
    <attribute>
        <name>Name</name>
        <type>ClickHouseDataType</type>
        <null_value></null_value>
        <expression>rand64()</expression>
        <hierarchical>true</hierarchical>
        <injective>true</injective>
        <is_object_id>true</is_object_id>
    </attribute>
</structure>
```

یا

``` sql
CREATE DICTIONARY somename (
    Name ClickHouseDataType DEFAULT '' EXPRESSION rand64() HIERARCHICAL INJECTIVE IS_OBJECT_ID
)
```

حوزههای پیکربندی:

| برچسب                                                | توصیف                                                                                                                                                                                                                                                                                                                    | مورد نیاز |
|------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------|
| `name`                                               | نام ستون.                                                                                                                                                                                                                                                                                                                | بله       |
| `type`                                               | نوع داده کلیک.<br/>تاتر تلاش می کند به بازیگران ارزش از فرهنگ لغت به نوع داده مشخص شده است. مثلا, برای خروجی زیر, زمینه ممکن است `TEXT`, `VARCHAR` یا `BLOB` در جدول منبع خروجی زیر, اما می تواند به عنوان ارسال `String` در فاحشه خانه.<br/>[Nullable](../../../sql-reference/data-types/nullable.md) پشتیبانی نمی شود. | بله       |
| `null_value`                                         | مقدار پیش فرض برای یک عنصر غیر موجود.<br/>در مثال این یک رشته خالی است. شما نمی توانید استفاده کنید `NULL` در این زمینه.                                                                                                                                                                                                 | بله       |
| `expression`                                         | [عبارت](../../syntax.md#syntax-expressions) که فاحشه خانه اجرا در ارزش.<br/>بیان می تواند یک نام ستون در پایگاه داده از راه دور گذاشتن. بدین ترتیب, شما می توانید برای ایجاد یک نام مستعار برای ستون از راه دور استفاده.<br/><br/>مقدار پیش فرض: بدون بیان.                                                              | نه        |
| <a name="hierarchical-dict-attr"></a> `hierarchical` | اگر `true`, ویژگی شامل ارزش یک کلید پدر و مادر برای کلید فعلی. ببینید [لغتنامهها سلسله مراتبی](external-dicts-dict-hierarchical.md).<br/><br/>مقدار پیشفرض: `false`.                                                                                                                                                     | نه        |
| `injective`                                          | پرچمی که نشان میدهد چه `id -> attribute` تصویر [تزریق](https://en.wikipedia.org/wiki/Injective_function).<br/>اگر `true`, کلیک خانه به طور خودکار می تواند پس از محل `GROUP BY` بند درخواست به لغت نامه با تزریق. معمولا به طور قابل توجهی میزان چنین درخواست را کاهش می دهد.<br/><br/>مقدار پیشفرض: `false`.            | نه        |
| `is_object_id`                                       | پرچمی که نشان میدهد پرسوجو برای سند مانگودیبی اجرا شده است `ObjectID`.<br/><br/>مقدار پیشفرض: `false`.                                                                                                                                                                                                                   | نه        |

## همچنین نگاه کنید به {#see-also}

-   [توابع برای کار با لغت نامه های خارجی](../../../sql-reference/functions/ext-dict-functions.md).

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts_dict_structure/) <!--hide-->
