---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 38
toc_title: "\u0646\u0645\u0648\u062F\u0627\u0631"
---

# نمودار {#graphitemergetree}

این موتور طراحی شده است برای نازک شدن و جمع/متوسط (خلاصه) [گرافیت](http://graphite.readthedocs.io/en/latest/index.html) داده ها. این ممکن است به توسعه دهندگان که می خواهند به استفاده از تاتر به عنوان یک فروشگاه داده ها برای گرافیت مفید است.

شما می توانید هر موتور جدول کلیک برای ذخیره داده گرافیت اگر شما رولپ نیاز ندارد استفاده, اما اگر شما نیاز به یک استفاده خلاصه `GraphiteMergeTree`. موتور حجم ذخیره سازی را کاهش می دهد و بهره وری نمایش داده شد از گرافیت را افزایش می دهد.

موتور خواص از ارث می برد [ادغام](mergetree.md).

## ایجاد یک جدول {#creating-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    Path String,
    Time DateTime,
    Value <Numeric_type>,
    Version <Numeric_type>
    ...
) ENGINE = GraphiteMergeTree(config_section)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

مشاهده شرح مفصلی از [CREATE TABLE](../../../sql-reference/statements/create.md#create-table-query) پرس و جو.

یک جدول برای داده های گرافیت باید ستون های زیر را برای داده های زیر داشته باشد:

-   نام متریک (سنسور گرافیت). نوع داده: `String`.

-   زمان اندازه گیری متریک. نوع داده: `DateTime`.

-   ارزش متریک. نوع داده: هر عددی.

-   نسخه از متریک. نوع داده: هر عددی.

    تاتر موجب صرفه جویی در ردیف با بالاترین نسخه و یا گذشته نوشته شده است اگر نسخه یکسان هستند. ردیف های دیگر در طول ادغام قطعات داده حذف می شوند.

نام این ستون ها باید در پیکربندی خلاصه مجموعه.

**پارامترهای نمودار**

-   `config_section` — Name of the section in the configuration file, where are the rules of rollup set.

**بندهای پرسوجو**

هنگام ایجاد یک `GraphiteMergeTree` جدول, همان [بند](mergetree.md#table_engine-mergetree-creating-a-table) در هنگام ایجاد یک مورد نیاز است `MergeTree` جدول

<details markdown="1">

<summary>روش منسوخ برای ایجاد یک جدول</summary>

!!! attention "توجه"
    هنوز این روش در پروژه های جدید استفاده کنید و, در صورت امکان, تغییر پروژه های قدیمی به روش بالا توضیح.

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    EventDate Date,
    Path String,
    Time DateTime,
    Value <Numeric_type>,
    Version <Numeric_type>
    ...
) ENGINE [=] GraphiteMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, config_section)
```

همه پارامترها به استثنای `config_section` همان معنی را در `MergeTree`.

-   `config_section` — Name of the section in the configuration file, where are the rules of rollup set.

</details>

## پیکربندی رولپ {#rollup-configuration}

تنظیمات برای خلاصه توسط تعریف [لغزش _ نمودار](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-graphite) پارامتر در پیکربندی سرور. نام پارامتر می تواند هر. شما می توانید تنظیمات متعددی ایجاد کنید و برای جداول مختلف استفاده کنید.

ساختار پیکربندی رولپ:

      required-columns
      patterns

### ستون های مورد نیاز {#required-columns}

-   `path_column_name` — The name of the column storing the metric name (Graphite sensor). Default value: `Path`.
-   `time_column_name` — The name of the column storing the time of measuring the metric. Default value: `Time`.
-   `value_column_name` — The name of the column storing the value of the metric at the time set in `time_column_name`. مقدار پیشفرض: `Value`.
-   `version_column_name` — The name of the column storing the version of the metric. Default value: `Timestamp`.

### الگوها {#patterns}

ساختار `patterns` بخش:

``` text
pattern
    regexp
    function
pattern
    regexp
    age + precision
    ...
pattern
    regexp
    function
    age + precision
    ...
pattern
    ...
default
    function
    age + precision
    ...
```

!!! warning "توجه"
    الگوها باید به شدت دستور داده شوند:

      1. Patterns without `function` or `retention`.
      1. Patterns with both `function` and `retention`.
      1. Pattern `default`.

هنگام پردازش یک ردیف, تاتر چک قوانین در `pattern` بخش. هر یک از `pattern` (شامل `default`) بخش می تواند شامل `function` پارامتر برای تجمع, `retention` پارامترها یا هر دو. اگر نام متریک با `regexp`, قوانین از `pattern` بخش (یا بخش) اعمال می شود; در غیر این صورت, قوانین از `default` بخش استفاده می شود.

زمینه برای `pattern` و `default` بخش ها:

-   `regexp`– A pattern for the metric name.
-   `age` – The minimum age of the data in seconds.
-   `precision`– How precisely to define the age of the data in seconds. Should be a divisor for 86400 (seconds in a day).
-   `function` – The name of the aggregating function to apply to data whose age falls within the range `[age, age + precision]`.

### مثال پیکربندی {#configuration-example}

``` xml
<graphite_rollup>
    <version_column_name>Version</version_column_name>
    <pattern>
        <regexp>click_cost</regexp>
        <function>any</function>
        <retention>
            <age>0</age>
            <precision>5</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>60</precision>
        </retention>
    </pattern>
    <default>
        <function>max</function>
        <retention>
            <age>0</age>
            <precision>60</precision>
        </retention>
        <retention>
            <age>3600</age>
            <precision>300</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>3600</precision>
        </retention>
    </default>
</graphite_rollup>
```

[مقاله اصلی](https://clickhouse.tech/docs/en/operations/table_engines/graphitemergetree/) <!--hide-->
