---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 41
toc_title: "\u0627\u0633\u062A\u0641\u0627\u062F\u0647 \u0627\u0632 \u0645\u062F\u0644\
  \ \u0647\u0627\u06CC \u0627\u062F\u0645 \u06A9\u0648\u062F\u0646 \u0648 \u0627\u062D\
  \u0645\u0642"
---

# استفاده از مدل ادم کودن و احمق در فاحشه خانه {#applying-catboost-model-in-clickhouse}

[مانتو](https://catboost.ai) یک کتابخانه تقویت شیب رایگان و منبع باز توسعه یافته در [یاندکس](https://yandex.com/company/) برای یادگیری ماشین.

با استفاده از این دستورالعمل یاد خواهید گرفت که با اجرای مدل استنتاج از میدان از مدل های پیش روت شده در خانه استفاده کنید.

برای اعمال یک مدل ادم کودن و احمق در خانه کلیک کنید:

1.  [ایجاد یک جدول](#create-table).
2.  [درج داده به جدول](#insert-data-to-table).
3.  [ادغام کاتبوست به کلیک](#integrate-catboost-into-clickhouse) (مرحله اختیاری).
4.  [اجرای مدل استنتاج از گذاشتن](#run-model-inference).

برای کسب اطلاعات بیشتر در مورد اموزش مدل های کاتبوست مراجعه کنید [اموزش و مدل سازی](https://catboost.ai/docs/features/training.html#training).

## پیش نیازها {#prerequisites}

اگر شما لازم نیست که [کارگر بارانداز](https://docs.docker.com/install/) هنوز, نصب کنید.

!!! note "یادداشت"
    [کارگر بارانداز](https://www.docker.com) یک پلت فرم نرم افزار است که اجازه می دهد تا به شما برای ایجاد ظروف که منزوی CatBoost و ClickHouse نصب و راه اندازی از بقیه سیستم.

قبل از استفاده از مدل ادم کودن و احمق:

**1.** بکش [تصویر کارگر بارانداز](https://hub.docker.com/r/yandex/tutorial-catboost-clickhouse) از رجیستری:

``` bash
$ docker pull yandex/tutorial-catboost-clickhouse
```

این Docker تصویر شامل همه چیز شما نیاز به اجرای CatBoost و ClickHouse: کد در زمان اجرا کتابخانه های محیط متغیر و فایل های پیکربندی.

**2.** اطمینان حاصل کنید که تصویر کارگر بارانداز شده است با موفقیت کشیده:

``` bash
$ docker image ls
REPOSITORY                            TAG                 IMAGE ID            CREATED             SIZE
yandex/tutorial-catboost-clickhouse   latest              622e4d17945b        22 hours ago        1.37GB
```

**3.** شروع یک ظرف کارگر بارانداز بر اساس این تصویر:

``` bash
$ docker run -it -p 8888:8888 yandex/tutorial-catboost-clickhouse
```

## 1. ایجاد یک جدول {#create-table}

برای ایجاد یک میز کلیک برای نمونه تمرین:

**1.** شروع مشتری کنسول کلیک در حالت تعاملی:

``` bash
$ clickhouse client
```

!!! note "یادداشت"
    سرور کلیک در حال حاضر در داخل ظرف کارگر بارانداز در حال اجرا.

**2.** ایجاد جدول با استفاده از دستور:

``` sql
:) CREATE TABLE amazon_train
(
    date Date MATERIALIZED today(),
    ACTION UInt8,
    RESOURCE UInt32,
    MGR_ID UInt32,
    ROLE_ROLLUP_1 UInt32,
    ROLE_ROLLUP_2 UInt32,
    ROLE_DEPTNAME UInt32,
    ROLE_TITLE UInt32,
    ROLE_FAMILY_DESC UInt32,
    ROLE_FAMILY UInt32,
    ROLE_CODE UInt32
)
ENGINE = MergeTree ORDER BY date
```

**3.** خروج از مشتری کنسول کلیک کنید:

``` sql
:) exit
```

## 2. درج داده به جدول {#insert-data-to-table}

برای وارد کردن داده ها:

**1.** دستور زیر را اجرا کنید:

``` bash
$ clickhouse client --host 127.0.0.1 --query 'INSERT INTO amazon_train FORMAT CSVWithNames' < ~/amazon/train.csv
```

**2.** شروع مشتری کنسول کلیک در حالت تعاملی:

``` bash
$ clickhouse client
```

**3.** اطمینان حاصل کنید که داده ها ارسال شده است:

``` sql
:) SELECT count() FROM amazon_train

SELECT count()
FROM amazon_train

+-count()-+
|   65538 |
+-------+
```

## 3. ادغام کاتبوست به کلیک {#integrate-catboost-into-clickhouse}

!!! note "یادداشت"
    **گام اختیاری.** این Docker تصویر شامل همه چیز شما نیاز به اجرای CatBoost و ClickHouse.

برای ادغام کاتبوست به کلیک:

**1.** ساخت کتابخانه ارزیابی.

سریعترین راه برای ارزیابی مدل ادم کودن و احمق کامپایل است `libcatboostmodel.<so|dll|dylib>` کتابخونه. برای کسب اطلاعات بیشتر در مورد چگونگی ساخت کتابخانه, دیدن [مستندات غلطیاب](https://catboost.ai/docs/concepts/c-plus-plus-api_dynamic-c-pluplus-wrapper.html).

**2.** ایجاد یک دایرکتوری جدید در هر کجا و با هر نام, مثلا, `data` و کتابخونه درستشون رو توش بذار. تصویر کارگر بارانداز در حال حاضر شامل کتابخانه `data/libcatboostmodel.so`.

**3.** ایجاد یک دایرکتوری جدید برای مدل پیکربندی در هر کجا و با هر نام, مثلا, `models`.

**4.** برای مثال یک فایل پیکربندی مدل با هر نام ایجاد کنید, `models/amazon_model.xml`.

**5.** توصیف پیکربندی مدل:

``` xml
<models>
    <model>
        <!-- Model type. Now catboost only. -->
        <type>catboost</type>
        <!-- Model name. -->
        <name>amazon</name>
        <!-- Path to trained model. -->
        <path>/home/catboost/tutorial/catboost_model.bin</path>
        <!-- Update interval. -->
        <lifetime>0</lifetime>
    </model>
</models>
```

**6.** اضافه کردن مسیر به CatBoost و مدل پیکربندی به پیکربندی ClickHouse:

``` xml
<!-- File etc/clickhouse-server/config.d/models_config.xml. -->
<catboost_dynamic_library_path>/home/catboost/data/libcatboostmodel.so</catboost_dynamic_library_path>
<models_config>/home/catboost/models/*_model.xml</models_config>
```

## 4. اجرای مدل استنتاج از گذاشتن {#run-model-inference}

برای مدل تست اجرای مشتری کلیک `$ clickhouse client`.

بیایید اطمینان حاصل کنیم که مدل کار می کند:

``` sql
:) SELECT
    modelEvaluate('amazon',
                RESOURCE,
                MGR_ID,
                ROLE_ROLLUP_1,
                ROLE_ROLLUP_2,
                ROLE_DEPTNAME,
                ROLE_TITLE,
                ROLE_FAMILY_DESC,
                ROLE_FAMILY,
                ROLE_CODE) > 0 AS prediction,
    ACTION AS target
FROM amazon_train
LIMIT 10
```

!!! note "یادداشت"
    تابع [مدلووات](../sql-reference/functions/other-functions.md#function-modelevaluate) را برمی گرداند تاپل با پیش بینی های خام در هر کلاس برای مدل های چند طبقه.

بیایید احتمال را پیش بینی کنیم:

``` sql
:) SELECT
    modelEvaluate('amazon',
                RESOURCE,
                MGR_ID,
                ROLE_ROLLUP_1,
                ROLE_ROLLUP_2,
                ROLE_DEPTNAME,
                ROLE_TITLE,
                ROLE_FAMILY_DESC,
                ROLE_FAMILY,
                ROLE_CODE) AS prediction,
    1. / (1 + exp(-prediction)) AS probability,
    ACTION AS target
FROM amazon_train
LIMIT 10
```

!!! note "یادداشت"
    اطلاعات بیشتر در مورد [خروج()](../sql-reference/functions/math-functions.md) تابع.

بیایید محاسبه لگ در نمونه:

``` sql
:) SELECT -avg(tg * log(prob) + (1 - tg) * log(1 - prob)) AS logloss
FROM
(
    SELECT
        modelEvaluate('amazon',
                    RESOURCE,
                    MGR_ID,
                    ROLE_ROLLUP_1,
                    ROLE_ROLLUP_2,
                    ROLE_DEPTNAME,
                    ROLE_TITLE,
                    ROLE_FAMILY_DESC,
                    ROLE_FAMILY,
                    ROLE_CODE) AS prediction,
        1. / (1. + exp(-prediction)) AS prob,
        ACTION AS tg
    FROM amazon_train
)
```

!!! note "یادداشت"
    اطلاعات بیشتر در مورد [میانگین()](../sql-reference/aggregate-functions/reference.md#agg_function-avg) و [ثبت()](../sql-reference/functions/math-functions.md) توابع.

[مقاله اصلی](https://clickhouse.tech/docs/en/guides/apply_catboost_model/) <!--hide-->
