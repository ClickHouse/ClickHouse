---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 32
toc_title: "\u06A9\u0627\u0641\u06A9\u0627"
---

# کافکا {#kafka}

این موتور با این نسخهها کار [نمایی کافکا](http://kafka.apache.org/).

کافکا به شما امکان می دهد:

-   انتشار یا اشتراک در جریان داده ها.
-   سازماندهی ذخیره سازی مقاوم در برابر خطا.
-   روند جریان به عنوان در دسترس تبدیل شده است.

## ایجاد یک جدول {#table_engine-kafka-creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'host:port',
    kafka_topic_list = 'topic1,topic2,...',
    kafka_group_name = 'group_name',
    kafka_format = 'data_format'[,]
    [kafka_row_delimiter = 'delimiter_symbol',]
    [kafka_schema = '',]
    [kafka_num_consumers = N,]
    [kafka_max_block_size = 0,]
    [kafka_skip_broken_messages = N,]
    [kafka_commit_every_batch = 0]
```

پارامترهای مورد نیاز:

-   `kafka_broker_list` – A comma-separated list of brokers (for example, `localhost:9092`).
-   `kafka_topic_list` – A list of Kafka topics.
-   `kafka_group_name` – A group of Kafka consumers. Reading margins are tracked for each group separately. If you don't want messages to be duplicated in the cluster, use the same group name everywhere.
-   `kafka_format` – Message format. Uses the same notation as the SQL `FORMAT` تابع مانند `JSONEachRow`. برای کسب اطلاعات بیشتر, دیدن [فرشها](../../../interfaces/formats.md) بخش.

پارامترهای اختیاری:

-   `kafka_row_delimiter` – Delimiter character, which ends the message.
-   `kafka_schema` – Parameter that must be used if the format requires a schema definition. For example, [سروان نیا](https://capnproto.org/) نیاز به مسیر به فایل طرح و نام ریشه `schema.capnp:Message` اعتراض.
-   `kafka_num_consumers` – The number of consumers per table. Default: `1`. مشخص مصرف کنندگان بیشتر اگر توان عملیاتی یک مصرف کننده کافی است. تعداد کل مصرف کنندگان باید تعداد پارتیشن در موضوع تجاوز نمی, از تنها یک مصرف کننده را می توان در هر پارتیشن اختصاص داده.
-   `kafka_max_block_size` - حداکثر اندازه دسته ای (در پیام) برای نظرسنجی (پیش فرض: `max_block_size`).
-   `kafka_skip_broken_messages` – Kafka message parser tolerance to schema-incompatible messages per block. Default: `0`. اگر `kafka_skip_broken_messages = N` سپس موتور پرش *N* پیام کافکا که نمی تواند تجزیه شود (یک پیام برابر یک ردیف از داده ها).
-   `kafka_commit_every_batch` - متعهد هر دسته مصرف و به کار گرفته به جای یک مرتکب پس از نوشتن یک بلوک کامل (به طور پیش فرض: `0`).

مثالها:

``` sql
  CREATE TABLE queue (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka('localhost:9092', 'topic', 'group1', 'JSONEachRow');

  SELECT * FROM queue LIMIT 5;

  CREATE TABLE queue2 (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka SETTINGS kafka_broker_list = 'localhost:9092',
                            kafka_topic_list = 'topic',
                            kafka_group_name = 'group1',
                            kafka_format = 'JSONEachRow',
                            kafka_num_consumers = 4;

  CREATE TABLE queue2 (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka('localhost:9092', 'topic', 'group1')
              SETTINGS kafka_format = 'JSONEachRow',
                       kafka_num_consumers = 4;
```

<details markdown="1">

<summary>روش منسوخ برای ایجاد یک جدول</summary>

!!! attention "توجه"
    از این روش در پروژه های جدید استفاده نکنید. در صورت امکان, تغییر پروژه های قدیمی به روش بالا توضیح.

``` sql
Kafka(kafka_broker_list, kafka_topic_list, kafka_group_name, kafka_format
      [, kafka_row_delimiter, kafka_schema, kafka_num_consumers, kafka_skip_broken_messages])
```

</details>

## توصیف {#description}

پیام تحویل به طور خودکار ردیابی, بنابراین هر پیام در یک گروه تنها یک بار شمارش. اگر شما می خواهید برای دریافت داده ها دو بار, سپس یک کپی از جدول با نام گروه دیگری ایجاد.

گروه انعطاف پذیر هستند و همگام سازی در خوشه. برای مثال, اگر شما 10 موضوعات و 5 نسخه از یک جدول در یک خوشه, سپس هر کپی می شود 2 موضوعات. اگر تعداد نسخه تغییر, موضوعات در سراسر نسخه توزیع به طور خودکار. اطلاعات بیشتر در مورد این در http://kafka.apache.org/intro.

`SELECT` به خصوص برای خواندن پیام های مفید نیست (به جز اشکال زدایی), چرا که هر پیام را می توان تنها یک بار به عنوان خوانده شده. این عملی تر است برای ایجاد موضوعات در زمان واقعی با استفاده از نمایش محقق. برای انجام این کار:

1.  از موتور برای ایجاد یک مصرف کننده کافکا استفاده کنید و جریان داده را در نظر بگیرید.
2.  ایجاد یک جدول با ساختار مورد نظر.
3.  یک دیدگاه محقق ایجاد کنید که داده ها را از موتور تبدیل می کند و به یک جدول قبلا ایجاد شده تبدیل می کند.

هنگامی که `MATERIALIZED VIEW` به موتور می پیوندد و شروع به جمع کردن داده ها در پس زمینه می کند. این اجازه می دهد تا شما را به طور مستمر دریافت پیام از کافکا و تبدیل به فرمت مورد نیاز با استفاده از `SELECT`.
یک جدول کافکا می تواند به عنوان بسیاری از دیدگاه های تحقق به عنوان دوست دارید, اطلاعات از جدول کافکا به طور مستقیم به عنوان خوانده شده, اما دریافت پرونده های جدید (در بلوک), به این ترتیب شما می توانید به چند جدول با سطح جزییات مختلف ارسال (با گروه بندی - تجمع و بدون).

مثال:

``` sql
  CREATE TABLE queue (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka('localhost:9092', 'topic', 'group1', 'JSONEachRow');

  CREATE TABLE daily (
    day Date,
    level String,
    total UInt64
  ) ENGINE = SummingMergeTree(day, (day, level), 8192);

  CREATE MATERIALIZED VIEW consumer TO daily
    AS SELECT toDate(toDateTime(timestamp)) AS day, level, count() as total
    FROM queue GROUP BY day, level;

  SELECT level, sum(total) FROM daily GROUP BY level;
```

برای بهبود عملکرد, پیام های دریافت شده را به بلوک های اندازه گروه بندی می شوند [ا\_فزونهها](../../../operations/server-configuration-parameters/settings.md#settings-max_insert_block_size). اگر بلوک در داخل تشکیل نشده است [\_خاله جریان](../../../operations/server-configuration-parameters/settings.md) میلی ثانیه, داده خواهد شد به جدول بدون در نظر گرفتن کامل از بلوک سرخ.

برای جلوگیری از دریافت داده های موضوع و یا تغییر منطق تبدیل جدا مشاهده محقق:

``` sql
  DETACH TABLE consumer;
  ATTACH TABLE consumer;
```

اگر شما می خواهید به تغییر جدول هدف با استفاده از `ALTER` توصیه می کنیم دیدگاه مادی را غیرفعال کنید تا از اختلاف بین جدول هدف و داده ها از نظر جلوگیری شود.

## پیکربندی {#configuration}

شبیه به GraphiteMergeTree های کافکا پشتیبانی از موتور تمدید پیکربندی با استفاده از ClickHouse فایل پیکربندی. دو کلید پیکربندی است که شما می توانید استفاده کنید وجود دارد: جهانی (`kafka`) و سطح موضوع (`kafka_*`). پیکربندی جهانی برای اولین بار اعمال می شود و سپس پیکربندی سطح موضوع اعمال می شود (در صورت وجود).

``` xml
  <!-- Global configuration options for all tables of Kafka engine type -->
  <kafka>
    <debug>cgrp</debug>
    <auto_offset_reset>smallest</auto_offset_reset>
  </kafka>

  <!-- Configuration specific for topic "logs" -->
  <kafka_logs>
    <retry_backoff_ms>250</retry_backoff_ms>
    <fetch_min_bytes>100000</fetch_min_bytes>
  </kafka_logs>
```

برای یک لیست از گزینه های پیکربندی ممکن, دیدن [مرجع پیکربندی کتابدار](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md). استفاده از تاکید (`_`) به جای یک نقطه در پیکربندی کلیک. به عنوان مثال, `check.crcs=true` خواهد بود `<check_crcs>true</check_crcs>`.

## ستونهای مجازی {#virtual-columns}

-   `_topic` — Kafka topic.
-   `_key` — Key of the message.
-   `_offset` — Offset of the message.
-   `_timestamp` — Timestamp of the message.
-   `_partition` — Partition of Kafka topic.

**همچنین نگاه کنید به**

-   [ستونهای مجازی](../index.md#table_engines-virtual_columns)

[مقاله اصلی](https://clickhouse.tech/docs/en/operations/table_engines/kafka/) <!--hide-->
