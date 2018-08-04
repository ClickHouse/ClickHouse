<div dir="rtl">

# ClickHouse چیست؟

ClickHouse یک مدیریت دیتابیس (DBMS) ستون گرا برای پردازش تحلیلی آنلاین (OLAP) می باشد.


در یک مدیریت دیتابیس ردیف گرا، داده ها به فرم زیر ذخیره سازی می شوند:


| Row | WatchID             | JavaEnable | Title              | GoodEvent | EventTime           |
| --- | ------------------- | ---------- | ------------------ | --------- | ------------------- |
| #0  | 5385521489354350662 | 1          | Investor Relations | 1         | 2016-05-18 05:19:20 |
| #1  | 5385521490329509958 | 0          | Contact us         | 1         | 2016-05-18 08:10:20 |
| #2  | 5385521489953706054 | 1          | Mission            | 1         | 2016-05-18 07:38:00 |
| #N  | ...                 | ...        | ...                | ...       | ...                 |

به این صورت، تمام مقادیر مربوط به یک سطر (رکورد) به صورت فیزیکی و در کنار یکدگیر ذخیره سازی می شوند.

دیتابیس های MySQL, Postgres و MS SQL Server از انواع دیتابیس های ردیف گرا می باشند.
{: .grey }

در یک دیتابیس ستون گرا، داده ها به شکل زیر ذخیره سازی می شوند:


| Row:        | #0                  | #1                  | #2                  | #N                  |
| ----------- | ------------------- | ------------------- | ------------------- | ------------------- |
| WatchID:    | 5385521489354350662 | 5385521490329509958 | 5385521489953706054 | ...                 |
| JavaEnable: | 1                   | 0                   | 1                   | ...                 |
| Title:      | Investor Relations  | Contact us          | Mission             | ...                 |
| GoodEvent:  | 1                   | 1                   | 1                   | ...                 |
| EventTime:  | 2016-05-18 05:19:20 | 2016-05-18 08:10:20 | 2016-05-18 07:38:00 | ...                 |


این مثال ها تنها نشان می دهند که داده ها منظم شده اند.
مقادیر ستون های مختلف به صورت جدا، و داده های مربوط به یک ستون در کنار یکدیگر ذخیره می شوند.

مثال های از دیتابیس های ستون گرا: Vertica, Paraccel (Actian Matrix, Amazon Redshift), Sybase IQ, Exasol, Infobright, InfiniDB, MonetDB (VectorWise, Actian Vector), LucidDB, SAP HANA, Google Dremel, Google PowerDrill, Druid, kdb+.
{: .grey }

ترتیب های مختلف برای ذخیره سازی داده ها، مناسب سناریو های مختلف هستند. سناریو دسترسی به داده اشاره دارد به، چه query هایی ساخته شده اند، چند وقت به چند وقت، در چه مقداری، چقدر داده در هنگام اجرای هر query خوانده می شود، چند رکورد، چند ستون و چند بایت؛ رابطه ی بین خوانده و نوشتن داده؛ سایز دیتاسی فعال مورد استفاده و نحوه ی استفاده آن به صورت محلی؛ آیا از تراکنش استفاده می شود؛ چگونه داده ها جدا می شوند؛ نیازمندی ها برای replication داده ها و یکپارچگی منطقی داده ها؛ نیازمندی ها برای latency و throughput برای هر نوع از query، و...

مهمتر از بالا بودن لود سیستم، سفارشی کردن سیستم مطابق با نیازمندی های سناریو می باشد، و این سفارشی سازی در ادامه دقیق تر می شود. هیج سیستمی وجود ندارد که مناسب انجام سناریو های متفاوت(بسیار متفاوت) باشد. اگر یک سیستم برای اجرای سناریو های مختلف آداپته شده باشد، در زمان بالا بودن لود، سیستم تمام سناریوها را به صورت ضعیف handle می کند.

## ویژگی های کلیدی یک سناریو OLAP

- اکثریت درخواست های برای خواندن می باشد.
- Data is ingested in fairly large batches (> 1000 rows), not by single rows; or it is not updated at all.
- داده ها به دیتابیس اضافه می شوند و تغییر پیدا نمی کنند.
- برای خواندن، تعداد زیادی از رکورد ها از دیتابیس استخراج می شوند، اما فقط چند ستون از رکورد ها.
- جداول "wide" هستند، به این معنی تعداد زیادی ستون دارند.
- query ها نسبتا کم هستند (معمولا صدها query در ثانیه به ازای هر سرور یا کمتر)
- برای query های ساده، زمان تاخیر 50 میلی ثانیه مجاز باشد.
- مقادیر ستون ها کوچک باشد: اعداد و رشته های کوتاه (برای مثال 60 بایت به ازای هر url)
- نیازمند throughput بالا در هنگام اجرای یک query (بالای یک میلیارد رکورد در هر ثانیه به ازای هر سرور)
- تراکنش واجب نیست.
- نیازمندی کم برای consistency بودن داده ها.
- فقط یک جدول بزرگ به ازای هر query وجود دارد. تمام جداول کوچک هستند، به جز یکی.
- A query result is significantly smaller than the source data. In other words, data is filtered or aggregated, so the result fits in a single server's RAM.
- نتیجه query به طول قابل توجهی کوچکتر از source داده ها می باشد. به عبارتی دیگر در یک query، داده ها فیلتر یا تجمیع می شوند، پس نتایج در RAM یک سرور فیت می شوند.

خوب خیلی ساده می توان دید که سناریو های OLAP خیلی متفاوت تر از دیگر سناریو های محبوب هستند (مثل OLTP یا Key-Value). پس اگر میخواهید performance مناسب داشته باشید، استفاده از دیتابیس های OLTP یا Key-Value برای اجرای query های OLAP معنی ندارد. برای مثال، اگر شما از دیتابیس MongoDB یا Redis برای آنالیز استفاده کنید، قطعا performance بسیار ضعیف تری نسبت به دیتابیس های OLAP خواهید داشت.

## Reasons why columnar databases are better suited for OLAP scenario


Column-oriented databases are better suited to OLAP scenarios (at least 100 times better in processing speed for most queries). The reasons for that are explained below in detail, but it's easier to be demonstrated visually:

**Row oriented**

![Row oriented](images/row_oriented.gif#)

**Column oriented**

![Column oriented](images/column_oriented.gif#)

See the difference? Read further to learn why this happens.

### Input/output

1. For an analytical query, only a small number of table columns need to be read. In a column-oriented database, you can read just the data you need. For example, if you need 5 columns out of 100, you can expect a 20-fold reduction in I/O.
2. Since data is read in packets, it is easier to compress. Data in columns is also easier to compress. This further reduces the I/O volume.
3. Due to the reduced I/O, more data fits in the system cache.

For example, the query "count the number of records for each advertising platform" requires reading one "advertising platform ID" column, which takes up 1 byte uncompressed. If most of the traffic was not from advertising platforms, you can expect at least 10-fold compression of this column. When using a quick compression algorithm, data decompression is possible at a speed of at least several gigabytes of uncompressed data per second. In other words, this query can be processed at a speed of approximately several billion rows per second on a single server. This speed is actually achieved in practice.

<details><summary>Example</summary>
<p>
<pre>
$ clickhouse-client
ClickHouse client version 0.0.52053.
Connecting to localhost:9000.
Connected to ClickHouse server version 0.0.52053.

:) SELECT CounterID, count() FROM hits GROUP BY CounterID ORDER BY count() DESC LIMIT 20

SELECT
    CounterID,
    count()
FROM hits
GROUP BY CounterID
ORDER BY count() DESC
LIMIT 20

┌─CounterID─┬──count()─┐
│    114208 │ 56057344 │
│    115080 │ 51619590 │
│      3228 │ 44658301 │
│     38230 │ 42045932 │
│    145263 │ 42042158 │
│     91244 │ 38297270 │
│    154139 │ 26647572 │
│    150748 │ 24112755 │
│    242232 │ 21302571 │
│    338158 │ 13507087 │
│     62180 │ 12229491 │
│     82264 │ 12187441 │
│    232261 │ 12148031 │
│    146272 │ 11438516 │
│    168777 │ 11403636 │
│   4120072 │ 11227824 │
│  10938808 │ 10519739 │
│     74088 │  9047015 │
│    115079 │  8837972 │
│    337234 │  8205961 │
└───────────┴──────────┘

20 rows in set. Elapsed: 0.153 sec. Processed 1.00 billion rows, 4.00 GB (6.53 billion rows/s., 26.10 GB/s.)

:)
</pre>
</p>
</details>

### CPU

Since executing a query requires processing a large number of rows, it helps to dispatch all operations for entire vectors instead of for separate rows, or to implement the query engine so that there is almost no dispatching cost. If you don't do this, with any half-decent disk subsystem, the query interpreter inevitably stalls the CPU.
It makes sense to both store data in columns and process it, when possible, by columns.

There are two ways to do this:

1. A vector engine. All operations are written for vectors, instead of for separate values. This means you don't need to call operations very often, and dispatching costs are negligible. Operation code contains an optimized internal cycle.

2. Code generation. The code generated for the query has all the indirect calls in it.

This is not done in "normal" databases, because it doesn't make sense when running simple queries. However, there are exceptions. For example, MemSQL uses code generation to reduce latency when processing SQL queries. (For comparison, analytical DBMSs require optimization of throughput, not latency.)

Note that for CPU efficiency, the query language must be declarative (SQL or MDX), or at least a vector (J, K). The query should only contain implicit loops, allowing for optimization.

</div>