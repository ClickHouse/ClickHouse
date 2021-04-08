---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 61
toc_title: "\u06A9\u0644\u06CC\u06A9-\u0645\u0639\u06CC\u0627\u0631"
---

# کلیک-معیار {#clickhouse-benchmark}

قابلیت اتصال به یک سرور کلیک و بارها و بارها نمایش داده شد مشخص می فرستد.

نحو:

``` bash
$ echo "single query" | clickhouse-benchmark [keys]
```

یا

``` bash
$ clickhouse-benchmark [keys] <<< "single query"
```

اگر شما می خواهید برای ارسال مجموعه ای از نمایش داده شد, ایجاد یک فایل متنی و قرار دادن هر پرس و جو در رشته های فردی در این فایل. به عنوان مثال:

``` sql
SELECT * FROM system.numbers LIMIT 10000000
SELECT 1
```

سپس این فایل را به یک ورودی استاندارد منتقل می کند `clickhouse-benchmark`.

``` bash
clickhouse-benchmark [keys] < queries_file
```

## کلید {#clickhouse-benchmark-keys}

-   `-c N`, `--concurrency=N` — Number of queries that `clickhouse-benchmark` می فرستد به طور همزمان. مقدار پیش فرض: 1.
-   `-d N`, `--delay=N` — Interval in seconds between intermediate reports (set 0 to disable reports). Default value: 1.
-   `-h WORD`, `--host=WORD` — Server host. Default value: `localhost`. برای [حالت مقایسه](#clickhouse-benchmark-comparison-mode) شما می توانید چند استفاده کنید `-h` کلیدا
-   `-p N`, `--port=N` — Server port. Default value: 9000. For the [حالت مقایسه](#clickhouse-benchmark-comparison-mode) شما می توانید چند استفاده کنید `-p` کلیدا
-   `-i N`, `--iterations=N` — Total number of queries. Default value: 0.
-   `-r`, `--randomize` — Random order of queries execution if there is more then one input query.
-   `-s`, `--secure` — Using TLS connection.
-   `-t N`, `--timelimit=N` — Time limit in seconds. `clickhouse-benchmark` متوقف می شود ارسال نمایش داده شد زمانی که محدودیت زمانی مشخص رسیده است. مقدار پیش فرض: 0 (محدودیت زمانی غیر فعال).
-   `--confidence=N` — Level of confidence for T-test. Possible values: 0 (80%), 1 (90%), 2 (95%), 3 (98%), 4 (99%), 5 (99.5%). Default value: 5. In the [حالت مقایسه](#clickhouse-benchmark-comparison-mode) `clickhouse-benchmark` انجام [تی تست دانشجویان مستقل دو نمونه](https://en.wikipedia.org/wiki/Student%27s_t-test#Independent_two-sample_t-test) تست برای تعیین اینکه دو توزیع با سطح انتخاب شده اعتماد به نفس متفاوت نیست.
-   `--cumulative` — Printing cumulative data instead of data per interval.
-   `--database=DATABASE_NAME` — ClickHouse database name. Default value: `default`.
-   `--json=FILEPATH` — JSON output. When the key is set, `clickhouse-benchmark` خروجی یک گزارش به مشخص جانسون فایل.
-   `--user=USERNAME` — ClickHouse user name. Default value: `default`.
-   `--password=PSWD` — ClickHouse user password. Default value: empty string.
-   `--stacktrace` — Stack traces output. When the key is set, `clickhouse-bencmark` خروجی پشته اثری از استثنا.
-   `--stage=WORD` — Query processing stage at server. ClickHouse stops query processing and returns answer to `clickhouse-benchmark` در مرحله مشخص شده. مقادیر ممکن: `complete`, `fetch_columns`, `with_mergeable_state`. مقدار پیشفرض: `complete`.
-   `--help` — Shows the help message.

اگر شما می خواهید به درخواست برخی از [تنظیمات](../../operations/settings/index.md) برای پرس و جو, عبور خود را به عنوان یک کلید `--<session setting name>= SETTING_VALUE`. به عنوان مثال, `--max_memory_usage=1048576`.

## خروجی {#clickhouse-benchmark-output}

به طور پیش فرض, `clickhouse-benchmark` گزارش برای هر `--delay` فاصله.

نمونه ای از گزارش:

``` text
Queries executed: 10.

localhost:9000, queries 10, QPS: 6.772, RPS: 67904487.440, MiB/s: 518.070, result RPS: 67721584.984, result MiB/s: 516.675.

0.000%      0.145 sec.
10.000%     0.146 sec.
20.000%     0.146 sec.
30.000%     0.146 sec.
40.000%     0.147 sec.
50.000%     0.148 sec.
60.000%     0.148 sec.
70.000%     0.148 sec.
80.000%     0.149 sec.
90.000%     0.150 sec.
95.000%     0.150 sec.
99.000%     0.150 sec.
99.900%     0.150 sec.
99.990%     0.150 sec.
```

در این گزارش شما می توانید پیدا کنید:

-   تعداد نمایش داده شد در `Queries executed:` رشته.

-   رشته وضعیت حاوی) به ترتیب ():

    -   نقطه پایانی از سرور کلیک.
    -   تعداد نمایش داده شد پردازش شده است.
    -   QPS: QPS: چگونه بسیاری از نمایش داده شد سرور انجام شده در هر ثانیه در طول یک دوره مشخص شده در `--delay` استدلال کردن.
    -   سرور مجازی: چند ردیف سرور در هر ثانیه در طول یک دوره مشخص شده در `--delay` استدلال کردن.
    -   مگابایت بر ثانیه: چگونه بسیاری از مگابایت سرور در ثانیه در طول یک دوره مشخص شده در خواندن `--delay` استدلال کردن.
    -   نتیجه ریسمانهای: چگونه بسیاری از ردیف توسط سرور به نتیجه یک پرس و جو در هر ثانیه در طول یک دوره مشخص شده در قرار داده شده `--delay` استدلال کردن.
    -   چگونه بسیاری از مگابایت توسط سرور به نتیجه یک پرس و جو در هر ثانیه در طول یک دوره مشخص شده در قرار داده شده `--delay` استدلال کردن.

-   صدک از نمایش داده شد زمان اجرای.

## حالت مقایسه {#clickhouse-benchmark-comparison-mode}

`clickhouse-benchmark` می توانید اجرای برای دو سرور در حال اجرا تاتر مقایسه.

برای استفاده از حالت مقایسه, مشخص نقطه پایانی هر دو سرور توسط دو جفت از `--host`, `--port` کلیدا کلید با هم توسط موقعیت در لیست استدلال همسان, اولین `--host` با اولین همسان `--port` و به همین ترتیب. `clickhouse-benchmark` ایجاد ارتباط به هر دو سرور, سپس نمایش داده شد می فرستد. هر پرس و جو خطاب به یک سرور به طور تصادفی انتخاب شده است. نتایج برای هر سرور به طور جداگانه نشان داده شده است.

## مثال {#clickhouse-benchmark-example}

``` bash
$ echo "SELECT * FROM system.numbers LIMIT 10000000 OFFSET 10000000" | clickhouse-benchmark -i 10
```

``` text
Loaded 1 queries.

Queries executed: 6.

localhost:9000, queries 6, QPS: 6.153, RPS: 123398340.957, MiB/s: 941.455, result RPS: 61532982.200, result MiB/s: 469.459.

0.000%      0.159 sec.
10.000%     0.159 sec.
20.000%     0.159 sec.
30.000%     0.160 sec.
40.000%     0.160 sec.
50.000%     0.162 sec.
60.000%     0.164 sec.
70.000%     0.165 sec.
80.000%     0.166 sec.
90.000%     0.166 sec.
95.000%     0.167 sec.
99.000%     0.167 sec.
99.900%     0.167 sec.
99.990%     0.167 sec.



Queries executed: 10.

localhost:9000, queries 10, QPS: 6.082, RPS: 121959604.568, MiB/s: 930.478, result RPS: 60815551.642, result MiB/s: 463.986.

0.000%      0.159 sec.
10.000%     0.159 sec.
20.000%     0.160 sec.
30.000%     0.163 sec.
40.000%     0.164 sec.
50.000%     0.165 sec.
60.000%     0.166 sec.
70.000%     0.166 sec.
80.000%     0.167 sec.
90.000%     0.167 sec.
95.000%     0.170 sec.
99.000%     0.172 sec.
99.900%     0.172 sec.
99.990%     0.172 sec.
```
