---
machine_translated: true
machine_translated_rev: 1cd5f0028d917696daf71ac1c9ee849c99c1d5c8
---

# clickhouse-бенчмарк {#clickhouse-benchmark}

Подключается к серверу ClickHouse и повторно отправляет указанные запросы.

Синтаксис:

``` bash
$ echo "single query" | clickhouse-benchmark [keys]
```

или

``` bash
$ clickhouse-benchmark [keys] <<< "single query"
```

Если вы хотите отправить набор запросов, создайте текстовый файл и поместите каждый запрос в отдельную строку в этом файле. Например:

``` sql
SELECT * FROM system.numbers LIMIT 10000000
SELECT 1
```

Затем передайте этот файл на стандартный вход `clickhouse-benchmark`.

``` bash
clickhouse-benchmark [keys] < queries_file
```

## Ключи {#clickhouse-benchmark-keys}

-   `-c N`, `--concurrency=N` — Number of queries that `clickhouse-benchmark` посылает одновременно. Значение по умолчанию: 1.
-   `-d N`, `--delay=N` — Interval in seconds between intermediate reports (set 0 to disable reports). Default value: 1.
-   `-h WORD`, `--host=WORD` — Server host. Default value: `localhost`. Для [режим сравнения](#clickhouse-benchmark-comparison-mode) вы можете использовать несколько `-h` ключи.
-   `-p N`, `--port=N` — Server port. Default value: 9000. For the [режим сравнения](#clickhouse-benchmark-comparison-mode) вы можете использовать несколько `-p` ключи.
-   `-i N`, `--iterations=N` — Total number of queries. Default value: 0.
-   `-r`, `--randomize` — Random order of queries execution if there is more then one input query.
-   `-s`, `--secure` — Using TLS connection.
-   `-t N`, `--timelimit=N` — Time limit in seconds. `clickhouse-benchmark` прекращает отправку запросов по достижении указанного срока. Значение по умолчанию: 0 (ограничение по времени отключено).
-   `--confidence=N` — Level of confidence for T-test. Possible values: 0 (80%), 1 (90%), 2 (95%), 3 (98%), 4 (99%), 5 (99.5%). Default value: 5. In the [режим сравнения](#clickhouse-benchmark-comparison-mode) `clickhouse-benchmark` выполняет следующие функции: [Независимый двухпробный t-тест Стьюдента](https://en.wikipedia.org/wiki/Student%27s_t-test#Independent_two-sample_t-test) проверьте, не отличаются ли эти два распределения с выбранным уровнем достоверности.
-   `--cumulative` — Printing cumulative data instead of data per interval.
-   `--database=DATABASE_NAME` — ClickHouse database name. Default value: `default`.
-   `--json=FILEPATH` — JSON output. When the key is set, `clickhouse-benchmark` выводит отчет в указанный JSON-файл.
-   `--user=USERNAME` — ClickHouse user name. Default value: `default`.
-   `--password=PSWD` — ClickHouse user password. Default value: empty string.
-   `--stacktrace` — Stack traces output. When the key is set, `clickhouse-bencmark` выводит трассировки стека исключений.
-   `--stage=WORD` — Query processing stage at server. ClickHouse stops query processing and returns answer to `clickhouse-benchmark` на указанном этапе. Возможное значение: `complete`, `fetch_columns`, `with_mergeable_state`. Значение по умолчанию: `complete`.
-   `--help` — Shows the help message.

Если вы хотите применить некоторые из них [настройки](../../operations/settings/index.md) для запросов передайте их в качестве ключа `--<session setting name>= SETTING_VALUE`. Например, `--max_memory_usage=1048576`.

## Выход {#clickhouse-benchmark-output}

По умолчанию, `clickhouse-benchmark` отчеты для каждого из них `--delay` интервал.

Пример отчета:

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

В отчете вы можете найти::

-   Количество запросов в системе `Queries executed:` поле.

-   Строка состояния, содержащая (по порядку):

    -   Конечная точка сервера ClickHouse.
    -   Количество обработанных запросов.
    -   QPS: QPS: сколько запросов сервер выполняет в секунду в течение периода, указанного в `--delay` аргумент.
    -   RPS: сколько строк сервер читает в секунду в течение периода, указанного в `--delay` аргумент.
    -   MiB/s: сколько мегабайт сервер читает в секунду в течение периода, указанного в `--delay` аргумент.
    -   result RPS: сколько строк помещается сервером в результат запроса в секунду в течение периода, указанного в `--delay` аргумент.
    -   результат MiB/s. сколько мебибайт помещается сервером в результат запроса в секунду в течение периода, указанного в `--delay` аргумент.

-   Процентили времени выполнения запросов.

## Режим сравнения {#clickhouse-benchmark-comparison-mode}

`clickhouse-benchmark` можно сравнить производительность для двух запущенных серверов ClickHouse.

Чтобы использовать режим сравнения, укажите конечные точки обоих серверов по двум парам `--host`, `--port` ключи. Ключи, сопоставленные вместе по позиции в списке аргументов, первые `--host` сопоставляется с первым `--port` и так далее. `clickhouse-benchmark` устанавливает соединения с обоими серверами, а затем отправляет запросы. Каждый запрос адресован случайно выбранному серверу. Результаты отображаются для каждого сервера отдельно.

## Пример {#clickhouse-benchmark-example}

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
