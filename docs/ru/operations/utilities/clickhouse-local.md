---
toc_priority: 60
toc_title: clickhouse-local
---

# clickhouse-local {#clickhouse-local}

Принимает на вход данные, которые можно представить в табличном виде и выполняет над ними операции, заданные на [языке запросов](../../operations/utilities/clickhouse-local.md) ClickHouse.

`clickhouse-local` использует движок сервера ClickHouse, т.е. поддерживает все форматы данных и движки таблиц, с которыми работает ClickHouse, при этом для выполнения операций не требуется запущенный сервер.

`clickhouse-local` при настройке по умолчанию не имеет доступа к данным, которыми управляет сервер ClickHouse, установленный на этом же хосте, однако можно подключить конфигурацию сервера с помощью ключа `--config-file`.

!!! warning "Warning"
    Мы не рекомендуем подключать серверную конфигурацию к `clickhouse-local`, поскольку данные можно легко повредить неосторожными действиями.

Для временных данных по умолчанию создается специальный каталог.

## Вызов программы {#usage}

Основной формат вызова:

``` bash
$ clickhouse-local --structure "table_structure" --input-format "format_of_incoming_data" \
    --query "query"
```

Ключи команды:

-   `-S`, `--structure` — структура таблицы, в которую будут помещены входящие данные.
-   `-if`, `--input-format` — формат входящих данных. По умолчанию — `TSV`.
-   `-f`, `--file` — путь к файлу с данными. По умолчанию — `stdin`.
-   `-q`, `--query` — запросы на выполнение. Разделитель запросов — `;`.
-   `-qf`, `--queries-file` - путь к файлу с запросами для выполнения. Необходимо задать либо параметр `query`, либо `queries-file`.
-   `-N`, `--table` — имя таблицы, в которую будут помещены входящие данные. По умолчанию - `table`.
-   `-of`, `--format`, `--output-format` — формат выходных данных. По умолчанию — `TSV`.
-   `-d`, `--database` — база данных по умолчанию. Если не указано, используется значение `_local`.
-   `--stacktrace` — вывод отладочной информации при исключениях.
-   `--echo` — перед выполнением  запрос выводится в консоль.
-   `--verbose` — подробный вывод при выполнении запроса.
-   `--logger.console` — логирование действий в консоль.
-   `--logger.log` — логирование действий в файл с указанным именем.
-   `--logger.level` — уровень логирования.
-   `--ignore-error` — не прекращать обработку если запрос выдал ошибку.
-   `-c`, `--config-file` — путь к файлу конфигурации. По умолчанию `clickhouse-local` запускается с пустой конфигурацией. Конфигурационный файл имеет тот же формат, что и для сервера ClickHouse, и в нём можно использовать все конфигурационные параметры сервера. Обычно подключение конфигурации не требуется; если требуется установить отдельный параметр, то это можно сделать ключом с именем параметра.
-   `--no-system-tables` — запуск без использования системных таблиц.
-   `--help` — вывод справочной информации о `clickhouse-local`.
-   `-V`, `--version` — вывод текущей версии и выход.

## Примеры вызова {#examples}

``` bash
$ echo -e "1,2\n3,4" | clickhouse-local --structure "a Int64, b Int64" \
    --input-format "CSV" --query "SELECT * FROM table"
Read 2 rows, 32.00 B in 0.000 sec., 5182 rows/sec., 80.97 KiB/sec.
1   2
3   4
```

Вызов выше эквивалентен следующему:

``` bash
$ echo -e "1,2\n3,4" | clickhouse-local --query "
    CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin);
    SELECT a, b FROM table;
    DROP TABLE table"
Read 2 rows, 32.00 B in 0.000 sec., 4987 rows/sec., 77.93 KiB/sec.
1   2
3   4
```


Необязательно использовать ключи `stdin` или `--file`. Вы можете открывать любое количество файлов с помощью [табличной функции `file`](../../sql-reference/table-functions/file.md):

``` bash
$ echo 1 | tee 1.tsv
1

$ echo 2 | tee 2.tsv
2

$ clickhouse-local --query "
    select * from file('1.tsv', TSV, 'a int') t1
    cross join file('2.tsv', TSV, 'b int') t2"
1	2
```

Объём оперативной памяти, занимаемой процессами, которые запустил пользователь (Unix):

Запрос:

``` bash
$ ps aux | tail -n +2 | awk '{ printf("%s\t%s\n", $1, $4) }' \
    | clickhouse-local --structure "user String, mem Float64" \
        --query "SELECT user, round(sum(mem), 2) as memTotal
            FROM table GROUP BY user ORDER BY memTotal DESC FORMAT Pretty"
```

Результат:

``` text
Read 186 rows, 4.15 KiB in 0.035 sec., 5302 rows/sec., 118.34 KiB/sec.
┏━━━━━━━━━━┳━━━━━━━━━━┓
┃ user     ┃ memTotal ┃
┡━━━━━━━━━━╇━━━━━━━━━━┩
│ bayonet  │    113.5 │
├──────────┼──────────┤
│ root     │      8.8 │
├──────────┼──────────┤
...
```

