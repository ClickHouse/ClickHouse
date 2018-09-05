<a name="utils-clickhouse-local"></a>

# clickhouse-local

Принимает на вход данные, которые можно представить в табличном виде и выполняет над ними операции, заданные на [языке запросов](../../query_language/index.md#queries) ClickHouse.

`clickhouse-local` использует движок сервера ClickHouse, т.е. поддерживает все форматы данных и движки таблиц, с которыми работает ClickHouse, при этом для выполнения операций не требуется запущенный сервер.

`clickhouse-local` при настройке по умолчанию не имеет доступа к данным, которыми управляет сервер ClickHouse, установленный на этом же хосте, однако можно подключить конфигурацию сервера с помощью ключа `--config-file`.

!!! warning
    Мы не рекомендуем подключать серверную конфигурацию к `clickhouse-local`, поскольку данные можно легко повредить неосторожными действиями.

## Вызов программы

Основной формат вызова:

``` bash
clickhouse-local --structure "table_structure" --input-format "format_of_incoming_data" -q "query"
```

Ключи команды:

- `-S`, `--structure` — структура таблицы, в которую будут помещены входящие данные.
- `-if`, `--input-format` — формат входящих данных. По умолчанию — `TSV`.
- `-f`, `--file` — путь к файлу с данными. По умолчанию — `stdin`.
- `-q`, `--query` — запросы на выполнение. Разделитель запросов — `;`.
- `-N`, `--table` — имя таблицы, в которую будут помещены входящие данные. По умолчанию - `table`.
- `-of`, `--format`, `--output-format` — формат выходных данных. По умолчанию — `TSV`.
- `--stacktrace` — вывод отладочной информации при исключениях.
- `--verbose` — подробный вывод при выполнении запроса.
- `-s` — отключает вывод системных логов в `stderr`.
- `--config-file` — путь к файлу конфигурации. По умолчанию `clickhouse-local` запускается с пустой конфигурацией. Конфигурационный файл имеет тот же формат, что и для сервера ClickHouse и в нём можно использовать все конфигурационные параметры сервера. Обычно подключение конфигурации не требуется, если требуется установить отдельный параметр, то это можно сделать ключом с именем параметра.
- `--help` — вывод справочной информации о `clickhouse-local`.


## Примеры вызова

``` bash
echo -e "1,2\n3,4" | clickhouse-local -S "a Int64, b Int64" -if "CSV" -q "SELECT * FROM table"
Read 2 rows, 32.00 B in 0.000 sec., 5182 rows/sec., 80.97 KiB/sec.
1	2
3	4
```

Вызов выше эквивалентен следующему:

``` bash
$ echo -e "1,2\n3,4" | clickhouse-local -q "CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin); SELECT a, b FROM table; DROP TABLE table"
Read 2 rows, 32.00 B in 0.000 sec., 4987 rows/sec., 77.93 KiB/sec.
1	2
3	4
```

А теперь давайте выведем на экран объем оперативной памяти, занимаемой пользователями (Unix):

``` bash
$ ps aux | tail -n +2 | awk '{ printf("%s\t%s\n", $1, $4) }' | clickhouse-local -S "user String, mem Float64" -q "SELECT user, round(sum(mem), 2) as memTotal FROM table GROUP BY user ORDER BY memTotal DESC FORMAT Pretty"
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
