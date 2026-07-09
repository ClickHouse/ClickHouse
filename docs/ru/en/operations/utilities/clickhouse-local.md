---
description: 'Руководство по использованию clickhouse-local для обработки данных без запуска сервера'
sidebar_label: 'clickhouse-local'
sidebar_position: 60
slug: /operations/utilities/clickhouse-local
title: 'clickhouse-local'
doc_type: 'reference'
---

<div id="when-to-use-clickhouse-local-vs-clickhouse">
  ## Когда использовать clickhouse-local, а когда — ClickHouse
</div>

`clickhouse-local` — это удобная в использовании версия ClickHouse, которая идеально подходит разработчикам, которым нужно быстро обрабатывать локальные и удалённые файлы с помощью SQL без установки полноценного сервера базы данных. С `clickhouse-local` разработчики могут выполнять SQL-команды (используя [диалект ClickHouse SQL](../../sql-reference/index.md)) прямо из командной строки, что даёт простой и эффективный доступ к возможностям ClickHouse без полной установки ClickHouse. Одно из главных преимуществ `clickhouse-local` в том, что он уже поставляется вместе с [клиентом ClickHouse](/ru/operations/utilities/clickhouse-local). Это означает, что разработчики могут быстро начать работу с `clickhouse-local` без сложного процесса установки.

Хотя `clickhouse-local` — отличный инструмент для разработки, тестирования и обработки файлов, он не подходит для обслуживания конечных пользователей или приложений. В таких случаях рекомендуется использовать [ClickHouse](/ru/install) с открытым исходным кодом. ClickHouse — это мощная OLAP база данных, предназначенная для работы с крупномасштабными аналитическими рабочими нагрузками. Она обеспечивает быструю и эффективную обработку сложных запросов на больших наборах данных, что делает её идеальным выбором для сред продакшн, где критически важна высокая производительность. Кроме того, ClickHouse предоставляет широкий набор возможностей, таких как репликация, сегментирование данных и Высокая доступность, которые необходимы для масштабирования при работе с большими наборами данных и обслуживании приложений. Если вам нужно работать с более крупными наборами данных или обслуживать конечных пользователей либо приложения, мы рекомендуем использовать ClickHouse с открытым исходным кодом вместо `clickhouse-local`.

Ознакомьтесь с документацией ниже, где приведены примеры использования `clickhouse-local`, например [запрос локального файла](#query_data_in_file) или [чтение файла Parquet в S3](#query-data-in-a-parquet-file-in-aws-s3).

<div id="download-clickhouse-local">
  ## Скачайте clickhouse-local
</div>

`clickhouse-local` запускается с помощью того же бинарного файла `clickhouse`, что и сервер ClickHouse и `клиент ClickHouse`. Проще всего скачать последнюю версию с помощью следующей команды:

```bash
curl https://clickhouse.com/ | sh
```

:::note
Бинарный файл, который вы только что скачали, может запускать самые разные инструменты и утилиты ClickHouse. Если вы хотите использовать ClickHouse в качестве сервера базы данных, ознакомьтесь с руководством [Быстрый старт](/ru/get-started/quick-start).
:::

<div id="query_data_in_file">
  ## Выполнение SQL-запросов к данным в файле
</div>

`clickhouse-local` часто используют для выполнения ad hoc-запросов к файлам, когда не требуется вставка данных в таблицу. `clickhouse-local` может в потоковом режиме загружать данные из файла во временную таблицу и выполнять ваш SQL.

Если файл находится на той же машине, что и `clickhouse-local`, достаточно просто указать файл для загрузки. Следующий файл `reviews.tsv` содержит выборку отзывов о товарах Amazon:

```bash
./clickhouse local -q "SELECT * FROM 'reviews.tsv'"
```

Эта команда — сокращённая форма:

```bash
./clickhouse local -q "SELECT * FROM file('reviews.tsv')"
```

ClickHouse определяет по расширению имени файла, что в нём используется формат с разделителями табуляцией. Если нужно явно указать format, просто добавьте один из [многочисленных input format ClickHouse](../../interfaces/formats.md):

```bash
./clickhouse local -q "SELECT * FROM file('reviews.tsv', 'TabSeparated')"
```

Табличная функция `file` создает таблицу, и с помощью `DESCRIBE` можно посмотреть автоматически определенную схему:

```bash
./clickhouse local -q "DESCRIBE file('reviews.tsv')"
```

:::tip
В имени файла можно использовать глоб-шаблоны (см. [подстановки глоб-шаблонов](/ru/sql-reference/table-functions/file.md/#globs-in-path)).

Примеры:

```bash
./clickhouse local -q "SELECT * FROM 'reviews*.jsonl'"
./clickhouse local -q "SELECT * FROM 'review_?.csv'"
./clickhouse local -q "SELECT * FROM 'review_{1..3}.csv'"
```

:::

```response
marketplace    Nullable(String)
customer_id    Nullable(Int64)
review_id    Nullable(String)
product_id    Nullable(String)
product_parent    Nullable(Int64)
product_title    Nullable(String)
product_category    Nullable(String)
star_rating    Nullable(Int64)
helpful_votes    Nullable(Int64)
total_votes    Nullable(Int64)
vine    Nullable(String)
verified_purchase    Nullable(String)
review_headline    Nullable(String)
review_body    Nullable(String)
review_date    Nullable(Date)
```

Найдём товар с самым высоким рейтингом:

```bash
./clickhouse local -q "SELECT
    argMax(product_title,star_rating),
    max(star_rating)
FROM file('reviews.tsv')"
```

```response
Monopoly Junior Board Game    5
```

<div id="query-data-in-a-parquet-file-in-aws-s3">
  ## Запрос данных из файла Parquet в AWS S3
</div>

Если у вас есть файл в S3, используйте `clickhouse-local` и табличную функцию `s3`, чтобы выполнить запрос к файлу напрямую (без вставки данных в таблицу ClickHouse). У нас есть файл `house_0.parquet` в публичном бакете, содержащий цены на жильё, проданное в Соединённом Королевстве. Посмотрим, сколько в нём строк:

```bash
./clickhouse local -q "
SELECT count()
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/house_parquet/house_0.parquet')"
```

Файл содержит 2,7 млн строк:

```response
2772030
```

Всегда полезно посмотреть, какую схему ClickHouse автоматически определяет на основе файла:

```bash
./clickhouse local -q "DESCRIBE s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/house_parquet/house_0.parquet')"
```

```response
price    Nullable(Int64)
date    Nullable(UInt16)
postcode1    Nullable(String)
postcode2    Nullable(String)
type    Nullable(String)
is_new    Nullable(UInt8)
duration    Nullable(String)
addr1    Nullable(String)
addr2    Nullable(String)
street    Nullable(String)
locality    Nullable(String)
town    Nullable(String)
district    Nullable(String)
county    Nullable(String)
```

Давайте посмотрим, какие районы самые дорогие:

```bash
./clickhouse local -q "
SELECT
    town,
    district,
    count() AS c,
    round(avg(price)) AS price,
    bar(price, 0, 5000000, 100)
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/house_parquet/house_0.parquet')
GROUP BY
    town,
    district
HAVING c >= 100
ORDER BY price DESC
LIMIT 10"
```

```response
LONDON    CITY OF LONDON    886    2271305    █████████████████████████████████████████████▍
LEATHERHEAD    ELMBRIDGE    206    1176680    ███████████████████████▌
LONDON    CITY OF WESTMINSTER    12577    1108221    ██████████████████████▏
LONDON    KENSINGTON AND CHELSEA    8728    1094496    █████████████████████▉
HYTHE    FOLKESTONE AND HYTHE    130    1023980    ████████████████████▍
CHALFONT ST GILES    CHILTERN    113    835754    ████████████████▋
AMERSHAM    BUCKINGHAMSHIRE    113    799596    ███████████████▉
VIRGINIA WATER    RUNNYMEDE    356    789301    ███████████████▊
BARNET    ENFIELD    282    740514    ██████████████▊
NORTHWOOD    THREE RIVERS    184    731609    ██████████████▋
```

:::tip
Когда будете готовы загрузить файлы в ClickHouse, запустите сервер ClickHouse и вставьте результаты табличных функций `file` и `s3` в таблицу `MergeTree`. Подробнее см. в разделе [Быстрый старт](/ru/get-started/quick-start).
:::

<div id="format-conversions">
  ## Преобразования форматов
</div>

Для преобразования данных между разными форматами можно использовать `clickhouse-local`. Пример:

```bash
$ clickhouse-local --input-format JSONLines --output-format CSV --query "SELECT * FROM table" < data.json > data.csv
```

Форматы автоматически определяются по расширениям файлов:

```bash
$ clickhouse-local --query "SELECT * FROM table" < data.json > data.csv
```

Для краткости можно использовать аргумент `--copy`:

```bash
$ clickhouse-local --copy < data.json > data.csv
```

<div id="usage">
  ## Использование
</div>

По умолчанию `clickhouse-local` имеет доступ к данным сервера ClickHouse на том же хосте и не зависит от конфигурации сервера. Он также поддерживает загрузку конфигурации сервера с помощью аргумента `--config-file`. Для временных данных по умолчанию создаётся уникальный временный каталог.

Базовое использование (Linux):

```bash
$ clickhouse-local --structure "table_structure" --input-format "format_of_incoming_data" --query "query"
```

Базовое использование (Mac):

```bash
$ ./clickhouse local --structure "table_structure" --input-format "format_of_incoming_data" --query "query"
```

:::note
`clickhouse-local` также поддерживается в Windows через WSL2.
:::

Аргументы:

* `-S`, `--structure` — структура таблицы для входных данных.
* `--input-format` — входной формат, по умолчанию `TSV`.
* `-F`, `--file` — путь к данным, по умолчанию `stdin`.
* `-q`, `--query` — запросы для выполнения, где `;` используется как разделитель. `--query` можно указывать несколько раз, например: `--query "SELECT 1" --query "SELECT 2"`. Нельзя использовать одновременно с `--queries-file`.
* `--queries-file` - путь к файлу с запросами для выполнения. `--queries-file` можно указывать несколько раз, например: `--query queries1.sql --query queries2.sql`. Нельзя использовать одновременно с `--query`.
* `--multiquery, -n` – если указано, после параметра `--query` можно перечислить несколько запросов, разделенных точкой с запятой. Для удобства можно также опустить `--query` и передать запросы сразу после `--multiquery`.
* `-N`, `--table` — имя таблицы, в которую следует поместить выходные данные, по умолчанию `table`.
* `-f`, `--format`, `--output-format` — выходной формат, по умолчанию `TSV`.
* `-d`, `--database` — база данных по умолчанию, `_local`.
* `--stacktrace` — выводить ли отладочную информацию в случае исключения.
* `--echo [ <bool> ]` — выводить каждый запрос перед выполнением. Принимает необязательное булево значение. По умолчанию включено в интерактивном режиме и отключено в пакетном режиме. Примечание: поскольку `--echo` теперь принимает необязательное значение, позиционный запрос, указанный сразу после `--echo` без значения, будет воспринят как его значение; вместо этого используйте `--echo --query "..."`, `--echo -q "..."`, `--echo=false` или передавайте запрос через `stdin`.
* `--echo-formatted [ <bool> ]` — форматировать выводимые запросы. Принимает необязательное булево значение. По умолчанию включено в интерактивном режиме и отключено в пакетном режиме.
* `--echo-query-id [ <bool> ]` — выводить `query_id` перед выполнением. Принимает необязательное булево значение. По умолчанию включено в интерактивном режиме и отключено в пакетном режиме.
* `--echo-query-separator <string>` — выводить этот разделитель перед отформатированным отображаемым запросом (требуется `--echo-formatted`), чтобы было проще отличить введенный запрос от его переформатированной версии. По умолчанию пусто (отключено).
* `--highlight`, `--hilite` `<bool>` — включать или выключать подсветку синтаксиса в командной строке и выводимых запросах. По умолчанию включено. Подсветка применяется только при выводе в терминал.
* `--hints <bool>` — показывать подсказки автодополнения по мере ввода (встроенный &quot;призрачный&quot; текст) для наиболее подходящего варианта, когда курсор находится в конце ввода. Переключаться между подсказками можно клавишами Up/Down (или Ctrl-Up/Ctrl-Down); принять встроенную подсказку — клавишей Tab или Right; `Enter` принимает подсказку только после ее явного выбора, а в противном случае выполняет запрос; `Tab` также открывает классический список автодополнения. Требуется `--highlight` (подсказкам нужен цвет) и механизм предложений (поэтому `--disable_suggestion` тоже их отключает). По умолчанию включено.
* `--verbose` — выводить больше сведений о выполнении запроса.
* `--logger.console` — выводить журнал в консоль.
* `--logger.log` — имя файла журнала.
* `--logger.level` — уровень журнала.
* `--ignore-error` — не останавливать обработку, если запрос завершился ошибкой.
* `-c`, `--config-file` — путь к файлу конфигурации в том же формате, что и для сервера ClickHouse; по умолчанию конфигурация пуста.
* `--no-system-tables` — не подключать системные таблицы.
* `--help` — справка по аргументам для `clickhouse-local`.
* `-V`, `--version` — вывести информацию о версии и выйти.

Кроме того, для каждой переменной конфигурации ClickHouse есть аргументы, которые обычно используются вместо `--config-file`.

<div id="commands">
  ## Команды
</div>

<div id="ls-command">
  ### Команда LS
</div>

Выводит список всех файлов в текущем рабочем каталоге, доступных для clickhouse-local.

Её можно запустить в интерактивном режиме следующим образом:

```sql title="Query"
ClickHouse local version 26.3.1.1.

:) ls

SELECT _file AS file
FROM file('*', 'One')
ORDER BY file ASC
```

```text title="Response"
┌─file────────┐
│ file1.csv   │
│ file2.json  │
│ file3.xml   │
└─────────────┘
```

Его также можно выполнить как запрос, используя аргумент `-q`:

```sh
./clickhouse-local -q ls
```

```text title="Response"
file1.csv
file2.json
file3.xml
```

<div id="clear-command">
  ### Команда CLEAR
</div>

Очищает экран терминала (аналогично команде `clear` в Linux или Ctrl+L во многих терминалах). Это действие на стороне клиента: оно не отправляется в SQL-движок.

В `clickhouse-local` метакоманда распознаётся в **интерактивном** режиме, а также при вводе через **`-q`** и **`--queries-file`** (тот же путь обработки на стороне клиента, что и у `-q`, по той же логике, что и для `ls`), поэтому простой `clear` не вызывает ошибку `UNKNOWN_IDENTIFIER`. Для удалённого **`клиент ClickHouse --queries-file`** ничего не меняется: содержимое файла по-прежнему выполняется только как SQL (без текстовых метакоманд).

В `клиент ClickHouse` она распознаётся только в **интерактивном** режиме. При использовании **`-q`** или файлов запросов `clear` по-прежнему разбирается как SQL, поэтому автоматизация сохраняет прежнее поведение с ошибкой, а опечатки не превращаются в тихий no-op.

Поддерживаемые формы: `clear`, `CLEAR`, `/clear` (необязательный завершающий `;` игнорируется). Если стандартный вывод не является терминалом (например, при передаче вывода по конвейеру), метакоманда при распознавании принимается, но управляющие последовательности не выводятся.

С `clickhouse-local` и `-q`:

```sh
./clickhouse-local -q clear
```

<div id="examples">
  ## Примеры
</div>

```bash title="Query"
$ echo -e "1,2\n3,4" | clickhouse-local --structure "a Int64, b Int64" \
    --input-format "CSV" --query "SELECT * FROM table"
Read 2 rows, 32.00 B in 0.000 sec., 5182 rows/sec., 80.97 KiB/sec.
1   2
3   4
```

Предыдущий пример аналогичен следующему:

```bash title="Query"
$ echo -e "1,2\n3,4" | clickhouse-local -n --query "
    CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin);
    SELECT a, b FROM table;
    DROP TABLE table;"
Read 2 rows, 32.00 B in 0.000 sec., 4987 rows/sec., 77.93 KiB/sec.
1   2
3   4
```

Необязательно использовать `stdin` или аргумент `--file` — можно открыть любое количество файлов с помощью [табличной функции `file`](../../sql-reference/table-functions/file.md):

```bash title="Query"
$ echo 1 | tee 1.tsv
1

$ echo 2 | tee 2.tsv
2

$ clickhouse-local --query "
    select * from file('1.tsv', TSV, 'a int') t1
    cross join file('2.tsv', TSV, 'b int') t2"
1    2
```

Теперь выведем memory user для каждого Unix-пользователя:

```bash title="Query"
$ ps aux | tail -n +2 | awk '{ printf("%s\t%s\n", $1, $4) }' \
    | clickhouse-local --structure "user String, mem Float64" \
        --query "SELECT user, round(sum(mem), 2) as memTotal
            FROM table GROUP BY user ORDER BY memTotal DESC FORMAT Pretty"
```

```text title="Response"
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

<div id="starting-listeners">
  ## Запуск TCP- и HTTP-слушателей
</div>

`clickhouse-local` можно превратить в легковесный сервер, принимающий TCP-соединения (по собственному протоколу) и HTTP-соединения. Это полезно, если вы хотите предоставить другим инструментам или приложениям ClickHouse доступ к базам данных и таблицам запущенного экземпляра `clickhouse-local`. Обратите внимание, что для каждого входящего соединения создаётся отдельный сеанс: временные таблицы и сеансовые настройки интерактивного сеанса `clickhouse-local` не видны внешним соединениям.

Используйте `SYSTEM START LISTEN`, чтобы открыть слушатель, и `SYSTEM STOP LISTEN`, чтобы закрыть его:

```bash
clickhouse-local \
    --listen_host 127.0.0.1 \
    --tcp_port 9000 \
    --http_port 8123 \
    --query "
        SYSTEM START LISTEN TCP;
        SYSTEM START LISTEN HTTP;
        SELECT * FROM url('http://127.0.0.1:8123/?query=SELECT+42', LineAsString);
        SYSTEM STOP LISTEN TCP;
        SYSTEM STOP LISTEN HTTP;
    "
```

Параметры `--listen_host`, `--tcp_port` и `--http_port` задают адрес привязки и порты. Порты по умолчанию: `9000` для TCP и `8123` для HTTP.

:::warning Безопасность
По умолчанию `clickhouse-local` запускается с временной конфигурацией пользователей, поэтому любой открытый им сетевой порт доступен без аутентификации. Используйте loopback-адрес (`127.0.0.1` или `::1`), если только вы явно не настроили пользователей и управление доступом, указав в параметре `users_config` путь к пользовательскому `users.xml` (например, через `--config-file`). Если прослушивать не-loopback-адрес без аутентификации, данные локального экземпляра будут доступны любому, кто сможет подключиться к выбранному порту.
:::

<div id="related-content-1">
  ## Связанные материалы
</div>

* [Извлечение, преобразование и выполнение запросов к данным в локальных файлах с помощью clickhouse-local](https://clickhouse.com/blog/extracting-converting-querying-local-files-with-sql-clickhouse-local)
* [Загрузка данных в ClickHouse — Часть 1](https://clickhouse.com/blog/getting-data-into-clickhouse-part-1)
* [Изучение огромных реальных массивов данных: более 100 лет погодных наблюдений в ClickHouse](https://clickhouse.com/blog/real-world-data-noaa-climate-data)
* Блог: [Извлечение, преобразование и выполнение запросов к данным в локальных файлах с помощью clickhouse-local](https://clickhouse.com/blog/extracting-converting-querying-local-files-with-sql-clickhouse-local)