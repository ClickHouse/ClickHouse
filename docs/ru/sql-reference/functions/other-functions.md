# Прочие функции {#prochie-funktsii}

## hostName() {#hostname}

Возвращает строку - имя хоста, на котором эта функция была выполнена. При распределённой обработке запроса, это будет имя хоста удалённого сервера, если функция выполняется на удалённом сервере.

## getMacro {#getmacro}

Возвращает именованное значение из секции [macros](../../operations/server-configuration-parameters/settings.md#macros) конфигурации сервера.

**Синтаксис** 

```sql
getMacro(name);
```

**Параметры**

- `name` — Имя, которое необходимо получить из секции `macros`. [String](../../sql-reference/data-types/string.md#string).

**Возвращаемое значение**

- Значение по указанному имени.

Тип: [String](../../sql-reference/data-types/string.md).

**Пример**

Пример секции `macros` в конфигурационном файле сервера:

```xml
<macros>
    <test>Value</test>
</macros>
```

Запрос:

```sql
SELECT getMacro('test');
```

Результат:

```text
┌─getMacro('test')─┐
│ Value            │
└──────────────────┘
```

Альтернативный способ получения значения:

```sql
SELECT * FROM system.macros
WHERE macro = 'test'
```

```text
┌─macro─┬─substitution─┐
│ test  │ Value        │
└───────┴──────────────┘
```


## FQDN {#fqdn}

Возвращает полное имя домена.

**Синтаксис**

``` sql
fqdn();
```

Эта функция регистронезависимая.

**Возвращаемое значение**

-   Полное имя домена.

Тип: `String`.

**Пример**

Запрос:

``` sql
SELECT FQDN();
```

Ответ:

``` text
┌─FQDN()──────────────────────────┐
│ clickhouse.ru-central1.internal │
└─────────────────────────────────┘
```

## basename {#basename}

Извлекает конечную часть строки после последнего слэша или бэкслэша. Функция часто используется для извлечения имени файла из пути.

``` sql
basename( expr )
```

**Параметры**

-   `expr` — Выражение, возвращающее значение типа [String](../../sql-reference/functions/other-functions.md). В результирующем значении все бэкслэши должны быть экранированы.

**Возвращаемое значение**

Строка, содержащая:

-   Конечную часть строки после последнего слэша или бэкслэша.

        Если входная строка содержит путь, заканчивающийся слэшем или бэкслэшем, например, `/` или `с:\`, функция возвращает пустую строку.

-   Исходная строка, если нет слэша или бэкслэша.

**Пример**

``` sql
SELECT 'some/long/path/to/file' AS a, basename(a)
```

``` text
┌─a──────────────────────┬─basename('some\\long\\path\\to\\file')─┐
│ some\long\path\to\file │ file                                   │
└────────────────────────┴────────────────────────────────────────┘
```

``` sql
SELECT 'some\\long\\path\\to\\file' AS a, basename(a)
```

``` text
┌─a──────────────────────┬─basename('some\\long\\path\\to\\file')─┐
│ some\long\path\to\file │ file                                   │
└────────────────────────┴────────────────────────────────────────┘
```

``` sql
SELECT 'some-file-name' AS a, basename(a)
```

``` text
┌─a──────────────┬─basename('some-file-name')─┐
│ some-file-name │ some-file-name             │
└────────────────┴────────────────────────────┘
```

## visibleWidth(x) {#visiblewidthx}

Вычисляет приблизительную ширину при выводе значения в текстовом (tab-separated) виде на консоль.
Функция используется системой для реализации Pretty форматов.

`NULL` представляется как строка, соответствующая отображению `NULL` в форматах `Pretty`.

``` sql
SELECT visibleWidth(NULL)
```

``` text
┌─visibleWidth(NULL)─┐
│                  4 │
└────────────────────┘
```

## toTypeName(x) {#totypenamex}

Возвращает строку, содержащую имя типа переданного аргумента.

Если на вход функции передать `NULL`, то она вернёт тип `Nullable(Nothing)`, что соответствует внутреннему представлению `NULL` в ClickHouse.

## blockSize() {#function-blocksize}

Получить размер блока.
В ClickHouse выполнение запроса всегда идёт по блокам (наборам кусочков столбцов). Функция позволяет получить размер блока, для которого её вызвали.

## materialize(x) {#materializex}

Превращает константу в полноценный столбец, содержащий только одно значение.
В ClickHouse полноценные столбцы и константы представлены в памяти по-разному. Функции по-разному работают для аргументов-констант и обычных аргументов (выполняется разный код), хотя результат почти всегда должен быть одинаковым. Эта функция предназначена для отладки такого поведения.

## ignore(…) {#ignore}

Принимает любые аргументы, в т.ч. `NULL`, всегда возвращает 0.
При этом, аргумент всё равно вычисляется. Это может использоваться для бенчмарков.

## sleep(seconds) {#sleepseconds}

Спит seconds секунд на каждый блок данных. Можно указать как целое число, так и число с плавающей запятой.

## currentDatabase() {#currentdatabase}

Возвращает имя текущей базы данных.
Эта функция может использоваться в параметрах движка таблицы в запросе CREATE TABLE там, где нужно указать базу данных.

## currentUser() {#other-function-currentuser}

Возвращает логин текущего пользователя. При распределенном запросе, возвращается имя пользователя, инициировавшего запрос.

``` sql
SELECT currentUser();
```

Алиас: `user()`, `USER()`.

**Возвращаемые значения**

-   Логин текущего пользователя.
-   При распределенном запросе — логин пользователя, инициировавшего запрос.

Тип: `String`.

**Пример**

Запрос:

``` sql
SELECT currentUser();
```

Ответ:

``` text
┌─currentUser()─┐
│ default       │
└───────────────┘
```

## isConstant {#is-constant}

Проверяет, является ли аргумент константным выражением.

Константное выражение — это выражение, результат которого известен на момент анализа запроса (до его выполнения). Например, выражения над [литералами](../syntax.md#literals) являются константными.

Используется в целях разработки, отладки или демонстрирования.

**Синтаксис**

``` sql
isConstant(x)
```

**Параметры**

- `x` — Выражение для проверки.

**Возвращаемые значения**

- `1` — Выражение `x` является константным.
- `0` — Выражение `x` не является константным.

Тип: [UInt8](../data-types/int-uint.md).

**Примеры**

Запрос:

```sql
SELECT isConstant(x + 1) FROM (SELECT 43 AS x)
```

Результат:

```text
┌─isConstant(plus(x, 1))─┐
│                      1 │
└────────────────────────┘
```

Запрос:

```sql
WITH 3.14 AS pi SELECT isConstant(cos(pi))
```

Результат:

```text
┌─isConstant(cos(pi))─┐
│                   1 │
└─────────────────────┘
```

Запрос:

```sql
SELECT isConstant(number) FROM numbers(1)
```

Результат:

```text
┌─isConstant(number)─┐
│                  0 │
└────────────────────┘
```

## isFinite(x) {#isfinitex}

Принимает Float32 или Float64 и возвращает UInt8, равный 1, если аргумент не бесконечный и не NaN, иначе 0.

## ifNotFinite {#ifnotfinite}

Проверяет, является ли значение дробного числа с плавающей точкой конечным.

**Синтаксис**

    ifNotFinite(x,y)

**Параметры**

-   `x` — Значение, которое нужно проверить на бесконечность. Тип: [Float\*](../../sql-reference/functions/other-functions.md).
-   `y` — Запасное значение. Тип: [Float\*](../../sql-reference/functions/other-functions.md).

**Возвращаемые значения**

-   `x`, если `x` принимает конечное значение.
-   `y`, если`x` принимает не конечное значение.

**Пример**

Запрос:

    SELECT 1/0 as infimum, ifNotFinite(infimum,42)

Результат:

    ┌─infimum─┬─ifNotFinite(divide(1, 0), 42)─┐
    │     inf │                            42 │
    └─────────┴───────────────────────────────┘

Аналогичный результат можно получить с помощью [тернарного оператора](conditional-functions.md#ternary-operator) `isFinite(x) ? x : y`.

## isInfinite(x) {#isinfinitex}

Принимает Float32 или Float64 и возвращает UInt8, равный 1, если аргумент бесконечный, иначе 0. Отметим, что в случае NaN возвращается 0.

## isNaN(x) {#isnanx}

Принимает Float32 или Float64 и возвращает UInt8, равный 1, если аргумент является NaN, иначе 0.

## hasColumnInTable(\[‘hostname’\[, ‘username’\[, ‘password’\]\],\] ‘database’, ‘table’, ‘column’) {#hascolumnintablehostname-username-password-database-table-column}

Принимает константные строки - имя базы данных, имя таблицы и название столбца. Возвращает константное выражение типа UInt8, равное 1,
если есть столбец, иначе 0. Если задан параметр hostname, проверка будет выполнена на удалённом сервере.
Функция кидает исключение, если таблица не существует.
Для элементов вложенной структуры данных функция проверяет существование столбца. Для самой же вложенной структуры данных функция возвращает 0.

## bar {#function-bar}

Позволяет построить unicode-art диаграмму.

`bar(x, min, max, width)` рисует полосу ширины пропорциональной `(x - min)` и равной `width` символов при `x = max`.

Параметры:

-   `x` — Величина для отображения.
-   `min, max` — Целочисленные константы, значение должно помещаться в `Int64`.
-   `width` — Константа, положительное число, может быть дробным.

Полоса рисуется с точностью до одной восьмой символа.

Пример:

``` sql
SELECT
    toHour(EventTime) AS h,
    count() AS c,
    bar(c, 0, 600000, 20) AS bar
FROM test.hits
GROUP BY h
ORDER BY h ASC
```

``` text
┌──h─┬──────c─┬─bar────────────────┐
│  0 │ 292907 │ █████████▋         │
│  1 │ 180563 │ ██████             │
│  2 │ 114861 │ ███▋               │
│  3 │  85069 │ ██▋                │
│  4 │  68543 │ ██▎                │
│  5 │  78116 │ ██▌                │
│  6 │ 113474 │ ███▋               │
│  7 │ 170678 │ █████▋             │
│  8 │ 278380 │ █████████▎         │
│  9 │ 391053 │ █████████████      │
│ 10 │ 457681 │ ███████████████▎   │
│ 11 │ 493667 │ ████████████████▍  │
│ 12 │ 509641 │ ████████████████▊  │
│ 13 │ 522947 │ █████████████████▍ │
│ 14 │ 539954 │ █████████████████▊ │
│ 15 │ 528460 │ █████████████████▌ │
│ 16 │ 539201 │ █████████████████▊ │
│ 17 │ 523539 │ █████████████████▍ │
│ 18 │ 506467 │ ████████████████▊  │
│ 19 │ 520915 │ █████████████████▎ │
│ 20 │ 521665 │ █████████████████▍ │
│ 21 │ 542078 │ ██████████████████ │
│ 22 │ 493642 │ ████████████████▍  │
│ 23 │ 400397 │ █████████████▎     │
└────┴────────┴────────────────────┘
```

## transform {#transform}

Преобразовать значение согласно явно указанному отображению одних элементов на другие.
Имеется два варианта функции:

### transform(x, array\_from, array\_to, default) {#transformx-array-from-array-to-default}

`x` - что преобразовывать.

`array_from` - константный массив значений для преобразования.

`array_to` - константный массив значений, в которые должны быть преобразованы значения из from.

`default` - какое значение использовать, если x не равен ни одному из значений во from.

`array_from` и `array_to` - массивы одинаковых размеров.

Типы:

`transform(T, Array(T), Array(U), U) -> U`

`T` и `U` - могут быть числовыми, строковыми, или Date или DateTime типами.
При этом, где обозначена одна и та же буква (T или U), могут быть, в случае числовых типов, не совпадающие типы, а типы, для которых есть общий тип.
Например, первый аргумент может иметь тип Int64, а второй - Array(UInt16).

Если значение x равно одному из элементов массива array\_from, то возвращает соответствующий (такой же по номеру) элемент массива array\_to; иначе возвращает default. Если имеется несколько совпадающих элементов в array\_from, то возвращает какой-нибудь из соответствующих.

Пример:

``` sql
SELECT
    transform(SearchEngineID, [2, 3], ['Yandex', 'Google'], 'Other') AS title,
    count() AS c
FROM test.hits
WHERE SearchEngineID != 0
GROUP BY title
ORDER BY c DESC
```

``` text
┌─title─────┬──────c─┐
│ Yandex    │ 498635 │
│ Google    │ 229872 │
│ Other     │ 104472 │
└───────────┴────────┘
```

### transform(x, array\_from, array\_to) {#transformx-array-from-array-to}

Отличается от первого варианта отсутствующим аргументом default.
Если значение x равно одному из элементов массива array\_from, то возвращает соответствующий (такой же по номеру) элемент массива array\_to; иначе возвращает x.

Типы:

`transform(T, Array(T), Array(T)) -> T`

Пример:

``` sql
SELECT
    transform(domain(Referer), ['yandex.ru', 'google.ru', 'vk.com'], ['www.yandex', 'example.com']) AS s,
    count() AS c
FROM test.hits
GROUP BY domain(Referer)
ORDER BY count() DESC
LIMIT 10
```

``` text
┌─s──────────────┬───────c─┐
│                │ 2906259 │
│ www.yandex     │  867767 │
│ ███████.ru     │  313599 │
│ mail.yandex.ru │  107147 │
│ ██████.ru      │  100355 │
│ █████████.ru   │   65040 │
│ news.yandex.ru │   64515 │
│ ██████.net     │   59141 │
│ example.com    │   57316 │
└────────────────┴─────────┘
```

## formatReadableSize(x) {#formatreadablesizex}

Принимает размер (число байт). Возвращает округленный размер с суффиксом (KiB, MiB и т.д.) в виде строки.

Пример:

``` sql
SELECT
    arrayJoin([1, 1024, 1024*1024, 192851925]) AS filesize_bytes,
    formatReadableSize(filesize_bytes) AS filesize
```

``` text
┌─filesize_bytes─┬─filesize───┐
│              1 │ 1.00 B     │
│           1024 │ 1.00 KiB   │
│        1048576 │ 1.00 MiB   │
│      192851925 │ 183.92 MiB │
└────────────────┴────────────┘
```

## least(a, b) {#leasta-b}

Возвращает наименьшее значение из a и b.

## greatest(a, b) {#greatesta-b}

Возвращает наибольшее значение из a и b.

## uptime() {#uptime}

Возвращает аптайм сервера в секундах.

## version() {#version}

Возвращает версию сервера в виде строки.

## rowNumberInBlock {#function-rownumberinblock}

Возвращает порядковый номер строки в блоке данных. Для каждого блока данных нумерация начинается с 0.

## rowNumberInAllBlocks() {#rownumberinallblocks}

Возвращает порядковый номер строки в блоке данных. Функция учитывает только задействованные блоки данных.

## neighbor {#neighbor}

Функция позволяет получить доступ к значению в колонке `column`, находящемуся на смещении `offset` относительно текущей строки. Является частичной реализацией [оконных функций](https://en.wikipedia.org/wiki/SQL_window_function) `LEAD()` и `LAG()`.

**Синтаксис**

``` sql
neighbor(column, offset[, default_value])
```

Результат функции зависит от затронутых блоков данных и порядка данных в блоке. Если сделать подзапрос с ORDER BY и вызывать функцию извне подзапроса, можно будет получить ожидаемый результат.

**Параметры**

-   `column` — Имя столбца или скалярное выражение.
-   `offset` - Смещение от текущей строки `column`. [Int64](../../sql-reference/functions/other-functions.md).
-   `default_value` - Опциональный параметр. Значение, которое будет возвращено, если смещение выходит за пределы блока данных.

**Возвращаемое значение**

-   Значение `column` в смещении от текущей строки, если значение `offset` не выходит за пределы блока.
-   Значение по умолчанию для `column`, если значение `offset` выходит за пределы блока данных. Если передан параметр `default_value`, то значение берется из него.

Тип: зависит от данных в `column` или переданного значения по умолчанию в `default_value`.

**Пример**

Запрос:

``` sql
SELECT number, neighbor(number, 2) FROM system.numbers LIMIT 10;
```

Ответ:

``` text
┌─number─┬─neighbor(number, 2)─┐
│      0 │                   2 │
│      1 │                   3 │
│      2 │                   4 │
│      3 │                   5 │
│      4 │                   6 │
│      5 │                   7 │
│      6 │                   8 │
│      7 │                   9 │
│      8 │                   0 │
│      9 │                   0 │
└────────┴─────────────────────┘
```

Запрос:

``` sql
SELECT number, neighbor(number, 2, 999) FROM system.numbers LIMIT 10;
```

Ответ:

``` text
┌─number─┬─neighbor(number, 2, 999)─┐
│      0 │                        2 │
│      1 │                        3 │
│      2 │                        4 │
│      3 │                        5 │
│      4 │                        6 │
│      5 │                        7 │
│      6 │                        8 │
│      7 │                        9 │
│      8 │                      999 │
│      9 │                      999 │
└────────┴──────────────────────────┘
```

Эта функция может использоваться для оценки year-over-year значение показателя:

Запрос:

``` sql
WITH toDate('2018-01-01') AS start_date
SELECT
    toStartOfMonth(start_date + (number * 32)) AS month,
    toInt32(month) % 100 AS money,
    neighbor(money, -12) AS prev_year,
    round(prev_year / money, 2) AS year_over_year
FROM numbers(16)
```

Ответ:

``` text
┌──────month─┬─money─┬─prev_year─┬─year_over_year─┐
│ 2018-01-01 │    32 │         0 │              0 │
│ 2018-02-01 │    63 │         0 │              0 │
│ 2018-03-01 │    91 │         0 │              0 │
│ 2018-04-01 │    22 │         0 │              0 │
│ 2018-05-01 │    52 │         0 │              0 │
│ 2018-06-01 │    83 │         0 │              0 │
│ 2018-07-01 │    13 │         0 │              0 │
│ 2018-08-01 │    44 │         0 │              0 │
│ 2018-09-01 │    75 │         0 │              0 │
│ 2018-10-01 │     5 │         0 │              0 │
│ 2018-11-01 │    36 │         0 │              0 │
│ 2018-12-01 │    66 │         0 │              0 │
│ 2019-01-01 │    97 │        32 │           0.33 │
│ 2019-02-01 │    28 │        63 │           2.25 │
│ 2019-03-01 │    56 │        91 │           1.62 │
│ 2019-04-01 │    87 │        22 │           0.25 │
└────────────┴───────┴───────────┴────────────────┘
```

## runningDifference(x) {#runningdifferencex}

Считает разницу между последовательными значениями строк в блоке данных.
Возвращает 0 для первой строки и разницу с предыдущей строкой для каждой последующей строки.

Результат функции зависит от затронутых блоков данных и порядка данных в блоке.
Если сделать подзапрос с ORDER BY и вызывать функцию извне подзапроса, можно будет получить ожидаемый результат.

Пример:

``` sql
SELECT
    EventID,
    EventTime,
    runningDifference(EventTime) AS delta
FROM
(
    SELECT
        EventID,
        EventTime
    FROM events
    WHERE EventDate = '2016-11-24'
    ORDER BY EventTime ASC
    LIMIT 5
)
```

``` text
┌─EventID─┬───────────EventTime─┬─delta─┐
│    1106 │ 2016-11-24 00:00:04 │     0 │
│    1107 │ 2016-11-24 00:00:05 │     1 │
│    1108 │ 2016-11-24 00:00:05 │     0 │
│    1109 │ 2016-11-24 00:00:09 │     4 │
│    1110 │ 2016-11-24 00:00:10 │     1 │
└─────────┴─────────────────────┴───────┘
```

Обратите внимание — размер блока влияет на результат. С каждым новым блоком состояние `runningDifference` сбрасывается.

``` sql
SELECT
    number,
    runningDifference(number + 1) AS diff
FROM numbers(100000)
WHERE diff != 1
```

``` text
┌─number─┬─diff─┐
│      0 │    0 │
└────────┴──────┘
┌─number─┬─diff─┐
│  65536 │    0 │
└────────┴──────┘
```

    set max_block_size=100000 -- по умолчанию 65536!

    SELECT
        number,
        runningDifference(number + 1) AS diff
    FROM numbers(100000)
    WHERE diff != 1

``` text
┌─number─┬─diff─┐
│      0 │    0 │
└────────┴──────┘
```

## runningDifferenceStartingWithFirstValue {#runningdifferencestartingwithfirstvalue}

То же, что и \[runningDifference\] (./other\_functions.md \# other\_functions-runningdifference), но в первой строке возвращается значение первой строки, а не ноль.

## MACNumToString(num) {#macnumtostringnum}

Принимает число типа UInt64. Интерпретирует его, как MAC-адрес в big endian. Возвращает строку, содержащую соответствующий MAC-адрес в формате AA:BB:CC:DD:EE:FF (числа в шестнадцатеричной форме через двоеточие).

## MACStringToNum(s) {#macstringtonums}

Функция, обратная к MACNumToString. Если MAC адрес в неправильном формате, то возвращает 0.

## MACStringToOUI(s) {#macstringtoouis}

Принимает MAC адрес в формате AA:BB:CC:DD:EE:FF (числа в шестнадцатеричной форме через двоеточие). Возвращает первые три октета как число в формате UInt64. Если MAC адрес в неправильном формате, то возвращает 0.

## getSizeOfEnumType {#getsizeofenumtype}

Возвращает количество полей в [Enum](../../sql-reference/functions/other-functions.md).

``` sql
getSizeOfEnumType(value)
```

**Параметры**

-   `value` — Значение типа `Enum`.

**Возвращаемые значения**

-   Количество полей входного значения типа `Enum`.
-   Исключение, если тип не `Enum`.

**Пример**

``` sql
SELECT getSizeOfEnumType( CAST('a' AS Enum8('a' = 1, 'b' = 2) ) ) AS x
```

``` text
┌─x─┐
│ 2 │
└───┘
```

## toColumnTypeName {#tocolumntypename}

Возвращает имя класса, которым представлен тип данных столбца в оперативной памяти.

``` sql
toColumnTypeName(value)
```

**Параметры**

-   `value` — Значение произвольного типа.

**Возвращаемые значения**

-   Строка с именем класса, который используется для представления типа данных `value` в оперативной памяти.

**Пример разницы между `toTypeName` и `toColumnTypeName`**

``` sql
SELECT toTypeName(CAST('2018-01-01 01:02:03' AS DateTime))
```

``` text
┌─toTypeName(CAST('2018-01-01 01:02:03', 'DateTime'))─┐
│ DateTime                                            │
└─────────────────────────────────────────────────────┘
```

``` sql
SELECT toColumnTypeName(CAST('2018-01-01 01:02:03' AS DateTime))
```

``` text
┌─toColumnTypeName(CAST('2018-01-01 01:02:03', 'DateTime'))─┐
│ Const(UInt32)                                             │
└───────────────────────────────────────────────────────────┘
```

В примере видно, что тип данных `DateTime` хранится в памяти как `Const(UInt32)`.

## dumpColumnStructure {#dumpcolumnstructure}

Выводит развернутое описание структур данных в оперативной памяти

``` sql
dumpColumnStructure(value)
```

**Параметры**

-   `value` — Значение произвольного типа.

**Возвращаемые значения**

-   Строка с описанием структуры, которая используется для представления типа данных `value` в оперативной памяти.

**Пример**

``` sql
SELECT dumpColumnStructure(CAST('2018-01-01 01:02:03', 'DateTime'))
```

``` text
┌─dumpColumnStructure(CAST('2018-01-01 01:02:03', 'DateTime'))─┐
│ DateTime, Const(size = 1, UInt32(size = 1))                  │
└──────────────────────────────────────────────────────────────┘
```

## defaultValueOfArgumentType {#defaultvalueofargumenttype}

Выводит значение по умолчанию для типа данных.

Не учитывает значения по умолчанию для столбцов, заданные пользователем.

``` sql
defaultValueOfArgumentType(expression)
```

**Параметры**

-   `expression` — Значение произвольного типа или выражение, результатом которого является значение произвольного типа.

**Возвращаемые значения**

-   `0` для чисел;
-   Пустая строка для строк;
-   `ᴺᵁᴸᴸ` для [Nullable](../../sql-reference/functions/other-functions.md).

**Пример**

``` sql
SELECT defaultValueOfArgumentType( CAST(1 AS Int8) )
```

``` text
┌─defaultValueOfArgumentType(CAST(1, 'Int8'))─┐
│                                           0 │
└─────────────────────────────────────────────┘
```

``` sql
SELECT defaultValueOfArgumentType( CAST(1 AS Nullable(Int8) ) )
```

``` text
┌─defaultValueOfArgumentType(CAST(1, 'Nullable(Int8)'))─┐
│                                                  ᴺᵁᴸᴸ │
└───────────────────────────────────────────────────────┘
```

## replicate {#other-functions-replicate}

Создает массив, заполненный одним значением.

Используется для внутренней реализации [arrayJoin](array-join.md#functions_arrayjoin).

``` sql
SELECT replicate(x, arr);
```

**Параметры**

-   `arr` — Исходный массив. ClickHouse создаёт новый массив такой же длины как исходный и заполняет его значением `x`.
-   `x` — Значение, которым будет заполнен результирующий массив.

**Возвращаемое значение**

Массив, заполненный значением `x`.

Тип: `Array`.

**Пример**

Запрос:

``` sql
SELECT replicate(1, ['a', 'b', 'c']);
```

Ответ:

``` text
┌─replicate(1, ['a', 'b', 'c'])─┐
│ [1,1,1]                       │
└───────────────────────────────┘
```

## filesystemAvailable {#filesystemavailable}

Возвращает объём доступного для записи данных места на файловой системе. Он всегда меньше общего свободного места ([filesystemFree](#filesystemfree)), потому что некоторое пространство зарезервировано для нужд операционной системы.

**Синтаксис**

``` sql
filesystemAvailable()
```

**Возвращаемое значение**

-   Объём доступного для записи данных места в байтах.

Тип: [UInt64](../../sql-reference/functions/other-functions.md).

**Пример**

Запрос:

``` sql
SELECT formatReadableSize(filesystemAvailable()) AS "Available space", toTypeName(filesystemAvailable()) AS "Type";
```

Ответ:

``` text
┌─Available space─┬─Type───┐
│ 30.75 GiB       │ UInt64 │
└─────────────────┴────────┘
```

## filesystemFree {#filesystemfree}

Возвращает объём свободного места на файловой системе. Смотрите также `filesystemAvailable`.

**Синтаксис**

``` sql
filesystemFree()
```

**Возвращаемое значение**

-   Объем свободного места в байтах.

Тип: [UInt64](../../sql-reference/functions/other-functions.md).

**Пример**

Запрос:

``` sql
SELECT formatReadableSize(filesystemFree()) AS "Free space", toTypeName(filesystemFree()) AS "Type";
```

Ответ:

``` text
┌─Free space─┬─Type───┐
│ 32.39 GiB  │ UInt64 │
└────────────┴────────┘
```

## filesystemCapacity {#filesystemcapacity}

Возвращает информацию о ёмкости файловой системы в байтах. Для оценки должен быть настроен [путь](../../sql-reference/functions/other-functions.md#server_configuration_parameters-path) к каталогу с данными.

**Синтаксис**

``` sql
filesystemCapacity()
```

**Возвращаемое значение**

-   Информация о ёмкости файловой системы в байтах.

Тип: [UInt64](../../sql-reference/functions/other-functions.md).

**Пример**

Запрос:

``` sql
SELECT formatReadableSize(filesystemCapacity()) AS "Capacity", toTypeName(filesystemCapacity()) AS "Type"
```

Ответ:

``` text
┌─Capacity──┬─Type───┐
│ 39.32 GiB │ UInt64 │
└───────────┴────────┘
```

## finalizeAggregation {#function-finalizeaggregation}

Принимает состояние агрегатной функции. Возвращает результат агрегирования.

## runningAccumulate {#function-runningaccumulate}

Принимает на вход состояния агрегатной функции и возвращает столбец со значениями, которые представляют собой результат мёржа этих состояний для выборки строк из блока от первой до текущей строки. Например, принимает состояние агрегатной функции (например, `runningAccumulate(uniqState(UserID))`), и для каждой строки блока возвращает результат агрегатной функции после мёржа состояний функции для всех предыдущих строк и текущей. Таким образом, результат зависит от разбиения данных по блокам и от порядка данных в блоке.

## joinGet {#joinget}

Функция позволяет извлекать данные из таблицы таким же образом как из [словаря](../../sql-reference/functions/other-functions.md).

Получает данные из таблиц [Join](../../sql-reference/functions/other-functions.md#creating-a-table) по ключу.

Поддерживаются только таблицы, созданные с `ENGINE = Join(ANY, LEFT, <join_keys>)`.

**Синтаксис**

``` sql
joinGet(join_storage_table_name, `value_column`, join_keys)
```

**Параметры**

-   `join_storage_table_name` — [идентификатор](../syntax.md#syntax-identifiers), который указывает, откуда производится выборка данных. Поиск по идентификатору осуществляется в базе данных по умолчанию (см. конфигурацию `default_database`). Чтобы переопределить базу данных по умолчанию, используйте команду `USE db_name`, или укажите базу данных и таблицу через разделитель `db_name.db_table`, см. пример.
-   `value_column` — столбец, из которого нужно произвести выборку данных.
-   `join_keys` — список ключей, по которым производится выборка данных.

**Возвращаемое значение**

Возвращает значение по списку ключей.

Если значения не существует в исходной таблице, вернется `0` или `null` в соответствии с настройками [join\_use\_nulls](../../operations/settings/settings.md#join_use_nulls).

Подробнее о настройке `join_use_nulls` в [операциях Join](../../sql-reference/functions/other-functions.md).

**Пример**

Входная таблица:

``` sql
CREATE DATABASE db_test
CREATE TABLE db_test.id_val(`id` UInt32, `val` UInt32) ENGINE = Join(ANY, LEFT, id) SETTINGS join_use_nulls = 1
INSERT INTO db_test.id_val VALUES (1,11)(2,12)(4,13)
```

``` text
┌─id─┬─val─┐
│  4 │  13 │
│  2 │  12 │
│  1 │  11 │
└────┴─────┘
```

Запрос:

``` sql
SELECT joinGet(db_test.id_val,'val',toUInt32(number)) from numbers(4) SETTINGS join_use_nulls = 1
```

Результат:

``` text
┌─joinGet(db_test.id_val, 'val', toUInt32(number))─┐
│                                                0 │
│                                               11 │
│                                               12 │
│                                                0 │
└──────────────────────────────────────────────────┘
```

## modelEvaluate(model\_name, …) {#function-modelevaluate}

Оценивает внешнюю модель.

Принимает на вход имя и аргументы модели. Возвращает Float64.

## throwIf(x\[, custom\_message\]) {#throwifx-custom-message}

Бросает исключение, если аргумент не равен нулю.
custom\_message - необязательный параметр, константная строка, задает текст сообщения об ошибке.

``` sql
SELECT throwIf(number = 3, 'Too many') FROM numbers(10);
```

``` text
↙ Progress: 0.00 rows, 0.00 B (0.00 rows/s., 0.00 B/s.) Received exception from server (version 19.14.1):
Code: 395. DB::Exception: Received from localhost:9000. DB::Exception: Too many.
```

## identity {#identity}

Возвращает свой аргумент. Используется для отладки и тестирования, позволяет отменить использование индекса, и получить результат и производительность полного сканирования таблицы. Это работает, потому что оптимизатор запросов не может «заглянуть» внутрь функции `identity`.

**Синтаксис**

``` sql
identity(x)
```

**Пример**

Query:

``` sql
SELECT identity(42)
```

Результат:

``` text
┌─identity(42)─┐
│           42 │
└──────────────┘
```

## randomPrintableASCII {#randomascii}

Генерирует строку со случайным набором печатных символов [ASCII](https://en.wikipedia.org/wiki/ASCII#Printable_characters).

**Синтаксис**

``` sql
randomPrintableASCII(length)
```

**Параметры**

-   `length` — Длина результирующей строки. Положительное целое число.

        Если передать `length < 0`, то поведение функции не определено.

**Возвращаемое значение**

-   Строка со случайным набором печатных символов [ASCII](https://en.wikipedia.org/wiki/ASCII#Printable_characters).

Тип: [String](../../sql-reference/functions/other-functions.md)

**Пример**

``` sql
SELECT number, randomPrintableASCII(30) as str, length(str) FROM system.numbers LIMIT 3
```

``` text
┌─number─┬─str────────────────────────────┬─length(randomPrintableASCII(30))─┐
│      0 │ SuiCOSTvC0csfABSw=UcSzp2.`rv8x │                               30 │
│      1 │ 1Ag NlJ &RCN:*>HVPG;PE-nO"SUFD │                               30 │
│      2 │ /"+<"wUTh:=LjJ Vm!c&hI*m#XTfzz │                               30 │
└────────┴────────────────────────────────┴──────────────────────────────────┘
```

## randomString {#randomstring}

Генерирует бинарную строку заданной длины, заполненную случайными байтами (в том числе нулевыми).

**Синтаксис**

``` sql
randomString(length)
```

**Параметры** 

-   `length` — длина строки. Положительное целое число.

**Возвращаемое значение**

-   Строка, заполненная случайными байтами.

Type: [String](../../sql-reference/data-types/string.md).

**Пример**

Запрос:

``` sql
SELECT randomString(30) AS str, length(str) AS len FROM numbers(2) FORMAT Vertical;
```

Ответ:

``` text
Row 1:
──────
str: 3 G  :   pT ?w тi  k aV f6
len: 30

Row 2:
──────
str: 9 ,]    ^   )  ]??  8
len: 30
```

**Смотрите также**

-   [generateRandom](../../sql-reference/table-functions/generate.md#generaterandom)
-   [randomPrintableASCII](../../sql-reference/functions/other-functions.md#randomascii)


[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/functions/other_functions/) <!--hide-->
