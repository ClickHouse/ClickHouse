---
sidebar_position: 66
sidebar_label: "Прочие функции"
---

# Прочие функции {#other-functions}

## hostName() {#hostname}

Возвращает строку - имя хоста, на котором эта функция была выполнена. При распределённой обработке запроса, это будет имя хоста удалённого сервера, если функция выполняется на удалённом сервере.
Если функция вызывается в контексте распределенной таблицы, то она генерирует обычный столбец со значениями, актуальными для каждого шарда. Иначе возвращается константа.

## getMacro {#getmacro}

Возвращает именованное значение из секции [macros](../../operations/server-configuration-parameters/settings.md#macros) конфигурации сервера.

**Синтаксис**

```sql
getMacro(name)
```

**Аргументы**

-   `name` — имя, которое необходимо получить из секции `macros`. [String](../../sql-reference/data-types/string.md#string).

**Возвращаемое значение**

-   Значение по указанному имени.

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
fqdn()
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

Результат:

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

**Аргументы**

-   `expr` — выражение, возвращающее значение типа [String](../../sql-reference/functions/other-functions.md). В результирующем значении все бэкслэши должны быть экранированы.

**Возвращаемое значение**

Строка, содержащая:

-   Конечную часть строки после последнего слэша или бэкслэша.

        Если входная строка содержит путь, заканчивающийся слэшем или бэкслэшем, например, `/` или `с:\`, функция возвращает пустую строку.

-   Исходная строка, если нет слэша или бэкслэша.

**Пример**

``` sql
SELECT 'some/long/path/to/file' AS a, basename(a);
```

``` text
┌─a──────────────────────┬─basename('some\\long\\path\\to\\file')─┐
│ some\long\path\to\file │ file                                   │
└────────────────────────┴────────────────────────────────────────┘
```

``` sql
SELECT 'some\\long\\path\\to\\file' AS a, basename(a);
```

``` text
┌─a──────────────────────┬─basename('some\\long\\path\\to\\file')─┐
│ some\long\path\to\file │ file                                   │
└────────────────────────┴────────────────────────────────────────┘
```

``` sql
SELECT 'some-file-name' AS a, basename(a);
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

## byteSize {#function-bytesize}

Возвращает оценку в байтах размера аргументов в памяти в несжатом виде.

**Синтаксис**

```sql
byteSize(argument [, ...])
```

**Аргументы**

-   `argument` — значение.

**Возвращаемое значение**

-   Оценка размера аргументов в памяти в байтах.

Тип: [UInt64](../../sql-reference/data-types/int-uint.md).

**Примеры**

Для аргументов типа [String](../../sql-reference/data-types/string.md) функция возвращает длину строки + 9 (нуль-терминатор + длина)

Запрос:

```sql
SELECT byteSize('string');
```

Результат:

```text
┌─byteSize('string')─┐
│                 15 │
└────────────────────┘
```

Запрос:

```sql
CREATE TABLE test
(
    `key` Int32,
    `u8` UInt8,
    `u16` UInt16,
    `u32` UInt32,
    `u64` UInt64,
    `i8` Int8,
    `i16` Int16,
    `i32` Int32,
    `i64` Int64,
    `f32` Float32,
    `f64` Float64
)
ENGINE = MergeTree
ORDER BY key;

INSERT INTO test VALUES(1, 8, 16, 32, 64,  -8, -16, -32, -64, 32.32, 64.64);

SELECT key, byteSize(u8) AS `byteSize(UInt8)`, byteSize(u16) AS `byteSize(UInt16)`, byteSize(u32) AS `byteSize(UInt32)`, byteSize(u64) AS `byteSize(UInt64)`, byteSize(i8) AS `byteSize(Int8)`, byteSize(i16) AS `byteSize(Int16)`, byteSize(i32) AS `byteSize(Int32)`, byteSize(i64) AS `byteSize(Int64)`, byteSize(f32) AS `byteSize(Float32)`, byteSize(f64) AS `byteSize(Float64)` FROM test ORDER BY key ASC FORMAT Vertical;
```

Результат:

``` text
Row 1:
──────
key:               1
byteSize(UInt8):   1
byteSize(UInt16):  2
byteSize(UInt32):  4
byteSize(UInt64):  8
byteSize(Int8):    1
byteSize(Int16):   2
byteSize(Int32):   4
byteSize(Int64):   8
byteSize(Float32): 4
byteSize(Float64): 8
```

Если функция принимает несколько аргументов, то она возвращает их совокупный размер в байтах.

Запрос:

```sql
SELECT byteSize(NULL, 1, 0.3, '');
```

Результат:

```text
┌─byteSize(NULL, 1, 0.3, '')─┐
│                         19 │
└────────────────────────────┘
```

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

Результат:

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

**Аргументы**

- `x` — выражение для проверки.

**Возвращаемые значения**

- `1` — выражение `x` является константным.
- `0` — выражение `x` не является константным.

Тип: [UInt8](../data-types/int-uint.md).

**Примеры**

Запрос:

```sql
SELECT isConstant(x + 1) FROM (SELECT 43 AS x);
```

Результат:

```text
┌─isConstant(plus(x, 1))─┐
│                      1 │
└────────────────────────┘
```

Запрос:

```sql
WITH 3.14 AS pi SELECT isConstant(cos(pi));
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

**Аргументы**

-   `x` — значение, которое нужно проверить на бесконечность. Тип: [Float\*](../../sql-reference/functions/other-functions.md).
-   `y` — запасное значение. Тип: [Float\*](../../sql-reference/functions/other-functions.md).

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

Аргументы:

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

### transform(x, array_from, array_to, default) {#transformx-array-from-array-to-default}

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

Если значение x равно одному из элементов массива array_from, то возвращает соответствующий (такой же по номеру) элемент массива array_to; иначе возвращает default. Если имеется несколько совпадающих элементов в array_from, то возвращает какой-нибудь из соответствующих.

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

### transform(x, array_from, array_to) {#transformx-array-from-array-to}

Отличается от первого варианта отсутствующим аргументом default.
Если значение x равно одному из элементов массива array_from, то возвращает соответствующий (такой же по номеру) элемент массива array_to; иначе возвращает x.

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

## formatReadableQuantity(x) {#formatreadablequantityx}

Принимает число. Возвращает округленное число с суффиксом (thousand, million, billion и т.д.) в виде строки.

Облегчает визуальное восприятие больших чисел живым человеком.

Пример:

``` sql
SELECT
    arrayJoin([1024, 1234 * 1000, (4567 * 1000) * 1000, 98765432101234]) AS number,
    formatReadableQuantity(number) AS number_for_humans
```

``` text
┌─────────number─┬─number_for_humans─┐
│           1024 │ 1.02 thousand     │
│        1234000 │ 1.23 million      │
│     4567000000 │ 4.57 billion      │
│ 98765432101234 │ 98.77 trillion    │
└────────────────┴───────────────────┘
```

## least(a, b) {#leasta-b}

Возвращает наименьшее значение из a и b.

## greatest(a, b) {#greatesta-b}

Возвращает наибольшее значение из a и b.

## uptime() {#uptime}

Возвращает аптайм сервера в секундах.
Если функция вызывается в контексте распределенной таблицы, то она генерирует обычный столбец со значениями, актуальными для каждого шарда. Иначе возвращается константа.

## version() {#version}

Возвращает версию сервера в виде строки.
Если функция вызывается в контексте распределенной таблицы, то она генерирует обычный столбец со значениями, актуальными для каждого шарда. Иначе возвращается константа.

## buildId() {#buildid}

Возвращает ID сборки, сгенерированный компилятором для данного сервера ClickHouse.
Если функция вызывается в контексте распределенной таблицы, то она генерирует обычный столбец со значениями, актуальными для каждого шарда. Иначе возвращается константа.

## rowNumberInBlock {#function-rownumberinblock}

Возвращает порядковый номер строки в блоке данных. Для каждого блока данных нумерация начинается с 0.

## rowNumberInAllBlocks() {#rownumberinallblocks}

Возвращает порядковый номер строки в блоке данных. Функция учитывает только задействованные блоки данных.

## neighbor {#neighbor}

Функция позволяет получить доступ к значению в столбце `column`, находящемуся на смещении `offset` относительно текущей строки. Является частичной реализацией [оконных функций](https://en.wikipedia.org/wiki/SQL_window_function) `LEAD()` и `LAG()`.

**Синтаксис**

``` sql
neighbor(column, offset[, default_value])
```

Результат функции зависит от затронутых блоков данных и порядка данных в блоке.

:::danger "Предупреждение"
    Функция может получить доступ к значению в столбце соседней строки только внутри обрабатываемого в данный момент блока данных.

Порядок строк, используемый при вычислении функции `neighbor`, может отличаться от порядка строк, возвращаемых пользователю.
Чтобы этого не случилось, вы можете сделать подзапрос с [ORDER BY](../../sql-reference/statements/select/order-by.md) и вызвать функцию извне подзапроса.

**Аргументы**

-   `column` — имя столбца или скалярное выражение.
-   `offset` — смещение от текущей строки `column`. [Int64](../../sql-reference/functions/other-functions.md).
-   `default_value` — опциональный параметр. Значение, которое будет возвращено, если смещение выходит за пределы блока данных.

**Возвращаемое значение**

-   Значение `column` в смещении от текущей строки, если значение `offset` не выходит за пределы блока.
-   Значение по умолчанию для `column`, если значение `offset` выходит за пределы блока данных. Если передан параметр `default_value`, то значение берется из него.

Тип: зависит от данных в `column` или переданного значения по умолчанию в `default_value`.

**Пример**

Запрос:

``` sql
SELECT number, neighbor(number, 2) FROM system.numbers LIMIT 10;
```

Результат:

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

Результат:

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

Результат:

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

## runningDifference(x) {#other_functions-runningdifference}

Считает разницу между последовательными значениями строк в блоке данных.
Возвращает 0 для первой строки и разницу с предыдущей строкой для каждой последующей строки.

:::danger "Предупреждение"
    Функция может взять значение предыдущей строки только внутри текущего обработанного блока данных.

Результат функции зависит от затронутых блоков данных и порядка данных в блоке.

Порядок строк, используемый при вычислении функции `runningDifference`, может отличаться от порядка строк, возвращаемых пользователю.
Чтобы этого не случилось, вы можете сделать подзапрос с [ORDER BY](../../sql-reference/statements/select/order-by.md) и вызвать функцию извне подзапроса.

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

То же, что и [runningDifference](./other-functions.md#other_functions-runningdifference), но в первой строке возвращается значение первой строки, а не ноль.

## runningConcurrency {#runningconcurrency}

Подсчитывает количество одновременно идущих событий.
У каждого события есть время начала и время окончания. Считается, что время начала включено в событие, а время окончания исключено из него. Столбцы со временем начала и окончания событий должны иметь одинаковый тип данных.
Функция подсчитывает количество событий, происходящих одновременно на момент начала каждого из событий в выборке.

:::danger "Предупреждение"
    События должны быть отсортированы по возрастанию времени начала. Если это требование нарушено, то функция вызывает исключение.
    Каждый блок данных обрабатывается независимо. Если события из разных блоков данных накладываются по времени, они не могут быть корректно обработаны.

**Синтаксис**

``` sql
runningConcurrency(start, end)
```

**Аргументы**

-   `start` — Столбец с временем начала событий. [Date](../../sql-reference/data-types/date.md), [DateTime](../../sql-reference/data-types/datetime.md) или [DateTime64](../../sql-reference/data-types/datetime64.md).
-   `end` — Столбец с временем окончания событий.  [Date](../../sql-reference/data-types/date.md), [DateTime](../../sql-reference/data-types/datetime.md) или [DateTime64](../../sql-reference/data-types/datetime64.md).

**Возвращаемое значение**

-   Количество одновременно идущих событий на момент начала каждого события.

Тип: [UInt32](../../sql-reference/data-types/int-uint.md)

**Пример**

Рассмотрим таблицу:

``` text
┌──────start─┬────────end─┐
│ 2021-03-03 │ 2021-03-11 │
│ 2021-03-06 │ 2021-03-12 │
│ 2021-03-07 │ 2021-03-08 │
│ 2021-03-11 │ 2021-03-12 │
└────────────┴────────────┘
```

Запрос:

``` sql
SELECT start, runningConcurrency(start, end) FROM example_table;
```

Результат:

``` text
┌──────start─┬─runningConcurrency(start, end)─┐
│ 2021-03-03 │                              1 │
│ 2021-03-06 │                              2 │
│ 2021-03-07 │                              3 │
│ 2021-03-11 │                              2 │
└────────────┴────────────────────────────────┘
```

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

**Аргументы**

-   `value` — значение типа `Enum`.

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

## blockSerializedSize {#blockserializedsize}

Возвращает размер на диске (без учета сжатия).

``` sql
blockSerializedSize(value[, value[, ...]])
```

**Аргументы**

-   `value` — значение произвольного типа.

**Возвращаемые значения**

-   Количество байтов, которые будут записаны на диск для блока значений (без сжатия).

**Пример**

Запрос:

``` sql
SELECT blockSerializedSize(maxState(1)) as x
```

Ответ:

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

**Аргументы**

-   `value` — значение произвольного типа.

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

**Аргументы**

-   `value` — значение произвольного типа.

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

**Аргументы**

-   `expression` — значение произвольного типа или выражение, результатом которого является значение произвольного типа.

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

## defaultValueOfTypeName {#defaultvalueoftypename}

Выводит значение по умолчанию для указанного типа данных.

Не включает значения по умолчанию для настраиваемых столбцов, установленных пользователем.

``` sql
defaultValueOfTypeName(type)
```

**Аргументы**

-   `type` — тип данных.

**Возвращаемое значение**

-   `0` для чисел;
-   Пустая строка для строк;
-   `ᴺᵁᴸᴸ` для [Nullable](../../sql-reference/data-types/nullable.md).

**Пример**

``` sql
SELECT defaultValueOfTypeName('Int8')
```

``` text
┌─defaultValueOfTypeName('Int8')─┐
│                              0 │
└────────────────────────────────┘
```

``` sql
SELECT defaultValueOfTypeName('Nullable(Int8)')
```

``` text
┌─defaultValueOfTypeName('Nullable(Int8)')─┐
│                                     ᴺᵁᴸᴸ │
└──────────────────────────────────────────┘
```

## indexHint {#indexhint}
Возвращает все данные из диапазона, в который попадают данные, соответствующие указанному выражению.
Переданное выражение не будет вычислено. Выбор диапазона производится по индексу.
Индекс в ClickHouse разреженный, при чтении диапазона в ответ попадают «лишние» соседние данные.

**Синтаксис**

```sql
SELECT * FROM table WHERE indexHint(<expression>)
```

**Возвращаемое значение**

Возвращает диапазон индекса, в котором выполняется заданное условие.

Тип: [Uint8](https://clickhouse.com/docs/ru/data_types/int_uint/#diapazony-uint).

**Пример**

Рассмотрим пример с использованием тестовых данных таблицы [ontime](../../getting-started/example-datasets/ontime.md).

Исходная таблица:

```sql
SELECT count() FROM ontime
```

```text
┌─count()─┐
│ 4276457 │
└─────────┘
```

В таблице есть индексы по полям `(FlightDate, (Year, FlightDate))`.

Выполним выборку по дате, где индекс не используется.

Запрос:

```sql
SELECT FlightDate AS k, count() FROM ontime GROUP BY k ORDER BY k
```

ClickHouse обработал всю таблицу (`Processed 4.28 million rows`).

Результат:

```text
┌──────────k─┬─count()─┐
│ 2017-01-01 │   13970 │
│ 2017-01-02 │   15882 │
........................
│ 2017-09-28 │   16411 │
│ 2017-09-29 │   16384 │
│ 2017-09-30 │   12520 │
└────────────┴─────────┘
```

Для подключения индекса выбираем конкретную дату.

Запрос:

```sql
SELECT FlightDate AS k, count() FROM ontime WHERE k = '2017-09-15' GROUP BY k ORDER BY k
```

При использовании индекса ClickHouse обработал значительно меньшее количество строк (`Processed 32.74 thousand rows`).

Результат:

```text
┌──────────k─┬─count()─┐
│ 2017-09-15 │   16428 │
└────────────┴─────────┘
```

Передадим в функцию `indexHint` выражение `k = '2017-09-15'`.

Запрос:

```sql
SELECT
    FlightDate AS k,
    count()
FROM ontime
WHERE indexHint(k = '2017-09-15')
GROUP BY k
ORDER BY k ASC
```

ClickHouse применил индекс по аналогии с примером выше (`Processed 32.74 thousand rows`).
Выражение `k = '2017-09-15'` не используется при формировании результата.
Функция `indexHint` позволяет увидеть соседние данные.

Результат:

```text
┌──────────k─┬─count()─┐
│ 2017-09-14 │    7071 │
│ 2017-09-15 │   16428 │
│ 2017-09-16 │    1077 │
│ 2017-09-30 │    8167 │
└────────────┴─────────┘
```

## replicate {#other-functions-replicate}

Создает массив, заполненный одним значением.

Используется для внутренней реализации [arrayJoin](array-join.md#functions_arrayjoin).

``` sql
SELECT replicate(x, arr);
```

**Аргументы**

-   `arr` — исходный массив. ClickHouse создаёт новый массив такой же длины как исходный и заполняет его значением `x`.
-   `x` — значение, которым будет заполнен результирующий массив.

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

Результат:

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

Результат:

``` text
┌─Capacity──┬─Type───┐
│ 39.32 GiB │ UInt64 │
└───────────┴────────┘
```

## initializeAggregation {#initializeaggregation}

Вычисляет результат агрегатной функции для каждой строки. Предназначена для инициализации агрегатных функций с комбинатором [-State](../../sql-reference/aggregate-functions/combinators.md#state). Может быть полезна для создания состояний агрегатных функций для последующей их вставки в столбцы типа [AggregateFunction](../../sql-reference/data-types/aggregatefunction.md#data-type-aggregatefunction) или использования в качестве значений по-умолчанию.

**Синтаксис**

``` sql
initializeAggregation (aggregate_function, arg1, arg2, ..., argN)
```

**Аргументы**

-   `aggregate_function` — название агрегатной функции, состояние которой нужно создать. [String](../../sql-reference/data-types/string.md#string).
-   `arg` — аргументы, которые передаются в агрегатную функцию.

**Возвращаемое значение**

- В каждой строке результат агрегатной функции, примененной к аргументам из этой строки.

Тип возвращаемого значения такой же, как и у функции, переданной первым аргументом.


**Пример**

Запрос:

```sql
SELECT uniqMerge(state) FROM (SELECT initializeAggregation('uniqState', number % 3) AS state FROM numbers(10000));
```
Результат:

```text
┌─uniqMerge(state)─┐
│                3 │
└──────────────────┘
```

Запрос:

```sql
SELECT finalizeAggregation(state), toTypeName(state) FROM (SELECT initializeAggregation('sumState', number % 3) AS state FROM numbers(5));
```
Результат:

```text
┌─finalizeAggregation(state)─┬─toTypeName(state)─────────────┐
│                          0 │ AggregateFunction(sum, UInt8) │
│                          1 │ AggregateFunction(sum, UInt8) │
│                          2 │ AggregateFunction(sum, UInt8) │
│                          0 │ AggregateFunction(sum, UInt8) │
│                          1 │ AggregateFunction(sum, UInt8) │
└────────────────────────────┴───────────────────────────────┘
```

Пример с движком таблиц `AggregatingMergeTree` и столбцом типа `AggregateFunction`:

```sql
CREATE TABLE metrics
(
    key UInt64,
    value AggregateFunction(sum, UInt64) DEFAULT initializeAggregation('sumState', toUInt64(0))
)
ENGINE = AggregatingMergeTree
ORDER BY key
```

```sql
INSERT INTO metrics VALUES (0, initializeAggregation('sumState', toUInt64(42)))
```

**Смотрите также**
-   [arrayReduce](../../sql-reference/functions/array-functions.md#arrayreduce)

## finalizeAggregation {#function-finalizeaggregation}

Принимает состояние агрегатной функции. Возвращает результат агрегирования (или конечное состояние при использовании комбинатора [-State](../../sql-reference/aggregate-functions/combinators.md#state)).

**Синтаксис**

``` sql
finalizeAggregation(state)
```

**Аргументы**

-   `state` — состояние агрегатной функции. [AggregateFunction](../../sql-reference/data-types/aggregatefunction.md#data-type-aggregatefunction).

**Возвращаемые значения**

-   Значения, которые были агрегированы.

Тип: соответствует типу агрегируемых значений.

**Примеры**

Запрос:

```sql
SELECT finalizeAggregation(( SELECT countState(number) FROM numbers(10)));
```

Результат:

```text
┌─finalizeAggregation(_subquery16)─┐
│                               10 │
└──────────────────────────────────┘
```

Запрос:

```sql
SELECT finalizeAggregation(( SELECT sumState(number) FROM numbers(10)));
```

Результат:

```text
┌─finalizeAggregation(_subquery20)─┐
│                               45 │
└──────────────────────────────────┘
```

Обратите внимание, что значения `NULL` игнорируются.

Запрос:

```sql
SELECT finalizeAggregation(arrayReduce('anyState', [NULL, 2, 3]));
```

Результат:

```text
┌─finalizeAggregation(arrayReduce('anyState', [NULL, 2, 3]))─┐
│                                                          2 │
└────────────────────────────────────────────────────────────┘
```

Комбинированный пример:

Запрос:

```sql
WITH initializeAggregation('sumState', number) AS one_row_sum_state
SELECT
    number,
    finalizeAggregation(one_row_sum_state) AS one_row_sum,
    runningAccumulate(one_row_sum_state) AS cumulative_sum
FROM numbers(10);
```

Результат:

```text
┌─number─┬─one_row_sum─┬─cumulative_sum─┐
│      0 │           0 │              0 │
│      1 │           1 │              1 │
│      2 │           2 │              3 │
│      3 │           3 │              6 │
│      4 │           4 │             10 │
│      5 │           5 │             15 │
│      6 │           6 │             21 │
│      7 │           7 │             28 │
│      8 │           8 │             36 │
│      9 │           9 │             45 │
└────────┴─────────────┴────────────────┘
```

**Смотрите также**

-   [arrayReduce](../../sql-reference/functions/array-functions.md#arrayreduce)
-   [initializeAggregation](#initializeaggregation)

## runningAccumulate {#runningaccumulate}

Накапливает состояния агрегатной функции для каждой строки блока данных.

:::danger "Warning"
    Функция обнуляет состояние для каждого нового блока.

**Синтаксис**

```sql
runningAccumulate(agg_state[, grouping])
```

**Аргументы**

-   `agg_state` — состояние агрегатной функции. [AggregateFunction](../../sql-reference/data-types/aggregatefunction.md#data-type-aggregatefunction).
-   `grouping` — ключ группировки. Опциональный параметр. Состояние функции обнуляется, если значение `grouping` меняется. Параметр может быть любого [поддерживаемого типа данных](../../sql-reference/data-types/index.md), для которого определен оператор равенства.

**Возвращаемое значение**

-   Каждая результирующая строка содержит результат агрегатной функции, накопленный для всех входных строк от 0 до текущей позиции. `runningAccumulate` обнуляет состояния для каждого нового блока данных или при изменении значения `grouping`.

Тип зависит от используемой агрегатной функции.

**Примеры**

Рассмотрим примеры использования `runningAccumulate` для нахождения кумулятивной суммы чисел без и с группировкой.

Запрос:

```sql
SELECT k, runningAccumulate(sum_k) AS res FROM (SELECT number as k, sumState(k) AS sum_k FROM numbers(10) GROUP BY k ORDER BY k);
```

Результат:

```text
┌─k─┬─res─┐
│ 0 │   0 │
│ 1 │   1 │
│ 2 │   3 │
│ 3 │   6 │
│ 4 │  10 │
│ 5 │  15 │
│ 6 │  21 │
│ 7 │  28 │
│ 8 │  36 │
│ 9 │  45 │
└───┴─────┘
```

Подзапрос формирует `sumState` для каждого числа от `0` до `9`. `sumState` возвращает состояние функции [sum](../../sql-reference/aggregate-functions/reference/sum.md#agg_function-sum), содержащее сумму одного числа.

Весь запрос делает следующее:

1. Для первой строки `runningAccumulate` берет `sumState(0)` и возвращает `0`.
2. Для второй строки функция объединяет `sumState (0)` и `sumState (1)`, что приводит к `sumState (0 + 1)`, и возвращает в результате `1`.
3. Для третьей строки функция объединяет `sumState (0 + 1)` и `sumState (2)`, что приводит к `sumState (0 + 1 + 2)`, и в результате возвращает `3`.
4. Действия повторяются до тех пор, пока не закончится блок.

В следующем примере показано использование параметра `grouping`:

Запрос:

```sql
SELECT
    grouping,
    item,
    runningAccumulate(state, grouping) AS res
FROM
(
    SELECT
        toInt8(number / 4) AS grouping,
        number AS item,
        sumState(number) AS state
    FROM numbers(15)
    GROUP BY item
    ORDER BY item ASC
);
```

Результат:

```text
┌─grouping─┬─item─┬─res─┐
│        0 │    0 │   0 │
│        0 │    1 │   1 │
│        0 │    2 │   3 │
│        0 │    3 │   6 │
│        1 │    4 │   4 │
│        1 │    5 │   9 │
│        1 │    6 │  15 │
│        1 │    7 │  22 │
│        2 │    8 │   8 │
│        2 │    9 │  17 │
│        2 │   10 │  27 │
│        2 │   11 │  38 │
│        3 │   12 │  12 │
│        3 │   13 │  25 │
│        3 │   14 │  39 │
└──────────┴──────┴─────┘
```

Как вы можете видеть, `runningAccumulate` объединяет состояния для каждой группы строк отдельно.

## joinGet {#joinget}

Функция позволяет извлекать данные из таблицы таким же образом как из [словаря](../../sql-reference/functions/other-functions.md).

Получает данные из таблиц [Join](../../sql-reference/functions/other-functions.md#creating-a-table) по ключу.

Поддерживаются только таблицы, созданные с `ENGINE = Join(ANY, LEFT, <join_keys>)`.

**Синтаксис**

``` sql
joinGet(join_storage_table_name, `value_column`, join_keys)
```

**Аргументы**

-   `join_storage_table_name` — [идентификатор](../syntax.md#syntax-identifiers), который указывает, откуда производится выборка данных. Поиск по идентификатору осуществляется в базе данных по умолчанию (см. конфигурацию `default_database`). Чтобы переопределить базу данных по умолчанию, используйте команду `USE db_name`, или укажите базу данных и таблицу через разделитель `db_name.db_table`, см. пример.
-   `value_column` — столбец, из которого нужно произвести выборку данных.
-   `join_keys` — список ключей, по которым производится выборка данных.

**Возвращаемое значение**

Возвращает значение по списку ключей.

Если значения не существует в исходной таблице, вернется `0` или `null` в соответствии с настройками [join_use_nulls](../../operations/settings/settings.md#join_use_nulls).

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

## modelEvaluate(model_name, …) {#function-modelevaluate}

Оценивает внешнюю модель.

Принимает на вход имя и аргументы модели. Возвращает Float64.

## throwIf(x\[, custom_message\]) {#throwifx-custom-message}

Бросает исключение, если аргумент не равен нулю.
custom_message - необязательный параметр, константная строка, задает текст сообщения об ошибке.

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

**Аргументы**

-   `length` — длина результирующей строки. Положительное целое число.

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

**Аргументы**

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


## randomFixedString {#randomfixedstring}

Генерирует бинарную строку заданной длины, заполненную случайными байтами, включая нулевые.

**Синтаксис**

``` sql
randomFixedString(length);
```

**Аргументы**

-   `length` — длина строки в байтах. [UInt64](../../sql-reference/data-types/int-uint.md).

**Возвращаемое значение**

-   Строка, заполненная случайными байтами.

Тип: [FixedString](../../sql-reference/data-types/fixedstring.md).

**Пример**

Запрос:

```sql
SELECT randomFixedString(13) as rnd, toTypeName(rnd)
```

Результат:

```text
┌─rnd──────┬─toTypeName(randomFixedString(13))─┐
│ j▒h㋖HɨZ'▒ │ FixedString(13)                 │
└──────────┴───────────────────────────────────┘

```

## randomStringUTF8 {#randomstringutf8}

Генерирует строку заданной длины со случайными символами в кодировке UTF-8.

**Синтаксис**

``` sql
randomStringUTF8(length)
```

**Аргументы**

-   `length` — длина итоговой строки в кодовых точках. [UInt64](../../sql-reference/data-types/int-uint.md).

**Возвращаемое значение**

-   Случайная строка в кодировке UTF-8.

Тип: [String](../../sql-reference/data-types/string.md).

**Пример**

Запрос:

```sql
SELECT randomStringUTF8(13)
```

Результат:

```text
┌─randomStringUTF8(13)─┐
│ 𘤗𙉝д兠庇󡅴󱱎󦐪􂕌𔊹𓰛   │
└──────────────────────┘

```

## getSetting {#getSetting}

Возвращает текущее значение [пользовательской настройки](../../operations/settings/overview.md#custom_settings).

**Синтаксис**

```sql
getSetting('custom_setting')
```

**Параметр**

-   `custom_setting` — название настройки. [String](../../sql-reference/data-types/string.md).

**Возвращаемое значение**

-   Текущее значение пользовательской настройки.

**Пример**

```sql
SET custom_a = 123;
SELECT getSetting('custom_a');
```

**Результат**

```
123
```

**См. также**

-   [Пользовательские настройки](../../operations/settings/overview.md#custom_settings)

## isDecimalOverflow {#is-decimal-overflow}

Проверяет, находится ли число [Decimal](../../sql-reference/data-types/decimal.md) вне собственной (или заданной) области значений.

**Синтаксис**

``` sql
isDecimalOverflow(d, [p])
```

**Аргументы**

-   `d` — число. [Decimal](../../sql-reference/data-types/decimal.md).
-   `p` — точность. Необязательный параметр. Если опущен, используется исходная точность первого аргумента. Использование этого параметра может быть полезно для извлечения данных в другую СУБД или файл. [UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges).

**Возвращаемое значение**

-   `1` — число имеет больше цифр, чем позволяет точность.
-   `0` — число удовлетворяет заданной точности.

**Пример**

Запрос:

``` sql
SELECT isDecimalOverflow(toDecimal32(1000000000, 0), 9),
       isDecimalOverflow(toDecimal32(1000000000, 0)),
       isDecimalOverflow(toDecimal32(-1000000000, 0), 9),
       isDecimalOverflow(toDecimal32(-1000000000, 0));
```

Результат:

``` text
1	1	1	1
```

## countDigits {#count-digits}

Возвращает количество десятичных цифр, необходимых для представления значения.

**Синтаксис**

``` sql
countDigits(x)
```

**Аргументы**

-   `x` — [целое](../../sql-reference/data-types/int-uint.md#uint8-uint16-uint32-uint64-int8-int16-int32-int64) или [дробное](../../sql-reference/data-types/decimal.md) число.

**Возвращаемое значение**

Количество цифр.

Тип: [UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges).

     :::note "Примечание"
    Для `Decimal` значений учитывается их масштаб: вычисляется результат по базовому целочисленному типу, полученному как `(value * scale)`. Например: `countDigits(42) = 2`, `countDigits(42.000) = 5`, `countDigits(0.04200) = 4`. То есть вы можете проверить десятичное переполнение для `Decimal64` с помощью `countDecimal(x) > 18`. Это медленный вариант [isDecimalOverflow](#is-decimal-overflow).
    :::
**Пример**

Запрос:

``` sql
SELECT countDigits(toDecimal32(1, 9)), countDigits(toDecimal32(-1, 9)),
       countDigits(toDecimal64(1, 18)), countDigits(toDecimal64(-1, 18)),
       countDigits(toDecimal128(1, 38)), countDigits(toDecimal128(-1, 38));
```

Результат:

``` text
10	10	19	19	39	39
```

## errorCodeToName {#error-code-to-name}

**Возвращаемое значение**

-   Название переменной для кода ошибки.

Тип: [LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md).

**Синтаксис**

``` sql
errorCodeToName(1)
```

Результат:

``` text
UNSUPPORTED_METHOD
```

## tcpPort {#tcpPort}

Вовращает номер TCP порта, который использует сервер для [нативного протокола](../../interfaces/tcp.md).

**Синтаксис**

``` sql
tcpPort()
```

**Аргументы**

-   Нет.

**Возвращаемое значение**

-   Номер TCP порта.

Тип: [UInt16](../../sql-reference/data-types/int-uint.md).

**Пример**

Запрос:

``` sql
SELECT tcpPort();
```

Результат:

``` text
┌─tcpPort()─┐
│      9000 │
└───────────┘
```

**Смотрите также**

-   [tcp_port](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-tcp_port)

## currentProfiles {#current-profiles}

Возвращает список [профилей настроек](../../operations/access-rights.md#settings-profiles-management) для текущего пользователя.

Для изменения текущего профиля настроек может быть использована команда SET PROFILE. Если команда `SET PROFILE` не применялась, функция возвращает профили, указанные при определении текущего пользователя (см. [CREATE USER](../../sql-reference/statements/create/user.md#create-user-statement)).

**Синтаксис**

``` sql
currentProfiles()
```

**Возвращаемое значение**

-   Список профилей настроек для текущего пользователя.

Тип: [Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md)).

## enabledProfiles {#enabled-profiles}

Возвращает профили настроек, назначенные пользователю как явно, так и неявно. Явно назначенные профили — это те же профили, которые возвращает функция [currentProfiles](#current-profiles). Неявно назначенные профили включают родительские профили других назначенных профилей; профили, назначенные с помощью предоставленных ролей; профили, назначенные с помощью собственных настроек; основной профиль по умолчанию (см. секцию `default_profile` в основном конфигурационном файле сервера).

**Синтаксис**

``` sql
enabledProfiles()
```

**Возвращаемое значение**

-   Список доступных профилей для текущего пользователя.

Тип: [Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md)).

## defaultProfiles {#default-profiles}

Возвращает все профили, указанные при объявлении текущего пользователя (см. [CREATE USER](../../sql-reference/statements/create/user.md#create-user-statement))

**Синтаксис**

``` sql
defaultProfiles()
```

**Возвращаемое значение**

-   Список профилей по умолчанию.

Тип: [Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md)).

## currentRoles {#current-roles}

Возвращает список текущих ролей для текущего пользователя. Список ролей пользователя можно изменить с помощью выражения [SET ROLE](../../sql-reference/statements/set-role.md#set-role-statement). Если выражение `SET ROLE` не использовалось, данная функция возвращает тот же результат, что и функция [defaultRoles](#default-roles).

**Синтаксис**

``` sql
currentRoles()
```

**Возвращаемое значение**

-   Список текущих ролей для текущего пользователя. 

Тип: [Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md)).

## enabledRoles {#enabled-roles}

Возвращает имена текущих ролей, а также ролей, которые разрешено использовать текущему пользователю путем назначения привилегий.

**Синтаксис**

``` sql
enabledRoles()
```

**Возвращаемое значение**

-   Список доступных ролей для текущего пользователя. 

Тип: [Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md)).

## defaultRoles {#default-roles}

Возвращает имена ролей, которые задаются по умолчанию для текущего пользователя при входе в систему. Изначально это все роли, которые разрешено использовать текущему пользователю (см. [GRANT](../../sql-reference/statements/grant/#grant-select)). Список ролей по умолчанию может быть изменен с помощью выражения [SET DEFAULT ROLE](../../sql-reference/statements/set-role.md#set-default-role-statement). 

**Синтаксис**

``` sql
defaultRoles()
```

**Возвращаемое значение**

-   Список ролей по умолчанию. 

Тип: [Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md)).

## getServerPort {#getserverport}

Возвращает номер порта сервера. Если порт не используется сервером, генерируется исключение.

**Синтаксис**

``` sql
getServerPort(port_name)
```

**Аргументы**

-   `port_name` — имя порта сервера. [String](../../sql-reference/data-types/string.md#string). Возможные значения:

    -   'tcp_port'
    -   'tcp_port_secure'
    -   'http_port'
    -   'https_port'
    -   'interserver_http_port'
    -   'interserver_https_port'
    -   'mysql_port'
    -   'postgresql_port'
    -   'grpc_port'
    -   'prometheus.port'

**Возвращаемое значение**

-   Номер порта сервера.

Тип: [UInt16](../../sql-reference/data-types/int-uint.md).

**Пример**

Запрос:

``` sql
SELECT getServerPort('tcp_port');
```

Результат:

``` text
┌─getServerPort('tcp_port')─┐
│ 9000                      │
└───────────────────────────┘
```

## queryID {#query-id}

Возвращает идентификатор текущего запроса. Другие параметры запроса могут быть извлечены из системной таблицы [system.query_log](../../operations/system-tables/query_log.md) через `query_id`.

В отличие от [initialQueryID](#initial-query-id), функция `queryID` может возвращать различные значения для разных шардов (см. пример).

**Синтаксис**

``` sql
queryID()
```

**Возвращаемое значение**

-   Идентификатор текущего запроса.

Тип: [String](../../sql-reference/data-types/string.md)

**Пример**

Запрос:

``` sql
CREATE TABLE tmp (str String) ENGINE = Log;
INSERT INTO tmp (*) VALUES ('a');
SELECT count(DISTINCT t) FROM (SELECT queryID() AS t FROM remote('127.0.0.{1..3}', currentDatabase(), 'tmp') GROUP BY queryID());
```

Результат:

``` text
┌─count()─┐
│ 3       │
└─────────┘
```

## initialQueryID {#initial-query-id}

Возвращает идентификатор родительского запроса. Другие параметры запроса могут быть извлечены из системной таблицы [system.query_log](../../operations/system-tables/query_log.md) через `initial_query_id`.

В отличие от [queryID](#query-id), функция `initialQueryID` возвращает одинаковые значения для разных шардов (см. пример).

**Синтаксис**

``` sql
initialQueryID()
```

**Возвращаемое значение**

-   Идентификатор родительского запроса.

Тип: [String](../../sql-reference/data-types/string.md)

**Пример**

Запрос:

``` sql
CREATE TABLE tmp (str String) ENGINE = Log;
INSERT INTO tmp (*) VALUES ('a');
SELECT count(DISTINCT t) FROM (SELECT initialQueryID() AS t FROM remote('127.0.0.{1..3}', currentDatabase(), 'tmp') GROUP BY queryID());
```

Результат:

``` text
┌─count()─┐
│ 1       │
└─────────┘
```

## shardNum {#shard-num}

Возвращает индекс шарда, который обрабатывает часть данных распределенного запроса. Индексы начинаются с `1`.
Если запрос не распределенный, то возвращается значение `0`.

**Синтаксис**

``` sql
shardNum()
```

**Возвращаемое значение**

-   индекс шарда или константа `0`.

Тип: [UInt32](../../sql-reference/data-types/int-uint.md).

**Пример**

В примере ниже используется конфигурация с двумя шардами. На каждом шарде выполняется запрос к таблице [system.one](../../operations/system-tables/one.md).

Запрос:

``` sql
CREATE TABLE shard_num_example (dummy UInt8) 
    ENGINE=Distributed(test_cluster_two_shards_localhost, system, one, dummy);
SELECT dummy, shardNum(), shardCount() FROM shard_num_example;
```

Результат:

``` text
┌─dummy─┬─shardNum()─┬─shardCount()─┐
│     0 │          2 │            2 │
│     0 │          1 │            2 │
└───────┴────────────┴──────────────┘
```

**См. также**

-   Табличный движок [Distributed](../../engines/table-engines/special/distributed.md)

## shardCount {#shard-count}

Возвращает общее количество шардов для распределенного запроса.
Если запрос не распределенный, то возвращается значение `0`.

**Синтаксис**

``` sql
shardCount()
```

**Возвращаемое значение**

-   Общее количество шардов или `0`.

Тип: [UInt32](../../sql-reference/data-types/int-uint.md).

**См. также**

- Пример использования функции [shardNum()](#shard-num) также содержит вызов `shardCount()`.

## getOSKernelVersion {#getoskernelversion}

Возвращает строку с текущей версией ядра ОС.

**Синтаксис**

``` sql
getOSKernelVersion()
```

**Аргументы**

-   Нет.

**Возвращаемое значение**

-   Текущая версия ядра ОС.

Тип: [String](../../sql-reference/data-types/string.md).

**Пример**

Запрос:

``` sql
SELECT getOSKernelVersion();
```

Результат:

``` text
┌─getOSKernelVersion()────┐
│ Linux 4.15.0-55-generic │
└─────────────────────────┘
```

## zookeeperSessionUptime {#zookeepersessionuptime}

Возвращает аптайм текущего сеанса ZooKeeper в секундах.

**Синтаксис**

``` sql
zookeeperSessionUptime()
```

**Аргументы**

-   Нет.

**Возвращаемое значение**

-   Аптайм текущего сеанса ZooKeeper в секундах.

Тип: [UInt32](../../sql-reference/data-types/int-uint.md).

**Пример**

Запрос:

``` sql
SELECT zookeeperSessionUptime();
```

Результат:

``` text
┌─zookeeperSessionUptime()─┐
│                      286 │
└──────────────────────────┘
```
