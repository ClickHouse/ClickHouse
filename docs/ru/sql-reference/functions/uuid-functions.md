---
slug: /ru/sql-reference/functions/uuid-functions
sidebar_position: 53
sidebar_label: "Функции для работы с UUID"
---

# Функции для работы с UUID {#funktsii-dlia-raboty-s-uuid}

## generateUUIDv4 {#uuid-function-generate}

Генерирует идентификатор [UUID версии 4](https://tools.ietf.org/html/rfc4122#section-4.4).

**Синтаксис**

``` sql
generateUUIDv4([x])
```

**Аргументы**

-   `x` — [выражение](../syntax.md#syntax-expressions), возвращающее значение одного из [поддерживаемых типов данных](../data-types/index.md#data_types). Значение используется, чтобы избежать [склейки одинаковых выражений](index.md#common-subexpression-elimination), если функция вызывается несколько раз в одном запросе. Необязательный параметр.
   
**Возвращаемое значение**

Значение типа [UUID](../../sql-reference/functions/uuid-functions.md).

**Пример использования**

Этот пример демонстрирует, как создать таблицу с UUID-колонкой и добавить в нее сгенерированный UUID.

``` sql
CREATE TABLE t_uuid (x UUID) ENGINE=TinyLog

INSERT INTO t_uuid SELECT generateUUIDv4()

SELECT * FROM t_uuid
```

``` text
┌────────────────────────────────────x─┐
│ f4bf890f-f9dc-4332-ad5c-0c18e73f28e9 │
└──────────────────────────────────────┘
```

**Пример использования, для генерации нескольких значений в одной строке**

```sql
SELECT generateUUIDv4(1), generateUUIDv4(2)
┌─generateUUIDv4(1)────────────────────┬─generateUUIDv4(2)────────────────────┐
│ 2d49dc6e-ddce-4cd0-afb8-790956df54c1 │ 8abf8c13-7dea-4fdf-af3e-0e18767770e6 │
└──────────────────────────────────────┴──────────────────────────────────────┘
```

## generateUUIDv7 {#uuidv7-function-generate}

Генерирует идентификатор [UUID версии 7](https://datatracker.ietf.org/doc/html/draft-peabody-dispatch-new-uuid-format-04). Генерируемый UUID состоит из 48-битной временной метки (Unix time в миллисекундах), маркеров версии 7 и варианта 2, монотонно возрастающего счётчика для данной временной метки и случайных данных в указанной ниже последовательности. Для каждой новой временной метки счётчик стартует с нового случайного значения, а для следующих UUIDv7 он увеличивается на единицу. В случае переполнения счётчика временная метка принудительно увеличивается на 1, и счётчик снова стартует со случайного значения. Монотонность возрастания счётчика для каждой временной метки гарантируется между всеми одновременно работающими функциями `generateUUIDv7`.
```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|                           unix_ts_ms                          |
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|          unix_ts_ms           |  ver  |   counter_high_bits   |
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|var|                   counter_low_bits                        |
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|                            rand_b                             |
└─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┘
```
::::note
На апрель 2024 года UUIDv7 находится в статусе черновика и его раскладка по битам может в итоге измениться.
::::

**Синтаксис**

``` sql
generateUUIDv7([x])
```

**Аргументы**

-   `x` — [выражение](../syntax.md#syntax-expressions), возвращающее значение одного из [поддерживаемых типов данных](../data-types/index.md#data_types). Значение используется, чтобы избежать [склейки одинаковых выражений](index.md#common-subexpression-elimination), если функция вызывается несколько раз в одном запросе. Необязательный параметр.
   
**Возвращаемое значение**

Значение типа [UUID](../../sql-reference/functions/uuid-functions.md).

**Пример использования**

Этот пример демонстрирует, как создать таблицу с UUID-колонкой и добавить в нее сгенерированный UUIDv7.

``` sql
CREATE TABLE t_uuid (x UUID) ENGINE=TinyLog

INSERT INTO t_uuid SELECT generateUUIDv7WithCounter()

SELECT * FROM t_uuid
```

``` text
┌────────────────────────────────────x─┐
│ 018f05c7-56e3-7ac3-93e9-1d93c4218e0e │
└──────────────────────────────────────┘
```

**Пример использования, для генерации нескольких значений в одной строке**

```sql
SELECT generateUUIDv7(1), generateUUIDv7(2)
┌─generateUUIDv7(1)────────────────────┬─generateUUIDv7(2)────────────────────┐
│ 018f05c9-4ab8-7b86-b64e-c9f03fbd45d1 │ 018f05c9-4ab8-7b86-b64e-c9f12efb7e16 │
└──────────────────────────────────────┴──────────────────────────────────────┘
```

## empty {#empty}

Проверяет, является ли входной UUID пустым.

**Синтаксис**

```sql
empty(UUID)
```

UUID считается пустым, если он содержит все нули (нулевой UUID).

Функция также поддерживает работу с типами [Array](array-functions.md#function-empty) и [String](string-functions.md#empty).

**Параметры**

-   `x` — UUID на входе функции. [UUID](../data-types/uuid.md).

**Возвращаемое значение**

-   Возвращает `1` для пустого UUID или `0` — для непустого UUID.

Тип: [UInt8](../data-types/int-uint.md).

**Пример**

Для генерации UUID-значений предназначена функция [generateUUIDv4](#uuid-function-generate).

Запрос:

```sql
SELECT empty(generateUUIDv4());
```

Ответ:

```text
┌─empty(generateUUIDv4())─┐
│                       0 │
└─────────────────────────┘
```

## notEmpty {#notempty}

Проверяет, является ли входной UUID непустым.

**Синтаксис**

```sql
notEmpty(UUID)
```

UUID считается пустым, если он содержит все нули (нулевой UUID).

Функция также поддерживает работу с типами [Array](array-functions.md#function-notempty) и [String](string-functions.md#function-notempty).

**Параметры**

-   `x` — UUID на входе функции. [UUID](../data-types/uuid.md).

**Возвращаемое значение**

-   Возвращает `1` для непустого UUID или `0` — для пустого UUID.

Тип: [UInt8](../data-types/int-uint.md).

**Пример**

Для генерации UUID-значений предназначена функция [generateUUIDv4](#uuid-function-generate).

Запрос:

```sql
SELECT notEmpty(generateUUIDv4());
```

Результат:

```text
┌─notEmpty(generateUUIDv4())─┐
│                          1 │
└────────────────────────────┘
```

## toUUID (x) {#touuid-x}

Преобразует значение типа String в тип UUID.

``` sql
toUUID(String)
```

**Возвращаемое значение**

Значение типа UUID.

**Пример использования**

``` sql
SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') AS uuid
```

``` text
┌─────────────────────────────────uuid─┐
│ 61f0c404-5cb3-11e7-907b-a6006ad3dba0 │
└──────────────────────────────────────┘
```

## toUUIDOrNull (x) {#touuidornull-x}

Принимает строку, и пытается преобразовать в тип UUID. При неудаче возвращает NULL.

``` sql
toUUIDOrNull(String)
```

**Возвращаемое значение**

Значение типа Nullable(UUID).

**Пример использования**

``` sql
SELECT toUUIDOrNull('61f0c404-5cb3-11e7-907b-a6006ad3dba0T') AS uuid
```

``` text
┌─uuid─┐
│ ᴺᵁᴸᴸ │
└──────┘
```

## toUUIDOrZero (x) {#touuidorzero-x}

Принимает строку, и пытается преобразовать в тип UUID. При неудаче возвращает нулевой UUID.

``` sql
toUUIDOrZero(String)
```

**Возвращаемое значение**

Значение типа UUID.

**Пример использования**

``` sql
SELECT toUUIDOrZero('61f0c404-5cb3-11e7-907b-a6006ad3dba0T') AS uuid
```

``` text
┌─────────────────────────────────uuid─┐
│ 00000000-0000-0000-0000-000000000000 │
└──────────────────────────────────────┘
```

## UUIDStringToNum {#uuidstringtonum}

Принимает строку, содержащую 36 символов в формате `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`, и возвращает в виде набора байт в [FixedString(16)](../../sql-reference/functions/uuid-functions.md).

``` sql
UUIDStringToNum(String)
```

**Возвращаемое значение**

FixedString(16)

**Пример использования**

``` sql
SELECT
    '612f3c40-5d3b-217e-707b-6a546a3d7b29' AS uuid,
    UUIDStringToNum(uuid) AS bytes
```

``` text
┌─uuid─────────────────────────────────┬─bytes────────────┐
│ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │ a/<@];!~p{jTj={) │
└──────────────────────────────────────┴──────────────────┘
```

## UUIDNumToString {#uuidnumtostring}

Принимает значение типа [FixedString(16)](../../sql-reference/functions/uuid-functions.md). Возвращает строку из 36 символов в текстовом виде.

``` sql
UUIDNumToString(FixedString(16))
```

**Возвращаемое значение**

Значение типа String.

**Пример использования**

``` sql
SELECT
    'a/<@];!~p{jTj={)' AS bytes,
    UUIDNumToString(toFixedString(bytes, 16)) AS uuid
```

``` text
┌─bytes────────────┬─uuid─────────────────────────────────┐
│ a/<@];!~p{jTj={) │ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │
└──────────────────┴──────────────────────────────────────┘
```

## UUIDToNum {#uuidtonum}

Принимает UUID и возвращает в виде набора байт в [FixedString(16)](../../sql-reference/functions/uuid-functions.md). Также принимает необязательный второй параметр - вариант представления UUID, по умолчанию 1 - `Big-endian` (2 означает представление в формате `Microsoft`). Данная функция заменяет последовательность из двух отдельных функций `UUIDStringToNum(toString(uuid))`, так что промежуточная конвертация из UUID в String для извлечения набора байт из UUID не требуется.

``` sql
UUIDToNum(UUID[, variant = 1])
```

**Возвращаемое значение**

FixedString(16)

**Примеры использования**

``` sql
SELECT
    toUUID('612f3c40-5d3b-217e-707b-6a546a3d7b29') AS uuid,
    UUIDToNum(uuid) AS bytes
```

``` text
┌─uuid─────────────────────────────────┬─bytes────────────┐
│ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │ a/<@];!~p{jTj={) │
└──────────────────────────────────────┴──────────────────┘
```
``` sql
SELECT
    toUUID('612f3c40-5d3b-217e-707b-6a546a3d7b29') AS uuid,
    UUIDToNum(uuid, 2) AS bytes
```

```text
┌─uuid─────────────────────────────────┬─bytes────────────┐
│ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │ @</a;]~!p{jTj={) │
└──────────────────────────────────────┴──────────────────┘
```

## UUIDv7ToDateTime {#uuidv7todatetime}

Принимает UUID версии 7 и извлекает из него временную метку.

``` sql
UUIDv7ToDateTime(uuid[, timezone])
```

**Параметры**

- `uuid` — [UUID](../data-types/uuid.md) версии 7.
- `timezone` — [Часовой пояс](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) для возвращаемого значения (необязательный параметр). [String](../../sql-reference/data-types/string.md).

**Возвращаемое значение**

- Временная метка с миллисекундной точностью (1970-01-01 00:00:00.000 в случае UUID не версии 7).

Type: [DateTime64(3)](/docs/ru/sql-reference/data-types/datetime64.md).

**Примеры использования**

``` sql
SELECT UUIDv7ToDateTime(toUUID('018f05c9-4ab8-7b86-b64e-c9f03fbd45d1'))
```

```text
┌─UUIDv7ToDateTime(toUUID('018f05c9-4ab8-7b86-b64e-c9f03fbd45d1'))─┐
│                                          2024-04-22 15:30:29.048 │
└──────────────────────────────────────────────────────────────────┘
```

``` sql
SELECT UUIDv7ToDateTime(toUUID('018f05c9-4ab8-7b86-b64e-c9f03fbd45d1'), 'America/New_York')
```

```text
┌─UUIDv7ToDateTime(toUUID('018f05c9-4ab8-7b86-b64e-c9f03fbd45d1'), 'America/New_York')─┐
│                                                              2024-04-22 08:30:29.048 │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

## serverUUID() {#server-uuid}

Возвращает случайный и уникальный UUID, который генерируется при первом запуске сервера и сохраняется навсегда. Результат записывается в файл `uuid`, расположенный в каталоге сервера ClickHouse `/var/lib/clickhouse/`.

**Синтаксис**

```sql
serverUUID()
```

**Возвращаемое значение**

-   UUID сервера. 

Тип: [UUID](../data-types/uuid.md).

## См. также: {#sm-takzhe}

-   [dictGetUUID](ext-dict-functions.md)
-   [dictGetUUIDOrDefault](ext-dict-functions.md)
