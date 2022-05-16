---
sidebar_position: 53
sidebar_label: "Функции для работы с UUID"
---

# Функции для работы с UUID {#funktsii-dlia-raboty-s-uuid}

## generateUUIDv4 {#uuid-function-generate}

Генерирует идентификатор [UUID версии 4](https://tools.ietf.org/html/rfc4122#section-4.4).

``` sql
generateUUIDv4()
```

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
