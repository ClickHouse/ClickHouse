---
toc_priority: 53
toc_title: "\u0424\u0443\u043d\u043a\u0446\u0438\u0438\u0020\u0434\u043b\u044f\u0020\u0440\u0430\u0431\u043e\u0442\u044b\u0020\u0441\u0020\u0055\u0055\u0049\u0044"
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

## См. также: {#sm-takzhe}

-   [dictGetUUID](ext-dict-functions.md)
-   [dictGetUUIDOrDefault](ext-dict-functions.md)

[Original article](https://clickhouse.tech/docs/en/query_language/functions/uuid_function/) <!--hide-->
