# Функции для работы с UUID

## generateUUIDv4 {#uuid_function-generate}

Генерирует идентификатор [UUID версии 4](https://tools.ietf.org/html/rfc4122#section-4.4).

```sql
generateUUIDv4()
```

**Возвращаемое значение**

Значение типа [UUID](../../data_types/uuid.md).

**Пример использования**

Этот пример демонстрирует, как создать таблицу с UUID-колонкой и добавить в нее сгенерированный UUID.

```sql
CREATE TABLE t_uuid (x UUID) ENGINE=TinyLog

INSERT INTO t_uuid SELECT generateUUIDv4()

SELECT * FROM t_uuid
```

```text
┌────────────────────────────────────x─┐
│ f4bf890f-f9dc-4332-ad5c-0c18e73f28e9 │
└──────────────────────────────────────┘
```

## toUUID (x)

Преобразует значение типа String в тип UUID.

```sql
toUUID(String)
```

**Возвращаемое значение**

Значение типа UUID.

**Пример использования**

```sql
SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') AS uuid
```

```text
┌─────────────────────────────────uuid─┐
│ 61f0c404-5cb3-11e7-907b-a6006ad3dba0 │
└──────────────────────────────────────┘
```

## UUIDStringToNum

Принимает строку, содержащую 36 символов в формате `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`, и возвращает в виде набора байт в [FixedString(16)](../../data_types/fixedstring.md).

```sql
UUIDStringToNum(String)
```

**Возвращаемое значение**

FixedString(16)

**Пример использования**

```sql
SELECT 
    '612f3c40-5d3b-217e-707b-6a546a3d7b29' AS uuid, 
    UUIDStringToNum(uuid) AS bytes
```

```text
┌─uuid─────────────────────────────────┬─bytes────────────┐
│ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │ a/<@];!~p{jTj={) │
└──────────────────────────────────────┴──────────────────┘
```

## UUIDNumToString

Принимает значение типа [FixedString(16)](../../data_types/fixedstring.md). Возвращает строку из 36 символов в текстовом виде.

```sql
UUIDNumToString(FixedString(16))
```

**Возвращаемое значение**

Значение типа String.

**Пример использования**

```sql
SELECT 
    'a/<@];!~p{jTj={)' AS bytes, 
    UUIDNumToString(toFixedString(bytes, 16)) AS uuid
```

```text
┌─bytes────────────┬─uuid─────────────────────────────────┐
│ a/<@];!~p{jTj={) │ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │
└──────────────────┴──────────────────────────────────────┘
```

## См. также:

- [dictGetUUID](ext_dict_functions.md)
- [dictGetUUIDOrDefault](ext_dict_functions.md)

[Original article](https://clickhouse.yandex/docs/en/query_language/functions/uuid_function/) <!--hide-->

