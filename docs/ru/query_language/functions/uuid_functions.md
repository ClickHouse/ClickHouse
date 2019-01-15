# Функции для работы с UUID

Ниже перечислены функции ClickHouse, которые предназначены для работы с UUID.

## generateUUIDv4 {#uuid_function-generate}

Генерирует [UUID](../../data_types/uuid.md) [версии](https://tools.ietf.org/html/rfc4122#section-4.4).

```sql
generateUUIDv4()
```

**Возвращаемое значение**

UUID

**Пример использование**

Пример ниже демонстрирует создание таблицы с UUID-колонкой и добавление в эту колонку записи.

``` sql
:) CREATE TABLE t_uuid (x UUID) ENGINE=TinyLog

:) INSERT INTO t_uuid SELECT generateUUIDv4()

:) SELECT * FROM t_uuid

┌────────────────────────────────────x─┐
│ f4bf890f-f9dc-4332-ad5c-0c18e73f28e9 │
└──────────────────────────────────────┘
```

## toUUID (x)	

Преобразует данные с типом String в UUID. 

```sql
toUUID(String)
```

**Возвращаемое значение**

UUID

**Пример использование**

``` sql
:) SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') AS uuid

┌─────────────────────────────────uuid─┐
│ 61f0c404-5cb3-11e7-907b-a6006ad3dba0 │
└──────────────────────────────────────┘
```

## UUIDStringToNum

Преобразует строку, состоящую из 36 символов (в формате `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`) в последовательность байт в [FixedString(16)](../../data_types/fixedstring.md).

``` sql
UUIDStringToNum(String)
```

**Возвращаемое значение**

FixedString(16)

**Usage examples**

``` sql
:) SELECT 
    '612f3c40-5d3b-217e-707b-6a546a3d7b29' AS uuid, 
    UUIDStringToNum(uuid) AS bytes

┌─uuid─────────────────────────────────┬─bytes────────────┐
│ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │ a/<@];!~p{jTj={) │
└──────────────────────────────────────┴──────────────────┘
```

## UUIDNumToString

Преобразует значение типа [FixedString(16)](../../data_types/fixedstring.md) в строку из 36 символов в текстовом виде.

``` sql
UUIDNumToString(FixedString(16))
```

**Returned value**

String

**Usage example**

``` sql
SELECT 
    'a/<@];!~p{jTj={)' AS bytes, 
    UUIDNumToString(toFixedString(bytes, 16)) AS uuid

┌─bytes────────────┬─uuid─────────────────────────────────┐
│ a/<@];!~p{jTj={) │ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │
└──────────────────┴──────────────────────────────────────┘
```

## См. также

- [dictGetUUID](ext_dict_functions.md)
- [dictGetUUIDOrDefault](ext_dict_functions#ext_dict_functions_dictGetTOrDefault)

[Original article](https://clickhouse.yandex/docs/en/query_language/functions/uuid_function/) <!--hide-->
