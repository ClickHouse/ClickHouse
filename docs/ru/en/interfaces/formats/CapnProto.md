---
alias: []
description: 'Документация по формату CapnProto'
input_format: true
keywords: ['CapnProto']
output_format: true
slug: /interfaces/formats/CapnProto
title: 'CapnProto'
doc_type: 'справочник'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

| Ввод | Вывод | Псевдоним |
| ---- | ----- | --------- |
| ✔    | ✔     |           |

<div id="description">
  ## Описание
</div>

Формат `CapnProto` — это бинарный формат сообщений, похожий на [`Protocol Buffers`](https://developers.google.com/protocol-buffers/) и [Thrift](https://en.wikipedia.org/wiki/Apache_Thrift), но не на [JSON](./JSON/JSON.md) или [MessagePack](https://msgpack.org/).
Сообщения CapnProto строго типизированы и не являются самоописывающимися, то есть требуют внешнего описания схемы. Схема применяется на лету и кэшируется для каждого запроса.

См. также [Format Schema](/ru/interfaces/formats/#formatschema).

<div id="data_types-matching-capnproto">
  ## Соответствие типов данных
</div>

В таблице ниже показаны поддерживаемые типы данных и их соответствие [типам данных](/ru/sql-reference/data-types/index.md) ClickHouse в запросах `INSERT` и `SELECT`.

| Тип данных CapnProto (`INSERT`)                      | Тип данных ClickHouse                                                                                                                                  | Тип данных CapnProto (`SELECT`)                      |
| ---------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------- |
| `UINT8`, `BOOL`                                      | [UInt8](/ru/sql-reference/data-types/int-uint.md)                                                                                                         | `UINT8`                                              |
| `INT8`                                               | [Int8](/ru/sql-reference/data-types/int-uint.md)                                                                                                          | `INT8`                                               |
| `UINT16`                                             | [UInt16](/ru/sql-reference/data-types/int-uint.md), [Date](/ru/sql-reference/data-types/date.md)                                                             | `UINT16`                                             |
| `INT16`                                              | [Int16](/ru/sql-reference/data-types/int-uint.md)                                                                                                         | `INT16`                                              |
| `UINT32`                                             | [UInt32](/ru/sql-reference/data-types/int-uint.md), [DateTime](/ru/sql-reference/data-types/datetime.md)                                                     | `UINT32`                                             |
| `INT32`                                              | [Int32](/ru/sql-reference/data-types/int-uint.md), [Decimal32](/ru/sql-reference/data-types/decimal.md)                                                      | `INT32`                                              |
| `UINT64`                                             | [UInt64](/ru/sql-reference/data-types/int-uint.md)                                                                                                        | `UINT64`                                             |
| `INT64`                                              | [Int64](/ru/sql-reference/data-types/int-uint.md), [DateTime64](/ru/sql-reference/data-types/datetime.md), [Decimal64](/ru/sql-reference/data-types/decimal.md) | `INT64`                                              |
| `FLOAT32`                                            | [Float32](/ru/sql-reference/data-types/float.md)                                                                                                          | `FLOAT32`                                            |
| `FLOAT64`                                            | [Float64](/ru/sql-reference/data-types/float.md)                                                                                                          | `FLOAT64`                                            |
| `TEXT, DATA`                                         | [String](/ru/sql-reference/data-types/string.md), [FixedString](/ru/sql-reference/data-types/fixedstring.md)                                                 | `TEXT, DATA`                                         |
| `union(T, Void), union(Void, T)`                     | [Nullable(T)](/ru/sql-reference/data-types/date.md)                                                                                                       | `union(T, Void), union(Void, T)`                     |
| `ENUM`                                               | [Enum(8/16)](/ru/sql-reference/data-types/enum.md)                                                                                                        | `ENUM`                                               |
| `LIST`                                               | [Array](/ru/sql-reference/data-types/array.md)                                                                                                            | `LIST`                                               |
| `STRUCT`                                             | [Tuple](/ru/sql-reference/data-types/tuple.md)                                                                                                            | `STRUCT`                                             |
| `UINT32`                                             | [IPv4](/ru/sql-reference/data-types/ipv4.md)                                                                                                              | `UINT32`                                             |
| `DATA`                                               | [IPv6](/ru/sql-reference/data-types/ipv6.md)                                                                                                              | `DATA`                                               |
| `DATA`                                               | [Int128/UInt128/Int256/UInt256](/ru/sql-reference/data-types/int-uint.md)                                                                                 | `DATA`                                               |
| `DATA`                                               | [Decimal128/Decimal256](/ru/sql-reference/data-types/decimal.md)                                                                                          | `DATA`                                               |
| `STRUCT(entries LIST(STRUCT(key Key, value Value)))` | [Map](/ru/sql-reference/data-types/map.md)                                                                                                                | `STRUCT(entries LIST(STRUCT(key Key, value Value)))` |

* Целочисленные типы можно преобразовывать друг в друга при вводе и выводе.
* Для работы с `Enum` в формате CapnProto используйте настройку [format&#95;capn&#95;proto&#95;enum&#95;comparising&#95;mode](/ru/operations/settings/settings-formats.md/#format_capn_proto_enum_comparising_mode).
* Массивы могут быть вложенными и могут принимать в качестве аргумента значение типа `Nullable`. Типы `Tuple` и `Map` также могут быть вложенными.

<div id="example-usage">
  ## Пример использования
</div>

<div id="inserting-and-selecting-data-capnproto">
  ### Вставка и выборка данных
</div>

Вы можете вставить данные CapnProto из файла в таблицу ClickHouse с помощью следующей команды:

```bash
$ cat capnproto_messages.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_schema = 'schema:Message' FORMAT CapnProto"
```

При этом `schema.capnp` выглядит следующим образом:

```capnp
struct Message {
  SearchPhrase @0 :Text;
  c @1 :Uint64;
}
```

Вы можете выбрать данные из таблицы ClickHouse и сохранить их в файл в формате `CapnProto` с помощью следующей команды:

```bash
$ clickhouse-client --query = "SELECT * FROM test.hits FORMAT CapnProto SETTINGS format_schema = 'schema:Message'"
```

<div id="using-autogenerated-capn-proto-schema">
  ### Использование автоматически сгенерированной схемы
</div>

Если у вас нет внешней схемы `CapnProto` для ваших данных, вы всё равно можете выводить и вводить данные в формате `CapnProto` с помощью автоматически сгенерированной схемы.

Например:

```sql
SELECT * FROM test.hits 
FORMAT CapnProto 
SETTINGS format_capn_proto_use_autogenerated_schema=1
```

В этом случае ClickHouse автоматически сгенерирует схему CapnProto в соответствии со структурой таблицы с помощью функции [structureToCapnProtoSchema](/ru/sql-reference/functions/other-functions.md#structureToCapnProtoSchema) и будет использовать эту схему для сериализации данных в формате CapnProto.

Вы также можете читать файл CapnProto с автоматически сгенерированной схемой (в этом случае файл должен быть создан по той же схеме):

```bash
$ cat hits.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_capn_proto_use_autogenerated_schema=1 FORMAT CapnProto"
```

<div id="format-settings">
  ## Настройки формата
</div>

Параметр [`format_capn_proto_use_autogenerated_schema`](../../operations/settings/settings-formats.md/#format_capn_proto_use_autogenerated_schema) включен по умолчанию и применяется, если [`format_schema`](/ru/interfaces/formats#formatschema) не задан.

Автоматически сгенерированную схему также можно сохранить в файл при вводе/выводе с помощью параметра [`output_format_schema`](/ru/operations/settings/formats#output_format_schema).

Например:

```sql
SELECT * FROM test.hits 
FORMAT CapnProto 
SETTINGS 
    format_capn_proto_use_autogenerated_schema=1,
    output_format_schema='path/to/schema/schema.capnp'
```

В этом случае автоматически сгенерированная схема `CapnProto` будет сохранена в файле `path/to/schema/schema.capnp`.