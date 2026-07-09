---
alias: []
description: 'Документация по формату Native'
input_format: true
keywords: ['Native']
output_format: true
slug: /interfaces/formats/Native
title: 'Native'
doc_type: 'справочник'
---

| Ввод | Вывод | Псевдоним |
| ---- | ----- | --------- |
| ✔    | ✔     |           |

<div id="description">
  ## Описание
</div>

Полная официальная спецификация формата `Native` доступна [здесь](/ru/interfaces/specs/NativeFormat), а сопутствующая спецификация протокола `Native` — TCP-протокола, по которому он передаётся, — [здесь](/ru/interfaces/specs/NativeProtocol).

:::note
Обе спецификации были сгенерированы LLM на основе исходного кода ClickHouse. Код остаётся основным источником истины: если спецификация и код расходятся, верным считается код.
:::

Формат `Native` — самый эффективный формат ClickHouse, поскольку он по-настоящему «столбцовый»:
он не преобразует столбцы в строки.

В этом формате данные записываются и читаются [блоками](/ru/development/architecture#block) в бинарном формате.
Для каждого блока друг за другом записываются количество строк, количество столбцов, имена и типы столбцов, а также части столбцов, входящие в блок.

Этот формат используется в нативном интерфейсе для взаимодействия между серверами, в клиенте командной строки, а также в клиентах C++.

:::tip
Вы можете использовать этот формат, чтобы быстро создавать дампы, которые может читать только ClickHouse.
Работать с этим форматом напрямую может быть непрактично.
:::

<div id="data-types-wire-format">
  ## Формат передачи данных для типов данных
</div>

Данные передаются в столбцовом формате, то есть каждый столбец передаётся отдельно,
а все значения столбца отправляются вместе как единый массив.

Каждый столбец в блоке содержит заголовок, аналогичный [RowBinaryWithNamesAndTypes](../formats/RowBinary/RowBinaryWithNamesAndTypes.md).

:::note
При использовании нативного бинарного протокола TCP (или когда конечная точка HTTP получает `?client_protocol_version=<n>`),
структура `BlockInfo` записывается перед количеством столбцов и строк. В примерах в этом разделе используется
обычный HTTP-интерфейс без указания версии протокола, поэтому `BlockInfo` опускается.
:::

<div id="block-structure">
  ### Структура блока
</div>

Следующий запрос возвращает два столбца, `number` и `str`, и три строки:

```bash
curl -XPOST "http://localhost:8123?default_format=Native" --data-binary "SELECT number, toString(number) AS str FROM system.numbers LIMIT 3" > out.bin
```

Результирующие данные умещаются в один блок ClickHouse и будут выглядеть так:

```js
const data = new Uint8Array([
  // --- Block Header ---
  0x02,                   // 2 columns
  0x03,                   // 3 rows
  // -- Column 1 Header --
  0x06,                   // LEB128 - column name 'number' has 6 bytes
  0x6e, 0x75, 0x6d,       
  0x62, 0x65, 0x72,       // column name: 'number'
  0x06,                   // LEB128 - column type 'UInt64' has 6 bytes
  0x55, 0x49, 0x6e,
  0x74, 0x36, 0x34,       // 'UInt64'
  0x00, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00, // 0 as UInt64
  0x01, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00, // 1 as UInt64
  0x02, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00, // 2 as UInt64
  0x03,                   // LEB128 - column name 'str' has 3 bytes
  0x73, 0x74, 0x72,       // column name: 'str'
  0x06,                   // LEB128 - column type 'String' has 6 bytes
  0x53, 0x74, 0x72, 
  0x69, 0x6e, 0x67,       // 'String'
  0x01,                   // LEB128 - the string has 1 byte
  0x30,                   // '0' as String
  0x01,                   // LEB128 - the string has 1 byte
  0x31,                   // '1' as String
  0x01,                   // LEB128 - the string has 1 byte
  0x32,                   // '2' as String
])
```

<div id="multiple-blocks">
  ### Несколько блоков
</div>

Однако во многих случаях данные не помещаются в один блок, и ClickHouse отправляет их в виде нескольких блоков.
Рассмотрим следующий запрос, который извлекает две строки при уменьшенном размере блока, чтобы принудительно разбить данные по одной строке на блок:

```bash
curl -XPOST "http://localhost:8123?default_format=Native" --data-binary "SELECT number, toString(number) AS str                FROM system.numbers LIMIT 2                 SETTINGS max_block_size=1" \  > out.bin
```

Результат:

```js
const data = new Uint8Array([
 
  // ----- Block 1 ----- 
  0x02,                   // 2 columns
  0x01,                   // 1 row
  0x06,                   // LEB128 - column name 'number' has 6 bytes
  0x6E, 0x75, 0x6D, 
  0x62, 0x65, 0x72,       // column name: 'number' 
  0x06,                   // LEB128 - column type 'UInt64' has 6 bytes
  0x55, 0x49, 0x6E, 
  0x74, 0x36, 0x34,       // 'UInt64' 
  0x00, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00, // 0 as UInt64
  0x03,                   // LEB128 - column name 'str' has 3 bytes
  0x73, 0x74, 0x72,       // column name: 'str'
  0x06,                   // LEB128 - column type 'String' has 6 bytes
  0x53, 0x74, 0x72, 
  0x69, 0x6E, 0x67,       // 'String'
  0x01,                   // LEB128 - the string has 1 byte
  0x30,                   // '0' as String
  
  // ----- Block 2 -----
  0x02,                   // 2 columns
  0x01,                   // 1 row
  0x06,                   // LEB128 - column name 'number' has 6 bytes
  0x6E, 0x75, 0x6D,  
  0x62, 0x65, 0x72,       // column name: 'number'
  0x06,                   // LEB128 - column type 'UInt64' has 6 bytes
  0x55, 0x49, 0x6E,  
  0x74, 0x36, 0x34,       // 'UInt64'
  0x01, 0x00, 0x00, 0x00,  
  0x00, 0x00, 0x00, 0x00, // 1 as UInt64
  0x03,                   // LEB128 - column name 'str' has 3 bytes
  0x73, 0x74, 0x72,       // column name: 'str'
  0x06,                   // LEB128 - column type 'String' has 6 bytes
  0x53, 0x74, 0x72,  
  0x69, 0x6E, 0x67,       // 'String'
  0x01,                   // LEB128 - the string has 1 byte
  0x31,                   // '1' as String
]);
```

<div id="simple-data-types">
  ### Простые типы данных
</div>

Формат передачи данных для отдельного значения одного из простых типов данных аналогичен `RowBinary`/`RowBinaryWithNamesAndTypes`.
Полный список типов, подпадающих под это описание, включает:

* (U)Int8, (U)Int16, (U)Int32, (U)Int64, (U)Int128, (U)Int256
* Float32, Float64
* Bool
* String
* FixedString(N)
* Date
* Date32
* DateTime
* DateTime64
* IPv4
* IPv6
* UUID

Подробнее см. в описаниях перечисленных выше типов в разделе [&quot;Формат передачи данных для типов данных RowBinary&quot;](/ru/interfaces/formats/RowBinary#data-types-wire-format).

<div id="complex-data-types">
  ### Сложные типы данных
</div>

Кодирование следующих типов отличается от `RowBinary` и `RowBinaryWithNamesAndTypes`.

* Nullable
* LowCardinality
* Array
* Map
* Variant
* Dynamic
* JSON

<div id="nullable">
  #### Nullable
</div>

В формате `Native` перед фактическими данными для Nullable-столбца записывается количество байтов, равное числу строк в блоке. Каждый из этих байтов указывает, является ли значение `NULL`. Например, в этом запросе каждое нечётное число будет `NULL`:

```bash
curl -XPOST "http://localhost:8123?default_format=Native" \  --data-binary "SELECT if(number % 2 = 0, number, NULL) :: Nullable(UInt64) AS maybe_null                 FROM system.numbers LIMIT 5" \  > out.bin
```

Вывод будет выглядеть так:

```js
const data = new Uint8Array([
  // --- Block Header ---
  0x01,                         // LEB128 - 1 column
  0x05,                         // LEB128 - 5 rows
  
  // -- Column Header --
  0x0A,                         // LEB128 - column name has 10 bytes
  0x6D, 0x61, 0x79, 0x62, 0x65, 
  0x5F, 0x6E, 0x75, 0x6C, 0x6C, // column name: 'maybe_null'
  
  0x10,                         // LEB128 - column type has 16 bytes
  0x4E, 0x75, 0x6C, 0x6C, 
  0x61, 0x62, 0x6C, 0x65, 
  0x28, 0x55, 0x49, 0x6E, 
  0x74, 0x36, 0x34, 0x29,       // column type: 'Nullable(UInt64)'
  
  // -- Nullable mask --
  0x00,                         // Row 0 is NOT NULL
  0x01,                         // Row 1 is NULL
  0x00,                         // Row 2 is NOT NULL
  0x01,                         // Row 3 is NULL
  0x00,                         // Row 4 is NOT NULL
  
  // -- UInt64 values --
  0x00, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00,       // Row 0: 0 as UInt64

  // even though we still might have a proper value for this number 
  // in the block, it should be still returned as NULL to the user!
  0x01, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00,       // Row #1: NULL
  
  0x02, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00,       // Row #2: 2 as UInt64
  
  0x03, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00,       // Row #3: NULL, similar to Row #1
  
  0x04, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00,       // Row #4: 4 as UInt64
]);
```

С `Nullable(String)` всё работает аналогично. Индикатор null всегда берётся из байта маски nullable —
значение маски `0x01` означает, что строка имеет значение `NULL` независимо от содержимого строки. Для строк со значением `NULL`
базовая строка хранится как пустая строка (длина LEB128 `0`). Обратите внимание, что пустая строка со значением не `NULL`
тоже имеет длину LEB128 `0`, поэтому эти два случая различаются только байтом маски. Например, следующий запрос:

```bash
curl -XPOST "http://localhost:8123?default_format=Native" \  --data-binary "SELECT if(number % 2 = 0, toString(number), NULL) :: Nullable(String) AS maybe_str                 FROM system.numbers LIMIT 5" \  > out.bin
```

Результат будет выглядеть так:

```js
const data = new Uint8Array([
  // --- Block Header ---
  0x01, // LEB128 - 1 column
  0x05, // LEB128 - 5 rows

  // -- Column Header --
  0x09, // LEB128 - column name has 9 bytes
  0x6d,
  0x61,
  0x79,
  0x62,
  0x65,
  0x5f,
  0x73,
  0x74,
  0x72, // column name: 'maybe_str'

  0x10, // LEB128 - column type has 16 bytes
  0x4e,
  0x75,
  0x6c,
  0x6c,
  0x61,
  0x62,
  0x6c,
  0x65,
  0x28,
  0x53,
  0x74,
  0x72,
  0x69,
  0x6e,
  0x67,
  0x29, // column type: 'Nullable(String)'

  // -- Nullable mask --
  0x00, // Row 0 is NOT NULL
  0x01, // Row 1 is NULL
  0x00, // Row 2 is NOT NULL
  0x01, // Row 3 is NULL
  0x00, // Row 4 is NOT NULL

  // -- String values --
  0x01,
  0x30, // Row 0: LEB128 == 1, '0' as String
  0x00, // Row 1: LEB128 == 0, NULL
  0x01,
  0x32, // Row 2: LEB128 == 1, '2' as String
  0x00, // Row 3: LEB128 == 0, NULL
  0x01,
  0x34, // Row 4: LEB128 == 1, '4' as String
])
```

<div id="lowcardinality">
  #### LowCardinality
</div>

В отличие от [RowBinary](RowBinary/RowBinary.md#lowcardinality), где `LowCardinality` обрабатывается прозрачно, формат Native использует словарное столбцовое кодирование. Столбец кодируется как префикс версии, затем словарь уникальных значений и массив целочисленных индексов в этом словаре.

:::note
Столбец можно определить как `LowCardinality(Nullable(T))`, но его нельзя определить как `Nullable(LowCardinality(T))` — это всегда приводит к ошибке сервера.
:::

Префикс версии — это `UInt64(LE)` со значением `1`, который записывается один раз для каждого столбца. Затем для каждого блока записывается следующее:

* `UInt64(LE)` — битовое поле `IndexesSerializationType`. Биты 0–7 кодируют ширину индекса (0 = UInt8, 1 = UInt16, 2 = UInt32, 3 = UInt64). Бит 8 (`NeedGlobalDictionaryBit`) никогда не устанавливается в формате Native (сервер генерирует исключение, если он встречается). Бит 9 указывает на наличие дополнительных ключей словаря. Бит 10 указывает, что словарь нужно сбросить.
* `UInt64(LE)` — количество ключей словаря, после которого сами ключи сериализуются массово с использованием кодирования внутреннего типа.
* `UInt64(LE)` — количество строк, после которого значения индексов сериализуются массово с использованием UInt соответствующей ширины.

Словарь всегда содержит значение по умолчанию по индексу 0 (например, пустую строку для `String`, 0 для числовых типов). Для `LowCardinality(Nullable(T))` индекс 0 соответствует `NULL`, а ключи сериализуются без обёртки `Nullable`.

Например, `LowCardinality(String)` с 5 строками `['foo', 'bar', 'baz', 'foo', 'bar']`:

```text
// Version prefix
01 00 00 00 00 00 00 00    // UInt64(LE) = 1

// IndexesSerializationType: UInt8 indexes, has keys, update dictionary
00 06 00 00 00 00 00 00    // UInt64(LE) = 0x0600

04 00 00 00 00 00 00 00    // 4 dictionary keys
00                          // key 0: "" (default)
03 66 6f 6f                 // key 1: "foo"
03 62 61 72                 // key 2: "bar"
03 62 61 7a                 // key 3: "baz"

05 00 00 00 00 00 00 00    // 5 rows
01 02 03 01 02              // indexes → "foo", "bar", "baz", "foo", "bar"
```

Для `LowCardinality(Nullable(String))` индекс 0 — это `NULL`:

```text
01 00 00 00 00 00 00 00    // version
00 06 00 00 00 00 00 00    // IndexesSerializationType
03 00 00 00 00 00 00 00    // 3 keys
00                          // key 0: NULL
00                          // key 1: "" (default)
03 79 65 73                 // key 2: "yes"
05 00 00 00 00 00 00 00    // 5 rows
02 00 02 00 02              // indexes → "yes", NULL, "yes", NULL, "yes"
```

<div id="array">
  #### Array
</div>

В отличие от [RowBinary](RowBinary/RowBinary.md#array), где перед каждым массивом записывается количество элементов в кодировке LEB128, формат Native кодирует массивы как два столбцовых подпотока:

* N накопительных смещений `UInt64` (little-endian, по 8 байт каждое). Строка `i` содержит `offset[i] - offset[i-1]` элементов, при этом `offset[-1]` неявно равно 0.
* Все вложенные элементы из всех строк, сериализованные подряд единым блоком.

Например, `Array(UInt32)` с 3 строками `[[0, 10], [1, 11], [2, 12]]`:

```text
// Offsets
02 00 00 00 00 00 00 00    // 2 (row 0: 2 elements)
04 00 00 00 00 00 00 00    // 4 (row 1: 2 elements)
06 00 00 00 00 00 00 00    // 6 (row 2: 2 elements)

// Nested UInt32 values (6 total)
00 00 00 00                 // 0
0a 00 00 00                 // 10
01 00 00 00                 // 1
0b 00 00 00                 // 11
02 00 00 00                 // 2
0c 00 00 00                 // 12
```

Пустой массив имеет то же смещение, что и предыдущая строка. Например, `Array(String)` с 4 строками `[[], ['0'], ['0','1'], ['0','1','2']]`:

```text
00 00 00 00 00 00 00 00    // 0 (empty)
01 00 00 00 00 00 00 00    // 1
03 00 00 00 00 00 00 00    // 3
06 00 00 00 00 00 00 00    // 6
01 30                       // "0"
01 30                       // "0"
01 31                       // "1"
01 30                       // "0"
01 31                       // "1"
01 32                       // "2"
```

<div id="map">
  #### Map
</div>

`Map(K, V)` кодируется как `Array(Tuple(K, V))` — сначала идут смещения массива, затем все ключи, а потом все значения. Это отличается от [RowBinary](RowBinary/RowBinary.md#map), где ключи и значения чередуются внутри каждой записи.

Например, `Map(String, UInt64)` с 3 строками `[{'a':0,'b':10}, {'a':1,'b':11}, {'a':2,'b':12}]`:

```text
// Array offsets
02 00 00 00 00 00 00 00    // 2
04 00 00 00 00 00 00 00    // 4
06 00 00 00 00 00 00 00    // 6

// All keys (6 Strings)
01 61                       // "a"
01 62                       // "b"
01 61                       // "a"
01 62                       // "b"
01 61                       // "a"
01 62                       // "b"

// All values (6 UInt64s)
00 00 00 00 00 00 00 00    // 0
0a 00 00 00 00 00 00 00    // 10
01 00 00 00 00 00 00 00    // 1
0b 00 00 00 00 00 00 00    // 11
02 00 00 00 00 00 00 00    // 2
0c 00 00 00 00 00 00 00    // 12
```

<div id="variant">
  #### Variant
</div>

В отличие от [RowBinary](RowBinary/RowBinary.md#variant), где каждая строка содержит собственный байт дискриминанта, за которым сразу следует встроенное значение, в формате Native дискриминанты отделены от данных.

:::warning
Как и в RowBinary, типы в определении всегда сортируются по алфавиту, а дискриминант — это индекс в этом отсортированном списке. `0xFF` (255) обозначает `NULL`.
:::

Столбец `Variant` кодируется следующим образом:

* Префикс режима дискриминантов `UInt64(LE)` (`0` = BASIC, `1` = COMPACT). Вывод в формате Native обычно использует BASIC (`0`); режим COMPACT может встречаться при чтении данных, сохранённых с включённым `use_compact_variant_discriminators_serialization`.
* N дискриминантов `UInt8`, по одному на строку.
* Данные каждого типа варианта в виде отдельного массового столбца, содержащего только соответствующие строки, в порядке дискриминантов.

Например, `Variant(String, UInt32)` с 5 строками `[0::UInt32, 'hello', NULL, 3::UInt32, 'hello']` (после сортировки: `String` = 0, `UInt32` = 1):

```text
00 00 00 00 00 00 00 00    // discriminators mode = BASIC
01 00 ff 01 00              // UInt32, String, NULL, UInt32, String

// String (2 values, rows 1 and 4)
05 68 65 6c 6c 6f          // "hello"
05 68 65 6c 6c 6f          // "hello"

// UInt32 (2 values, rows 0 and 3)
00 00 00 00                 // 0
03 00 00 00                 // 3
```

<div id="dynamic">
  #### Dynamic
</div>

В отличие от [RowBinary](RowBinary/RowBinary.md#dynamic), где каждое значение является самоописывающимся (префикс типа + значение), формат Native сериализует `Dynamic` как префикс структуры, после которого идет столбец [Variant](#variant).

Префикс структуры содержит версию сериализации `UInt64(LE)`, затем количество динамических типов (в формате VarUInt), а затем имена типов в виде строк. В версии V1 количество типов для совместимости записывается дважды. Далее идет столбец `Variant`, список типов которого включает динамические типы и внутренний тип `SharedVariant`, отсортированные по алфавиту.

Например, `Dynamic` с 5 строками `[0::UInt32, 'hello', NULL, 3::UInt32, 'hello']`:

```text
// Structure prefix (V1)
01 00 00 00 00 00 00 00    // version = V1
02                          // num types (V1 writes twice)
02                          // num types
06 53 74 72 69 6e 67       // "String"
06 55 49 6e 74 33 32       // "UInt32"

// Variant data: Variant(SharedVariant, String, UInt32)
// discriminants: SharedVariant=0, String=1, UInt32=2
00 00 00 00 00 00 00 00    // discriminators mode = BASIC
02 01 ff 02 01              // UInt32, String, NULL, UInt32, String
// SharedVariant: 0 values
05 68 65 6c 6c 6f          // String: "hello"
05 68 65 6c 6c 6f          // String: "hello"
00 00 00 00                 // UInt32: 0
03 00 00 00                 // UInt32: 3
```

<div id="json">
  #### JSON
</div>

В отличие от [RowBinary](RowBinary/RowBinary.md#json), где каждая строка самоописывающаяся и содержит имена путей и значения, формат Native сериализует `JSON` в столбцовом виде. Схема кодирования сложна и зависит от версии: она состоит из префикса структуры с версией сериализации, имён динамических путей и структуры общих данных, после чего следуют типизированные пути (каждый как bulk-столбец), динамические пути (каждый как столбец [Dynamic](#dynamic)) и общие данные для overflow-путей.

Для упрощения совместимости можно использовать настройку `output_format_native_write_json_as_string=1`, которая сериализует JSON-столбцы как обычные текстовые строки JSON (по одному `String` на строку).