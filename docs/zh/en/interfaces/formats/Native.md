---
alias: []
description: 'Native 格式文档'
input_format: true
keywords: ['Native']
output_format: true
slug: /interfaces/formats/Native
title: 'Native'
doc_type: '参考'
---

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 说明
</div>

`Native` 格式的完整官方规范可在[此处](/zh/interfaces/specs/NativeFormat)查看，而配套的 `Native` 协议规范——即承载该格式的 TCP wire protocol——可在[此处](/zh/interfaces/specs/NativeProtocol)查看。

:::note
这两份规范均由 LLM 基于 ClickHouse 源代码生成。代码仍然是主要的事实依据：如果规范与代码不一致，以代码为准。
:::

`Native` 格式是 ClickHouse 效率最高的格式，因为它是真正的“列式”格式，
不会将列转换为行。

在这种格式中，数据以二进制格式按[块](/zh/development/architecture#block)写入和读取。
对于每个块，会依次记录行数、列数、列名和类型，以及该块中各列的数据。

这是服务器之间通过原生接口交互、使用命令行客户端以及 C++ 客户端时采用的格式。

:::tip
你可以使用这种格式快速生成只能由 ClickHouse DBMS 读取的转储。
但通常并不适合手动处理这种格式。
:::

<div id="data-types-wire-format">
  ## 数据类型传输格式
</div>

数据在线上传输时采用列式格式，这意味着每一列都会单独发送，
并且某一列的所有值会作为一个数组一并发送。

块中的每一列都包含一个与 [RowBinaryWithNamesAndTypes](../formats/RowBinary/RowBinaryWithNamesAndTypes.md) 类似的头部信息。

:::note
使用原生 TCP 二进制协议时 (或者当 HTTP 端点接收 `?client_protocol_version=<n>` 时) ，
会在列数和行数之前写入一个 `BlockInfo` 结构。本节中的示例使用的是
不带协议版本的纯 HTTP interface，因此会省略 `BlockInfo`。
:::

<div id="block-structure">
  ### 块结构
</div>

以下查询会返回两列：`number` 和 `str`，共三行：

```bash
curl -XPOST "http://localhost:8123?default_format=Native" --data-binary "SELECT number, toString(number) AS str FROM system.numbers LIMIT 3" > out.bin
```

输出数据会放入单个 ClickHouse 块中，如下所示：

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
  ### 多个块
</div>

不过，在很多情况下，数据无法装入单个块中，ClickHouse 会将其拆分为多个块发送。
来看下面这个查询：它会获取两行数据，并通过减小块大小来强制按每个块一行的方式拆分数据：

```bash
curl -XPOST "http://localhost:8123?default_format=Native" --data-binary "SELECT number, toString(number) AS str                FROM system.numbers LIMIT 2                 SETTINGS max_block_size=1" \  > out.bin
```

输出：

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
  ### 简单数据类型
</div>

这些较简单数据类型的单个值，其传输格式与 `RowBinary`/`RowBinaryWithNamesAndTypes` 类似。
符合这一描述的完整类型列表包括：

* (U)Int8, (U)Int16, (U)Int32, (U)Int64, (U)Int128, (U)Int256
* Float32, Float64
* Bool
* String
* FixedString(N)
* Date
* Date32
* 日期时间
* DateTime64
* IPv4
* IPv6
* UUID

更多详情请参阅[&quot;RowBinary 数据类型传输格式&quot;](/zh/interfaces/formats/RowBinary#data-types-wire-format)中上述类型的说明。

<div id="complex-data-types">
  ### 复杂数据类型
</div>

以下类型的编码格式与 `RowBinary` 和 `RowBinaryWithNamesAndTypes` 不同。

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

在 `Native` 格式中，可为空的列在实际数据前会有一段字节，其字节数等于块中的行数。每个字节都表示对应的值是否为 `NULL`。例如，在这个查询中，每个奇数将改为 `NULL`：

```bash
curl -XPOST "http://localhost:8123?default_format=Native" \  --data-binary "SELECT if(number % 2 = 0, number, NULL) :: Nullable(UInt64) AS maybe_null                 FROM system.numbers LIMIT 5" \  > out.bin
```

输出如下：

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

`Nullable(String)` 的工作方式也类似。null 指示符始终来自 nullable 掩码字节——
掩码值为 `0x01` 表示该行是 `NULL`，与字符串内容无关。对于 `NULL` 行，
底层字符串会存储为空字符串 (LEB128 长度为 `0`) 。请注意，非 `NULL` 的空
字符串的 LEB128 长度同样也是 `0`，因此只有掩码字节才能区分这两种情况。例如，以下查询：

```bash
curl -XPOST "http://localhost:8123?default_format=Native" \  --data-binary "SELECT if(number % 2 = 0, toString(number), NULL) :: Nullable(String) AS maybe_str                 FROM system.numbers LIMIT 5" \  > out.bin
```

输出如下：

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

不同于在 [RowBinary](RowBinary/RowBinary.md#lowcardinality) 中 `LowCardinality` 是透明的，Native 格式使用基于字典的列式编码。一个列会被编码为：版本前缀、唯一值字典，以及一个指向该字典的整数索引 Array。

:::note
列可以定义为 `LowCardinality(Nullable(T))`，但不能定义为 `Nullable(LowCardinality(T))`——这一定会导致服务器返回错误。
:::

版本前缀是一个值为 `1` 的 `UInt64(LE)`，每列写入一次。然后，每个块会写入以下内容：

* `UInt64(LE)` — `IndexesSerializationType` 比特字段。位 0–7 用于编码索引宽度 (0 = UInt8，1 = UInt16，2 = UInt32，3 = UInt64) 。位 8 (`NeedGlobalDictionaryBit`) 在 Native 格式中永远不会被设置 (如果遇到，服务器会抛出异常) 。位 9 表示存在额外的字典键。位 10 表示应重置字典。
* `UInt64(LE)` — 字典键的数量，后跟使用内部类型编码批量序列化的键。
* `UInt64(LE)` — 行数，后跟使用相应 UInt 宽度批量序列化的索引值。

字典在索引 0 处始终包含一个默认值 (例如，`String` 的空字符串、数值类型的 0) 。对于 `LowCardinality(Nullable(T))`，索引 0 表示 `NULL`，并且键在序列化时不带 `Nullable` 包装层。

例如，`LowCardinality(String)` 有 5 行 `['foo', 'bar', 'baz', 'foo', 'bar']`：

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

对于 `LowCardinality(Nullable(String))`，索引 0 为 `NULL`：

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

与 [RowBinary](RowBinary/RowBinary.md#array) 中每个数组前都有一个以 LEB128 编码的元素数量不同，Native 格式 会将数组编码为两个列式子流：

* N 个累计 `UInt64` 偏移量 (小端序，每个 8 字节) 。第 `i` 行有 `offset[i] - offset[i-1]` 个元素，其中 `offset[-1]` 默认为 0。
* 所有行中的全部嵌套元素会作为一个连续块进行批量序列化。

例如，`Array(UInt32)` 有 3 行 `[[0, 10], [1, 11], [2, 12]]`：

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

空数组的偏移量与前一行相同。例如，`Array(String)` 有 4 行 `[[], ['0'], ['0','1'], ['0','1','2']]`：

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

`Map(K, V)` 编码为 `Array(Tuple(K, V))`——先是数组偏移量，再是所有键，最后是所有值。这与 [RowBinary](RowBinary/RowBinary.md#map) 不同：后者中，键和值是按每个条目交错存储的。

例如，包含 3 行 `[{'a':0,'b':10}, {'a':1,'b':11}, {'a':2,'b':12}]` 的 `Map(String, UInt64)`：

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

与 [RowBinary](RowBinary/RowBinary.md#variant) 不同，RowBinary 中每一行都会携带自己的判别值字节，后面紧跟内联值；而 Native 格式 会将判别值与数据分开存储。

:::warning
与 RowBinary 一样，定义中的类型始终按字母顺序排序，判别值则是该排序列表中的索引。`0xFF` (255) 表示 `NULL`。
:::

`Variant` 列的编码方式如下：

* `UInt64(LE)` 判别值模式前缀 (`0` = BASIC，`1` = COMPACT) 。Native 格式 输出通常使用 BASIC (`0`) ；读取以启用 `use_compact_variant_discriminators_serialization` 方式存储的数据时，可能会出现 COMPACT 模式。
* N 个 `UInt8` 判别值，每行一个。
* 每个 Variant 类型的数据会编码为单独的批量列，其中只包含与之匹配的行，并按判别值顺序排列。

例如，`Variant(String, UInt32)` 有 5 行 `[0::UInt32, 'hello', NULL, 3::UInt32, 'hello']` (排序后：`String` = 0，`UInt32` = 1) ：

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

与[RowBinary](RowBinary/RowBinary.md#dynamic)中每个值都是自描述的 (类型前缀 + 值) 不同，Native 格式会将 `Dynamic` 序列化为结构前缀，后接一个 [Variant](#variant) 列。

结构前缀包含一个 `UInt64(LE)` 序列化版本，然后是 Dynamic 类型的数量 (以 VarUInt 形式表示) ，接着是字符串形式的类型名称。在 V1 版本中，出于兼容性考虑，类型数量会写入两次。后续数据是一个 `Variant` 列，其类型列表由这些 Dynamic 类型以及一个内部 `SharedVariant` 类型组成，并按字母顺序排序。

例如，包含 5 行 `[0::UInt32, 'hello', NULL, 3::UInt32, 'hello']` 的 `Dynamic`：

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

不同于 [RowBinary](RowBinary/RowBinary.md#json) 中每一行都带有路径名称和值、因此是自描述的，Native 格式 以列式结构对 `JSON` 进行序列化。这种编码较为复杂且依赖版本：它由一个结构前缀组成，其中包含序列化版本、动态路径名称和共享数据布局；随后是类型化路径 (每个都作为一个 批量列) 、动态路径 (每个都作为一个 [Dynamic](#dynamic) 列) ，以及用于溢出路径的共享数据。

为获得更简单的互操作性，可考虑使用设置 `output_format_native_write_json_as_string=1`，该设置会将 JSON 列序列化为纯 JSON 文本字符串 (每行一个 `String`) 。