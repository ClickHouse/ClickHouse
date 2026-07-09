---
description: 'ClickHouse Native 列式格式规范：传输基本类型、块 和 Column 结构、各种数据类型编码，以及压缩帧'
sidebar_label: 'Native 格式'
sidebar_position: 30
slug: /interfaces/specs/NativeFormat
title: 'Native 格式'
doc_type: 'reference'
keywords: ['native format', 'columnar', 'block', 'wire format', 'serialization', 'compression']
---

Native 格式是 ClickHouse 用于传输表格数据的列式传输格式。它会出现在以下几种场景中：

* [native TCP 协议](/zh/interfaces/specs/NativeProtocol)中 `Data`、`Totals`、`Extremes`、`Log` 和 `ProfileEvents` packet 的 body； (`TableColumns` packet **不是** Native 块——它承载的是两个二进制字符串，因此其布局应归入 [native protocol 规范](/zh/interfaces/specs/NativeProtocol)) ；
* 通过 HTTP 返回的 `SELECT ... FORMAT Native` 输出；
* 使用 `INTO OUTFILE ... FORMAT Native` 写出的文件导出；
* 服务器间复制载荷。

本页介绍 块 内部的字节内容——也就是列式载荷——以及构成它的各列类型编码。数据包分帧、连接状态和版本协商属于 [native protocol 规范](/zh/interfaces/specs/NativeProtocol) 的内容。

所有多字节整数字段均采用小端序。有符号整数使用二进制补码。

:::tip
如需查看面向用户的 `Native` 格式介绍 (含 `curl` 示例) ，请参阅 [Native 格式页面](/zh/interfaces/formats/Native)。本规范是更底层的传输参考。
:::

<div id="overview">
  ## 概览
</div>

凡是在传输过程中承载行数据的内容都是一个 **块**：它是一个自描述的数据块，按列存储。列 1 的所有值先出现，然后是列 2 的所有值，依此类推。一个 块 只携带查询引用的列，绝不会携带整个表。

某一列的 `data` 布局取决于其类型所属的*家族*。这些家族按解码器复杂度从低到高依次如下：

```mermaid
flowchart TD
    B[Block]
    B --> BI[BlockInfo]
    B --> NC[num_columns]
    B --> NR[num_rows]
    B --> Cs["columns[ ]"]

    Cs --> Col[Column]
    Col --> Cname[name]
    Col --> Ctype[type]
    Col --> Chcs[has_custom_serialization]
    Col --> Cdata["data — layout depends on type family"]

    Cdata --> Fixed["Fixed-width<br/>bytes_per_value × num_rows"]
    Cdata --> Comp["Composite<br/>recursive, shape from type string"]
    Cdata --> Ver["Versioned / stateful<br/>per-block version prefix"]

    Fixed --> FixedEx["Int*, UInt*, Float*, Decimal*<br/>Date, DateTime, DateTime64<br/>UUID, IPv4, IPv6, FixedString(N)"]
    Comp --> CompEx["Nullable(T), Array(T)<br/>Tuple(...), Map(K, V), Nested(...)"]
    Ver --> VerEx["LowCardinality(T), JSON<br/>Variant(...), Dynamic"]
```

* **固定宽度**类型将 `data` 布局为 `bytes_per_value × num_rows` 个原始字节，不包含按行分帧。
* **复合**类型 (`Nullable`、`Array`、`Tuple`、`Map`、`Nested`) 具有可由类型字符串完全递归推导出的结构形态，没有版本前缀，也没有跨块状态。
* **版本化 / 有状态**类型 (`LowCardinality`、`JSON`、`Variant`、`Dynamic`) 会在每个非空块的开头带有一个 序列化-version/state prefix。在 `Native` 传输中，这个前缀以及任何字典都**按块**存在——该 format 不会在块*之间*携带状态 (writer 会为每个块创建全新的 序列化 state，并设置 `low_cardinality_max_dictionary_size = 0`) 。跨块状态属于 MergeTree 的磁盘层面问题，而不是 Native 传输布局的一部分。

<div id="wire-primitives">
  ## 线传基本类型
</div>

Native 格式基于四种基本编码方式。

| 基本类型            | 大小                   | 说明                       |
| --------------- | -------------------- | ------------------------ |
| VarUInt         | 1–10 B               | LEB-128 可变长度无符号整数        |
| Fixed-width int | 1, 2, 4, 8, 16, 32 B | 小端序，有符号值采用二进制补码          |
| String          | variable             | VarUInt 长度前缀 + 原始字节      |
| Bool            | 1 B                  | `0x00` = false，非零 = true |

<div id="varuint">
  ### VarUInt
</div>

一种采用 LEB-128 编码的变长无符号整数。每个字节在 0–6 位包含 7 个数据位，在第 7 位包含 1 个延续位。若后面还有更多字节，延续位为 `1`；最后一个字节的延续位为 `0`。

| 值范围             | 字节数   |
| --------------- | ----- |
| 0 – 127         | 1     |
| 128 – 16383     | 2     |
| 16384 – 2097151 | 3     |
| 最高可达完整的 UInt64  | 最多 10 |

对值 `300` 进行编码：

```text
300 = 0b100101100

Byte 0: 0xAC = 0b10101100   (data: 0101100, continuation: 1)
Byte 1: 0x02 = 0b00000010   (data: 0000010, continuation: 0)
```

对字节 `0xAC 0x02` 进行解码：

```text
Byte 0: data = 0x2C, continuation = 1 → accumulator = 0x2C, shift = 7
Byte 1: data = 0x02, continuation = 0 → accumulator = (0x02 << 7) | 0x2C = 300
```

<div id="fixed-width-integers">
  ### 定长整数
</div>

| 类型      | 字节 | 编码               |
| ------- | -- | ---------------- |
| UInt8   | 1  | 原始字节             |
| UInt16  | 2  | 小端序              |
| UInt32  | 4  | 小端序              |
| UInt64  | 8  | 小端序              |
| UInt128 | 16 | 小端序              |
| UInt256 | 32 | 小端序              |
| Int8    | 1  | 原始字节，二进制补码       |
| Int16   | 2  | 小端序，二进制补码        |
| Int32   | 4  | 小端序，二进制补码        |
| Int64   | 8  | 小端序，二进制补码        |
| Int128  | 16 | 小端序，二进制补码        |
| Int256  | 32 | 小端序，二进制补码        |
| Float32 | 4  | IEEE 754 单精度，小端序 |
| Float64 | 8  | IEEE 754 双精度，小端序 |

例如，UInt32 的值 `1` 编码为 `01 00 00 00`，而 Int32 的值 `-1` 编码为 `FF FF FF FF`。

<div id="string">
  ### String
</div>

一种带长度前缀的字节序列：

```text
[VarUInt: byte_length] [byte_length bytes: raw value]
```

该字节序列不一定是有效的 UTF-8。空字符串编码为单个 `0x00` 字节，字符串可包含任意字节值，包括嵌入的 NUL。字符串 `"ab"` 编码为 `02 61 62`；解码时，先读取 VarUInt 长度 (`2`) ，然后读取对应数量的字节。

<div id="bool">
  ### Bool
</div>

单个字节。`0x00` 表示 false；任何非零值都表示 true (规范形式为 `0x01`) 。

<div id="block-and-column-structure">
  ## 块和列结构
</div>

<div id="block-wire-layout">
  ### 块的传输布局
</div>

```text
[BlockInfo]               metadata (only on the TCP Data-packet path; see below)
[VarUInt: num_columns]    number of columns in this block
[VarUInt: num_rows]       number of rows in this block
[Column × num_columns]    column entries, omitted when num_columns = 0
```

`BlockInfo` 前缀是否存在取决于通道，因为写入器以 *修订版本* 作为参数 (完整说明见[协议修订版本与 Native 格式](#protocol-revision)，其中也包括 `client_protocol_version` 仅影响输出这一点) ：

* 在 **native TCP protocol** 上，服务器会按连接协商出的修订版本写入块 (一个较大的值——`DBMS_TCP_PROTOCOL_VERSION`，见 `src/Core/ProtocolDefines.h`) 。只要该修订版本大于零，就会写入 `BlockInfo`，而真实连接始终如此。每一列中的 `has_custom_serialization` 字节 (见[列的线协议布局](#column-wire-layout)) 会在修订版本 `54454` 及以上时写入。
* `Native` *输出格式*——通过 HTTP 执行的 `SELECT ... FORMAT Native`、`INTO OUTFILE ... FORMAT Native`，以及由 `clickhouse-client` 生成的 `Native` 格式——*默认*按修订版本 `0` 进行序列化。在修订版本 `0` 下，`BlockInfo` 前缀和 `has_custom_serialization` 字节都会被省略，因此一个块仅包含 `num_columns`、`num_rows` 和各列。

  对 HTTP 来说，这个修订版本并不是固定的：客户端可以通过 `?client_protocol_version=<n>` 查询参数提高它，而服务器会将该值用作响应的序列化修订版本。

  当该值足够高时，HTTP 输出会包含 `BlockInfo` 前缀 (只要修订版本大于 `0` 就会写入) 以及 `has_custom_serialization` 字节 (在修订版本 `54454` 及以上时写入) ，与 TCP 路径完全一致。因此，客户端不能假定每个 HTTP `FORMAT Native` 载荷都是修订版本 `0`。

换句话说，本节中那些以 `BlockInfo` 前缀开头的字节示例描述的是 TCP Data 数据包的载荷。对同一个查询，如果通过 `FORMAT Native` 获取，则会生成旁边所示的较短形式。

<div id="blockinfo">
  ### BlockInfo
</div>

BlockInfo 是一组字段序列，每个字段前都有一个 VarUInt 类型的字段 ID，并以字段 ID `0` 结束。其传输格式**不是**自描述的：字段 ID 本身不编码其值的长度或类型，因此 reader 必须预先知道它可能遇到的每个字段 ID 对应的类型。ClickHouse 自身的 reader 会将无法识别的字段 ID 视为数据损坏，并抛出异常 (`UNKNOWN_BLOCK_INFO_FIELD`) 。向前兼容性则由协议修订版本处理：只有在协商出的修订版本不低于某字段的最低修订版本时，发送方才会写入该字段，因此较旧的 receiver 永远不会看到自己不认识的字段。

| Field ID | Field                            | Type          | Min revision | Description                                            |
| -------- | -------------------------------- | ------------- | ------------ | ------------------------------------------------------ |
| 1        | is&#95;overflows                 | UInt8         | 0            | GROUP BY 产生的 overflow 块。非 overflow 块时为 `0`。            |
| 2        | bucket&#95;number                | Int32         | 0            | 聚合桶编号。非分桶块时为 `-1`。                                     |
| 3        | out&#95;of&#95;order&#95;buckets | List of Int32 | 54480        | 分布式聚合期间被延后的桶。编码方式为：先写入一个 VarUInt 计数，后跟对应数量的 `Int32` 值。 |
| 0        | (terminator)                     | —             | —            | BlockInfo 结束标记。始终必需。                                   |

字段 `1` 和 `2` 的最低修订版本为 `0`，因此只要写入了 `BlockInfo`，它们就一定会出现。字段 `3` 仅在修订版本为 `54480` 及以上时写入。常见情况下 (修订版本低于 `54480`) 的传输布局如下：

```text
[VarUInt: 1] [UInt8: is_overflows]
[VarUInt: 2] [Int32: bucket_number]
[VarUInt: 0]
```

<div id="column-wire-layout">
  ### 列的传输布局
</div>

一个块中会出现 `num_columns` 个列。

| # | 字段                               | 类型                 | 条件                                  | 描述                                                                                                                                                |
| - | -------------------------------- | ------------------ | ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1 | name                             | String             | 始终                                  | 列名                                                                                                                                                |
| 2 | type                             | String *或* 二进制类型编码 | 始终                                  | 默认情况下为 ClickHouse 类型字符串 (例如 `"UInt64"`、`"Array(String)"`) ；当 `output_format_native_encode_types_in_binary_format = 1` 时，则为二进制类型编码 (见下方说明)         |
| 3 | has&#95;custom&#95;serialization | UInt8              | 功能 `CUSTOM_SERIALIZATION` (v54454)  | `0` = 默认，`1` = 自定义 (后面跟着 kind&#95;stack)                                                                                                          |
| 4 | kind&#95;stack                   | bytes              | 当字段 3 = `1` 时                       | 一个 UInt8 枚举字节 (见下文) ，用于描述非默认序列化方式 (如稀疏等) 。对于 `COMBINATION` 值，后面还会跟一个 VarUInt 计数，以及相应数量的额外 kind 字节。对于 `Tuple` (以及其他带有元素级序列化信息的复合类型) ，其载荷是递归的——见下文。 |
| 5 | data                             | bytes              | 始终                                  | 所有 `num_rows` 行的列值。布局因类型而异——参见 [数据类型](#data-types)。对于稀疏列，见下文。                                                                                     |

解码器根据 `type` 字符串进行分派。类型字符串通常会在括号中带有参数；解码器会去掉 `(...)` 后缀以找出基本类型，然后再解析这些参数，以决定大小、标度或内部类型。解析包含嵌套类型的参数列表时 (例如在 `Array` 中嵌套一个 `Tuple`) ，需要使用能感知深度的逗号分隔器，跟踪括号的嵌套关系，而不能简单按 `,` 拆分。

:::note 二进制类型编码
`type` 字段仅在默认模式下才是文本 `String`。当设置了查询设置 `output_format_native_encode_types_in_binary_format = 1` 时，该字段会变为**二进制类型编码**——也就是 [data type binary encoding](/zh/sql-reference/data-types/data-types-binary-encoding) 中记录的同一种基于标签的编码——而展平后的 `Dynamic` 类型列表也会对其中各类型名称使用相同的二进制编码。如果解码器始终将字段 2 读取为带长度前缀的字符串，就会把第一个二进制类型标签当成字符串长度，从而导致流失去同步，因此它必须知道该数据流使用的是哪种模式。
:::

```mermaid
flowchart TD
    T["type string<br/>(e.g. Array(String))"]
    T --> P["strip outer (...)<br/>to find the base type"]
    P --> F{"base type family?"}
    F -->|fixed-width| FW["read bytes_per_value × num_rows<br/>(no per-row framing)"]
    F -->|variable-length| VL["read per-value length prefixes"]
    F -->|composite| CO["read each sub-stream;<br/>recurse on the inner types"]
    F -->|versioned| VE["read state prefix (version)<br/>at the start of each non-empty block,<br/>then that block's payload"]
```

<div id="kind-stack-and-sparse-encoding">
  #### kind_stack 和稀疏编码
</div>

`kind_stack` 字节用于枚举每列的非默认序列化方式：

| 字节     | 名称                           | 含义                                   | 对 `data` 的传输格式影响                                |
| ------ | ---------------------------- | ------------------------------------ | ----------------------------------------------- |
| `0x00` | DEFAULT                      | 默认序列化                                | 与 `has_custom = 0` 完全相同                         |
| `0x01` | SPARSE                       | 稀疏序列化 (v54465+)                      | 偏移流 + 非默认值；见下文                                  |
| `0x02` | DETACHED                     | 由并行块封送包装为 `ColumnBLOB` 的列 (v54478+)  | 预先封送的 blob：`VarUInt size` + 对应数量的字节；见下文         |
| `0x03` | DETACHED&#95;OVER&#95;SPARSE | 包装在 `ColumnBLOB` 中的稀疏列               | 与 `DETACHED` 相同的 blob 载荷；见下文                    |
| `0x04` | REPLICATED                   | 用于重复值的字典形式 (v54482+)                 | 索引流 + 稠密元素值；见下文                                 |
| `0x05` | COMBINATION                  | 多 kind 栈                             | 后跟 VarUInt `count` 和另外 `count` 个 kind 字节——见下方说明 |

**`COMBINATION` 载荷使用的是另一套枚举。** 上表前五行是*紧凑*的一字节编码。`COMBINATION` (`0x05`) 是对所有未被这些编码覆盖的栈的通用转义形式：其后先跟一个 `VarUInt` `count`，再跟 `count` 个一字节条目。这些条目**不是**表中的紧凑编码——它们是原始的 `ISerialization::Kind` 值：

| 字节     | 嵌套 `Kind`  |
| ------ | ---------- |
| `0x00` | DEFAULT    |
| `0x01` | SPARSE     |
| `0x02` | DETACHED   |
| `0x03` | REPLICATED |

这些字节值与紧凑编码不同：`REPLICATED` 在这套嵌套枚举中是 `0x03`，但作为紧凑编码时是 `0x04`；并且没有 `DETACHED_OVER_SPARSE` 条目——该组合会表示为连续两个条目 `SPARSE`、`DETACHED`。如果解码器仍按紧凑编码表来解释这些嵌套字节，就会把 `0x03`/`0x04` 映射错，并导致流失去同步。

`count` 是完整的栈长度，**包括每个栈开头的前导 `DEFAULT` 条目**。紧凑编码已经覆盖了所有一项和两项的栈，因此 `COMBINATION` 的 `count` 一定至少为三。

**`Tuple` 列的递归 `kind_stack`。** 上面的 `kind_stack` 载荷，是某一列自身序列化信息对应的字节 (或 `COMBINATION` 序列) 。`Tuple` 携带一个 `SerializationInfoTuple`：它先写入 tuple *自身* 的 kind-stack 载荷，然后按顺序为*每个*元素各写入一个完整的 kind-stack 载荷；解码器也会按同样的递归结构读取。因此对于 `Tuple(A, B, C)`，field-4 字节为 `[tuple_kind][A_kind][B_kind][C_kind]`；如果某个元素本身又是复合类型，则该元素的载荷也会继续递归。只要 tuple 自身的信息*或任一元素*的信息不是默认值，就会设置 `has_custom_serialization` 字节 (field 3) ，因此即使一个 `Tuple` 只有某个元素是 sparse、replicated 或 detached，仍会触发 kind-stack 载荷。如果解码器对 `Tuple` 只读取开头那一个枚举字节，就会过早停止，并把后续元素的 kind 字节误读为列数据。

**稀疏传输格式。** 当 `kind_stack = 0x01` 时，列 `data` 由两个流组成，并在同一个共享 TCP 流中首尾相接地写入：

1. **偏移流** —— 一系列 `VarUInt`。每个值 `v` 都属于以下两种情况之一：
   * `v` 的第 62 位高位比特未设置：`(v & 0x3FFFFFFFFFFFFFFF)` 表示下一个显式非默认值之前的默认位置数，记为 `group_size`。该非默认位置为 `cursor + group_size`，其中 `cursor` 是当前游标位置；随后 `cursor` 前进 `group_size + 1`。
   * `v` 的第 62 位已设置 (`END_OF_GRANULE_FLAG`) ：清除该标志后的值 = 最后一个非默认值之后尾随的默认位置数。这标志着该块的偏移流结束。
2. **值流** —— 用内部类型对 `count` 个非默认值进行稠密编码，其中 `count` 是上文读取出的非 EOG VarUInt 数量。

解码器会用内部类型的默认值填充每个未显式指定的位置，从而重建一个包含 `num_rows` 个条目的稠密列 (整数和浮点数为 `0`，`String` 为 `""`，`Date` 为 `0` 天，等等) 。

稀疏 `Nullable(T)` 列是一个特例，因为 `Nullable(T)` 的默认值是 **NULL**。稀疏编码会完全省略通常的 `Nullable` null-map stream：offset stream 标识出非默认值——也就是非 NULL——的位置，values stream 仅以稠密形式保存这些非 NULL 的 `T` 类型值，而每个未显式指定的位置都会还原为 NULL。因此，解码器*不应*在 values stream 中查找 null map，也*不应*用存在值 `0` 来填充空缺；它应填充为 NULL。

**副本传输格式。** 当 `kind_stack = 0x04` 时，列 `data` 是一个字典：由去重后的元素值列表，以及指向该列表的逐行索引组成 (查找形态与 `LowCardinality` 相同) 。当内部类型本身是版本化的——例如 `LowCardinality(T)`——其状态前缀会**先**写入，也就是先于索引 stream：副本序列化会先将前缀阶段委托给内部类型处理，然后再写入 `num_rows`。前缀为空的内部类型 (叶子类型和普通复合类型) 在这里不会写入任何字节。

```text
[inner type's state prefix]              empty for leaf inners; e.g. LowCardinality version (Int64 = 1)
[VarUInt num_rows]
[UInt8  size_of_indexes_type]            width of each index: 1, 2, 4, or 8 bytes
[indexes: num_rows × size_of_indexes_type bytes]
[VarUInt num_elements]
[elements: num_elements dense inner-type values]
```

解码器通过为每个输出行 `i` 选取 `elements[indexes[i]]` 来重建稠密列。复合内部类型会递归处理：先在内部类型中 materialize 元素列表，再按索引取值。支持的内部类型包括叶子类型、`Nullable(T)`、`Array(T)`、`Tuple(...)`、`Map(K, V)`、`Nested(...)` (每个字段都像 `Array` 一样展开) 以及 `LowCardinality(T)` (保留共享字典；仅对逐元素的键进行索引) 。

**分离传输格式。** `DETACHED` (`0x02`) 和 `DETACHED_OVER_SPARSE` (`0x03`) 确实会在传输中出现——它们并非纯粹的内部表示。在 TCP 路径上，当启用压缩且协商出的修订版本至少为 `DBMS_MIN_REVISON_WITH_PARALLEL_BLOCK_MARSHALLING` (v54478) 时，该列会经过三个步骤：

1. 每个符合条件的列 (非 `const`、非 `Tuple`，且所在块包含多于一行) 都会被包装为一个 `ColumnBLOB`，其中保存的是已在主线程之外完成编组和压缩的列。
2. `DETACHED` 会被追加到该包装列的 kind 栈中。
3. 列的 `data` 会被写成一个 `VarUInt` blob 大小，后面紧跟恰好这么多个 blob 字节。

如果被包装的列是稀疏的，那么它的栈就是 `{DEFAULT, SPARSE, DETACHED}`，序列化后即为 `DETACHED_OVER_SPARSE`。客户端在解码此类列时，会先读取 blob 的长度和字节，然后对 blob 进行解压，以恢复内部列的载荷 (参见压缩部分中的 [`ColumnBLOB` 注释](#compression-negotiation)) 。

<div id="block-variants">
  ### 块变体
</div>

所有 Data 家族的数据包都共享相同的块传输格式。各个变体仅在列数和行数上有所不同：

| 变体  | num&#95;columns | num&#95;rows | 用途                            |
| --- | --------------- | ------------ | ----------------------------- |
| 头部块 | N &gt; 0        | 0            | 用于声明结果 schema (列名 + 类型) 。     |
| 结果块 | N &gt; 0        | M &gt; 0     | 实际的结果行。                       |
| 空块  | 0               | 0            | 哨兵值——在客户端侧表示输入结束；在服务端侧表示边界标记。 |

<div id="byte-level-examples">
  ### 字节级示例
</div>

本节中的所有示例均取自 **TCP Data-packet path**，因此都包含 `BlockInfo` 前缀和 `has_custom_serialization` 字节。在 `FORMAT Native` 中，相同的块会更短——在有助于理解的地方给出了对应的短格式。

一个空块 (包含 BlockInfo) ，总共 8 字节：

```text
01 00                   BlockInfo: field_id=1, is_overflows=0
02 FF FF FF FF          BlockInfo: field_id=2, bucket_number=-1
00                      BlockInfo terminator
00                      num_columns = 0
00                      num_rows = 0
```

`SELECT 1` 的头部块表示：有一列名为 `"1"`、类型为 `UInt8`，且行数为零。在协议版本 ≥ 54454 时，还会包含 `has_custom_serialization` 字节：

```text
01 00                   BlockInfo: is_overflows = 0
02 FF FF FF FF          BlockInfo: bucket_number = -1
00                      BlockInfo terminator
01                      num_columns = 1
00                      num_rows = 0
01 "1"                  Column[0].name = "1"
05 "UInt8"              Column[0].type = "UInt8"
00                      Column[0].has_custom_serialization = 0
                        Column[0].data: no bytes (num_rows = 0)
```

同一查询的结果块，仅一行：

```text
01 00                   BlockInfo: is_overflows = 0
02 FF FF FF FF          BlockInfo: bucket_number = -1
00                      BlockInfo terminator
01                      num_columns = 1
01                      num_rows = 1
01 "1"                  Column[0].name = "1"
05 "UInt8"              Column[0].type = "UInt8"
00                      Column[0].has_custom_serialization = 0
01                      Column[0].data: one UInt8 byte = 1
```

通过 `FORMAT Native` (修订版本 `0`) 时，同一个结果块既不包含 `BlockInfo`，也没有 `has_custom_serialization` 字节——`SELECT 1 FORMAT Native` 的大小为 11 字节：

```text
01                      num_columns = 1
01                      num_rows = 1
01 "1"                  Column[0].name = "1"
05 "UInt8"              Column[0].type = "UInt8"
01                      Column[0].data: one UInt8 byte = 1
```

(零行结果 (例如仅包含表头的块) 通过 `FORMAT Native` 完全不会产生任何字节：这种输出格式不会输出空块。)

<div id="protocol-revision">
  ## 协议修订版本与 Native 格式
</div>

Native 字节流的形态主要由写入端和读取端所使用的**协议修订版本**决定。修订版本信息并不包含在字节本身中——在传输格式中没有修订版本字段——但它仍会决定某些特性是否会出现。因此，解码器必须先知道某个载荷是按哪个修订版本写入的，之后才能解析它。由于修订版本不在流中，读取端和写入端必须通过其他方式就此达成一致。

它是一个单独的 `UInt64`，`NativeWriter` 和 `NativeReader` 都将其作为构造函数参数接收。写入端将它称为 `client_revision`，读取端将它称为 `server_revision`，但它们其实是同一个数值。此版本所知的最新修订版本是 `DBMS_TCP_PROTOCOL_VERSION` (参见 `src/Core/ProtocolDefines.h`) 。

<div id="what-the-revision-gates">
  ### 修订版本控制了什么
</div>

每个功能都对应一个 `DBMS_MIN_REVISION_WITH_*` 阈值。只有当写入端的修订版本达到该阈值时，才会写出该功能；读取端也会按完全相同的规则判断是否读取，因此两端才能保持同步——任一端的修订版本判断出错，双方就会不同步。对 Native format 来说，关键的控制点包括：

| 功能                                    | 阈值常量                                                               | 修订版本    | 低于阈值时的影响                                                                                                                     |
| ------------------------------------- | ------------------------------------------------------------------ | ------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `BlockInfo` 前缀                        |  (任意 `> 0` 的值)                                                     | `1`     | [`BlockInfo`](#blockinfo) 前缀会被完全省略；一个块只包含 `num_columns`、`num_rows` 和各列。                                                      |
| `has_custom_serialization` 字节         | `DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION`                      | `54454` | 每列的 [`has_custom_serialization`](#column-wire-layout) 字节会被省略；所有列都使用默认 序列化 (没有 sparse、replicated 或 detached 形式) 。   |
| 在传输格式中的 `LowCardinality`              | `DBMS_MIN_REVISION_WITH_LOW_CARDINALITY_TYPE`                      | `54405` | 特殊情况——**不**遵循简单的“低于阈值”规则。只有当修订版本*非零*且低于 `54405` 时，或另外被强制剥离时，`LowCardinality(T)` 才会被剥离为基本类型 `T`。修订版本为 `0` 时会保留它。见下方说明。        |
| V2 `Dynamic` / `JSON` 序列化   | `DBMS_MIN_REVISION_WITH_V2_DYNAMIC_AND_JSON_SERIALIZATION`         | `54473` | `Dynamic` 和 `JSON`/`Object` 会使用 V1 序列化 (带 `max_dynamic_*` parameter) ，而不是 V2。                                      |
| 聚合函数版本控制                              | `DBMS_MIN_REVISION_WITH_AGGREGATE_FUNCTIONS_VERSIONING`            | `54452` | `AggregateFunction` 状态写入时不包含嵌入版本。                                                                                            |
| `BlockInfo` 中的 `out_of_order_buckets` | `DBMS_MIN_REVISION_WITH_OUT_OF_ORDER_BUCKETS_IN_AGGREGATION`       | `54480` | 不会写入 `BlockInfo` field ID `3` (参见 [BlockInfo](#blockinfo)) 。                                                                 |
| 并行块编组 (`DETACHED`)                    | `DBMS_MIN_REVISON_WITH_PARALLEL_BLOCK_MARSHALLING`                 | `54478` | 列不会被包装成 `ColumnBLOB`；也不会出现 `DETACHED` / `DETACHED_OVER_SPARSE` kind (参见 [kind&#95;stack](#kind-stack-and-sparse-encoding)) 。 |
| `DateTime(tz)` 类型 parameter           | `DBMS_MIN_REVISION_WITH_TIME_ZONE_PARAMETER_IN_DATETIME_DATA_TYPE` | `54337` | timezone parameter 会从 `type` string 中去掉——`DateTime('UTC')` 会声明为不带参数的 `DateTime`。                                             |

因此，修订版本 `0` 对几乎所有内容来说都是最保守的编码方式：stream 中不包含 `BlockInfo`、没有 `has_custom_serialization` 字节、使用 V1 `Dynamic`/`JSON`、没有聚合函数版本信息，而且 `DateTime` 也会去掉 timezone parameter。

`LowCardinality` 是唯一的例外，而且这一点非常重要。写入端的判断条件是 `remove_low_cardinality || (client_revision && client_revision < DBMS_MIN_REVISION_WITH_LOW_CARDINALITY_TYPE)`。关键就在前面的 `client_revision &&`：当修订版本恰好为 `0` 时，整个条件会短路为 false。

因此，在修订版本 `0` 下——也就是 `FORMAT Native` 的默认情况——`LowCardinality(T)` **不会**被剥离。它的类型 string 和每个块的 state prefix 都会保留在 stream 中，而修订版本为 `0` 的读取端也会原样读回。

只有在修订版本为非零且低于 `54405` 时，或者在不考虑修订版本的情况下被强制处理时，才会触发剥离。

这种强制行为由 `remove_low_cardinality` flag 控制。`FORMAT Native` 输出永远不会设置它，但 native TCP 路径会设置——当 `low_cardinality_allow_in_native_format = 0` 时 (默认值为 `1`) 。换句话说，这个 setting 会改变 native TCP 输出，但对 `FORMAT Native` 没有任何影响。

实际结论是：默认的 `FORMAT Native` stream 完全可能合法地包含 `LowCardinality`，因此不要把它当作一个在修订版本 `0` 下不存在的功能。

<div id="revision-per-channel">
  ### 修订版本从何而来，取决于数据的传输路径
</div>

相同的 Native 字节可以通过不同路径传输：原生 TCP 协议、HTTP 请求，或磁盘上的文件。每条路径都会以各自的方式确定修订版本。有一点需要注意：读取端和写入端是分别设置的，因此它们最终可能使用不同的修订版本。

<div id="revision-tcp">
  #### Native TCP 协议——双向协商确定
</div>

在 [native TCP protocol](/zh/interfaces/specs/NativeProtocol) 中，修订版本来自 Hello 握手。客户端发送 `DBMS_TCP_PROTOCOL_VERSION`，服务器回传自己的版本；从那时起，双方都会按**对方声明的修订版本**进行序列化：服务器用 `client_tcp_protocol_version` 构建其 `NativeReader`/`NativeWriter`，而客户端使用收到的 `server_revision`。这里没有显式的 &#96;min&#96;&#96;，但双方都不可能发送自己尚未实现的特性，因此每个方向实际上都受限于两个对端中较旧的一方。

当两个对端都是相同的较新构建时，两个方向会落在同一个修订版本上 (`DBMS_TCP_PROTOCOL_VERSION`，见 `src/Core/ProtocolDefines.h`) ，所有受版本控制的开关都会开启。这是最常见的情况，但并不绝对。对于混合版本或第三方对端，两个方向可能落在不同的修订版本上，因此这些开关必须按方向分别理解：任何非零修订版本都包含 `BlockInfo`，但其余内容——包括 `has_custom_serialization`——只有当该方向的有效修订版本达到各自阈值时才会出现。例如，声明修订版本低于 `54454` 的对端，既不会发送，也不会接收 `has_custom_serialization` 这个字节。

<div id="revision-output">
  #### `FORMAT Native` 输出——默认修订版本为 0，可通过 HTTP 提高
</div>

`Native` *输出*格式默认使用修订版本 **`0`**。这包括通过 HTTP 执行的 `SELECT ... FORMAT Native`、`INTO OUTFILE ... FORMAT Native`，以及 `clickhouse-client` 写出的 `Native` 输出；在每种情况下，输出工厂都会将 `FormatSettings::client_protocol_version` 直接传给 `NativeWriter`。

不过，对于 HTTP 来说，默认值并非全部情况。客户端可以通过 `?client_protocol_version=<n>` 查询参数提高该值，HTTP 处理程序会将其视为保留参数，而不是 SQL 设置：它会进入查询上下文，随后由格式层复制到 `FormatSettings` 中。只要设置得足够高，HTTP `FORMAT Native` 输出就会像 TCP 路径一样，开始包含 `BlockInfo` 前缀和 `has_custom_serialization` 字节——因此，不要想当然地认为 HTTP `FORMAT Native` 载荷始终都是修订版本 `0`。文件导出和本地 `clickhouse-client` 输出没有这样的调节选项，因此会保持为 `0`。

<div id="revision-input">
  #### `FORMAT Native` 输入——始终为修订版本 0
</div>

`Native` *输入*格式则正好相反：它被**硬编码为修订版本 `0`**，完全不考虑 `client_protocol_version`。无论是解析 `INSERT ... FORMAT Native` 的请求体，还是读取 `Native` 文件，它都会用字面值 `0` 来构建 `NativeReader`，因此永远不会预期有 `BlockInfo` 前缀，不会读取 `has_custom_serialization` 字节，并且始终假定使用默认序列化。

所以，`client_protocol_version` 只对输出生效。在 `INSERT ... FORMAT Native` 请求中设置较高的 `?client_protocol_version=` (例如 `DBMS_TCP_PROTOCOL_VERSION`) ，不会对请求体的读取方式产生任何影响——请求体仍然必须是修订版本 `0`。如果提供的请求体带有 `BlockInfo` 前缀或 `has_custom_serialization` 字节，读取器就会失去同步，结果会表现为解析错误 (`INCORRECT_DATA` 或 `CANNOT_READ_ALL_DATA`) ，而不是成功插入。

<div id="revision-round-trip">
  ### 往返传输的影响
</div>

对于 `FORMAT Native`，两端都使用修订版本 `0` 是最稳妥的选择，这也是默认行为。通过修订版本 `0` 的 `SELECT ... FORMAT Native` 写出的数据，可以直接读回 `INSERT ... FORMAT Native`，不会有任何意外。

只有在你刻意提高输出修订版本时，问题才会出现。带有 `?client_protocol_version=<large>` 的 `SELECT ... FORMAT Native` 会生成一个包含 `BlockInfo` 和 `has_custom_serialization` 字节的 stream，而修订版本为 `0` 的输入路径无法再将其读回。如果你需要这类数据支持往返传输，要么在生成数据的 `SELECT` 中不要设置 `client_protocol_version`，要么改用 native TCP protocol 传输数据——此时每个方向都会使用通过握手协商出的修订版本——而不是使用 `FORMAT Native`。

| 通道                                                        | 写入修订版本                              | 读取修订版本                                 | `BlockInfo` / 自定义序列化                                                    |
| --------------------------------------------------------- | ----------------------------------- | -------------------------------------- | ----------------------------------------------------------------------- |
| Native TCP Data packet                                    | 对端声明的修订版本 (每个方向分别计算)                | 对端声明的修订版本 (每个方向分别计算)                   | 修订版本 `> 0` 时始终带有 `BlockInfo`；在 `≥ 54454` 时带有 `has_custom_serialization` |
| 通过 HTTP 的 `SELECT ... FORMAT Native`                      | `client_protocol_version` (默认 `0`)  | n/a                                    | 仅在提高 `client_protocol_version` 时出现                                      |
| 通过 HTTP 的 `INSERT ... FORMAT Native`                      | n/a                                 | `0` (固定，忽略 `client_protocol_version`)  | 永不读取                                                                    |
| `INTO OUTFILE` / 文件 / `clickhouse-client` `FORMAT Native` | `0`                                 | `0`                                    | 不包含 (但会保留 `LowCardinality`——参见上文说明)                                     |

:::note 协议修订版本与序列化版本
不要将协议修订版本与[序列化版本](#serialization-version-concept)混淆。这里的修订版本作用于整个连接或请求，且不会出现在字节流中。序列化版本则是按列区分的，由[版本化类型](#versioned-types)携带，并会写入每个非空块。修订版本决定某项特性是否存在；而序列化版本则是在进入某个版本化列之后，决定该类型后续采用哪一种编码变体。
:::

<div id="data-types">
  ## 数据类型
</div>

本节说明 Native format 在列 `data` 中可承载的各类类型的线级编码，并按解码器复杂度递增分为四个家族。有两种类型——`AggregateFunction(func, ...)` 和 `QBit(T, N[, stride])`——是有效的 `Native` 列类型，但它们的载荷依赖具体函数或类型，不在本文讨论范围内；下文会在原本可能被误认为别名的位置特别指出它们。

| 家族         | 章节                               | 每列流 | 跨块状态                                  |
| ---------- | -------------------------------- | --- | ------------------------------------- |
| 固定宽度       | [固定宽度类型](#fixed-width-types)     | 一个  | 无                                     |
| 可变长度       | [可变长度类型](#variable-length-types) | 一个  | 无                                     |
| 复合 (固定形态)  | [复合类型](#composite-types)         | 多个  | 无                                     |
| 版本化 / 有状态  | [版本化类型](#versioned-types)        | 多个  | Native 线级编码中无——每个块都有独立的状态前缀，且每块都会重新开始 |

<div id="fixed-width-types">
  ### 定宽类型
</div>

每个值都占用固定数量的字节。一个包含 `M` 行的列在传输格式中恰好占用 `bytes_per_row × M` 字节，按顺序直接拼接，不带任何分隔符或填充。

| Type string         | Bytes per value | Logical value                                                               | Wire encoding                              |
| ------------------- | --------------- | --------------------------------------------------------------------------- | ------------------------------------------ |
| `UInt8`             | 1               | 无符号 8 位整数                                                                   | 原始字节                                       |
| `UInt16`            | 2               | 无符号 16 位整数                                                                  | 小端序                                        |
| `UInt32`            | 4               | 无符号 32 位整数                                                                  | 小端序                                        |
| `UInt64`            | 8               | 无符号 64 位整数                                                                  | 小端序                                        |
| `UInt128`           | 16              | 无符号 128 位整数                                                                 | 小端序                                        |
| `UInt256`           | 32              | 无符号 256 位整数                                                                 | 小端序                                        |
| `Int8`              | 1               | 有符号 8 位整数，采用二进制补码表示                                                         | 原始字节                                       |
| `Int16`             | 2               | 有符号 16 位整数，采用二进制补码表示                                                        | 小端序                                        |
| `Int32`             | 4               | 有符号 32 位整数，采用二进制补码表示                                                        | 小端序                                        |
| `Int64`             | 8               | 有符号 64 位整数，采用二进制补码表示                                                        | 小端序                                        |
| `Int128`            | 16              | 有符号 128 位整数，采用二进制补码表示                                                       | 小端序                                        |
| `Int256`            | 32              | 有符号 256 位整数，采用二进制补码表示                                                       | 小端序                                        |
| `Float32`           | 4               | IEEE 754 单精度                                                                | 小端序                                        |
| `Float64`           | 8               | IEEE 754 双精度                                                                | 小端序                                        |
| `BFloat16`          | 2               | IEEE 754 `Float32` 的高 16 位                                                  | 小端序                                        |
| `Bool`              | 1               | `0x00` = false，`0x01` = true                                                | 原始字节                                       |
| `Date`              | 2               | 自 `1970-01-01` 起的天数                                                         | 小端序 UInt16                                 |
| `Date32`            | 4               | 自 `1970-01-01` 起的天数 (有符号；支持 1970 年之前)                                       | 小端序 Int32                                  |
| `DateTime`          | 4               | 以秒为单位的 Unix timestamp                                                       | 小端序 UInt32                                 |
| `DateTime(tz)`      | 4               | 与 `DateTime` 相同；timezone 属于 metadata                                        | 小端序 UInt32                                 |
| `DateTime64(s)`     | 8               | 标度为 `s` 的 ticks (自 epoch 起的 10^-s 秒)                                        | 小端序 Int64                                  |
| `DateTime64(s, tz)` | 8               | 与 `DateTime64(s)` 相同；timezone 属于 metadata                                   | 小端序 Int64                                  |
| `Time`              | 4               | 以秒为单位的有符号时长                                                                 | 小端序 Int32                                  |
| `Time64(s)`         | 8               | 标度为 `s` 的 ticks 表示的有符号时长                                                    | 小端序 Int64                                  |
| `Interval<Unit>`    | 8               | 有符号计数；单位信息包含在 type string 中                                                 | 小端序 Int64                                  |
| `UUID`              | 16              | 128 位标识符                                                                    | 两个经过字节交换的小端序 UInt64 半部 (参见 [UUID](#uuid))  |
| `IPv4`              | 4               | IPv4 地址                                                                     | 小端序 UInt32                                 |
| `IPv6`              | 16              | IPv6 地址                                                                     | 网络字节序，不交换                                  |
| `Enum8`             | 1               | 有符号 8 位整数 (Variant 索引)                                                      | 原始字节                                       |
| `Enum16`            | 2               | 有符号 16 位整数 (Variant 索引)                                                     | 小端序                                        |
| `Decimal(P, S)`     | 4 / 8 / 16 / 32 | 作为有符号整数的 `value × 10^S`；宽度取决于 P (≤9 → 4 B，≤18 → 8 B，≤38 → 16 B，≤76 → 32 B)  | 小端序有符号整数                                   |

<div id="integer-types">
  #### 整数类型
</div>

`UInt8`–`UInt256` 和 `Int8`–`Int256` 是整数值的直接二进制编码。解码器会读取 `bytes_per_row × num_rows` 个字节，并按相应类型进行解析。

一个 `UInt32` 列，包含 `[1, 256, 65536]`：

```text
01 00 00 00              row 0: 1
00 01 00 00              row 1: 256
00 00 01 00              row 2: 65536
```

一个保存 `[-1, 42]` 的 `Int32` 列：

```text
FF FF FF FF              row 0: -1
2A 00 00 00              row 1: 42
```

<div id="float32-and-float64">
  #### Float32 和 Float64
</div>

标准 IEEE 754 二进制浮点数：4 字节单精度 (`binary32`) 和 8 字节双精度 (`binary64`) ，两者均采用小端序。NaN、±Infinity、±0.0 和次正规数在写出后再读回时都能保持原样，无需归一化。

`Float32` 值 `1.5` (`0x3FC00000`) ：

```text
00 00 C0 3F              little-endian IEEE 754
```

`Float64` 值 `1.5` (`0x3FF8000000000000`):

```text
00 00 00 00 00 00 F8 3F  little-endian IEEE 754
```

<div id="bfloat16">
  #### BFloat16
</div>

brain-floating-point 格式：IEEE 754 `Float32` 的高 16 位——1 个符号位、8 个指数位、7 个尾数位。每个值占 2 字节，采用小端序，保存原始的 16 位模式。要还原数值，可将该模式放入高半部分，并将低半部分清零 (把 `bits << 16` 重新解释为 `Float32`) ，将其扩展回 `Float32`；扩展后的值随后与 `Float32` 具有相同的文本格式化方式。

`BFloat16` 值 `1.5` (模式为 `0x3FC0`，即 `Float32` `0x3FC00000` 的高半部分) ：

```text
C0 3F                    little-endian, widens to Float32 1.5
```

<div id="bool-type">
  #### Bool
</div>

与 `UInt8` 在线路格式上兼容：每行占 1 字节，`0x00` = `false`，`0x01` = `true`。在传输格式中，类型字符串字面值是 `Bool` (不是 `UInt8`) ，因此按类型字符串分派的解码器必须将其单独识别。

一个 `Bool` 列 `[true, false, true]`：

```text
01 00 01
```

<div id="date-and-date32">
  #### Date 和 Date32
</div>

两者都将日期编码为相对于 Unix 纪元 `1970-01-01` 的整数天数，且都不包含时间部分。

| 类型       | 字节 | 编码         | 范围                          |
| -------- | -- | ---------- | --------------------------- |
| `Date`   | 2  | 小端序 UInt16 | `1970-01-01` 到 `2149-06-06` |
| `Date32` | 4  | 小端序 Int32  | 有符号范围较大，支持 1970 年前          |

`Date` 值 `1970-01-02` (1 天) ：

```text
01 00                    UInt16 LE = 1
```

`Date32` 值 `1900-01-01` (-25567 天) ：

```text
21 9C FF FF              Int32 LE = -25567
```

<div id="datetime">
  #### 日期时间
</div>

与 `UInt32` 在线路格式上兼容：它是一个以秒为单位的 Unix timestamp，采用 4 字节小端序。该类型可能显示为 `DateTime` 或 `DateTime('Timezone')`；时区仅影响显示，不属于线路值的一部分。对于同一时刻，带有不同时区参数的两个 `DateTime` 列会生成完全相同的字节。解码器会去掉 `(...)` 参数后缀，并将该列按 `UInt32` 处理。

`DateTime('UTC')` 值 `2024-03-15 14:30:00 UTC` (timestamp `1710513000`) ：

```text
68 5B F4 65              UInt32 LE = 1710513000
```

<div id="datetime64">
  #### DateTime64(scale[, timezone])
</div>

8 字节，采用 little-endian Int64，表示自 Unix 纪元 以来、按 `10^-scale` 秒为单位的 ticks。`scale` 参数 (0–9) 位于 类型字符串 中，用于设置时间单位：

| 标度 | 时间粒度 | 常用名称 |
| -- | ---- | ---- |
| 0  | 1 秒  | 秒    |
| 3  | 1 毫秒 | ms   |
| 6  | 1 微秒 | µs   |
| 9  | 1 纳秒 | ns   |

该类型显示为 `DateTime64(s)` (隐式使用 server 默认时区) 或 `DateTime64(s, 'TimezoneName')` (显式指定时区，仅用于显示) 。负值表示 纪元 之前的 ticks。

`DateTime64(3, 'UTC')` 的值 `2024-01-15 12:30:45.123 UTC` (1705321845123 ms) ：

```text
83 51 1A 0D 8D 01 00 00  Int64 LE = 1705321845123
```

`DateTime64(0)` 值 `2024-01-15 12:30:45 UTC` (1705321845 s) :

```text
75 25 A5 65 00 00 00 00  Int64 LE = 1705321845
```

<div id="time-and-time64">
  #### Time 和 Time64(scale)
</div>

表示时长，而不是时间点。`Time` 是有符号的秒计数，4 字节小端序 Int32；`Time64(scale)` 是在给定十进制标度 (0–9) 下的有符号 tick 计数，8 字节小端序 Int64——其 wire 形态与 `DateTime64` 相同。

其文本形式为 `[-]HH:MM:SS[.fraction]`，但与 `DateTime` 不同，小时字段**不会**按 24 小时制回绕：它表示总小时数，并且可以超过 23。显示的最大值为 `999:59:59` (`3599999` 秒) ；更大的数值在渲染时会显示为该上限，且小数部分清零 (`999:59:59.000`) 。`CAST` 也会将存储的值钳制到这个范围内，不过算术运算可以产生超出范围的值，而这些值仅在显示时才会被钳制。这些都不会影响 wire 字节，它们就是普通的有符号整数。

`Time` 值 `45296` (`12:34:56`) ：

```text
F0 B0 00 00              Int32 LE = 45296
```

`Time64(3)` 值 `45296789` 个时间刻度 (`12:34:56.789`) ：

```text
95 2C B3 02 00 00 00 00  Int64 LE = 45296789
```

:::note
`Time` 和 `Time64` 仍处于 Experimental 阶段，需要在 server 上启用 `allow_experimental_time_time64_type = 1`。
:::

<div id="interval">
  #### Interval
</div>

`Interval<Unit>` — `IntervalSecond`、`IntervalMinute`、`IntervalHour`、`IntervalDay`、`IntervalWeek`、`IntervalMonth`、`IntervalQuarter`、`IntervalYear`、`IntervalNanosecond` 等。所有单位共用同一种 wire encoding：其计数值编码为有符号的 8 字节 little-endian Int64。单位**仅**存在于类型字符串中——它既不会改变 wire 字节，也不会改变其文本形式；文本形式就是裸整数。所有单位都由同一条解码路径处理。

`IntervalDay` 的值 `5`：

```text
05 00 00 00 00 00 00 00  Int64 LE = 5
```

<div id="uuid">
  #### UUID
</div>

每个值占 16 字节。其 wire 编码**不是**规范的 16 个大端序字节——而是将两个 8 字节半段分别独立进行字节反转。

其逻辑模型是一个 128 比特标识符，采用规范文本形式 `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`，其中各字节按惯例以大端序书写。wire 模型会取这 16 个规范字节，将其拆分为两个 8 字节半段，并将每个半段按小端序写入：

* Wire 字节 0..7 = 规范字节 0..7 反转后的结果。
* Wire 字节 8..15 = 规范字节 8..15 反转后的结果。

UUID `550e8400-e29b-41d4-a716-446655440000`：

```text
Canonical bytes (16):    55 0E 84 00 E2 9B 41 D4  A7 16 44 66 55 44 00 00

Wire bytes:
D4 41 9B E2 00 84 0E 55  high half byte-reversed
00 00 44 55 66 44 16 A7  low half byte-reversed
```

nil UUID (全为零) 在这两种表示中完全相同。

<div id="ipv4-and-ipv6">
  #### IPv4 和 IPv6
</div>

两种彼此相关但编码方式不同的地址类型。

`IPv4` 占 4 字节，编码为一个采用 little-endian 的 UInt32，用于存储规范的 32 位地址 (即由 `a.b.c.d` 得到的值 `(a << 24) | (b << 16) | (c << 8) | d`) 。wire 字节则是将网络字节序的字节倒序后得到的。

`192.168.1.10` (规范 32 位值为 `0xC0A8010A`) ：

```text
0A 01 A8 C0              Little-endian UInt32
```

`IPv6` 为 16 字节，**按网络字节序直接写入**，不进行字节交换——其字节序与 `inet_pton(AF_INET6, ...)` 相同。

`2001:db8::1`:

```text
20 01 0D B8 00 00 00 00  network bytes 0..7
00 00 00 00 00 00 00 01  network bytes 8..15
```

这种不对称性是有意设计的：IPv4 以 `u32` 形式存储，便于进行算术运算和紧凑的范围查询；而 IPv6 则保留了大多数网络 API 常用的网络字节序布局。

<div id="enum8-and-enum16">
  #### Enum8 and Enum16
</div>

分别与 `Int8` 和 `Int16` 在线路格式上兼容：每行占 1 或 2 个字节，其中 16 位 Variant 采用二进制补码小端序。完整的 Variant 映射位于类型字符串中：

```text
Enum8('active' = 1, 'inactive' = 2, 'banned' = -1)
Enum16('a' = 1, 'b' = 30000)
```

解码器可能会去掉 `(...)` 参数后缀，并按 `Int8` / `Int16` 分发——wire 字节只是整数索引。若某个 client 需要显示标签，就会从类型字符串中解析出 `'name' = value` 映射，并将其与该列一同保留：仅凭整数本身无法还原标签。面向文本的输出显示的是标签 (`active`) 而非索引；当枚举嵌套在复合类型中时，则会使用单引号 (`'active'`) 。由于无法从整数列中恢复该映射，因此对于 `Array(Enum8(...))` 或 `Map(Enum16(...), V)` 这类嵌套枚举，必须保留该映射。

一个 `Enum8('active' = 1, 'inactive' = 2)` 列 `[active, inactive, active]`：

```text
01 02 01
```

`Enum16(...)` 类型的值 `30000`：

```text
30 75                    Int16 LE = 30000
```

<div id="decimal">
  #### Decimal(P, S)
</div>

按 10 的幂缩放的有符号整数。该整数的字节宽度由 **精度** `P` 隐含决定；**标度** `S` 是负指数 (即小数点后的位数) 。二者都体现在类型字符串中。

| 精度 (P)      | 底层整数   | 字节数 |
| ----------- | ------ | --- |
| 1 ≤ P ≤ 9   | Int32  | 4   |
| 10 ≤ P ≤ 18 | Int64  | 8   |
| 19 ≤ P ≤ 38 | Int128 | 16  |
| 39 ≤ P ≤ 76 | Int256 | 32  |

传输格式编码就是采用 little-endian 二进制补码表示的底层整数，而逻辑上的十进制值为 `wire_integer × 10^(-S)`。

无论类型最初如何声明，ClickHouse 始终都会输出 `Decimal(P, S)`。`Decimal32(S)`、`Decimal64(S)` 等各种写法，在传输格式中都会规范化为 `Decimal(P, S)` (其中 `P` 设为该宽度的自然最大值：9、18、38、76) 。只识别 `Decimal(P, S)` 的解码器，就能覆盖服务器输出的所有写法。

`Decimal(9, 4)` 的值 `123.4567` → 底层整数 `1234567`：

```text
87 D6 12 00              Int32 LE = 1234567
```

`Decimal(18, 1)` 值 `-1.5` → 对应的底层整数 `-15`：

```text
F1 FF FF FF FF FF FF FF  Int64 LE = -15
```

`Decimal(38, 4)` 值 `123.4567` (共 16 字节) ：

```text
87 D6 12 00 00 00 00 00 00 00 00 00 00 00 00 00
```

<div id="nothing">
  #### Nothing
</div>

`Nothing` 类型不包含任何值。实际上，它只会作为 `Nullable(Nothing)` 的内部类型出现——也就是 server 对 `SELECT NULL` 这类表达式返回的类型，因为这类表达式唯一合法的值就是“没有值”。从概念上讲，它是一种单元类型。

在线上传输中，它**每行**恰好占用 **一个占位字节**。server 会输出 ASCII 字符 `'0'` (`0x30`) ，但反序列化器会忽略这些字节——其内容是未定义的，解码器 不得依赖任何特定值。写入的总字节数为 `num_rows × 1`，因此列头中的 `num_rows` 就完全决定了需要读取多少字节。

这种每行一个字节的设计保持了 Block 的不变式：每一列都具有一个可由 `num_rows` 推导出的长度，因此 解码器 可以直接向前扫描，而不需要为每个单元添加长度前缀。外层的 `Nullable` 始终将每个位置标记为 NULL，因此这些占位字节永远不会被检查。

一个包含 3 行 (全部为 NULL) 的 `Nullable(Nothing)` 列：

```text
01 01 01                 null map: 1, 1, 1 (three NULLs)
30 30 30                 Nothing placeholder bytes (one per row)
```

null-map 前缀采用标准的 `Nullable` 帧格式 (参见 [Nullable](#nullable)) ；内部的三个字节是 `Nothing` 载荷，解码器会将其跳过。

<div id="variable-length-types">
  ### 变长类型
</div>

在线上传输时，每个值都自带长度信息。

<div id="string-type">
  #### String
</div>

类型字符串：`String`。`String` 列由 `num_rows` 个带长度前缀的字节序列构成：

```text
[VarUInt: byte_length] [byte_length bytes: raw value]
[VarUInt: byte_length] [byte_length bytes: raw value]
...
```

除长度前缀外，行与行之间没有其他分隔符，也没有行级状态。空字符串占用单个 `0x00` 字节。ClickHouse 的 `String` 是面向字节而非面向文本的：不会强制校验 UTF-8 的有效性，而且某个值可以包含任意字节，包括嵌入的 NUL。面向 UTF-8 字符串类型的 解码器 要么在读取时进行校验，要么向调用方暴露原始字节。该列占用的总字节数为所有行的 `Σ (varuint_size(len_i) + len_i)` 之和。

一列包含 3 个字符串 `["ab", "", "c"]` (总计 6 字节) ：

```text
02 61 62                 row 0: length 2, "ab"
00                       row 1: length 0, empty
01 63                    row 2: length 1, "c"
```

<div id="fixedstring">
  #### FixedString(N)
</div>

类型字符串：`FixedString(N)`，其中 `N` 为正整数 (例如 `FixedString(16)`) 。该列恰好由 `N × num_rows` 个原始字节组成，没有长度前缀，也没有分隔符。解码器会从类型字符串中解析出 `N`，并为每一行读取对应数量的字节。

当 SQL 插入的值短于 `N` 字节时 (例如 `CAST('abc' AS FixedString(5))`) ，服务器会在右侧用 NUL 字节 (`0x00`) 填充到声明的长度。这些填充字节是存储值的一部分，并会在传输时按原样发送；是否裁剪由客户端侧处理。与 `String` 一样，`FixedString(N)` 更像字节数组而非文本——通常用于定宽标识符、地址字节或哈希摘要。

两个 `FixedString(3)` 值 `["abc", "de\0"]` (共 6 字节) ：

```text
61 62 63                 row 0: 3 bytes, "abc"
64 65 00                 row 1: 3 bytes, "de" + NUL padding
```

比较这两种字符串类型：

| 属性          | `String`     | `FixedString(N)` |
| ----------- | ------------ | ---------------- |
| 每行长度前缀      | 是 (VarUInt)  | 否                |
| 行大小         | 可变           | 恰好为 `N` 字节       |
| 整列字节总数      | 可变           | `N × num_rows`   |
| NUL 字节填充    | 不适用          | 由 server 在右侧填充   |
| 是否预期为 UTF-8 | 通常是 (不强制)    | 否 (按原始字节处理)      |
| 类型参数        | 无            | 必需，为整数 `N`       |

<div id="composite-types">
  ### 复合类型
</div>

复合类型会包装一个或多个内部类型，并共享一种通用的 wire 模型：**每列包含多个流**。单个逻辑列会被编码为两个或多个可独立读取的字节序列，并拼接在一起。

它们有三个共同的结构特性：

* **每个 schema 的形态固定。** 其结构在解码时完全由类型字符串决定。`Array(UInt32)` 的流布局始终相同，不会因块而异。
* **自身不带版本前缀。** 复合包装器本身不会添加版本字节；其 framing (offsets、null map、元素流) 在各个 ClickHouse 发行版之间保持稳定。这一点只适用于 *wrapper* 本身——关于内部版本化类型，请参见下文的 前缀阶段 说明。
* **自身不保留跨块状态。** 包装器的 framing 在每个块内都是完全 self-describing 的；任何与跨块状态有关的问题都来自内部版本化类型，而不是包装器本身。

复合类型是递归的——内部类型本身也可能是复合类型。

**数据流之前的前缀阶段。** 读取一列分为两个阶段，顺序如下：先是 **state-前缀阶段**，然后是 **data-stream phase**。复合包装器自身没有前缀字节，但它会在写入自身数据流之前，把 前缀阶段 *委托* 给其内部序列化：`SerializationArray` 会在写入数组 offsets 之前先运行其内部类型的 前缀阶段，`Tuple`、`Map`、`Nested` 和 `Nullable` 也会通过各自的元素序列化执行相同操作 (`Nullable` 会在其 null map 之前运行内部前缀) 。

因此，当复合类型包装的是 [版本化/有状态类型](#versioned-types) (`LowCardinality`、`Variant`、`Dynamic`、`JSON`) 时，该内部类型的版本/状态前缀会*优先*写出，位于包装器的 offsets 和元素载荷之前。例如，`Array(LowCardinality(String))` 的布局是 `[LowCardinality state prefix]` → `[array offsets]` → `[flattened LowCardinality element payload]`，而不是先写 offsets。

如果解码器在运行内部 前缀阶段 之前就先读取 offsets，那么在处理任何包含 `LowCardinality`、`Variant`、`Dynamic` 或 `JSON` 的复合类型时都会失去同步。当所有内部类型都是普通叶子类型或其他非版本化复合类型时，前缀阶段 不会输出任何字节，此时下文关于 offsets-first 的描述可直接原样适用。

<div id="nullable">
  #### Nullable(T)
</div>

类型字符串：`Nullable(InnerType)`。示例：`Nullable(UInt32)`、`Nullable(String)`、`Nullable(FixedString(16))`、`Nullable(DateTime('UTC'))`。

与其他复合类型一样，`Nullable` 会先将[前缀阶段](#composite-types)委托给其内部类型的序列化，再写入 null map：当内部类型是版本化的时，会**先**输出内部类型的状态前缀。因此，`Nullable(Tuple(LowCardinality(String)))` 是以 `LowCardinality` 的状态前缀开头，而不是 null map。当内部类型是叶子类型或其他非版本化类型时，前缀阶段不会输出任何字节。

传输布局由内部前缀阶段 (除非内部类型是版本化的，否则为空) 以及随后拼接的两个流组成，其中 null-map 在前：

```text
[inner type's state prefix]   empty for leaf/non-versioned inners; emitted first when the inner is versioned
[null-map stream]             num_rows × UInt8
[values stream]               inner type's encoding for num_rows values
```

null-map 的大小恰好是 `num_rows` 字节，每行对应一个字节：

| 字节值                 | 含义                                 |
| ------------------- | ---------------------------------- |
| `0x00`              | 该行有值。                              |
| 非零值 (规范形式为 `0x01`)  | 该值为 NULL。values stream 中对应的字节是占位符。 |

values stream 包含内部类型对 **全部** `num_rows` 行的标准编码，包括为 null 位置写入的内容。解码器 仍然必须读取 null 位置的占位符字节以推进 stream，但在解释任何单个值之前，必须先查看 null-map。发送方可以在 null 位置写入任意字节，因此 解码器 不能依赖某个特定的占位符值。

按内部类型家族划分的占位符值：

| 内部类型家族                                          | null 位置的占位符        |
| ----------------------------------------------- | ------------------ |
| Fixed-width (UInt/Int/Float/DateTime/UUID/etc.) | 按该类型宽度用零初始化的字节     |
| `String`                                        | 空字符串——单个 `0x00` 字节 |
| `FixedString(N)`                                | `N` 个零字节           |
| `Array(T)`                                      | 空数组——offsets 不前进   |
| `Tuple(T1, T2, ...)`                            | 每个元素使用各自的占位符       |

`Nullable(T)` 可以出现在 `Array`、`Tuple`、`Map` 和 `Nested` 内部——`Array(Nullable(T))` 和 `Tuple(Nullable(T1), T2)` 很常见。Nullable 不能与自身嵌套组合：`Nullable(Nullable(T))` 会被 server 拒绝。

一个有三行 `[5, NULL, 9]` 的 `Nullable(UInt8)` (总计 6 字节) ：

```text
00 01 00                 null-map: present, null, present
05 00 09                 values:   5, placeholder, 9
```

一个包含 3 行 `["hello", NULL, "world"]` 的 `Nullable(String)` (总计 15 字节) ：

```text
00 01 00                 null-map
05 'h' 'e' 'l' 'l' 'o'   row 0: "hello"
00                       row 1: placeholder (empty string)
05 'w' 'o' 'r' 'l' 'd'   row 2: "world"
```

<div id="array">
  #### Array(T)
</div>

类型字符串：`Array(InnerType)`。示例：`Array(UInt32)`、`Array(String)`、`Array(Nullable(UInt32))`、`Array(Array(UInt8))`。

传输布局由内部 [前缀阶段](#composite-types) (除非内部类型是版本化的，否则为空) 以及随后拼接的两个流组成，其中偏移量流在前：

```text
[inner type's state prefix]   empty for leaf/non-versioned inners; emitted first when the inner is versioned
[offsets stream]              num_rows × UInt64 LE
[values stream]               inner type's encoding for offsets[num_rows - 1] values
```

offsets 流恰好由 `num_rows` 个按小端序编码的 UInt64 值组成，每个值都是该行元素之后 values 流中的**累计结束位置**：

* 行 `N` 的元素起始索引 = `offsets[N - 1]` (或当 `N == 0` 时为 `0`) 。
* 行 `N` 的元素结束索引 (exclusive)  = `offsets[N]`。
* 行 `N` 的元素个数 = `offsets[N] - offsets[N - 1]`。

因此，`offsets[num_rows - 1]` 就是所有行的元素总数，而 values 流中则按首尾相连的方式拼接存放了这么多个内层值。

offsets 必须是**单调非递减**的；连续两个相等的 offset 表示空行，解码器应将非单调的 offset 视为损坏并拒绝处理。空列 (`num_rows == 0`) 会写入零字节——既没有 offsets 流，也没有 values 流。内层类型可以是任意类型，包括其他复合类型：`Array(Array(T))`、`Array(Tuple(...))` 和 `Array(Nullable(T))` 都是合法的。

包含行 `[[10, 20, 30], [], [40, 50]]` 的 `Array(UInt32)` (总计 44 字节) ：

```text
Offsets (3 × UInt64 LE = 24 bytes):
03 00 00 00 00 00 00 00      offsets[0] = 3
03 00 00 00 00 00 00 00      offsets[1] = 3 (empty row)
05 00 00 00 00 00 00 00      offsets[2] = 5

Values (5 × UInt32 LE = 20 bytes):
0A 00 00 00                  10
14 00 00 00                  20
1E 00 00 00                  30
28 00 00 00                  40
32 00 00 00                  50
```

每个 offset 都表示共享值流中某一行切片累计的*结束*位置；起始位置是前一个 offset (第 `0` 行则为 `0`) 。如果连续两个 offset 相等，则表示空行：

```mermaid
flowchart LR
    subgraph V["values stream: [10, 20, 30, 40, 50]"]
        direction LR
        v0["10"] --- v1["20"] --- v2["30"] --- v3["40"] --- v4["50"]
    end
    r0["row 0"] -->|"[0 .. offsets[0]=3)"| v0
    r1["row 1"] -.->|"[3 .. offsets[1]=3) empty"| V
    r2["row 2"] -->|"[offsets[1]=3 .. offsets[2]=5)"| v3
```

`Array(String)`，行数据为 `[["a", "bb"], []]` (总计 20 字节) ：

```text
Offsets (2 × UInt64 LE = 16 bytes):
02 00 00 00 00 00 00 00      offsets[0] = 2
02 00 00 00 00 00 00 00      offsets[1] = 2 (empty row)

Values (2 strings, 4 bytes total):
01 'a'                       row's first string: "a"
02 'b' 'b'                   row's second string: "bb"
```

行数据为 `[[[1,2]], [], [[3], [4,5]]]` 的 `Array(Array(UInt32))` 具有相同的嵌套形态：

* 外层 offsets：`[1, 1, 3]` — 第 0 行有 1 个内层数组，第 1 行有 0 个，第 2 行有 2 个。
* 中间层 `Array(UInt32)` 解码为 3 行，offsets 为 `[2, 3, 5]`。
* 最内层 `UInt32` 解码为 5 个值：`[1, 2, 3, 4, 5]`。

总共是 24 (外层偏移量) + 24 (中层偏移量) + 20 (值) = 68 字节。

<div id="tuple">
  #### Tuple(T1, T2, ...)
</div>

类型字符串：`Tuple(T1, T2, ..., Tn)`。示例：`Tuple(UInt32, String)`、`Tuple(Int32)`、`Tuple(Array(UInt32), String)`、`Tuple(UInt8, Tuple(Int32, String))`。ClickHouse 还支持通过 `Tuple(a UInt32, b String)` 定义**命名元组**；名称仅用于元数据，不会影响传输格式。

传输布局由各元素的[前缀阶段](#composite-types)组成 (每个版本化元素都会按声明顺序提供其状态前缀；非版本化元素则为空) ，随后是按声明顺序拼接的 *N* 个流，每种元素类型对应一个：

```text
[element state prefixes]   in declaration order; empty unless an element type is versioned
[stream for T1]    inner T1's encoding for num_rows values
[stream for T2]    inner T2's encoding for num_rows values
 ...
[stream for Tn]    inner Tn's encoding for num_rows values
```

每个流都恰好编码 `num_rows` 个值。没有长度前缀，没有 offsets 流，流与流之间也没有分隔符。空列 (`num_rows == 0`) 在每个流中都会写入零字节。元素类型可以是任意类型，包括其他复合类型——`Tuple(Tuple(...), ...)`、`Tuple(Array(...), ...)` 和 `Tuple(Nullable(T1), T2)` 都是合法的。

零元素元组 `Tuple()` 也是合法的——它会由 `SELECT tuple()` 或 `CAST(x AS Tuple())` 这样的表达式产生。由于它没有元素流，因此会像 [Nothing](#nothing) 那样序列化：**每行一个占位字节 (`0x30`，ASCII `'0'`)&#x20;**，反序列化器会将其丢弃。行数来自块头部，这一点与 `Nothing` 完全相同。

具有 3 行 `(1,4), (2,5), (3,6)` 的 `Tuple(UInt8, UInt8)`：

```text
Element 0 stream (3 × UInt8 = 3 bytes):
01 02 03

Element 1 stream (3 × UInt8 = 3 bytes):
04 05 06
```

这种布局**不是**按行主序排列：将原始字节读回后，元素 0 得到的是 `[1, 2, 3]`，元素 1 得到的是 `[4, 5, 6]`。

`Tuple(UInt32, String)` 包含 2 行 `(10, "a")`、`(20, "bb")` (共 13 字节) ：

```text
Element 0 stream (2 × UInt32 LE = 8 bytes):
0A 00 00 00                  10
14 00 00 00                  20

Element 1 stream (2 strings, 5 bytes total):
01 'a'                       "a"
02 'b' 'b'                   "bb"
```

<div id="map">
  #### Map(K, V)
</div>

类型字符串：`Map(KeyType, ValueType)`。示例：`Map(String, UInt32)`、`Map(String, Array(UInt32))`、`Map(UInt8, Tuple(Int32, String))`、`Map(Array(String), Int8)`。传输格式对这两种类型没有任何限制——`K` 和 `V` 都可以是任何受支持的类型，包括复合类型。 (ClickHouse 在 SQL 层面对允许使用的键类型的规则在不同发行版之间有所变化；请查阅目标服务器版本对应的 SQL 文档。)

传输布局与 `Array(Tuple(K, V))` 在字节级别完全一致，因此它从内部的 [前缀阶段](#composite-types) 开始 (除非 `K` 或 `V` 是版本化的，否则该阶段为空) ：

```text
[K/V state prefixes]   from the inner Tuple's prefix phase; empty unless K or V is versioned
[offsets stream]    num_rows × UInt64 LE                   ← from Array
[keys stream]       K's encoding for total_pairs values    ┐ from Tuple's
[values stream]     V's encoding for total_pairs values    ┘ per-element streams
```

其中 `total_pairs = offsets[num_rows - 1]` (当 `num_rows == 0` 时为 `0`) 。offsets 流的语义与 [Array](#array) 相同。keys 与 values 按位置一一对应：第 `i` 对为 `(keys[i], values[i])`。

ClickHouse 中 Map 列的内存表示是元组数组；类型系统将其作为一种独立类型呈现，以提升 SQL 的易用性 (`m['key']`、`mapKeys`、`mapValues`) 。其传输格式是该存储表示的直接序列化，因此 `Map` 和 `Array(Tuple(K, V))` 在字节级别完全可以互换。

offsets 是单调不减的，并且 keys 和 values 流都恰好包含 `total_pairs` 个值。空列会写入零字节。在单行内，keys 通常是唯一的，但这是一条语义规则，而不是传输格式强制规定的规则：传输格式允许重复键在往返读写后保持不变，而服务端语义只有在某个支持 Map 的函数消费该行时才会解析重复键。

包含 2 行 `{1:10, 2:20}`、`{3:30}` 的 `Map(UInt8, UInt8)` (总计 22 字节) ：

```text
Offsets (2 × UInt64 LE = 16 bytes):
02 00 00 00 00 00 00 00      offsets[0] = 2
03 00 00 00 00 00 00 00      offsets[1] = 3

Keys (3 × UInt8 = 3 bytes):
01 02 03                     keys: 1, 2, 3

Values (3 × UInt8 = 3 bytes):
0A 14 1E                     values: 10, 20, 30
```

键和值分别存储在不同的流中，而不是交错存储——第 `i` 对是通过同时读取 `keys[i]` 和 `values[i]` 还原出来的。

包含 1 行 `{'a':1, 'b':2}` 的 `Map(String, UInt32)` (总共 20 字节) ：

```text
Offsets (1 × UInt64 LE = 8 bytes):
02 00 00 00 00 00 00 00      offsets[0] = 2

Keys (2 strings, 4 bytes total):
01 'a'                       "a"
01 'b'                       "b"

Values (2 × UInt32 LE = 8 bytes):
01 00 00 00                  1
02 00 00 00                  2
```

<div id="nested">
  #### Nested(name1 T1, name2 T2, ...)
</div>

`Nested` 的线上传输表示形式取决于服务端的 `flatten_nested` 设置，因此分为两种不同情况。

```mermaid
flowchart TD
    N["column declared Nested(a T1, b T2, ...)"]
    N --> Q{"flatten_nested?"}
    Q -->|"= 1 (server default)"| A["N parallel Array(T_i) columns<br/>with dotted names (n.a, n.b)<br/>— no Nested wire type"]
    Q -->|"= 0"| B["one column, type string Nested(...)<br/>laid out byte-identically to<br/>Array(Tuple(T1, ..., Tn))"]
```

**情况 A：`flatten_nested = 1` (服务器默认值) 。** 当在默认设置下创建该表时，`Nested` **不是一种线上传输类型**。服务器会将该列存储并显示为 N 个并行的 `Array(T_i)` 列，并使用**点分名称** (`outer.field1`、`outer.field2` 等) 。对于格式层来说，没有任何新变化——每个点分列都是一个常规的 [Array](#array)：

```text
DESCRIBE TABLE t   -- t has column n Nested(a UInt8, b String)
id     UInt8
n.a    Array(UInt8)
n.b    Array(String)
```

**情况 B：`flatten_nested = 0`。** 当创建表时使用了 `flatten_nested = 0`，该列在线上传输时表现为单个列，其 类型字符串 为 `Nested(name1 T1, name2 T2, ...)`；并且在 类型字符串 之后，其布局与 **`Array(Tuple(T1, T2, ..., Tn))` 完全字节级一致** —— 包括内部的 [前缀阶段](#composite-types)，因此任何版本化字段 `T_i` 都会先输出其 state prefix，再输出 offsets。下面的示例使用的是非版本化字段，因此 前缀阶段 为空：

```text
Nested(a UInt8, b String) bytes (after type string):
  02 00 00 00 00 00 00 00       offsets[0] = 2
  03 00 00 00 00 00 00 00       offsets[1] = 3
  0A 14 1E                       UInt8 stream
  01 'x' 01 'y' 01 'z'           String stream

Array(Tuple(a UInt8, b String)) bytes (after type string):
  02 00 00 00 00 00 00 00       offsets[0] = 2
  03 00 00 00 00 00 00 00       offsets[1] = 3
  0A 14 1E                       UInt8 stream
  01 'x' 01 'y' 01 'z'           String stream
```

唯一的区别在于类型字符串：`Nested` 会保留字段名 (`a`、`b`) ，而 `Array(Tuple)` 不会将它们保留为具名槽位。

情况 B 的类型字符串是一个由逗号分隔的 (名称、类型) 对列表。第一个空白字符用于分隔名称和类型；类型本身可能还包含额外的空白、逗号和括号，因此解析时需要使用与 `Tuple` 相同的、能感知嵌套深度的分隔器。传输布局：

```text
[offsets stream]    num_rows × UInt64 LE                       ← from Array
[field1 stream]     T1's encoding for total_elements values    ┐ from Tuple's
[field2 stream]     T2's encoding for total_elements values    │ per-element
 ...                                                            │ streams
[fieldn stream]     Tn's encoding for total_elements values    ┘
```

其中 `total_elements = offsets[num_rows - 1]` (或在 `num_rows == 0` 时为 `0`) 。offsets 单调不减，并且每个字段流都恰好包含 `total_elements` 个值。服务器会在 INSERT 时强制检查：在同一行内，所有字段包含的元素个数必须相同。空列会写入零字节。

`Nested(a UInt8, b String)`，包含 2 行 `[(10,'x'),(20,'y')]` 和 `[(30,'z')]` (类型字符串之后占 25 字节) ：

```text
Offsets (2 × UInt64 LE = 16 bytes):
02 00 00 00 00 00 00 00      offsets[0] = 2
03 00 00 00 00 00 00 00      offsets[1] = 3

Field 'a' stream (3 × UInt8 = 3 bytes):
0A 14 1E                     10, 20, 30

Field 'b' stream (3 strings, 6 bytes):
01 'x' 01 'y' 01 'z'         "x", "y", "z"
```

<div id="type-aliases">
  ### 类型别名
</div>

有些类型只是纯别名：server 在列头中发送的是别名名称，但后续字节实际对应的是某个底层类型。解码器 会将该别名映射到底层类型，并复用其 codec——不会引入新的传输格式。

地理类型会别名到底层的嵌套数组和 Tuple：

| 类型字符串                  | 底层传输类型                    |
| ---------------------------- | ------------------------- |
| `Point`                      | `Tuple(Float64, Float64)` |
| `Ring`, `LineString`         | `Array(Point)`            |
| `Polygon`, `MultiLineString` | `Array(Ring)`             |
| `MultiPolygon`               | `Array(Polygon)`          |

因此，`Point` 列的解码方式与 `Tuple(Float64, Float64)` 完全相同 (显示为 `(1,2)`) ；`Ring` 则与 `Array(Tuple(Float64, Float64))` 完全相同 (`[(0,0),(1,1)]`) ；其余类型也可依此沿层级逐级推导。

`Geometry` 也是别名，但它别名到的不是嵌套数组，而是 [`Variant`](#variant)：它的载荷是上述六种 geo types 构成的 variant。列头中只携带类型字符串 `Geometry`——**不会**展开写出该 variant——因此 解码器 必须自行将其展开。与任何 `Variant` 一样，判别值遵循 geo 别名按规范名称排序后的顺序：`0` = `LineString`，`1` = `MultiLineString`，`2` = `MultiPolygon`，`3` = `Point`，`4` = `Polygon`，`5` = `Ring`。随后，每个选中的值都会通过上面对应的 geo 别名解码 (`NULL` 使用 `Variant` 的 `NULL` 判别值 `255`) 。

`SimpleAggregateFunction(func, T)` 是其值类型 `T` 的别名。它存储的是已完成最终计算的聚合值，因此其传输形式和显示结果与 `T` 完全相同 (`SimpleAggregateFunction(sum, UInt64)` 会按 `UInt64` 解码) 。只有这种单值类型形式才属于这种别名；其底层类型本身也可以是复合类型。

:::note
有两个相关类型**不是**别名。它们都是有效的 `Native` 列类型——例如，client 可以从 `-State` 组合器或分布式聚合中接收到 `AggregateFunction` 列——但它们各自都携带专用载荷，不在本页讨论范围内：

* `AggregateFunction(func, ...)` 保存的是*中间*聚合状态 (不是最终值) ；其二进制布局取决于具体 aggregate function 和版本。
* `QBit(T, N[, stride])` 存储的是一个向量，其比特平面会为了向量搜索 workload 而转置；它的线上传输 stream 布局 (按组优先的 `FixedString` 比特平面流，共 `element_size * (N / stride)` 个，并显式带有 `stride`) 以及它的二进制类型编码 (标签为 `0x36`，或者当 `stride != N` 时为 `0x37` `QBitWithStride`) 已在 [`QBit` 数据类型页面](/zh/sql-reference/data-types/qbit) 和 [binary type encoding](/zh/sql-reference/data-types/data-types-binary-encoding) Reference 中说明，因此 `Native` reader 无需再从 C++ 源码中自行还原这些信息。
  :::

<div id="versioned-types">
  ### 版本化类型
</div>

版本化类型带有一个线上传输的序列化版本前缀，用于声明后续编码采用的是哪种变体。它们也可能使用多个流 (类似复合类型) 。在 `Native` 线上传输格式中，该前缀以及任何字典都是按块划分的——这些类型不维护跨块状态 (请参见下方的[按块前缀说明](#serialization-version-concept)) ；跨块序列化状态仅存在于 MergeTree 磁盘 stream 中。

与固定形态的复合类型相比，这些类型要复杂得多，因此面向简单分析查询的客户端可以暂时不处理它们。

<div id="serialization-version-concept">
  #### 序列化版本：概念
</div>

**序列化版本**是按类型、按列区分的线上传输版本号，用于表明发送方正在使用某种类型编码的哪个变体。它是该列状态前缀中的第一个内容，因此解码器会先读取它，再将该列其余部分交给相应的解析器处理。

它与协议版本不同：

| 维度     | 协议版本           | 序列化版本 (本节)     |
| ------ | -------------- | -------------- |
| 范围     | 整个连接           | 按类型、按列         |
| 是否协商   | 是，在握手时         | 否——发送方写入，接收方读取 |
| 控制内容   | 哪些数据包级特性处于启用状态 | 单个类型使用哪种线上传输变体 |
| 读取是否必需 | 是              | 是，对每个版本化列都必需   |

大多数版本化类型都会在任何其他状态前缀数据之前，先将版本写为小端序 UInt64；少数则使用 VarUInt 或 UInt8。解码器会先读取版本，并拒绝未知值——更高的版本意味着发送方使用了更新的格式，而解码器并不理解；一旦误解析，后续的每一个字节都会被误读。

状态前缀会在**每个行数大于零的块**开头输出，紧接在该块的载荷之前。

Native 写入器和读取器**不会**跨块保留序列化状态：`NativeWriter` 会为它写入的每个非空列块创建全新的序列化状态并写入状态前缀，而 `NativeReader` 会为它读取的每个非空块创建全新的反序列化状态并读取它 (当 `rows == 0` 时，两者都会完全跳过该前缀) 。

因此，头部块 (rows = 0) 和空块都不会输出任何内容，解码器必须在每个非空块开头重新读取状态前缀。如果解码器只读取一次前缀，并将后续块都视为仅包含载荷，它就会把下一个块的前缀当作数据来读取，从而失去同步：

```mermaid
sequenceDiagram
    participant S as Server (writer)
    participant C as Client (decoder)
    S->>C: Header block (num_rows = 0)
    Note right of C: no state prefix
    S->>C: First block with rows > 0
    Note right of C: read state prefix,<br/>then block payload
    S->>C: Next block with rows > 0
    Note right of C: read state prefix again,<br/>then block payload
    S->>C: Empty block (end marker)
    Note right of C: no state prefix
```

<div id="serialization-version-reference">
  #### 序列化版本参考
</div>

| 类型                                                                          | 字段宽度      | 值   | 名称                                     | 含义                                              |
| --------------------------------------------------------------------------- | --------- | --- | -------------------------------------- | ----------------------------------------------- |
| **Object** (JSON 的基础类型)                                                     | UInt64 LE | `0` | `V1`                                   | 原始编码。包含 `max_dynamic_paths` 参数和动态路径列表。          |
|                                                                             |           | `1` | `STRING`                               | 原生格式兼容模式——Object 以单个 `String` 列传输，其中包含 JSON 文本。 |
|                                                                             |           | `2` | `V2`                                   | V1 布局，但不包含 `max_dynamic_paths` 参数。              |
|                                                                             |           | `3` | `FLATTENED`                            | 原生格式兼容模式——扁平化路径表示。                              |
|                                                                             |           | `4` | `V3`                                   | 在 V2 的基础上增加 shared-data 序列化版本子字段和 STATISTICS 标志。        |
| **Object shared data** (Object `V3` 中使用的子 stream)                           | VarUInt   | `0` | `MAP`                                  | 共享数据编码为 `Map(String, String)`。                  |
|                                                                             |           | `1` | `MAP_WITH_BUCKETS`                     | 与 `MAP` 相同，但会拆分为 N 个桶以提高扫描效率。                   |
|                                                                             |           | `2` | `ADVANCED`                             | 紧凑粒度格式，路径 / 标记 / 元数据分别使用独立流。                    |
| **Dynamic**                                                                 | UInt64 LE | `1` | `V1`                                   | 原始编码。包含 `max_dynamic_types` 和运行时 Variant 类型列表。  |
|                                                                             |           | `2` | `V2`                                   | V1 去掉了 `max_dynamic_types` 参数。                  |
|                                                                             |           | `3` | `FLATTENED`                            | 原生格式兼容模式。                                       |
|                                                                             |           | `4` | `V3`                                   | 在 V2 的基础上增加二进制编码的 Variant 类型名称以及空 STATISTICS 支持。        |
| **Variant** 判别值模式                                                           | UInt64 LE | `0` | `BASIC`                                | 每一行的判别值都会按原样写入。                                 |
|                                                                             |           | `1` | `COMPACT`                              | 如果一个粒度中的所有行共享同一个判别值，则只写入单个值 + 粒度标记。             |
| **Variant** 粒度格式 (当模式为 `COMPACT` 时)                                         | UInt8     | `0` | `PLAIN`                                | 粒度中的判别值不全相同。                                    |
|                                                                             |           | `1` | `COMPACT`                              | 粒度中的所有行共用一个判别值。                                 |
| **LowCardinality** 键序列化                                                     | Int64     | `1` | `sharedDictionariesWithAdditionalKeys` | 当前定义的唯一版本。                                      |
| **JSON-as-String** 回退机制 (启用 `output_format_native_write_json_as_string` 时)  | UInt64 LE | `1` | `JSONStringSerializationVersion`       | JSON 列会以 `String` 列形式传输，并以前缀标识开头。               |

关于这张表，有几点值得注意：

* **这些值不是连续的。** `Dynamic` 使用 `1`、`2`、`3`、`4`，其中 `V3` 是 `4`，`FLATTENED` 是 `3`。数值更大并不一定代表版本更新。
* **有些值仅用于原生格式。** `Object::STRING`、`Object::FLATTENED` 和 `Dynamic::FLATTENED` 的存在，是为了与未实现完整 Object/Dynamic 的客户端保持原生协议兼容。它们不会出现在 MergeTree 的磁盘存储中。
* **`V3` 主要用于磁盘存储。** 使用原生 TCP 协议的客户端通常看到的是 `FLATTENED` (值为 `3`) ，而不是 `V3` (值为 `4`) 。

<div id="lowcardinality">
  #### LowCardinality(T)
</div>

最简单的版本化类型。它会将一列包含 `N` 个内部值的数据，替换为一个仅包含唯一值的小型字典，以及指向该字典的 `N` 个索引。

类型字符串：`LowCardinality(InnerType)`。示例：`LowCardinality(String)`、`LowCardinality(FixedString(4))`、`LowCardinality(Nullable(String))`。

```text
[per block with rows > 0]:
  [8 bytes:  Int64 LE state prefix = 1]             ← repeated at the start of every non-empty block
  [8 bytes:  UInt64 LE metadata]                    ← key type code (low byte) + flag bits
  [8 bytes:  UInt64 LE dict_size]                   ← number of dict entries (incl. placeholder slot)
  [N bytes:  dict values]                           ← inner type's encoding for dict_size values
  [8 bytes:  UInt64 LE keys_count]                  ← number of values at this recursive level (see below)
  [K bytes:  keys]                                  ← (1 << key_type_code) bytes per key
```

状态前缀 (Int64 LE = 1) 是唯一已定义的版本，即 `sharedDictionariesWithAdditionalKeys`；其他值均为保留值。

每个块的元数据 UInt64 是一个位字段：

| 位范围          | 含义                                                                                                                                                                                                                                            |
| ------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 0..7         | 键类型编码：`0` = UInt8，`1` = UInt16，`2` = UInt32，`3` = UInt64。会选择能够为 `dict_size` 个条目建立索引的最小类型。                                                                                                                                                     |
| 8 (`0x100`)  | `NeedGlobalDictionaryBit` — 跨块共享的单个字典。**在 `Native` 格式中绝不能设置**：写入器使用 `low_cardinality_max_dictionary_size = 0`，而读取器会拒绝此位 (`native_format` 会抛出 `INCORRECT_DATA` — &quot;cannot use global dictionary&quot;) 。它属于 MergeTree 的磁盘 stream，而不属于传输格式。 |
| 9 (`0x200`)  | `HasAdditionalKeysBit` — 当块携带额外的字典键时设置 (写在索引之前) 。对于非空的 `Native` 块，此位始终会设置。                                                                                                                                                                    |
| 10 (`0x400`) | `NeedUpdateDictionary` — 当块携带字典更新时设置。对于非空的 `Native` 块，此位始终会设置，因为每个块都会携带自身完整且自包含的字典。                                                                                                                                                           |

对于每列只有单个数据块的典型查询响应，元数据为 `0x600` (HasAdditionalKeys + NeedUpdateDictionary) 。

dict 的值是使用内部类型 T 编码的 `dict_size` 个值。字典会在开头为特殊值预留槽位：非 Nullable 列会预留一个 (`dict[0]` 保存内部类型的默认值，例如 `String` 的 `""`) ，真正的不同值从 `dict[1]` 开始。

对于 `LowCardinality(Nullable(T))`，dict 仍然按普通 T 编码 (没有 null-map 流) ，但会预留**两个**槽位：`dict[0]` 是 NULL 标记，`dict[1]` 是内部类型的默认值 (例如 `String` 的 `""`) ；真正的不同值从 `dict[2]` 开始。NULL 行的键会指向 `dict[0]`，而该槽位在线上传输时会写成内部类型默认值对应的字节。

键是 dict 中的索引；每个索引占 `1 << key_type_code` 字节 (1、2、4 或 8) ，值 `N` 会还原为 `dict[keys[N]]`。

`keys_count` 是**当前递归层级**上的 `LowCardinality` 值数量，不一定等于块的行数。对于顶层 `LowCardinality` 列，这两者是一致的。但当 `LowCardinality` 位于复合类型内部时，这个计数就是该复合类型向下传递的扁平化值数量：对于 `Array(LowCardinality(String))`，如果三行总共包含五个元素，那么 `keys_count` 是 `5`，而不是 `3`；对于 `Map(K, LowCardinality(V))`，它是键值对总数，依此类推。解码器必须从这个字段中读取 `keys_count`，而不能假定它等于块的行数。当该扁平化计数为零时——例如一个块中的数组全为空——`LowCardinality` 的数据阶段**完全不会写入任何内容**：只会有状态前缀 (在[复合类型的前缀阶段](#composite-types)中发出) ，后面不会再跟随元数据、字典或 `keys_count`。

每个行数大于 0 的块都会在开头读取状态前缀——头部块 (rows = 0) 和空块都不会输出任何内容。在一个块内，`keys_count` 等于行数，`dict_size` 等于 dict stream 中的值数量，并且每个键都能用 `1 << key_type_code` 字节容纳。

:::note
在 `Native` format 中，每个块都会携带一个**自包含的块内字典**——不存在跨块的字典状态。Native 写入器会将 `low_cardinality_max_dictionary_size` 设为 `0`，因此 `SerializationLowCardinality` 不会构建共享字典：每个非空块都会将其键作为块内附加键写入，并且不设置 `NeedGlobalDictionaryBit` (metadata `0x600`) ；而当 `native_format` 为 true 时，Native 读取器会拒绝 `NeedGlobalDictionaryBit`。因此，解码器必须在每个块开始时重置字典，并读取该块中的 `dict_size` 个条目；如果沿用前一个块的字典，就会误读下一个块的键。 (跨块持久化 LC 字典属于 MergeTree 的磁盘存储问题，而不是 Native 传输布局的问题。)
:::

值为 `['a', 'b', 'a', 'c', 'b']` 的 `LowCardinality(String)`：

```text
01 00 00 00 00 00 00 00      state prefix Int64 = 1
00 06 00 00 00 00 00 00      metadata UInt64 = 0x600
04 00 00 00 00 00 00 00      dict_size = 4
00                           dict[0] = "" (placeholder)
01 'a'                       dict[1] = "a"
01 'b'                       dict[2] = "b"
01 'c'                       dict[3] = "c"
05 00 00 00 00 00 00 00      keys_count = 5
01 02 01 03 02               keys (UInt8): 1, 2, 1, 3, 2
```

重建后：`dict[1], dict[2], dict[1], dict[3], dict[2]` = `["a", "b", "a", "c", "b"]`。

取值为 `['a', NULL, '', 'b']` 的 `LowCardinality(Nullable(String))` 会显示两个保留槽位——`dict[0]` 用于 NULL，`dict[1]` 用于空字符串默认值：

```text
01 00 00 00 00 00 00 00      state prefix Int64 = 1
00 06 00 00 00 00 00 00      metadata UInt64 = 0x600
04 00 00 00 00 00 00 00      dict_size = 4
00                           dict[0] = "" → NULL marker
00                           dict[1] = "" → inner default value
01 'a'                       dict[2] = "a"
01 'b'                       dict[3] = "b"
04 00 00 00 00 00 00 00      keys_count = 4
02 00 01 03                  keys (UInt8): 2, 0, 1, 3
```

重建后：`dict[2]` = `"a"`、`dict[0]` = `NULL`、`dict[1]` = `""`、`dict[3]` = `"b"`，即 `["a", NULL, "", "b"]`。`dict[0]` 和 `dict[1]` 在线上传输中都是空字节；是否为 `NULL` 取决于键指向槽位 `0`，而不是这些字节本身。

<div id="json-tier-1-string-fallback">
  #### JSON (Tier 1：String fallback)
</div>

ClickHouse 的 `JSON` 类型有多种 wire 编码 (请参阅[序列化版本参考](#serialization-version-reference)) 。Tier 1 是最简单的一种：启用查询级设置 `output_format_native_write_json_as_string = 1` 时，服务器会将每个 JSON 值展平为其序列化后的文本，并以带有状态前缀标记的 `String` 形式输出该列。

类型字符串：`JSON`。

```text
[8 bytes:  Int64 LE state prefix = 1]        ← JSONStringSerializationVersion
[per block with rows > 0]:
  [N bytes: String column encoding for num_rows JSON text values]
```

对于这种 String fallback，状态前缀值为 `1`。其他值表示不同的 `JSON`/`Object` 编码：`0` = V1，`2` = V2 (native TCP protocol 上的默认值) ，`3` = FLATTENED，`4` = V3 (参见[序列化版本参考](#serialization-version-reference)) 。如果 解码器 在这里看到的值不是 `1`，说明它看到的不是 String fallback。对于每个行数 &gt; 0 的块，都会在开头读取此前缀，而 values stream 则是一个包含 `num_rows` 行的标准 [String](#string-type) 列。

`JSON` 值 `'{"a":1}'` (一行) ：

```text
01 00 00 00 00 00 00 00      state prefix Int64 = 1
07 7B 22 61 22 3A 31 7D      String: 7 bytes {"a":1}
```

该值会以紧凑的 JSON 文本形式输出——`{"a":1}`，其中整数仍保留为整数。该文本只是一个 `String` 值，因此客户端接收到的只是用于不透明传输的 JSON，而无法还原各个路径及其对应的 ClickHouse 类型；若要忠实保留按路径区分的类型信息，则需要使用下面的第 2 层级编码。

<div id="variant">
  #### Variant(T1, T2, ...)
</div>

一种带判别值的联合类型：每一行恰好保存一个 Variant 类型中的值，或为 NULL。每一行都带有一个单字节的**全局判别值**来选择其类型，随后各类型对应的值会以紧凑方式存储，每种 Variant 类型各自形成一段连续区间。

类型字符串：`Variant(T1, T2, ...)`。服务器会规范化类型顺序 (Variant 类型按名称排序) ，因此接收到的类型字符串已经按**全局判别值顺序**列出这些类型：判别值 `0` 选择列出的第一个类型，`1` 选择第二个，依此类推。`255` (`NULL_DISCRIMINATOR`) 表示该行是 NULL。Variant 元素绝不会是 `Nullable`——NULL 由判别值表示。示例：`Variant(String, UInt64)`、`Variant(Array(UInt8), String)`。

状态前缀包含一个 `UInt64 LE` 判别值模式：`0` = BASIC (直接写入每一行的判别值) ，`1` = COMPACT (按粒度进行游程编码) 。默认情况下，服务器通过 native protocol 使用 BASIC (`use_compact_variant_discriminators_serialization = false`) ；这里只规定 BASIC。

```text
[per block with rows > 0]:
  [8 bytes:  UInt64 LE discriminators mode = 0]    ← state prefix, repeated at the start of every non-empty block;
                                                     followed by each variant element's own state prefix
                                                     (empty for leaf types)
  [num_rows bytes: UInt8 discriminators]           ← one global discriminator per row; 255 = NULL
  [for each variant type i, in declared order]:
    [values for the rows whose discriminator == i] ← dense encoding in type i; count = #rows selecting i
```

要重建数据，需要从左到右遍历判别值，并为每种类型维护一个持续递增的计数器。判别值为 `d` (≠ 255) 的第 `r` 行，会从 Variant 类型 `d` 的值序列中取出索引为 `counter[d]` 的值，然后将 `counter[d]` 加 1。判别值为 `255` 的行是 NULL，不会从任何值序列中取值，因此各类型计数器之和等于非 NULL 行数。

状态前缀 (即 mode `UInt64`) 会在每个行数 &gt; 0 的块开头读取；请求头和空块不输出任何内容。每个非 NULL 判别值都小于 Variant 类型的数量，并且 Variant 类型 `i` 会恰好为 `count[i]` 行解码。

:::note
如果 Variant 元素本身是有状态的 (`LowCardinality`、`Variant`、`Dynamic`、`JSON`) ，它们会在每元素的状态前缀阶段中，在 mode `UInt64` 之后输出各自的状态前缀。叶子类型以及简单复合类型 (叶子类型的 `Array`、`Tuple`、`Map`) 的状态前缀为空，因此可以自由组合。
:::

`Variant(String, UInt64)` 的值为 `[42, 'hi', NULL]` (规范顺序会将 `String` 排在 `UInt64` 前面，因此判别值 0 = String，1 = UInt64) ：

```text
00 00 00 00 00 00 00 00      state prefix: UInt64 discriminators mode = 0 (BASIC)
01 00 FF                     discriminators (3 rows): 1 (UInt64), 0 (String), 255 (NULL)
02 68 69                     String run (1 value): len=2 "hi"
2A 00 00 00 00 00 00 00      UInt64 run (1 value): 42
```

重建结果：行 0 = UInt64 run[0] = `42`；行 1 = String run[0] = `"hi"`；行 2 = NULL。

判别值流就是索引；每个非 NULL 判别值都会从其对应类型的稠密 run 中取出下一个值，而 `255` (NULL) 则不消耗任何内容。通过同样的遍历过程也可以重建 [Dynamic](#dynamic)，二者唯一的区别在于 NULL 的编码方式：

```mermaid
flowchart LR
    subgraph D["discriminators (one per row)"]
        direction TB
        d0["row 0 → 1"]
        d1["row 1 → 0"]
        d2["row 2 → 255"]
    end
    subgraph SR["String run (discriminator 0)"]
        s0["[0] = hi"]
    end
    subgraph UR["UInt64 run (discriminator 1)"]
        u0["[0] = 42"]
    end
    d0 -->|"counter[1] = 0"| u0
    d1 -->|"counter[0] = 0"| s0
    d2 -.->|"255 = NULL,<br/>no value consumed"| X["(skip)"]
```

<div id="dynamic">
  #### Dynamic
</div>

一种值类型在运行时才确定的列：每一行保存的值都属于运行时确定的一组类型之一，或者为 NULL。与 `Variant` 不同，类型集合**不**包含在该列的类型字符串中，而是保存在状态前缀里。

类型字符串：`Dynamic` 或 `Dynamic(max_types=N)`。`max_types` 参数限制该列可跟踪的不同类型数量上限，但不影响下文所述的传输格式。

`Dynamic` 有四种编码——`V1 = 1`、`V2 = 2`、`FLATTENED = 3`、`V3 = 4`。服务器输出哪一种取决于通道以及查询设置：

* 通过 `clickhouse-client` 和 HTTP `FORMAT Native` 时，writer 的修订版本为 `0` (除非通过 `client_protocol_version` 提高) ，因此默认是 **V1**。
* 通过 native TCP protocol 且使用其协商后的修订版本时，默认是 **V2**。`Native` writer 会保持 STATISTICS 禁用，因此默认的 `V2` 载荷不携带任何按 variant 划分的 STATISTICS——type list 之后直接就是嵌套的 `Variant` 前缀和数据。 (按 variant 划分的 STATISTICS 属于 MergeTree 的磁盘存储事项，不是 Native 传输格式的一部分。)
* 查询设置 `output_format_native_use_flattened_dynamic_and_json_serialization = 1` 会覆盖前两者，无论修订版本如何都输出 **FLATTENED (version 3)&#x20;**。

:::note 范围
本页仅规定 **`FLATTENED`** 布局。非扁平的 `V1`/`V2`/`V3` 二进制布局属于内部/磁盘表示 (以二进制编码的 type list、按 variant 划分的 STATISTICS) ，此处**不**作规定。想要依据本页解码 `Dynamic` 的 client，必须通过设置 `output_format_native_use_flattened_dynamic_and_json_serialization = 1` 来请求 `FLATTENED`；下方布局即基于该设置。由于 version 字节位于前缀开头，解码器 可以识别实际收到的编码；如果它只实现了 `FLATTENED`，则可以拒绝 `V1`/`V2`/`V3`。
:::

该设置选择的 **FLATTENED (version 3)&#x20;**&#x20;布局：

```text
[per block with rows > 0]:
  [8 bytes:  UInt64 LE version = 3]                ← state prefix, repeated at the start of every non-empty block
  [VarUInt num_types]                              ← number of runtime types
  [num_types × type]                               ← type names, in wire order; each a String, or a binary
                                                     type encoding when output_format_native_encode_types_in_binary_format = 1
  [per type: its own state prefix]                 ← empty for leaf types; + indexes-type prefix (empty, integer)
  [num_rows × discriminator]                       ← width by num_types (UInt8 if ≤ 255, else UInt16/32/64);
                                                     NULL discriminator = num_types (one past the last type)
  [for each type i, in wire order]:
    [values for the rows whose discriminator == i] ← dense encoding in type i
```

判别值宽度是能够为 `num_types` 种类型以及 NULL 槽位建立索引的最小无符号整数——当 `num_types ≤ 255` 时为 `UInt8`，再往上依次为 `UInt16`、`UInt32`、`UInt64`。NULL 的判别值就是 `num_types` 本身，这与 `Variant` 不同：在 `Variant` 中，NULL 的固定值是 `255`。重建方式与 `Variant` 相同，都是致密遍历：为每种类型维护一个独立计数器，第 `r` 行的判别值为 `d` (≠ `num_types`) 时，就从类型 `d` 对应的序列中取值 `counter[d]`。

状态前缀 (版本 + 类型列表) 会在每个行数 &gt; 0 的块开头读取；头部和空块都不会输出任何内容。

:::note
序列化具有状态的运行时类型 (`LowCardinality`、`Variant`、`Dynamic`、`JSON`) 会在类型名列表之后携带嵌套的状态前缀。
:::

运行时类型列表通常遵循 `Variant` 的规范化规则——常规变体槽位会按 `DataTypeVariant` (类型名) 顺序写入，因此其传输顺序并不遵循插入顺序。不过，它**并不总是**全局有序的：溢出到共享变体的类型 (例如在 `Dynamic(max_types=N)` 下) 会按首次出现的顺序追加到常规槽位之后，因此列表尾部可能会打破按类型名排序的顺序。因此，解码器必须将传输过来的类型列表视为判别值分配的权威依据，不能自行重新排序。对于行 `[42::UInt64, "hi", NULL]`，两种类型是 `String` 和 `UInt64`，且 `"String"` 排在 `"UInt64"` 之前，因此判别值分别为 `0` = String、`1` = UInt64、`2` = NULL：

```text
03 00 00 00 00 00 00 00      state prefix: UInt64 version = 3 (FLATTENED)
02                           VarUInt num_types = 2
06 53 74 72 69 6E 67         type[0] = "String"
06 55 49 6E 74 36 34         type[1] = "UInt64"
01 00 02                     discriminators (3 rows): 1 (UInt64), 0 (String), 2 (NULL)
02 68 69                     String run (type[0], 1 value): len=2 "hi"
2A 00 00 00 00 00 00 00      UInt64 run (type[1], 1 value): 42
```

重建后：行 0 = UInt64 run[0] = `42`；行 1 = String run[0] = `"hi"`；行 2 = NULL。各类型的 run 的 wire order 与类型列表相同 (`String` 在 `UInt64` 之前) 。

<div id="json-tier-2-flattened-object">
  #### JSON (层级 2：FLATTENED Object)
</div>

一种更丰富的 JSON 编码：它不再像层级 1 那样将每个值都扁平化为文本，而是按每个 JSON path 将该列拆分为一个 sub-column。选择这种编码的方式是：在启用 flattened-serialization flag (`output_format_native_use_flattened_dynamic_and_json_serialization = 1`) 的同时，**不** 请求层级 1 回退 (`output_format_native_write_json_as_string = 0`) ；随后 server 会输出序列化 **version 3**。

path 分为两种：

* **Typed paths** 在 type string 中声明，例如 `JSON(a UInt32, b String)`，并按声明的类型解码。名称中包含点号的 path 会在 type string 中用反引号括起。
* **Dynamic paths** 在 runtime 时发现，并且每个都会解码为一个 [Dynamic](#dynamic) 列。

在 FLATTENED 模式下，**没有 shared-data 列** (该 overflow 存储属于非扁平 V2/V3 Object 编码) 。每个 path 都是一个包含 `num_rows` 个值的普通列。

```text
[per block with rows > 0]:
  -- prefix phase (repeated at the start of every non-empty block):
  [8 bytes:  UInt64 LE version = 3]                ← state prefix
  [VarUInt num_dynamic_paths]
  [num_dynamic_paths × String]                     ← dynamic path names, in wire order
  [per typed path: its column's state prefix]      ← empty for leaf types
  [per dynamic path: a Dynamic state prefix]       ← version + type list (see Dynamic)
  -- data phase:
  [for each typed path:   its column's data]       ← num_rows values in the declared type
  [for each dynamic path: its Dynamic data]        ← num_rows values (discriminators + runs)
```

请注意这种两阶段形态：**所有**路径的状态前缀都先出现，然后才是**所有**路径数据。因此，动态路径的 `Dynamic` 前缀 (在前缀阶段) 会与其数据 (在数据阶段) 分开。状态前缀会在每个行数 &gt; 0 的块开头读取，而每个路径列 (无论是类型化还是动态) 都恰好包含 `num_rows` 个值。第 `r` 行的对象是通过读取每条路径在索引 `r` 处的值组装而成的；如果某条动态路径在该行的 `Dynamic` 判别值为 NULL，则不会贡献任何键。

`JSON` 值 `{"a": 42, "b": "hi"}` (一行，两条路径均为动态) 。JSON 整数会被推断为 `Int64`：

```text
03 00 00 00 00 00 00 00      version = 3 (Object)
02                           num_dynamic_paths = 2
01 61                        path "a"
01 62                        path "b"
03 00 00 00 00 00 00 00 01 05 49 6E 74 36 34      "a" Dynamic prefix: version 3, 1 type, "Int64"
03 00 00 00 00 00 00 00 01 06 53 74 72 69 6E 67   "b" Dynamic prefix: version 3, 1 type, "String"
00 2A 00 00 00 00 00 00 00   "a" data: discriminator 0, Int64 42
00 02 68 69                  "b" data: discriminator 0, String "hi"
```

<div id="json-non-flat">
  #### JSON 非扁平 (V2/V3)
</div>

非扁平的 `Object` 编码 (`V1`/`V2`/`V3`) 用于 MergeTree 的磁盘存储；当 flattened 标志关闭时，这也是服务器在线路上传输时输出的编码——`V1` 通过 `clickhouse-client` / HTTP `FORMAT Native` (修订版本 `0`) 传输，`V2` 通过原生 TCP 协议传输。它们包含一个 shared-data 列，并且**不**在本页说明。请注意，它们**不会**通过 Native 线路传输按路径统计信息：`NativeWriter` 会保持统计信息禁用，因此 `Object` 结构前缀中不包含统计信息段，后面的字节直接就是 typed/dynamic/shared-data 前缀和数据。统计信息只会出现在启用了它们的 MergeTree 磁盘路径上。要使用本页内容解码 `JSON` 列，客户端必须选择文档中说明的层级之一：对于 [String fallback](#json-tier-1-string-fallback)，设置 `output_format_native_write_json_as_string = 1`；或者对于 [FLATTENED Object](#json-tier-2-flattened-object) 布局，设置 `output_format_native_use_flattened_dynamic_and_json_serialization = 1` (同时将 `output_format_native_write_json_as_string = 0`) 。

<div id="compression-frame">
  ## 压缩帧
</div>

ClickHouse 可以使用内部帧格式压缩 `Native` stream 的列数据。下面的[帧布局](#frame-format)与**传输方式无关**——相同的帧既会出现在 native TCP 协议中，也会出现在 HTTP 上传输时——但压缩的请求方式以及帧外层的封装内容会因传输方式而异。

* **native TCP 协议。** 压缩通过[Query 数据包](/zh/interfaces/specs/NativeProtocol#query)中的 `compression` flag 按查询选择启用。启用后，每个 `Data`、`Totals`、`Extremes`、`Log` 和 `ProfileEvents` 数据包的主体——即 `table_name` 字符串之后的字节——都会按该帧格式封装。数据包封装本身、数据包类型代码以及 `table_name` 字符串**不会**被压缩；server 会将它们直接写入原始流。`NativeWriter` 输出的所有内容都会进入压缩流，因此 `BlockInfo` 前缀会与维度和列一起成为帧内的第一部分内容。因此，client 必须先解压该帧，才能读取 `BlockInfo`。
* **HTTP。** `SELECT ... FORMAT Native&compress=1` 会使用相同的帧封装整个 `FORMAT Native` 字节流 (server 使用相同的内部 `CompressedWriteBuffer`) ，而 `?decompress=1` 则要求 `Native` *input* body 使用相同的帧，并通过对应的 `CompressedReadBuffer` 进行解码。在这一路径中，没有 TCP 数据包类型、`table_name` 或数据包封装：整个压缩载荷只是带帧的 `Native` 块 (只有在协商的修订版本大于 `0` 时，才会像上文未压缩布局那样包含 `BlockInfo` 前缀) 。这种内部 `compress`/`decompress` 分帧不同于 HTTP 传输压缩 (`Content-Encoding: gzip`/`zstd`，由 `enable_http_compression` 启用) ；后者是在 HTTP 层封装响应，而不是下面这种帧格式。

因此，只实现了未压缩 `FORMAT Native` 布局的 client，如果要读取压缩的 HTTP `Native` 响应，或发送 `decompress=1` request body，仍然必须增加这一帧层。

<div id="frame-format">
  ### 帧格式
</div>

```text
[16 bytes: CityHash128 checksum over the 9-byte header + compressed body]
[1 byte:   method]                 ← 0x82 = LZ4, 0x90 = ZSTD, 0x02 = NONE
[4 bytes:  compressed_size LE u32] ← INCLUDES the 9-byte header, EXCLUDES the 16-byte checksum
[4 bytes:  uncompressed_size LE u32]
[N bytes:  compressed body]        ← N = compressed_size - 9
```

带帧后的总大小为 `16 + compressed_size` = `16 + 9 + body_size` = `25 + body_size`。注意这两个部分：`校验和` 覆盖 9 字节的头部加上数据体，而 `compressed_size` 计算的是头部加数据体，但**不**包括 `校验和` 本身：

```mermaid
flowchart LR
    CK["checksum<br/>16 B<br/>CityHash128"]
    subgraph SPAN["counted by compressed_size (9 + N)"]
        direction LR
        M["method<br/>1 B"]
        CS["compressed_size<br/>4 B LE"]
        US["uncompressed_size<br/>4 B LE"]
        BODY["compressed body<br/>N = compressed_size − 9 B"]
        M --> CS --> US --> BODY
    end
    CK --> M
```

<div id="method-byte-values">
  ### 方法字节取值
</div>

| Byte   | Method | Body encoding                                      |
| ------ | ------ | -------------------------------------------------- |
| `0x02` | NONE   | Body 为原始字节 (无压缩) 。仍会输出该帧；接收方会验证校验和。                |
| `0x82` | LZ4    | Body 采用 **LZ4 块格式**——*不是* LZ4 帧格式。没有魔数。            |
| `0x90` | ZSTD   | Body 是原始 zstd 单帧 stream (标准 zstd 魔数属于 Body 的一部分) 。 |

<div id="checksum">
  ### 校验和
</div>

ClickHouse 使用 CityHash v1.0.2 (历史版本) ，**不是**现代的 Google CityHash；两者的输出不同。

校验和基于 9 个头部字节 (method + compressed&#95;size + uncompressed&#95;size) 以及 N 个数据体字节计算，也就是校验和之后到帧末尾之间的全部内容。16 字节的 CityHash128 输出中，前 8 个字节是低半部分 (LE) ，后 8 个字节是高半部分 (LE) 。解码器会根据接收到的头部和数据体重新计算 CityHash128，并与开头的 16 个字节进行比对；如果不一致，就说明数据已损坏，解码器会报错。

<div id="per-block-boundaries">
  ### 按块划分的边界
</div>

一个 Block 的压缩载荷是**由一个或多个帧组成的流**，不一定只有单个帧。发送方通过 `CompressedWriteBuffer` 写入已序列化的块；每当其内部缓冲区填满 (约 1 MB，即 `DBMS_DEFAULT_BUFFER_SIZE`) 时，就会输出一个帧，并在块被刷写时输出最后一个帧。因此，小块对应一个帧；大块则对应多个连续帧。

这个不变式只在一个方向上成立：由于发送方会在每个块结束时刷写压缩缓冲区，**每个块的结束都恰好与一个帧边界重合**——但反过来并不成立。在块处理中途因缓冲区填满而输出的中间帧边界，落在块的*中间*，并不是块边界。因此，解码器必须使用块自身的维度信息 (`num_columns`/`num_rows`) 来判断块在何处结束；不能假定每个帧都是一个完整块。

接收方会以流式方式处理这些帧：先读取 16 + 9 字节，再精确读取 `compressed_size - 9` 字节的 body，将其解压为恰好 `uncompressed_size` 字节，然后把这些字节交给块解码器；当解码器需要的数据超出当前帧所包含的内容时，再读取下一个帧。由于发送方按块刷写，在一个块被完全解码后，帧缓冲区会为空，而下一个块会从一个新的帧开始。

在原生 TCP 协议上，数据包封装——即数据包类型 VarUInt 和 `table_name` 字符串——会被写入**原始**流，位于压缩载荷之外；只有块体 (BlockInfo + columns) 会被分帧。HTTP `compress`/`decompress` 路径没有这样的封装：整个流都是由分帧块组成的。

<div id="compression-negotiation">
  ### 协商
</div>

在原生 TCP 协议中，压缩是按查询而非按连接生效的。`Query 数据包` 的 `compression: bool` 字段用于为该次查询请求压缩。服务器会遵从该请求，并在整个查询生命周期内输出经过压缩的 `Data`/`Totals`/`Extremes`/`Log`/`ProfileEvents` 负载 (`Log`/`ProfileEvents` 仅在 v54481+ 可用) 。它还要求客户端发送的 Data 块——外部表、表示数据结束的空标记以及 INSERT 行——也采用相同的帧封装方式。同一连接上的后续查询可以使用不同设置。

在 HTTP 中没有 Query 数据包：`compress=1` 查询参数会为该请求启用带帧输出，而 `decompress=1` 则表示请求体采用带帧格式。`compress=1` 的输出使用服务器默认的编解码器 (`LZ4`) 写出，而不是 `network_compression_method`；`decompress=1` 读取器则从每个帧的方法字节中获取编解码器，因此输入时接受任意编解码器。

:::note
启用压缩后，服务器还可能将列切换到并行块封送 / `ColumnBLOB` 路径 (`PARALLEL_BLOCK_MARSHALLING`，v54478) ，用于包含多于一行的块。实现压缩 INSERT 数据时，必须准备好处理该路径 (或显式选择不使用它) ，以避免流不同步。
:::

<div id="glossary">
  ## 术语表
</div>

**Block** — Native format 中的数据交换单位。它是一个以列式方式存储行的自描述数据块。参见 [块与列结构](#block-and-column-structure)。

**BlockInfo** — 在 TCP Data-packet 路径上位于 Block 之前的元数据头 (当 connection 的 修订版本 大于 0 时都会写入) 。它是一组受 修订版本 控制、带字段 ID 标记的字段序列。`Native` output format 会省略它，因为该格式按 修订版本 `0` 进行序列化。参见 [BlockInfo](#blockinfo)。

**Column body** — Column 中保存实际值的字节部分，位于列头 (名称、类型、has&#95;custom&#95;serialization 字节) 之后。其布局取决于具体类型。参见 [列的线协议布局](#column-wire-layout)。

**Composite type** — 由一个或多个内部类型构成的类型，每列会编码为多个流。其传输格式是稳定且未版本化的。参见 [复合类型](#composite-types)。

**Dictionary (LowCardinality)** — `LowCardinality(T)` 列通过整数索引引用的唯一值数组。参见 [LowCardinality](#lowcardinality)。

**Empty block** — 一个 `num_columns = 0` 且 `num_rows = 0` 的 Block。它用作哨兵：既是 client-side 的输入结束标记，也是 server-side 的流边界标记。参见 [块变体](#block-variants)。

**Header block** — 一个 `num_columns > 0` 且 `num_rows = 0` 的 Block，由 server 作为查询响应的第一个 Data packet 发送。它用于声明结果 schema。参见 [块变体](#block-variants)。

**Inner type** — 复合类型所包装的类型。`Array(UInt32)` 的内部类型是 `UInt32`；`Nullable(T)` 的内部类型是 `T`。

**Offsets stream** — `Array`、`Map` 和 `Nested` 用于界定每行元素边界的累计结束位置 UInt64 数组。参见 [Array](#array)。

**Placeholder value** — 在 `Nullable(T)` 列的值流中写入到 NULL 位置的字节。decoder 会读取这些字节以推进流，但会忽略其内容。参见 [Nullable](#nullable)。

**Result block** — 一个携带实际查询结果行且 `num_rows > 0` 的 Block。参见 [块变体](#block-variants)。

**Schema block** — header block 的同义词，用于描述 INSERT 阶段；在该阶段中，schema block 会告知 client 预期的列结构。

**Serialization version** — 版本化类型使用的、按类型划分的线上传输版本号，用于声明后续采用哪一种编码变体。它不同于协议版本。参见 [serialization version: concept](#serialization-version-concept)。

**State prefix** — 位于版本化类型每个块载荷之前的字节。它携带 serialization version，以及 (对于 LowCardinality) 每块的字典元数据。在每个 `rows > 0` 的块开头输出；不会跨块保留。

**Stream** — 列体中的一段连续字节，用于编码一个逻辑子组件 (如 null-map、offsets 数组、值流) 。多流类型会为每列串接两个或更多个流。