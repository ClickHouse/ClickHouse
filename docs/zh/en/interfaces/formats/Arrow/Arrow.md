---
alias: []
description: 'Arrow 格式文档'
input_format: true
keywords: ['Arrow']
output_format: true
slug: /interfaces/formats/Arrow
title: 'Arrow'
doc_type: 'reference'
---

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 描述
</div>

[Apache Arrow](https://arrow.apache.org/) 提供了两种内置的列式存储格式。
ClickHouse 支持对这些格式进行读写。
`Arrow` 是 Apache Arrow 的“文件模式”格式，专为在内存中进行随机访问而设计。

<div id="data-types-matching">
  ## 数据类型匹配
</div>

下表列出了支持的数据类型，以及它们在 `INSERT` 和 `SELECT` 查询中与 ClickHouse [数据类型](/zh/sql-reference/data-types/index.md) 的对应关系。

| Arrow 数据类型 (`INSERT`)                   | ClickHouse 数据类型                                                                                                  | Arrow 数据类型 (`SELECT`) |
| --------------------------------------- | ---------------------------------------------------------------------------------------------------------------- | --------------------- |
| `BOOL`                                  | [Bool](/zh/sql-reference/data-types/boolean.md)                                                                     | `BOOL`                |
| `UINT8`, `BOOL`                         | [UInt8](/zh/sql-reference/data-types/int-uint.md)                                                                   | `UINT8`               |
| `INT8`                                  | [Int8](/zh/sql-reference/data-types/int-uint.md)/[Enum8](/zh/sql-reference/data-types/enum.md)                         | `INT8`                |
| `UINT16`                                | [UInt16](/zh/sql-reference/data-types/int-uint.md)                                                                  | `UINT16`              |
| `INT16`                                 | [Int16](/zh/sql-reference/data-types/int-uint.md)/[Enum16](/zh/sql-reference/data-types/enum.md)                       | `INT16`               |
| `UINT32`                                | [UInt32](/zh/sql-reference/data-types/int-uint.md)                                                                  | `UINT32`              |
| `INT32`                                 | [Int32](/zh/sql-reference/data-types/int-uint.md)                                                                   | `INT32`               |
| `UINT64`                                | [UInt64](/zh/sql-reference/data-types/int-uint.md)                                                                  | `UINT64`              |
| `INT64`                                 | [Int64](/zh/sql-reference/data-types/int-uint.md)                                                                   | `INT64`               |
| `FLOAT`, `HALF_FLOAT`                   | [Float32](/zh/sql-reference/data-types/float.md)                                                                    | `FLOAT32`             |
| `DOUBLE`                                | [Float64](/zh/sql-reference/data-types/float.md)                                                                    | `FLOAT64`             |
| `DATE32`                                | [Date32](/zh/sql-reference/data-types/date32.md)                                                                    | `UINT16`              |
| `DATE64`                                | [DateTime](/zh/sql-reference/data-types/datetime.md)                                                                | `UINT32`              |
| `TIMESTAMP`                             | [DateTime64](/zh/sql-reference/data-types/datetime64.md)                                                            | `TIMESTAMP`           |
| `TIME32`, `TIME64`                      | [Time64](/zh/sql-reference/data-types/time64.md)                                                                    | `TIME32`, `TIME64`    |
| `STRING`, `BINARY`                      | [String](/zh/sql-reference/data-types/string.md)                                                                    | `BINARY`              |
| `STRING`, `BINARY`, `FIXED_SIZE_BINARY` | [FixedString](/zh/sql-reference/data-types/fixedstring.md)                                                          | `FIXED_SIZE_BINARY`   |
| `DECIMAL`                               | [Decimal](/zh/sql-reference/data-types/decimal.md)                                                                  | `DECIMAL`             |
| `DECIMAL256`                            | [Decimal256](/zh/sql-reference/data-types/decimal.md)                                                               | `DECIMAL256`          |
| `LIST`                                  | [Array](/zh/sql-reference/data-types/array.md)                                                                      | `LIST`                |
| `STRUCT`                                | [Tuple](/zh/sql-reference/data-types/tuple.md)                                                                      | `STRUCT`              |
| `MAP`                                   | [Map](/zh/sql-reference/data-types/map.md)                                                                          | `MAP`                 |
| `UINT32`                                | [IPv4](/zh/sql-reference/data-types/ipv4.md)                                                                        | `UINT32`              |
| `FIXED_SIZE_BINARY`, `BINARY`           | [IPv6](/zh/sql-reference/data-types/ipv6.md)                                                                        | `FIXED_SIZE_BINARY`   |
| `FIXED_SIZE_BINARY`, `BINARY`           | [Int128/UInt128/Int256/UInt256](/zh/sql-reference/data-types/int-uint.md)                                           | `FIXED_SIZE_BINARY`   |
| `DURATION`                              | [Interval](/zh/sql-reference/data-types/special-data-types/interval.md) (Nanosecond/Microsecond/Millisecond/Second) | `DURATION`            |
| `INT64`                                 | [Interval](/zh/sql-reference/data-types/special-data-types/interval.md) (Minute/Hour/Day/Week/Month/Quarter/Year)   | `INT64`               |

数组可以嵌套，且其元素类型参数可以为 `Nullable` 类型。`Tuple` 和 `Map` 类型也可以嵌套。

`DICTIONARY` 类型支持用于 `INSERT` 查询；对于 `SELECT` 查询，有一个 [`output_format_arrow_low_cardinality_as_dictionary`](/zh/operations/settings/formats#output_format_arrow_low_cardinality_as_dictionary) 设置，可将 [LowCardinality](/zh/sql-reference/data-types/lowcardinality.md) 类型输出为 `DICTIONARY` 类型。请注意，`LowCardinality` 字典中可能包含未使用的值，这可能会导致输出时 Arrow `DICTIONARY` 中也出现未使用的值。

不支持的 Arrow 数据类型：

* `JSON`
* `ENUM`.

ClickHouse 表列的数据类型不必与对应的 Arrow 数据字段完全一致。插入数据时，ClickHouse 会根据上表解释数据类型，然后将数据 [转换](/zh/sql-reference/functions/type-conversion-functions#CAST) 为 ClickHouse 表列所设置的数据类型。

<div id="example-usage">
  ## 示例用法
</div>

在下面的示例中，我们使用了 [ClickHouse SQL playground](https://sql.clickhouse.com) 中提供的 `forex` 数据集。

<div id="selecting-data">
  ### 选择数据
</div>

我们从 Playground 中选取 `EUR/USD` 的一天汇率数据，并将其保存
到本地 `forex_eurusd.arrow` 文件中。我们通过 HTTP
接口查询 Playground，其中主机为 `sql-clickhouse.clickhouse.com`，用户为
`demo` (无需密码) ：

```bash
curl "https://sql-clickhouse.clickhouse.com:8443/?user=demo&database=forex" \
    --data-binary "
        SELECT
            concat(base, '.', quote) AS base_quote,
            datetime AS last_update,
            CAST(bid, 'Float32') AS bid,
            CAST(ask, 'Float32') AS ask,
            ask - bid AS spread
        FROM forex
        WHERE base = 'EUR' AND quote = 'USD'
            AND datetime >= '2020-01-01' AND datetime < '2020-01-02'
        ORDER BY datetime ASC
        FORMAT Arrow
        SETTINGS output_format_arrow_compression_method='zstd'" > forex_eurusd.arrow
```

<div id="reading-data">
  ### 读取文件
</div>

现在，我们可以使用
[`clickhouse-local`](/zh/operations/utilities/clickhouse-local) 和
[`file`](/zh/sql-reference/table-functions/file) 表函数读取本地 Arrow 文件。该文件是
自描述的，因此 `Arrow` 格式会自动推断 schema：

```bash
clickhouse-local --query "
    SELECT *
    FROM file('forex_eurusd.arrow', Arrow)
    ORDER BY last_update ASC
    LIMIT 5
    FORMAT PrettyCompact"
```

```response title="Response"
   ┌─base_quote─┬─────────────last_update─┬─────bid─┬─────ask─┬────────────────spread─┐
1. │ EUR.USD    │ 2020-01-01 17:00:00.065 │  1.1212 │ 1.12172 │ 0.0005199909210205078 │
2. │ EUR.USD    │ 2020-01-01 17:00:10.447 │  1.1212 │ 1.12192 │ 0.0007200241088867188 │
3. │ EUR.USD    │ 2020-01-01 17:00:10.498 │ 1.12117 │ 1.12161 │ 0.0004400014877319336 │
4. │ EUR.USD    │ 2020-01-01 17:00:12.579 │  1.1212 │ 1.12161 │ 0.0004100799560546875 │
5. │ EUR.USD    │ 2020-01-01 17:00:12.630 │  1.1212 │ 1.12172 │ 0.0005199909210205078 │
   └────────────┴─────────────────────────┴─────────┴─────────┴───────────────────────┘
```

<div id="inserting-data">
  ### 插入数据
</div>

要将 Arrow 文件加载到 ClickHouse 表中，请使用 `FORMAT Arrow` 将其通过管道传给 `clickhouse-client`：

```bash
cat forex_eurusd.arrow | clickhouse-client --query="INSERT INTO some_table FORMAT Arrow"
```

<div id="format-settings">
  ## 格式设置
</div>

| 设置                                                                           | 说明                                                                                                                       | 默认值         |
| ---------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ | ----------- |
| `input_format_arrow_allow_missing_columns`                                   | 读取 Arrow 输入格式时允许缺失列                                                                                                      | `1`         |
| `input_format_arrow_case_insensitive_column_matching`                        | 匹配 Arrow 列与 CH 列时忽略大小写。                                                                                                  | `0`         |
| `input_format_arrow_import_nested`                                           | 已废弃，无实际作用。                                                                                                               | `0`         |
| `input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference` | 对 Arrow 格式进行 schema inference 时，跳过类型不受支持的列                                                                               | `0`         |
| `input_format_arrow_use_native_reader`                                       | 对 `Arrow` 和 `ArrowStream` 格式使用 ClickHouse 原生读取器，而不是 Apache Arrow 库。将其设为 `0` 以使用 Apache Arrow 库读取器。                       | `1`         |
| `output_format_arrow_compression_method`                                     | Arrow 输出格式的压缩方法。支持的编解码器：lz4&#95;frame、zstd、none (未压缩)                                                                    | `lz4_frame` |
| `output_format_arrow_fixed_string_as_fixed_byte_array`                       | 对 FixedString 列使用 Arrow FIXED&#95;SIZE&#95;BINARY 类型，而不是 Binary 类型。                                                      | `1`         |
| `output_format_arrow_low_cardinality_as_dictionary`                          | 启用将 LowCardinality 类型输出为 Arrow 的 Dictionary 类型                                                                           | `0`         |
| `output_format_arrow_string_as_string`                                       | 对 String 列使用 Arrow String 类型，而不是 Binary 类型                                                                               | `1`         |
| `output_format_arrow_unsupported_types_as_binary`                            | 将没有对应 Arrow 等价类型的类型 (例如 `BFloat16`、`AggregateFunction`) 输出为原始二进制数据。如果为 false，此类类型会引发异常。此设置同时适用于原生写入器和 Apache Arrow 库写入器。 | `1`         |
| `output_format_arrow_use_64_bit_indexes_for_dictionary`                      | 在 Arrow 格式中始终对字典索引使用 64 位整数                                                                                              | `0`         |
| `output_format_arrow_use_native_writer`                                      | 对 `Arrow` 和 `ArrowStream` 格式使用 ClickHouse 原生写入器，而不是 Apache Arrow 库。将其设为 `0` 以使用 Apache Arrow 库写入器。                       | `1`         |
| `output_format_arrow_use_signed_indexes_for_dictionary`                      | 在 Arrow 格式中对字典索引使用有符号整数                                                                                                  | `1`         |