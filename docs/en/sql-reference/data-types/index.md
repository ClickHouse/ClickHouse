---
slug: /en/sql-reference/data-types/
sidebar_label: List of data types
sidebar_position: 1
---

# Data Types in ClickHouse

ClickHouse can store various kinds of data in table cells. This section describes the supported data types and special considerations for using and/or implementing them if any.

:::note
You can check whether a data type name is case-sensitive in the [system.data_type_families](../../operations/system-tables/data_type_families.md#system_tables-data_type_families) table.
:::

ClickHouse data types include:

- **Integer types**: [signed and unsigned integers](./int-uint.md) (`UInt8`, `UInt16`, `UInt32`, `UInt64`, `UInt128`, `UInt256`, `Int8`, `Int16`, `Int32`, `Int64`, `Int128`, `Int256`)
- **Floating-point numbers**: [floats](./float.md)(`Float32` and `Float64`) and [`Decimal` values](./decimal.md)
- **Boolean**: ClickHouse has a [`Boolean` type](./boolean.md)
- **Strings**: [`String`](./string.md) and [`FixedString`](./fixedstring.md)
- **Dates**: use [`Date`](./date.md) and [`Date32`](./date32.md) for days, and [`DateTime`](./datetime.md) and [`DateTime64`](./datetime64.md) for instances in time
- **Object**: the [`Object`](./json.md) stores a JSON document in a single column (deprecated)
- **JSON**: the [`JSON` object](./newjson.md) stores a JSON document in a single column
- **UUID**: a performant option for storing [`UUID` values](./uuid.md)
- **Low cardinality types**: use an [`Enum`](./enum.md) when you have a handful of unique values, or use [`LowCardinality`](./lowcardinality.md) when you have up to 10,000 unique values of a column
- **Arrays**: any column can be defined as an [`Array` of values](./array.md)
- **Maps**: use [`Map`](./map.md) for storing key/value pairs
- **Aggregation function types**: use [`SimpleAggregateFunction`](./simpleaggregatefunction.md) and [`AggregateFunction`](./aggregatefunction.md) for storing the intermediate status of aggregate function results
- **Nested data structures**: A [`Nested` data structure](./nested-data-structures/index.md) is like a table inside a cell
- **Tuples**: A [`Tuple` of elements](./tuple.md), each having an individual type.
- **Nullable**: [`Nullable`](./nullable.md) allows you to store a value as `NULL` when a value is "missing" (instead of the column settings its default value for the data type)
- **IP addresses**: use [`IPv4`](./ipv4.md) and [`IPv6`](./ipv6.md) to efficiently store IP addresses
- **Geo types**: for [geographical data](./geo.md), including `Point`, `Ring`, `Polygon` and `MultiPolygon`
- **Special data types**: including [`Expression`](./special-data-types/expression.md), [`Set`](./special-data-types/set.md), [`Nothing`](./special-data-types/nothing.md) and [`Interval`](./special-data-types/interval.md)
