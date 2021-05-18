---
toc_priority: 40
toc_title: UInt8, UInt16, UInt32, UInt64, UInt256, Int8, Int16, Int32, Int64, Int128, Int256
---

# UInt8, UInt16, UInt32, UInt64, UInt256, Int8, Int16, Int32, Int64, Int128, Int256 {#uint8-uint16-uint32-uint64-uint256-int8-int16-int32-int64-int128-int256}

Fixed-length integers, with or without a sign.

When creating tables, numeric parameters for integer numbers can be set (e.g. `TINYINT(8)`, `SMALLINT(16)`, `INT(32)`, `BIGINT(64)`), but ClickHouse ignores them. 

## Int Ranges {#int-ranges}

-   `Int8` — \[-128 : 127\]
-   `Int16` — \[-32768 : 32767\]
-   `Int32` — \[-2147483648 : 2147483647\]
-   `Int64` — \[-9223372036854775808 : 9223372036854775807\]
-   `Int128` — \[-170141183460469231731687303715884105728 : 170141183460469231731687303715884105727\]
-   `Int256` — \[-57896044618658097711785492504343953926634992332820282019728792003956564819968 : 57896044618658097711785492504343953926634992332820282019728792003956564819967\]

Aliases:

-   `Int8` — `TINYINT`, `BOOL`, `BOOLEAN`, `INT1`.
-   `Int16` — `SMALLINT`, `INT2`.
-   `Int32` — `INT`, `INT4`, `INTEGER`.
-   `Int64` — `BIGINT`.

## Uint Ranges {#uint-ranges}

-   `UInt8` — \[0 : 255\]
-   `UInt16` — \[0 : 65535\]
-   `UInt32` — \[0 : 4294967295\]
-   `UInt64` — \[0 : 18446744073709551615\]
-   `UInt256` — \[0 : 115792089237316195423570985008687907853269984665640564039457584007913129639935\]

`UInt128` is not supported yet.

[Original article](https://clickhouse.tech/docs/en/data_types/int_uint/) <!--hide-->
