---
description: 'Documentation for signed and unsigned integer data types in ClickHouse,
  ranging from 8-bit to 256-bit'
sidebar_label: 'Int | UInt'
sidebar_position: 2
slug: /sql-reference/data-types/int-uint
title: 'Int | UInt Types'
---

ClickHouse offers a number of fixed-length integers, 
with a sign (`Int`) or without a sign (unsigned `UInt`) ranging from one byte to 32 bytes.

When creating tables, numeric parameters for integer numbers can be set (e.g. `TINYINT(8)`, `SMALLINT(16)`, `INT(32)`, `BIGINT(64)`), but ClickHouse ignores them.

## Integer Ranges {#integer-ranges}

Integer types have the following ranges:

| Type     | Range                                                                                                                                                              |
|----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Int8`   | \[-128 : 127\]                                                                                                                                                     |
| `Int16`  | \[-32768 : 32767\]                                                                                                                                                 |
| `Int32`  | \[-2147483648 : 2147483647\]                                                                                                                                       |
| `Int64`  | \[-9223372036854775808 : 9223372036854775807\]                                                                                                                     |
| `Int128` | \[-170141183460469231731687303715884105728 : 170141183460469231731687303715884105727\]                                                                             |
| `Int256` | \[-57896044618658097711785492504343953926634992332820282019728792003956564819968 : 57896044618658097711785492504343953926634992332820282019728792003956564819967\] |

Unsigned integer types have the following ranges:

| Type      | Range                                                                                  |
|-----------|----------------------------------------------------------------------------------------|
| `UInt8`   | \[0 : 255\]                                                                            |
| `UInt16`  | \[0 : 65535\]                                                                          |
| `UInt32`  | \[0 : 4294967295\]                                                                     |
| `UInt64`  | \[0 : 18446744073709551615\]                                                           |
| `UInt128` | \[0 : 340282366920938463463374607431768211455\]                                        |
| `UInt256` | \[0 : 115792089237316195423570985008687907853269984665640564039457584007913129639935\] |

## Integer Aliases {#integer-aliases}

Integer types have the following aliases:

| Type    | Alias                                                                             |
|---------|-----------------------------------------------------------------------------------|
| `Int8`  | `TINYINT`, `INT1`, `BYTE`, `TINYINT SIGNED`, `INT1 SIGNED`                        |
| `Int16` | `SMALLINT`, `SMALLINT SIGNED`                                                     |
| `Int32` | `INT`, `INTEGER`, `MEDIUMINT`, `MEDIUMINT SIGNED`, `INT SIGNED`, `INTEGER SIGNED` |
| `Int64` | `BIGINT`, `SIGNED`, `BIGINT SIGNED`, `TIME`                                       |

Unsigned integer types have the following aliases:

| Type     | Alias                                                    |
|----------|----------------------------------------------------------|
| `UInt8`  | `TINYINT UNSIGNED`, `INT1 UNSIGNED`                      |
| `UInt16` | `SMALLINT UNSIGNED`                                      |
| `UInt32` | `MEDIUMINT UNSIGNED`, `INT UNSIGNED`, `INTEGER UNSIGNED` |
| `UInt64` | `UNSIGNED`, `BIGINT UNSIGNED`, `BIT`, `SET`              |

