---
title : Npy
slug : /en/interfaces/formats/Npy
keywords : [Npy]
input_format: true
output_format: true
alias: []
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description

The `Npy` format is designed to load a NumPy array from a `.npy` file into ClickHouse. 
The NumPy file format is a binary format used for efficiently storing arrays of numerical data. 
During import, ClickHouse treats the top level dimension as an array of rows with a single column. 

The table below gives the supported Npy data types and their corresponding type in ClickHouse:

## Data Types Matching {#data_types-matching}


| Npy data type (`INSERT`) | ClickHouse data type                                            | Npy data type (`SELECT`) |
|--------------------------|-----------------------------------------------------------------|-------------------------|
| `i1`                     | [Int8](/docs/en/sql-reference/data-types/int-uint.md)           | `i1`                    |
| `i2`                     | [Int16](/docs/en/sql-reference/data-types/int-uint.md)          | `i2`                    |
| `i4`                     | [Int32](/docs/en/sql-reference/data-types/int-uint.md)          | `i4`                    |
| `i8`                     | [Int64](/docs/en/sql-reference/data-types/int-uint.md)          | `i8`                    |
| `u1`, `b1`               | [UInt8](/docs/en/sql-reference/data-types/int-uint.md)          | `u1`                    |
| `u2`                     | [UInt16](/docs/en/sql-reference/data-types/int-uint.md)         | `u2`                    |
| `u4`                     | [UInt32](/docs/en/sql-reference/data-types/int-uint.md)         | `u4`                    |
| `u8`                     | [UInt64](/docs/en/sql-reference/data-types/int-uint.md)         | `u8`                    |
| `f2`, `f4`               | [Float32](/docs/en/sql-reference/data-types/float.md)           | `f4`                    |
| `f8`                     | [Float64](/docs/en/sql-reference/data-types/float.md)           | `f8`                    |
| `S`, `U`                 | [String](/docs/en/sql-reference/data-types/string.md)           | `S`                     |
|                          | [FixedString](/docs/en/sql-reference/data-types/fixedstring.md) | `S`                     |

## Example Usage

### Saving an array in .npy format using Python

```Python
import numpy as np
arr = np.array([[[1],[2],[3]],[[4],[5],[6]]])
np.save('example_array.npy', arr)
```

### Reading a NumPy file in ClickHouse

```sql title="Query"
SELECT *
FROM file('example_array.npy', Npy)
```

```response title="Response"
┌─array─────────┐
│ [[1],[2],[3]] │
│ [[4],[5],[6]] │
└───────────────┘
```

### Selecting Data

You can select data from a ClickHouse table and save it into a file in the Npy format using the following command with clickhouse-client:

```bash
$ clickhouse-client --query="SELECT {column} FROM {some_table} FORMAT Npy" > {filename.npy}
```

## Format Settings
