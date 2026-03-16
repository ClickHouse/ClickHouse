---
description: 'Documentation for the QBit data type in ClickHouse, which allows fine-grained quantization for approximate vector search'
keywords: ['qbit', 'data type']
sidebar_label: 'QBit'
sidebar_position: 64
slug: /sql-reference/data-types/qbit
title: 'QBit Data Type'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<ExperimentalBadge/>

The `QBit` data type reorganizes vector storage for faster approximate searches. Instead of storing each vector's elements together, it groups the same binary digit positions across all vectors.
This stores vectors at full precision while letting you choose the fine-grained quantization level at search time: read fewer bits for less I/O and faster calculations, or more bits for higher accuracy. You get the speed benefits of reduced data transfer and computation from quantization, but all the original data remains available when needed.

:::note
`QBit` data type and distance functions associated with it are currently experimental.
To enable them, please first run `SET allow_experimental_qbit_type = 1`.
If you run into problems, kindly open an issue in the [ClickHouse repository](https://github.com/clickhouse/clickhouse/issues).
:::

To declare a column of `QBit` type, use the following syntax:

```sql
column_name QBit(element_type, dimension)
```

* `element_type` – the type of each vector element. The allowed types are `BFloat16`, `Float32` and `Float64`
* `dimension` – the number of elements in each vector

## Creating QBit {#creating-qbit}

Using the `QBit` type in table column definition:

```sql
CREATE TABLE test (id UInt32, vec QBit(Float32, 8)) ENGINE = Memory;
INSERT INTO test VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8]), (2, [9, 10, 11, 12, 13, 14, 15, 16]);
SELECT vec FROM test ORDER BY id;
```

```text
┌─vec──────────────────────┐
│ [1,2,3,4,5,6,7,8]        │
│ [9,10,11,12,13,14,15,16] │
└──────────────────────────┘
```

## QBit subcolumns {#qbit-subcolumns}

`QBit` implements a subcolumn access pattern that allows you to access individual bit planes of the stored vectors. Each bit position can be accessed using the `.N` syntax, where `N` is the bit position:

```sql
CREATE TABLE test (id UInt32, vec QBit(Float32, 8)) ENGINE = Memory;
INSERT INTO test VALUES (1, [0, 0, 0, 0, 0, 0, 0, 0]);
INSERT INTO test VALUES (1, [-0, -0, -0, -0, -0, -0, -0, -0]);
SELECT bin(vec.1) FROM test;
```

```text
┌─bin(tupleElement(vec, 1))─┐
│ 00000000                  │
│ 11111111                  │
└───────────────────────────┘
```

The number of accessible subcolumns depends on the element type:

* `BFloat16`: 16 subcolumns (1-16)
* `Float32`: 32 subcolumns (1-32)
* `Float64`: 64 subcolumns (1-64)

## Vector search functions {#vector-search-functions}

These are the distance functions for vector similarity search that use `QBit` data type:

* [`L2DistanceTransposed`](../functions/distance-functions.md#L2DistanceTransposed)
