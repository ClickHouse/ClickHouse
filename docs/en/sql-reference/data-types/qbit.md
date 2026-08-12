---
description: 'Documentation for the QBit data type in ClickHouse, which allows fine-grained quantization for approximate vector search'
keywords: ['qbit', 'data type']
sidebar_label: 'QBit'
sidebar_position: 64
slug: /sql-reference/data-types/qbit
title: 'QBit Data Type'
doc_type: 'reference'
---

The `QBit` data type reorganizes vector storage for faster approximate searches. Instead of storing each vector's elements together, it groups the same binary digit positions across all vectors.
This stores vectors at full precision while letting you choose the fine-grained quantization level at search time: read fewer bits for less I/O and faster calculations, or more bits for higher accuracy. You get the speed benefits of reduced data transfer and computation from quantization, but all the original data remains available when needed.

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

## Converting arrays to QBit {#converting-arrays-to-qbit}

Arrays convert to `QBit` when the array length matches the `QBit` dimension. The array's element type does not need to match the `QBit` element type. Any numeric element type is converted to it automatically. This lets you move an existing column of embeddings straight into a `QBit` column:

```sql
CREATE TABLE embeddings (id UInt32, embedding Array(Float32)) ENGINE = Memory;
INSERT INTO embeddings VALUES (1, [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]), (2, [0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1]);

CREATE TABLE vectors (id UInt32, vec QBit(Float32, 8)) ENGINE = Memory;
INSERT INTO vectors SELECT id, embedding FROM embeddings;

SELECT * FROM vectors ORDER BY id;
```

```text
┌─id─┬─vec───────────────────────────────┐
│  1 │ [0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8] │
│  2 │ [0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1] │
└────┴───────────────────────────────────┘
```

The conversion also works explicitly with `CAST`, for example `CAST(embedding AS QBit(Float32, 8))`.

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
* [`cosineDistanceTransposed`](../functions/distance-functions.md#cosineDistanceTransposed)
