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
column_name QBit(element_type, dimension[, stride])
```

* `element_type` – the type of each vector element. The allowed types are `Int8`, `BFloat16`, `Float32` and `Float64`
* `dimension` – the number of elements in each vector
* `stride` – optional. The number of dimensions stored together in one group of streams. When omitted it defaults to `dimension` (a single group). When provided, `dimension` must be a multiple of `stride`, and, when `stride` is smaller than `dimension`, `stride` must be a multiple of 8. See [Strides](#strides).

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

## Converting QBit to arrays {#converting-qbit-to-arrays}

The reverse conversion reconstructs the original vector from the bit-transposed representation, so casting a `QBit` to an `Array` returns the stored values. This is the inverse of [converting arrays to `QBit`](#converting-arrays-to-qbit):

```sql
SELECT [1, 2, 3, 4]::QBit(Float32, 4)::Array(Float32) AS vec;
```

```text
┌─vec───────┐
│ [1,2,3,4] │
└───────────┘
```

The reconstructed array uses the `QBit`'s element type, and its elements are then converted to the requested array element type. A cast that also changes the element type, such as `QBit(Float32, N)` to `Array(Float64)`, therefore works as well.

An `Array` -> `QBit` -> `Array` round trip is lossless for `Int8`, `Float32` and `Float64`. For `BFloat16` it matches a direct conversion to `BFloat16` — the only precision lost is that of `BFloat16` itself.

When the `dimension` is not a multiple of 8, the trailing padding elements present in the internal representation are dropped, so the result always has exactly `dimension` elements.

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

The number of accessible subcolumns depends on the element type (and, when strided, on the number of stride groups — see [Strides](#strides)):

* `Int8`: 8 subcolumns per stride group (1-8)
* `BFloat16`: 16 subcolumns per stride group (1-16)
* `Float32`: 32 subcolumns per stride group (1-32)
* `Float64`: 64 subcolumns per stride group (1-64)

## Strides {#strides}

By default a `QBit` stores each bit plane as a single stream spanning all `dimension` dimensions, so a search always reads whole bit planes across the full vector. The optional `stride` parameter partitions the `dimension` dimensions into `dimension / stride` contiguous groups and stores each group's bit planes in separate streams. This lets a search over only the first `D` dimensions (with `D` a multiple of `stride`) read just the streams of the groups that cover those dimensions — useful for [Matryoshka embeddings](https://arxiv.org/abs/2205.13147), where the leading dimensions form a usable lower-dimensional embedding.

```sql
CREATE TABLE test (id UInt32, vec QBit(BFloat16, 4096, 1024)) ENGINE = MergeTree ORDER BY id;
```

Here the 4096 dimensions are split into 4 groups of 1024. The subcolumns follow a group-major order: with `BFloat16` (16 bit planes), `vec.1` … `vec.16` are the 16 bit planes of the first stride group (dimensions 1–1024), `vec.17` … `vec.32` belong to the second group (dimensions 1025–2048), and so on. In general `vec.N` reads bit plane `(N-1) % element_size` of stride group `(N-1) / element_size`.

To run a reduced-dimension search, pass the number of dimensions to read as the fourth argument of the transposed distance functions (see below). The reference vector must have exactly that many elements, and the value must be a multiple of `stride`.

## Vector search functions {#vector-search-functions}

These are the distance functions for vector similarity search that use `QBit` data type:

* [`L2DistanceTransposed`](../functions/distance-functions.md#L2DistanceTransposed)
* [`cosineDistanceTransposed`](../functions/distance-functions.md#cosineDistanceTransposed)
* [`dotProductTransposed`](../functions/distance-functions.md#dotProductTransposed)

For a strided `QBit`, these functions accept an optional fourth argument `used_dims` — the number of leading dimensions to read — which only reads the stride groups covering those dimensions:

```sql
-- read 8 bit planes over the first 2048 of 4096 dimensions
SELECT id, L2DistanceTransposed(vec, reference_vec, 8, 2048) AS dist FROM test ORDER BY dist;
```
