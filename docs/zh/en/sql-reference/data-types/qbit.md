---
description: 'ClickHouse 中 QBit 数据类型的文档，它支持用于近似向量搜索的细粒度量化'
keywords: ['qbit', 'data type']
sidebar_label: 'QBit'
sidebar_position: 64
slug: /sql-reference/data-types/qbit
title: 'QBit 数据类型'
doc_type: 'reference'
---

`QBit` 数据类型会重新组织向量的存储方式，以加快近似搜索。它不是将每个向量的各个元素存储在一起，而是将所有向量中相同二进制位位置的值分组存储。
这样既能以全精度存储向量，又允许你在搜索时选择细粒度的量化级别：读取更少的位可减少 I/O 并加快计算，读取更多的位则可获得更高的准确率。你既能获得量化在减少数据传输和计算方面带来的速度优势，又能在需要时使用全部原始数据。

要声明 `QBit` 类型的列，请使用以下语法：

```sql
column_name QBit(element_type, dimension[, stride])
```

* `element_type` – 每个向量元素的类型。允许的类型有 `Int8`、`BFloat16`、`Float32` 和 `Float64`
* `dimension` – 每个向量中的元素个数
* `stride` – 可选。表示在同一组stream中一起存储的维度数。省略时，默认值为 `dimension` (即单组) 。指定该参数时，`dimension` 必须是 `stride` 的倍数；并且当 `stride` 小于 `dimension` 时，`stride` 还必须是 8 的倍数。请参见 [Strides](#strides)。

<div id="creating-qbit">
  ## 创建 QBit
</div>

在表的列定义中使用 `QBit` 类型：

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

<div id="converting-arrays-to-qbit">
  ## 将数组转换为 QBit
</div>

当数组长度与 `QBit` 的维度一致时，数组可转换为 `QBit`。数组的元素类型不必与 `QBit` 的元素类型一致。任何数值型元素类型都会自动转换为 `QBit` 的元素类型。因此，你可以将现有的嵌入向量列直接迁移到 `QBit` 列中：

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

这种转换也可以显式地使用 `CAST` 来完成，例如 `CAST(embedding AS QBit(Float32, 8))`。

<div id="converting-qbit-to-arrays">
  ## 将 QBit 转换为数组
</div>

反向转换会从按位转置后的表示中还原原始向量，因此将 `QBit` 转换为 `Array` 会返回存储的值。这一过程与[将数组转换为 `QBit`](#converting-arrays-to-qbit)互为逆过程：

```sql
SELECT [1, 2, 3, 4]::QBit(Float32, 4)::Array(Float32) AS vec;
```

```text
┌─vec───────┐
│ [1,2,3,4] │
└───────────┘
```

重建后的数组使用 `QBit` 的元素类型，然后其元素会被转换为所请求的数组元素类型。因此，连同元素类型一起更改的类型转换也同样可行，例如从 `QBit(Float32, N)` 转换为 `Array(Float64)`。

对于 `Int8`、`Float32` 和 `Float64`，`Array` -&gt; `QBit` -&gt; `Array` 的往返转换是无损的。对于 `BFloat16`，其结果与直接转换为 `BFloat16` 一致——唯一损失的精度仅来自 `BFloat16` 本身。

当 `dimension` 不是 8 的倍数时，内部表示中存在的尾部填充元素会被丢弃，因此结果始终恰好包含 `dimension` 个元素。

<div id="qbit-subcolumns">
  ## QBit 子列
</div>

`QBit` 实现了一种子列访问方式，允许你访问已存储向量中的各个位平面。每个比特位都可以使用 `.N` 语法访问，其中 `N` 表示比特位的位置：

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

可访问的子列数量取决于元素类型 (采用 stride 分组时，还取决于 stride 组的数量——参见 [Strides](#strides)) ：

* `Int8`：每个 stride 组 8 个子列 (1-8) 
* `BFloat16`：每个 stride 组 16 个子列 (1-16) 
* `Float32`：每个 stride 组 32 个子列 (1-32) 
* `Float64`：每个 stride 组 64 个子列 (1-64)

<div id="strides">
  ## 步幅
</div>

默认情况下，`QBit` 会将每个位平面存储为一个覆盖全部 `dimension` 维度的单个 stream，因此搜索时总是需要读取整个向量的完整位平面。可选的 `stride` 参数会将 `dimension` 维度划分为 `dimension / stride` 个连续分组，并将每个分组的位平面分别存储在独立的stream中。这样一来，如果只在前 `D` 个维度上进行搜索 (其中 `D` 是 `stride` 的倍数) ，就只需读取覆盖这些维度的那些分组对应的stream——这对 [Matryoshka embeddings](https://arxiv.org/abs/2205.13147) 很有用，因为前面的维度可以构成可用的低维嵌入向量。

```sql
CREATE TABLE test (id UInt32, vec QBit(BFloat16, 4096, 1024)) ENGINE = MergeTree ORDER BY id;
```

这里将 4096 个维度拆分为 4 组，每组 1024 个。子列按组优先顺序排列：对于 `BFloat16` (16 个位平面) ，`vec.1` … `vec.16` 是第一个 stride group (维度 1–1024) 的 16 个位平面，`vec.17` … `vec.32` 属于第二组 (维度 1025–2048) ，依此类推。一般情况下，`vec.N` 读取 stride group `(N-1) / element_size` 的位平面 `(N-1) % element_size`。

要运行降维搜索，请将要读取的维度数作为转置距离函数的第四个参数传入 (见下文) 。参考向量必须恰好包含这么多个元素，并且该值必须是 `stride` 的倍数。

<div id="vector-search-functions">
  ## 向量搜索函数
</div>

以下是用于向量相似度搜索的距离函数，使用 `QBit` 数据类型：

* [`L2DistanceTransposed`](../functions/distance-functions.md#L2DistanceTransposed)
* [`cosineDistanceTransposed`](../functions/distance-functions.md#cosineDistanceTransposed)
* [`dotProductTransposed`](../functions/distance-functions.md#dotProductTransposed)

对于带 stride 的 `QBit`，这些函数接受一个可选的第四个参数 `used_dims`——即要读取的前几个维度——并且只会读取覆盖这些维度的 stride 分组：

```sql
-- read 8 bit planes over the first 2048 of 4096 dimensions
SELECT id, L2DistanceTransposed(vec, reference_vec, 8, 2048) AS dist FROM test ORDER BY dist;
```