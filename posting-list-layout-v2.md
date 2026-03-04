# `.lpst` 流增加 `last_doc_ids` / `offsets` 后置索引数组（v2）

## 1. 背景与动机

### 1.1 当前架构

当前一个 Large Posting List 包含多个 Large Block，每个 Large Block 包含多个 128-doc delta block（TurboPFor 压缩）。

当前 `.lpst` 流中**只存储** TurboPFor 压缩的 128-doc delta block 数据。每个 large block 的元数据（`last_doc_id` 和 `offset`）**仅存储在 `data.bin` 的 dictionary 流中**。

当前 V1 的 `.lpst` 布局：

```
Large Block 0:                      Large Block 1:
┌─────────────────────────┐         ┌─────────────────────────┐
│ [VarInt: bytes] [P4data]│ ×N      │ [VarInt: bytes] [P4data]│ ×M
│  (128-doc blocks)       │         │  (128-doc blocks)       │
└─────────────────────────┘         └─────────────────────────┘
```

Dictionary 流 (`data.bin`) 中存储 Large Block 级别的定位信息：

```
[doc_count] [first_doc_id] [num_large_blocks]
[last_doc_id[0], offset[0]]  [last_doc_id[1], offset[1]] ...
```

### 1.2 存在的问题

要读取某个 token 的 large posting list 中**任意一个 128-doc block**，必须：

1. 从 dictionary 流中解析出全部 `[last_doc_id, offset]` 对，定位到所属的 large block。
2. 从 large block 的起始位置**顺序扫描**所有 128-doc block 的 `[VarInt: bytes]` 头部，逐个跳过，直到到达目标 block。

这导致：
- **无法按 128-doc block 粒度做延迟物化**：当前 `ReaderStreamCursor::loadNextBlock` 必须从 large block 头部顺序读取，无法跳转到中间某个 128-doc block。
- **对高频 token 的查询代价大**：一个高频 token 可能有数百万文档，分布在成千上万个 128-doc block 中。如果查询只需要某个 doc_id 区间内的数据，仍然需要解码不相关的 block。

### 1.3 目标

在 `.lpst` 流中，每个 large block 的 Data Section **之后**，额外写入一组**后置索引数组**（`last_doc_ids[]` 和 `offsets[]`），索引该 large block 内每个 128-doc block 的位置。

写入时，`addBlock` 直接将 sub-block 数据写入输出流并记录其绝对偏移，无需临时缓存。large block 结束后将收集的元数据一次性写出为 Index Section。

对于V2, Dictionary 流中增加一个字段 `index_offset_in_lpst[i]`, 指向这个Large block的 Index Section在lpst文件中的offset; 而`offset_in_lpst[i]` 还是指向Data Section 起始。普通读路径通过 `offset_in_lpst[i]` 定位数据. 如果要支持lazy方式读, 通过`index_offset_in_lpst[i]` 直接 seek 到 Index Section 读取 packed block 索引，再利用 `absolute_offset[]` 实现 sub-block 级随机定位。

V1 和 V2 区别在于, V2 在dictionary流中有指向index section的offset, 在lpst中有index section的数据.

---

## 2. 格式定义

### 2.1 `data.bin`（dictionary 流）

```
[PrefixVarInt: doc_count]
[PrefixVarInt: first_doc_id]
[PrefixVarInt: num_large_blocks]
for i in range(num_large_blocks):
    [PrefixVarInt: last_doc_id[i]]
    [VarUInt64: offset_in_lpst[i]]
    [VarUInt64: index_offset_in_lpst[i]]   (V2 only)
```

- **v1**：每个 large block 存储 `(last_doc_id, offset_in_lpst)` 二元组。`offset_in_lpst[i]` 指向对应 large block 在 `.lpst` 中的 **Data Section 起始位置**。
- **v2**：每个 large block 存储 `(last_doc_id, offset_in_lpst, index_offset_in_lpst)` 三元组。`offset_in_lpst[i]` **仍然指向 Data Section 起始位置**（与 v1 语义相同），`index_offset_in_lpst[i]` 指向该 large block 的 **Index Section 起始位置**（Data Section 之后）。

### 2.2 `.lpst` 流 — v2 格式

每个 large block 在 `.lpst` 中由两部分组成：**Data Section**（压缩数据）+ **Index Section**（后置索引）。

```
Large Block i (in .lpst):
┌──────────────────────────────────────────────────────────┐
│ Data Section (offset_in_lpst[i] 指向此处)                 │
│ ┌──────────────────────────────────────────────────────┐ │
│ │ Sub-block 0:                                         │ │
│ │   [PrefixVarInt: packed_bytes_len]                   │ │
│ │   [TurboPFor compressed 128-doc delta data]          │ │
│ ├──────────────────────────────────────────────────────┤ │
│ │ Sub-block 1:                                         │ │
│ │   [PrefixVarInt: packed_bytes_len]                   │ │
│ │   [TurboPFor compressed 128-doc delta data]          │ │
│ ├──────────────────────────────────────────────────────┤ │
│ │ ...                                                  │ │
│ ├──────────────────────────────────────────────────────┤ │
│ │ Sub-block K (tail, possibly < 128 docs):             │ │
│ │   [PrefixVarInt: packed_bytes_len]                   │ │
│ │   [TurboPFor compressed tail delta data]             │ │
│ └──────────────────────────────────────────────────────┘ │
├──────────────────────────────────────────────────────────┤
│ Index Section (index_offset_in_lpst[i] 指向此处)          │
│ ┌──────────────────────────────────────────────────────┐ │
│ │ [PrefixVarInt: num_sub_blocks]                       │ │
│ │ for j in range(num_sub_blocks):                      │ │
│ │     [PrefixVarInt: last_doc_id[j]]                   │ │
│ │ for j in range(num_sub_blocks):                      │ │
│ │     [VarUInt64: absolute_offset[j]]                  │ │
│ └──────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────┘
```

**字段说明**：

| 字段 | 类型 | 说明 |
|------|------|------|
| `num_sub_blocks` | PrefixVarInt | 该 large block 内的 128-doc sub-block 数量（含尾部 tail block） |
| `last_doc_id[j]` | PrefixVarInt | 第 j 个 sub-block 中最后一个文档的 doc_id |
| `absolute_offset[j]` | VarUInt64 | 第 j 个 sub-block 的数据起始位置在 `.lpst` 流中的**绝对字节偏移** |

**设计要点**：

1. **Index Section 位于 Data Section 之后**：写入时 `addBlock` 直接将 sub-block 数据写入 `data_out`，同时记录其绝对偏移。large block 结束后，将收集的元数据写出为 Index Section。**无需临时缓存 Data Section，无需预计算 Index Section 大小，无需迭代收敛**。
2. **双 offset 设计**：dictionary 流中 `offset_in_lpst[i]` 指向 Data Section 起始，`index_offset_in_lpst[i]` 指向 Index Section 起始。普通读路径（v1 风格顺序读取）通过 `offset_in_lpst[i]` 定位数据；lazy 读路径通过 `index_offset_in_lpst[i]` 定位 Index Section，再利用 `absolute_offset[]` 随机定位到目标 sub-block。
3. **`last_doc_id[]` 和 `absolute_offset[]` 分开存储**而非交错存储 `(last_doc_id, offset)` 对，目的是查询时可以先只读取 `last_doc_id[]` 数组做二分查找，再按需读取对应的 `absolute_offset`，对 CPU cache 更友好。
4. **使用绝对偏移**：`absolute_offset[j]` 是第 j 个 sub-block 在 `.lpst` 流中的绝对字节位置。读取时可直接 `seek(absolute_offset[j])` 定位到目标 sub-block，无需额外计算。
5. **`num_sub_blocks` 的计算**：`num_sub_blocks = ceil(large_block_doc_count / 128)`。当 large block 包含的文档数不是 128 的整数倍时，最后一个 sub-block 是 tail block（文档数 < 128）。

### 2.3 v1 与 v2 的布局对比

v1 格式（无 Index Section）：
```
Large Block i:
┌──────────────────────────────────────────────────────────┐
│ Data Section (offset_in_lpst[i] 指向此处)                 │
│   Sub-block 0 ... Sub-block K                            │
└──────────────────────────────────────────────────────────┘
```

v2 格式（追加 Index Section + 双 offset）：
```
Large Block i:
┌──────────────────────────────────────────────────────────┐
│ Data Section (offset_in_lpst[i] 指向此处)                 │
│   Sub-block 0 ... Sub-block K                            │
├──────────────────────────────────────────────────────────┤
│ Index Section (index_offset_in_lpst[i] 指向此处)          │
│   [num_sub_blocks] [last_doc_ids...] [absolute_offsets...]│
└──────────────────────────────────────────────────────────┘
```

v1 和 v2 的 `offset_in_lpst[i]` 语义相同（均指向 Data Section 起始），因此 **v1 读路径可以直接兼容 v2 数据**——v1 reader 通过 `offset_in_lpst[i]` seek 到 Data Section 顺序读取即可，Index Section 位于 Data Section 之后不影响顺序读取（通过 `remaining_count` 控制读取边界）。

---

## 3. 写入路径

### 3.1 `LargePostingBlockWriter` 设计

`LargePostingBlockWriter` 通过 `write_block_index` 参数控制 v1/v2 行为：

- **v1** (`write_block_index = false`)：只写 Data Section，不记录 sub-block 元数据，dictionary 中只写 `(last_doc_id, offset_in_lpst)`
- **v2** (`write_block_index = true`)：写 Data Section 的同时记录元数据，flush 时追加 Index Section，dictionary 中写 `(last_doc_id, offset_in_lpst, index_offset_in_lpst)`

两种模式下，**Data Section 的写入方式完全相同**（直接写 `data_out`），**`offset_in_lpst` 的语义也完全相同**（均指向 Data Section 起始）。

**成员变量**：

```cpp
WriteBuffer & meta_out;
WriteBuffer & data_out;

UInt32 docs_per_large_block;
bool write_block_index;
UInt32 docs_in_current_block = 0;
UInt32 current_block_last_doc_id = 0;
UInt32 num_large_blocks_written = 0;
UInt64 large_block_start_offset = 0;   /// Data Section 起始偏移（v1/v2 共用）

std::vector<UInt32> packed_block_last_doc_ids;  /// 仅 v2 使用
std::vector<UInt64> packed_block_offsets;        /// 仅 v2 使用
```

**`addBlock`**：

```cpp
void addBlock(UInt32 last_doc_id, const char * data, UInt32 bytes)
{
    if (docs_in_current_block == 0)
        large_block_start_offset = data_out.count();

    if (write_block_index)
    {
        /// Record the absolute offset and last_doc_id of this sub-block before writing.
        packed_block_last_doc_ids.push_back(last_doc_id);
        packed_block_offsets.push_back(data_out.count());
    }

    VarInt::writeVarUInt32(bytes, data_out);
    data_out.write(data, bytes);

    docs_in_current_block += 128;
    current_block_last_doc_id = last_doc_id;

    if (docs_in_current_block >= docs_per_large_block)
        flushLargeBlock();
}
```

**`flushLargeBlock`**：

```cpp
void flushLargeBlock()
{
    /// v1/v2 共用: offset_in_lpst 始终指向 Data Section 起始
    VarInt::writeVarUInt32(current_block_last_doc_id, meta_out);
    writeVarUInt(large_block_start_offset, meta_out);

    if (write_block_index)
    {
        /// Data Section is already written. Now append the Index Section.
        /// Record the Index Section start offset for index_offset_in_lpst.
        UInt64 index_section_offset = data_out.count();

        UInt32 num_packed_blocks = static_cast<UInt32>(packed_block_last_doc_ids.size());

        VarInt::writeVarUInt32(num_packed_blocks, data_out);
        for (const auto & id : packed_block_last_doc_ids)
            VarInt::writeVarUInt32(id, data_out);
        for (const auto & off : packed_block_offsets)
            writeVarUInt(off, data_out);

        packed_block_last_doc_ids.clear();
        packed_block_offsets.clear();

        /// Write index_offset_in_lpst to dictionary stream (v2 only).
        writeVarUInt(index_section_offset, meta_out);
    }

    docs_in_current_block = 0;
    ++num_large_blocks_written;
}
```

### 3.2 与旧实现对比

| 方面 | 旧 v2 设计（单 offset 指向 Index） | 新 v2 设计（双 offset） |
|------|-------------------------------------|------------------------|
| `offset_in_lpst` 语义 | v2 指向 Index Section | **v1/v2 统一指向 Data Section** |
| dictionary 流每 large block 字段数 | 2 (last_doc_id, offset) | v1: 2, **v2: 3 (last_doc_id, offset, index_offset)** |
| v1 读路径兼容 v2 数据 | **不兼容** | **兼容** |
| 需要 `resolveDataSectionOffset` | 是（merge 路径反查 Data Section） | **不需要** |
| 临时缓存 | 不需要 | **不需要** |
| `addBlock` 写入目标 | 直接写 `data_out` | **直接写 `data_out`** |

---

## 4. 读取路径

### 4.1 v1 读路径（兼容 v2 数据）

v1 读路径中 dictionary 流的 `offset_in_lpst` 指向 Data Section 起始，`ReaderStreamCursor` 从该位置顺序读取所有 sub-block，通过 `remaining_count` 控制结束。

在新设计中，**v1 读路径可以直接读取 v2 数据**——因为 v2 的 `offset_in_lpst` 仍然指向 Data Section 起始，Data Section 的内部格式与 v1 完全相同。Index Section 位于 Data Section 之后，v1 reader 通过 `remaining_count` 控制读取边界，不会越界读到 Index Section。

v2 dictionary 流中额外的 `index_offset_in_lpst` 字段对 v1 reader 不可见（v1 reader 不读取该字段）。这要求反序列化代码根据 `format_version` 决定是否读取 `index_offset_in_lpst`。

涉及的函数：
- `materializeLargeBlockIntoBitmap`：seek 到 `offset_in_lpst`，顺序读取
- `PostingListStream::write`（merge 读路径）：seek 到 `offset_in_lpst`，顺序读取
- `PostingListStream::collect`：seek 到 `offset_in_lpst`，顺序读取

### 4.2 v2 读路径 — `PostingListCursor`

v2 读路径通过 `PostingListCursor` 实现，利用 Index Section 实现 sub-block 级别的随机定位。

**定位 Index Section 的方式**：dictionary 流中的 `index_offset_in_lpst[i]` 直接指向 Index Section 起始，`PostingListCursor::prepare` 直接 seek 到该位置读取索引。

v2 读路径的查询流程：

```
1. 从 dictionary 流获取 large block 列表:
   [(last_doc_id, offset_in_lpst, index_offset_in_lpst)]
2. 通过 large block 的 last_doc_id 判断目标 doc_id 范围覆盖哪些 large block
3. 对需要读取的 large block 调用 prepare(large_block):
   a. seek 到 index_offset_in_lpst
   b. 读取 Index Section: num_sub_blocks, last_doc_ids[], absolute_offsets[]
   c. 不解码任何 packed block，延迟到首次 access
4. seek(target) 时:
   a. 在 packed_block_last_doc_ids[] 上 lower_bound(target)，O(log N) 定位目标 packed block j
   b. seek 到 packed_block_offsets[j]，读取并 TurboPFor 解码
   c. 在解码结果中 lower_bound(target) 定位具体 doc_id
5. next() / linearOr() / linearAnd() 顺序遍历时:
   依次解码后续 packed block，无需 seek（流式顺序读取）
```

### 4.3 读路径选择

对于 v2 数据，有两种读取方式可选：

- **顺序读取**（v1 风格）：通过 `offset_in_lpst` seek 到 Data Section，顺序解码所有 sub-block。适用于需要物化整个 large block 的场景（如 `materializeLargeBlockIntoBitmap`）。
- **随机定位**（v2 Cursor）：通过 `index_offset_in_lpst` seek 到 Index Section，利用索引做 sub-block 级别的随机访问。适用于只需要特定 doc_id 范围内数据的场景。

两种读路径可以共存——同一份 v2 数据既支持顺序读取也支持随机定位。

---

## 5. Merge 路径

### 5.1 `PostingListStream::write` — 使用新格式

Merge 时 `PostingListStream::write` 通过 `mergePostingCursors` 做多路归并排序后写出。输出路径复用改造后的 `LargePostingBlockWriter`，可选产生 v2 格式（带 Index Section）的 `.lpst` 数据。

由于新设计中 `offset_in_lpst` 始终指向 Data Section 起始（与 v1 语义一致），merge 的读取端**不再需要 `resolveDataSectionOffset` 来反查 Data Section 位置**——直接使用 `offset_in_lpst` 即可 seek 到 Data Section 顺序读取。

### 5.2 `PostingListStream::merge` — 无需修改

`PostingListStream::merge` 操作的是内存中的 `LazyPostingStream` 和 `ReaderStreamVector`，不涉及磁盘格式。merge 路径只在最终 `write` 时才序列化到磁盘，此时由新的 `LargePostingBlockWriter` 处理。

---

## 6. 空间开销分析

### 6.1 Index Section 大小估算

每个 large block 的 Index Section 包含：
- 1 个 `num_sub_blocks`（PrefixVarInt，通常 1-2 字节）
- N 个 `last_doc_id`（PrefixVarInt，每个 1-5 字节，通常 3-4 字节）
- N 个 `absolute_offset`（VarUInt64，每个 1-9 字节，通常 4-6 字节）

其中 N = `docs_per_large_block / 128`。

| `posting_list_block_size` | N (sub-blocks) | Index Section 大小估算 |
|---------------------------|----------------|----------------------|
| 1M (默认) | 8192 | ~56-80 KB |
| 128K | 1024 | ~7-10 KB |
| 16K | 128 | ~896 B - 1.2 KB |

### 6.2 与 Data Section 的比例

一个包含 1M 文档的 large block，Data Section 大小取决于 TurboPFor 压缩率，通常 1-4 字节/文档，约 1-4 MB。Index Section 约 56-80 KB，**额外开销约 1.5-8%**。

### 6.3 Dictionary 流额外开销

v2 每个 large block 在 dictionary 流中额外存储一个 `index_offset_in_lpst`（VarUInt64，通常 4-6 字节）。对于一个 3M 文档的 token（3 个 large block），额外 12-18 字节，可忽略不计。

### 6.4 与 dictionary 流的冗余

Dictionary 流中每个 large block 存储 `(last_doc_id, offset_in_lpst[, index_offset_in_lpst])` 元组。Index Section 存储 N 个 sub-block 的索引。两者不冗余——dictionary 流提供 large block 级定位，Index Section 提供 sub-block 级定位。

---

## 7. 示例

假设一个 token 有 `doc_count = 3,000,000`，`posting_list_block_size = 1M`（对齐后 1,048,576），则：

- `large_doc_count = 2,999,999`（第一个 doc_id 内联）
- `num_large_blocks = ceil(2,999,999 / 1,048,576) = 3`
- Large Block 0: 1,048,576 docs -> `num_sub_blocks = 1,048,576 / 128 = 8,192`
- Large Block 1: 1,048,576 docs -> `num_sub_blocks = 8,192`
- Large Block 2: 902,847 docs -> `num_sub_blocks = ceil(902,847 / 128) = 7,054`

**Dictionary 流** (`data.bin`)：
```
[VarInt: 3000000]                       // doc_count
[VarInt: 42]                            // first_doc_id (示例)
[VarInt: 3]                             // num_large_blocks

// large block 0:
[VarInt: 1200000]                       // last_doc_id
[VarUInt64: 0]                          // offset_in_lpst → Data Section 0 起始
[VarUInt64: ~4.0MB]                     // index_offset_in_lpst → Index Section 0 起始 (V2 only)

// large block 1:
[VarInt: 2500000]                       // last_doc_id
[VarUInt64: ~4.08MB]                    // offset_in_lpst → Data Section 1 起始
[VarUInt64: ~8.1MB]                     // index_offset_in_lpst → Index Section 1 起始 (V2 only)

// large block 2:
[VarInt: 3500000]                       // last_doc_id
[VarUInt64: ~8.18MB]                    // offset_in_lpst → Data Section 2 起始
[VarUInt64: ~12.1MB]                    // index_offset_in_lpst → Index Section 2 起始 (V2 only)
```

**`.lpst` 流中 Large Block 0 的结构**（v2）：
```
offset 0:       === Large Block 0 Data Section === (offset_in_lpst[0] 指向此处)
                [VarInt: 240] [P4 data: 128-doc block 0]
                [VarInt: 238] [P4 data: 128-doc block 1]
                ...
                [VarInt: 242] [P4 data: 128-doc block 8191]
offset ~4.0MB:  === Large Block 0 Index Section === (index_offset_in_lpst[0] 指向此处)
                [VarInt: 8192]                            // num_sub_blocks
                [VarInt: 170] [VarInt: 298] ... (x8192)   // last_doc_ids
                [VarUInt64: 0] [VarUInt64: 242] ... (x8192)  // absolute_offsets
offset ~4.08MB: === Large Block 1 Data Section === (offset_in_lpst[1] 指向此处)
                ...
```

**查询示例 1**（v1 风格顺序读取）：物化 Large Block 1 的全部文档

1. Dictionary 流: 获取 `offset_in_lpst[1] = ~4.08MB`
2. seek 到 `~4.08MB`（Data Section 1 起始）
3. 顺序读取 8,192 个 sub-block，逐个解码为 bitmap

**查询示例 2**（v2 Cursor 随机定位）：查找 `doc_id = 1,800,000`

1. Dictionary 流: `last_doc_id[0]=1200000 < 1800000 <= last_doc_id[1]=2500000` -> 需要 Large Block 1
2. seek 到 `index_offset_in_lpst[1]`（Index Section 1 起始）
3. 读取 Index Section: `num_sub_blocks=8192`, `last_doc_ids[0..8191]`, `absolute_offsets[0..8191]`
4. 二分查找 `last_doc_ids[]`: 找到 `j` 使得 `last_doc_ids[j-1] < 1800000 <= last_doc_ids[j]`
5. 直接 seek 到 `absolute_offsets[j]`
6. 读取并解码该 128-doc block
7. 在解码结果中查找 `doc_id = 1,800,000`

---

## 8. 关键实现要点

### 8.1 `absolute_offset` 的计算

由于 Index Section 位于 Data Section 之后，`absolute_offset[j]` 在 `addBlock` 写入时就已确定（= `data_out.count()`，即写入前的当前流位置）。**无需预计算 Index Section 大小，无需迭代收敛**。这是将 Index Section 放在 Data Section 之后的核心优势。

读取时拿到 `absolute_offset[j]` 后可直接 `seek(absolute_offset[j])`，无需知道 Data Section 或 Index Section 的起始位置。

### 8.2 delta 解码基准

当从 large block 中间的某个 sub-block 开始读取时，delta 解码需要一个 base doc_id。对于 sub-block j：

- `j == 0` 且是 large block 0：base = `first_doc_id`（来自 dictionary 流），且 `first_doc_id` 本身也包含在结果中。
- `j == 0` 且不是 large block 0：base = 前一个 large block 的 `last_doc_id`（来自 dictionary 流）。
- `j > 0`：base = `last_doc_ids[j-1]`（来自 Index Section）。

这是 v2 Index Section 存储 `last_doc_ids[]` 的关键原因——不仅用于二分查找定位，还充当 sub-block 级别的 delta 解码基准。

### 8.3 尾部 sub-block

每个 large block 的最后一个 sub-block 可能是 tail block（包含 < 128 个文档）。Index Section 中它与普通 sub-block 无区别——`last_doc_id` 和 `absolute_offset` 正常记录。读取时通过 `remaining_count` 判断实际文档数。

### 8.4 内存使用

读取 Index Section 时需要在内存中持有 `last_doc_ids[]` 和 `absolute_offsets[]` 数组。对于默认的 `posting_list_block_size = 1M`，N = 8192，内存开销约 8192 * (4 + 8) = 96 KB。这在可接受范围内，且可通过减小 `posting_list_block_size` 进一步控制。

### 8.5 v1 与 v2 的兼容性

新设计中 `offset_in_lpst` 的语义在 v1 和 v2 之间**完全一致**（均指向 Data Section 起始），因此：

- **v1 读路径可以读取 v2 数据**：v1 reader 只使用 `offset_in_lpst` seek 到 Data Section 顺序读取，忽略 `index_offset_in_lpst`。Index Section 位于 Data Section 之后，不影响顺序读取（通过 `remaining_count` 控制边界）。
- **v2 读路径可以读取 v1 数据**：v1 数据没有 `index_offset_in_lpst` 字段，v2 reader 退化为 v1 风格的顺序读取（不使用 lazy cursor）。
- **v2 数据反序列化兼容**：反序列化 dictionary 流时，根据 `format_version` 决定是否读取 `index_offset_in_lpst` 字段。v1 格式不包含该字段，v2 格式包含。

这消除了旧设计中的不兼容问题，简化了 merge 路径的实现。

### 8.6 dictionary 流的反序列化

反序列化 dictionary 流中 large block 元信息时，需要根据 `format_version` 区分处理：

```cpp
for (UInt32 i = 0; i < num_large_blocks; ++i)
{
    readPrefixVarUInt32(last_doc_id, buf);
    readVarUInt(offset_in_lpst, buf);

    if (postingListFormatHasBlockIndex(format_version))
        readVarUInt(index_offset_in_lpst, buf);
}
```

`LargeBlockMeta` 结构需要增加 `index_offset_in_lpst` 字段（v1 数据中该字段为 0 或未设置）。

---

## 9. 与 V1 格式对比总结

| 方面 | V1 | V2 |
|------|----|----|
| Large Block 布局 | 仅 Data Section | Data Section + Index Section |
| Index Section 位置 | 无 | Data Section **之后** |
| `offset_in_lpst` 指向 | Data Section 起始 | Data Section 起始（**与 v1 相同**） |
| `index_offset_in_lpst` | 无 | **Index Section 起始** |
| Dictionary 流每 block 字段 | `(last_doc_id, offset)` | `(last_doc_id, offset, index_offset)` |
| Sub-block offset 类型 | 无 | 绝对偏移（VarUInt64） |
| Sub-block 定位 | 顺序扫描 | Index Section 二分查找 + 直接 seek |
| 写入临时缓存 | 无 | **无**（直接写入） |
| 额外空间开销 | 无 | ~1.5-8%（Index Section）+ 每 block ~5 字节（dictionary 流） |
| 读取放大 | 读整个 large block | 可选：读整个 large block **或** 读 Index Section + 目标 sub-block(s) |
| v1 读路径兼容 v2 数据 | — | **兼容** |
| v2 读路径兼容 v1 数据 | — | **兼容**（退化为顺序读取） |
