---
description: 'Memory 引擎以未压缩形式将数据存储在 RAM 中。数据按接收时的原样存储。换句话说，读取该表几乎没有任何开销。'
sidebar_label: 'Memory'
sidebar_position: 110
slug: /engines/table-engines/special/memory
title: 'Memory 表引擎'
doc_type: 'reference'
---

:::note
在 ClickHouse Cloud 上使用 Memory 表引擎时，数据不会在所有节点之间复制 (这是设计使然) 。为确保所有查询都路由到同一个节点，并使 Memory 表引擎按预期工作，可以采用以下任一方法：

* 在同一会话中执行所有操作
* 使用采用 TCP 或原生接口的客户端 (这样可支持粘性连接) ，例如 [ClickHouse 客户端](/zh/interfaces/client)
  :::

Memory 引擎以未压缩形式将数据存储在 RAM 中。数据按接收时的原样存储。换句话说，读取该表几乎没有任何开销。
并发数据访问会进行同步。锁持有时间很短：读写操作彼此不会阻塞。
不支持索引。读取会并行执行。

对于简单查询，由于无需从磁盘读取数据，也无需解压或反序列化，因此可以获得最高吞吐量 (超过 10 GB/秒) 。 (需要说明的是，在很多情况下，MergeTree 引擎的吞吐量也几乎同样高。)
服务器重启后，表中的数据会消失，表将变为空表。
通常情况下，使用这种表引擎并无充分理由。不过，它可用于测试，以及在相对较少的行数 (最多约 100,000,000 行) 下需要最高速度的任务。

系统会将 Memory 引擎用于带有外部查询数据的临时表 (参见“用于处理查询的外部数据”一节) ，以及实现 `GLOBAL IN` (参见“IN 运算符”一节) 。

可以指定上下限来限制 Memory 引擎表的大小，从而使其能够有效充当循环缓冲区 (参见[引擎参数](#engine-parameters)) 。

<div id="engine-parameters">
  ## 引擎参数
</div>

* `min_bytes_to_keep` — 当内存表设置了大小上限时，保留的最小字节数。
  * 默认值：`0`
  * 需要 `max_bytes_to_keep`
* `max_bytes_to_keep` — 内存表中保留的最大字节数；每次插入时都会删除最旧的行 (即循环缓冲区) 。如果添加较大的块时，待删除的最旧一批行会使保留量低于 `min_bytes_to_keep` 限制，则最大字节数可能超过设定的限制。
  * 默认值：`0`
* `min_rows_to_keep` — 当内存表设置了大小上限时，保留的最小行数。
  * 默认值：`0`
  * 需要 `max_rows_to_keep`
* `max_rows_to_keep` — 内存表中保留的最大行数；每次插入时都会删除最旧的行 (即循环缓冲区) 。如果添加较大的块时，待删除的最旧一批行会使保留量低于 `min_rows_to_keep` 限制，则最大行数可能超过设定的限制。
  * 默认值：`0`
* `compress` - 是否压缩内存中的数据。
  * 默认值：`false`

<div id="usage">
  ## 用法
</div>

**初始化相关设置**

```sql
CREATE TABLE memory (i UInt32) ENGINE = Memory SETTINGS min_rows_to_keep = 100, max_rows_to_keep = 1000;
```

**修改设置**

```sql
ALTER TABLE memory MODIFY SETTING min_rows_to_keep = 100, max_rows_to_keep = 1000;
```

**注意：** `bytes` 和 `rows` 这两个封顶参数可以同时设置，不过实际会采用 `max` 和 `min` 中较小的限制值。

<div id="examples">
  ## 示例
</div>

```sql
CREATE TABLE memory (i UInt32) ENGINE = Memory SETTINGS min_bytes_to_keep = 4096, max_bytes_to_keep = 16384;

/* 1. testing oldest block doesn't get deleted due to min-threshold - 3000 rows */
INSERT INTO memory SELECT * FROM numbers(0, 1600); -- 8'192 bytes

/* 2. adding block that doesn't get deleted */
INSERT INTO memory SELECT * FROM numbers(1000, 100); -- 1'024 bytes

/* 3. testing oldest block gets deleted - 9216 bytes - 1100 */
INSERT INTO memory SELECT * FROM numbers(9000, 1000); -- 8'192 bytes

/* 4. checking a very large block overrides all */
INSERT INTO memory SELECT * FROM numbers(9000, 10000); -- 65'536 bytes

SELECT total_bytes, total_rows FROM system.tables WHERE name = 'memory' AND database = currentDatabase();
```

```text
┌─total_bytes─┬─total_rows─┐
│       65536 │      10000 │
└─────────────┴────────────┘
```

另外，对于行：

```sql
CREATE TABLE memory (i UInt32) ENGINE = Memory SETTINGS min_rows_to_keep = 4000, max_rows_to_keep = 10000;

/* 1. testing oldest block doesn't get deleted due to min-threshold - 3000 rows */
INSERT INTO memory SELECT * FROM numbers(0, 1600); -- 1'600 rows

/* 2. adding block that doesn't get deleted */
INSERT INTO memory SELECT * FROM numbers(1000, 100); -- 100 rows

/* 3. testing oldest block gets deleted - 9216 bytes - 1100 */
INSERT INTO memory SELECT * FROM numbers(9000, 1000); -- 1'000 rows

/* 4. checking a very large block overrides all */
INSERT INTO memory SELECT * FROM numbers(9000, 10000); -- 10'000 rows

SELECT total_bytes, total_rows FROM system.tables WHERE name = 'memory' AND database = currentDatabase();
```

```text
┌─total_bytes─┬─total_rows─┐
│       65536 │      10000 │
└─────────────┴────────────┘
```