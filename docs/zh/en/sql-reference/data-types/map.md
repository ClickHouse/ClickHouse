---
description: 'ClickHouse 中 Map 数据类型的文档'
sidebar_label: 'Map(K, V)'
sidebar_position: 36
slug: /sql-reference/data-types/map
title: 'Map(K, V)'
doc_type: 'reference'
---

数据类型 `Map(K, V)` 用于存储键值对。

与其他数据库不同，在 ClickHouse 中，Map 中的键不要求唯一，也就是说，一个 Map 可以包含两个键相同的元素。
(这是因为 Map 在内部实现为 `Array(Tuple(K, V))`。)

你可以使用语法 `m[k]` 获取 Map `m` 中键 `k` 对应的值。
另外，`m[k]` 会扫描整个 Map，也就是说，该操作的运行时间与 Map 的大小呈线性关系。

**参数**

* `K` — Map 键的类型。可以是任意类型，但 [Nullable](../../sql-reference/data-types/nullable.md) 以及嵌套了 [Nullable](../../sql-reference/data-types/nullable.md) 类型的 [LowCardinality](../../sql-reference/data-types/lowcardinality.md) 除外。
* `V` — Map 值的类型。可以是任意类型。

**示例**

创建一个包含 Map 类型列的表：

```sql title="Query"
CREATE TABLE tab (m Map(String, UInt64)) ENGINE=Memory;
INSERT INTO tab VALUES ({'key1':1, 'key2':10}), ({'key1':2,'key2':20}), ({'key1':3,'key2':30});
```

要获取 `key2` 的值：

```sql title="Query"
SELECT m['key2'] FROM tab;
```

```text title="Response"
┌─arrayElement(m, 'key2')─┐
│                      10 │
│                      20 │
│                      30 │
└─────────────────────────┘
```

如果请求的键 `k` 不在 map 中，`m[k]` 会返回该值类型的默认值，例如整数类型返回 `0`，字符串类型返回 `''`。
要检查某个键是否存在于 map 中，可以使用函数 [mapContains](/zh/sql-reference/functions/tuple-map-functions#mapContainsKey)。

```sql title="Query"
CREATE TABLE tab (m Map(String, UInt64)) ENGINE=Memory;
INSERT INTO tab VALUES ({'key1':100}), ({});
SELECT m['key1'] FROM tab;
```

```text title="Response"
┌─arrayElement(m, 'key1')─┐
│                     100 │
│                       0 │
└─────────────────────────┘
```

<div id="converting-tuple-to-map">
  ## 将 Tuple 转换为 Map
</div>

`Tuple()` 类型的值可以通过 [CAST](/zh/sql-reference/functions/type-conversion-functions#CAST) 函数转换为 `Map()` 类型的值：

**示例**

```sql title="Query"
SELECT CAST(([1, 2, 3], ['Ready', 'Steady', 'Go']), 'Map(UInt8, String)') AS map;
```

```text title="Response"
┌─map───────────────────────────┐
│ {1:'Ready',2:'Steady',3:'Go'} │
└───────────────────────────────┘
```

<div id="reading-subcolumns-of-map">
  ## 读取 Map 的子列
</div>

为避免读取整个 Map，在某些情况下可以使用 `keys` 和 `values` 子列。

**示例**

```sql title="Query"
CREATE TABLE tab (m Map(String, UInt64)) ENGINE = Memory;
INSERT INTO tab VALUES (map('key1', 1, 'key2', 2, 'key3', 3));

SELECT m.keys FROM tab; --   same as mapKeys(m)
SELECT m.values FROM tab; -- same as mapValues(m)
```

```text title="Response"
┌─m.keys─────────────────┐
│ ['key1','key2','key3'] │
└────────────────────────┘

┌─m.values─┐
│ [1,2,3]  │
└──────────┘
```

<div id="bucketed-map-serialization">
  ## MergeTree 中的分桶 Map 序列化
</div>

默认情况下，MergeTree 中的 `Map` 列存储为单个 `Array(Tuple(K, V))` 流。
使用 `m['key']` 读取单个键时，需要扫描整个列——也就是每一行中的所有键值对——即使只需要这一个键也是如此。
对于包含大量不同键的 Map，这会成为性能瓶颈。

分桶序列化 (`with_buckets`) 会通过对键进行哈希，将键值对拆分为多个彼此独立的子流 (桶) 。
当查询访问 `m['key']` 时，只会从磁盘读取包含该键的那个桶，跳过所有其他桶。

<div id="enabling-bucketed-serialization">
  ### 启用分桶序列化
</div>

```sql
CREATE TABLE tab (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    max_buckets_in_map = 32,
    map_buckets_strategy = 'sqrt';
```

为避免拖慢插入操作，你可以对 0 级 parts (在 `INSERT` 期间创建) 保留 `basic` 序列化，并且仅对合并后的 parts 使用 `with_buckets`：

```sql
CREATE TABLE tab (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'basic',
    max_buckets_in_map = 32,
    map_buckets_strategy = 'sqrt';
```

<div id="how-it-works">
  ### 工作原理
</div>

当数据分区片段以 `with_buckets` 序列化方式写入时：

1. 根据块统计信息计算每行的平均键数量。
2. 根据已配置的策略确定桶数 (参见 [Settings](#bucketed-map-settings)) 。
3. 通过对键进行哈希，将每个键值对分配到对应的桶中：`bucket = hash(key) % num_buckets`。
4. 每个桶都会作为独立的子流存储，拥有各自的键、值和偏移量。
5. `buckets_info` 元数据流会记录桶数和统计信息。

当查询读取某个特定键 (`m['key']`) 时，优化器会将该表达式重写为键子列 (`m.key_<serialized_key>`) 。
序列化层会计算请求的键 属于哪个桶，并且只从磁盘读取这一个桶。

当读取完整的 Map 时 (例如 `SELECT m`) ，系统会读取所有桶，并将它们重新组装为原始 Map。由于需要读取并合并多个子流，这比 `basic` 序列化更慢。

:::note
使用 `with_buckets` 序列化时，Map 值中的键 顺序可能与原始插入顺序不同。键 会按哈希分布到各个桶中，并按桶顺序重新组装，而不是按插入顺序。使用 `basic` 序列化时，会保留已插入 Map 中的键 顺序。
:::

不同的 parts 之间，桶数可以不同。当合并桶数不同的 parts 时，新 part 的桶数会根据合并后的统计信息重新计算。使用 `basic` 和 `with_buckets` 序列化的 parts 可以在同一张表中共存，并且会被透明地合并。

<div id="bucketed-map-settings">
  ### 设置
</div>

| 设置项                                              | 默认值     | 描述                                                                                                                                                                                                |
| ------------------------------------------------ | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `map_serialization_version`                      | `basic` | `Map` 列的序列化格式。`basic` 以单个数组 stream 存储。`with_buckets` 会将键拆分到多个桶中，以加快单键读取。                                                                                                                          |
| `map_serialization_version_for_zero_level_parts` | `basic` | 零级 parts (由 `INSERT` 创建) 的序列化格式。允许插入时继续使用 `basic` 以避免写入开销，而合并后的 parts 则使用 `with_buckets`。                                                                                                         |
| `max_buckets_in_map`                             | `32`    | 桶数量的上限。实际数量取决于 `map_buckets_strategy`。允许的最大值为 256。                                                                                                                                                |
| `map_buckets_strategy`                           | `sqrt`  | 根据 map 平均大小计算桶数量的策略：`constant` — 始终使用 `max_buckets_in_map`；`sqrt` — 使用 `round(coefficient * sqrt(avg_size))`；`linear` — 使用 `round(coefficient * avg_size)`。结果会被限制在 `[1, max_buckets_in_map]` 范围内。 |
| `map_buckets_coefficient`                        | `1.0`   | `sqrt` 和 `linear` 策略的乘数。策略为 `constant` 时会忽略此值。                                                                                                                                                    |
| `map_buckets_min_avg_size`                       | `32`    | 启用分桶所需的每行平均最小键数。如果平均值低于该阈值，则无论其他设置如何，都只使用单个桶。设为 `0` 可禁用该阈值。                                                                                                                                       |

<div id="performance-trade-offs">
  ### 性能权衡
</div>

下表汇总了在不同 Map 大小下 (每行 10 到 10,000 个键) ，`with_buckets` 相比 `basic` 序列化带来的性能影响。桶数量由 `sqrt` 策略确定，上限为 32。具体数值取决于键/值类型、数据分布和硬件。

| 操作                                             | 10 个键       | 100 个键      | 1,000 个键    | 10,000 个键   | 说明                                                                      |
| ---------------------------------------------- | ----------- | ----------- | ----------- | ----------- | ----------------------------------------------------------------------- |
| **单键查找** (`m['key']`)                          | 快 1.6–3.2 倍 | 快 4.5–7.7 倍 | 快 16–39 倍   | 快 21–49 倍   | 只需读取一个桶，而不是整个列。                                                         |
| **5 个键查找**                                     | ~1x         | 快 1.5–3.1 倍 | 快 2.9–8.3 倍 | 快 4.5–6.7 倍 | 每个键读取各自所在的桶；部分桶可能重叠。                                                    |
| **PREWHERE** (`SELECT m WHERE m['key'] = ...`) | 快 1.5–3.0 倍 | 快 2.9–7.3 倍 | 快 5.3–31 倍  | 快 20–45 倍   | PREWHERE 过滤器只读取一个桶；仅对匹配的行读取完整 Map。加速效果取决于选择性——匹配的粒度越少，完整 Map 的 I/O 就越少。 |
| **完整 Map 扫描** (`SELECT m`)                     | 慢 ~2 倍      | 慢 ~2 倍      | 慢 ~2 倍      | 慢 ~2 倍      | 必须读取并重组所有桶。                                                             |
| **INSERT**                                     | 慢 1.5–2.5 倍 | 慢 1.5–2.5 倍 | 慢 1.5–2.5 倍 | 慢 1.5–2.5 倍 | 对键进行哈希并写入多个子流会带来额外开销。                                                   |

<div id="recommendations">
  ### 建议
</div>

* **小型 Map (平均 &lt; 32 个键) ：** 保持使用 `basic` 序列化。对于小型 Map，分桶带来的额外开销并不值得。默认的 `map_buckets_min_avg_size = 32` 会自动应用这一限制。
* **中型 Map (32–100 个键) ：** 如果查询经常访问单个键，请使用 `with_buckets` 并配合 `sqrt` 策略。单键查找可提速 4–8 倍。
* **大型 Map (100+ 个键) ：** 使用 `with_buckets`。单键查找可快 16–49 倍。可考虑设置 `map_serialization_version_for_zero_level_parts = 'basic'`，以使 insert 速度接近基线。
* **工作负载以完整 Map 扫描为主：** 保持使用 `basic`。分桶序列化会给全量扫描带来约 2 倍的额外开销。
* **混合工作负载 (部分键查找，部分全量扫描) ：** 使用 `with_buckets`，并将零级 parts 设为 `basic`。`PREWHERE` 优化会先只读取与过滤条件相关的桶，然后仅对匹配的行读取完整 Map，从而带来显著的整体加速。

<div id="map-alternatives">
  ### 替代方案
</div>

如果分桶 `Map` 序列化不适用于你的用例，还可以通过另外两种方案来提升键级访问性能：

<div id="using-the-json-data-type">
  #### 使用 JSON 数据类型
</div>

[JSON](/zh/sql-reference/data-types/newjson) 数据类型会将每个高频 path 存储为单独的动态 子列。超过 `max_dynamic_paths` 限制的 path 会进入[共享数据结构](/zh/sql-reference/data-types/newjson#shared-data-structure)，该结构可使用 `advanced` 序列化来优化单个 path 的读取。有关 `advanced` 序列化的详细说明，请参阅这篇[博客文章](https://clickhouse.com/blog/json-data-type-gets-even-better)。

| 方面         | `Map` with buckets                                                | `JSON`                                                                             |
| ---------- | ----------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| 单个 键 读取  | 读取一个 桶 (其中可能包含其他 键) 。该 桶 中的所有 键-值 pair 都会被反序列化。 | 高频 path 可直接从动态 子列 中读取。低频 path 会进入共享数据；使用 `advanced` 序列化时，只会读取对应精确 path 的数据。 |
| 值类型        | 所有值共享同一种类型 `V`                                                    | 每个 path 都可以有自己的类型。没有类型 hint 的 path 会使用 `Dynamic`。                                  |
| 跳过索引支持     | 适用于在 `mapKeys`/`mapValues` 上创建的某些索引类型                             | 跳过索引只能创建在特定 path 的 子列 上，不能一次同时作用于所有 path/值。                                 |
| 普通列读取      | 由于需要重组 桶，速度比 `basic` 约慢 2 倍                                  | 会产生 `Dynamic` 类型编码和 path 重建带来的额外开销。                                                |
| 存储开销       | 额外 metadata 很少                                                    | 由于 `Dynamic` 类型编码、path 名称存储，以及 `advanced` 序列化中的额外 metadata，开销更高。                   |
| schema 灵活性 | 在创建表时固定 键 和 值 类型                                            | 完全动态——键 和 值 类型可以因行而异。还可以为已知 path 声明带类型的 path hint，以便直接访问 子列。          |

当不同 键 需要不同的 值 类型、各行之间的 键 集合差异很大，或者已知某些 键 会被频繁访问并可预先声明为带类型的 path 以实现直接 子列 访问时，请使用 `JSON`。

<div id="manual-sharding-into-multiple-map-columns">
  #### 手动分片到多个 Map 列
</div>

你可以在应用层按键的哈希值，将单个 `Map` 手动拆分到多个列中：

```sql
CREATE TABLE tab (
    id UInt64,
    m0 Map(String, UInt64),
    m1 Map(String, UInt64),
    m2 Map(String, UInt64),
    m3 Map(String, UInt64)
) ENGINE = MergeTree ORDER BY id;
```

插入时，将每个键值对路由到列 `m{hash(key) % 4}`。查询时，从对应的列读取：`m{hash('target_key') % 4}['target_key']`。

| 方面        | 带桶的 `Map`               | 手动分片                               |
| --------- | ----------------------- | ---------------------------------- |
| 易用性       | 透明——由存储引擎处理             | 需要应用层为插入和查询实现路由逻辑                  |
| 垂直合并      | 不支持——所有桶都属于同一列          | 支持——每个 `Map` 列都是独立列，可以进行垂直合并       |
| schema 变更 | 每个 part 的桶数量会自动适配       | 更改分片数量需要重写数据或新增列                   |
| 查询语法      | `m['key']` 可直接使用        | 必须计算正确的列：`m0['key']`、`m1['key']` 等 |
| 桶粒度       | 按 part 划分，并根据数据统计信息自动调整 | 在创建表时固定                            |

当垂直合并对于减少多列表在合并期间的内存使用很重要时，或者当分片数量必须固定并进行显式控制时，手动分片会更有优势。对于大多数用例，自动分桶序列化更简单，也已足够。

**另请参见**

* [map()](/zh/sql-reference/functions/tuple-map-functions#map) 函数
* [CAST()](/zh/sql-reference/functions/type-conversion-functions#CAST) 函数
* [-Map 数据类型的 Map 组合器](../aggregate-functions/combinators.md#-map)

<div id="related-content">
  ## 相关内容
</div>

* 博客：[使用 ClickHouse 构建可观测性解决方案 - 第 2 部分 - 链路追踪](https://clickhouse.com/blog/storing-traces-and-spans-open-telemetry-in-clickhouse)