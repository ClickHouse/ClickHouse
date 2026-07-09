---
description: '精确向量搜索与近似向量搜索文档'
keywords: ['向量相似性搜索', 'ann', 'knn', 'hnsw', '索引', '索引', '最近邻', '向量搜索']
sidebar_label: '精确向量搜索与近似向量搜索'
slug: /engines/table-engines/mergetree-family/annindexes
title: '精确向量搜索与近似向量搜索'
doc_type: 'guide'
---

在多维 (向量) 空间中，为给定点寻找最近的 N 个点这一问题，被称为[最近邻搜索](https://en.wikipedia.org/wiki/Nearest_neighbor_search)，简称向量搜索。
解决向量搜索通常有两种方法：

* 精确向量搜索会计算给定点与向量空间中所有点之间的距离。这可以确保达到最佳准确性，也就是说，返回的点一定是真正的最近邻。由于需要穷举整个向量空间，精确向量搜索在实际应用中可能会过慢。
* 近似向量搜索是指一类技术 (例如图、随机森林等特殊数据结构) ，其计算速度比精确向量搜索快得多。其结果准确性通常对于实际应用来说“足够好”。许多近似技术还提供参数，用于在结果准确性与搜索时间之间进行权衡。

向量搜索 (精确或近似) 可以用如下 SQL 表示：

```sql
WITH [...] AS reference_vector
SELECT [...]
FROM table
WHERE [...] -- a WHERE clause is optional
ORDER BY <DistanceFunction>(vectors, reference_vector)
LIMIT <N>
```

向量空间中的点存储在名为 `vectors` 的 Array 类型列中，例如 [Array(Float64)](../../../sql-reference/data-types/array.md)、[Array(Float32)](../../../sql-reference/data-types/array.md) 或 [Array(BFloat16)](../../../sql-reference/data-types/array.md)。
参考向量是一个常量数组，以公用表表达式的形式给出。
`<DistanceFunction>` 计算参考点与所有存储点之间的距离。
为此可以使用任意一种可用的[距离函数](/zh/sql-reference/functions/distance-functions)。
`<N>` 指定应返回多少个邻居。

<div id="exact-nearest-neighbor-search">
  ## 精确向量搜索
</div>

可以直接使用上述 SELECT 查询执行精确向量搜索。
此类查询的运行时间通常与已存储向量的数量及其维度 (即数组元素的数量) 成正比。
此外，由于 ClickHouse 会对所有向量执行穷举扫描，因此运行时间还取决于查询使用的线程数 (请参见设置 [max&#95;threads](../../../operations/settings/settings.md#max_threads)) 。

<div id="exact-nearest-neighbor-search-example">
  ### 示例
</div>

```sql
CREATE TABLE tab(id Int32, vec Array(Float32)) ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [1.5, 0.0]), (6, [0.0, 2.0]), (7, [0.0, 2.1]), (8, [0.0, 2.2]), (9, [0.0, 2.3]), (10, [0.0, 2.4]), (11, [0.0, 2.5]);

WITH [0., 2.] AS reference_vec
SELECT id, vec
FROM tab
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 3;
```

返回值

```result
   ┌─id─┬─vec─────┐
1. │  6 │ [0,2]   │
2. │  7 │ [0,2.1] │
3. │  8 │ [0,2.2] │
   └────┴─────────┘
```

<div id="approximate-nearest-neighbor-search">
  ## 近似向量搜索
</div>

<div id="vector-similarity-index">
  ### 向量相似度索引
</div>

ClickHouse 提供了一种特殊的“向量相似度”索引，可用于执行近似向量搜索。

:::note
向量相似度索引适用于 ClickHouse 25.8 及更高版本。
如果您遇到问题，请在 [ClickHouse 仓库](https://github.com/clickhouse/clickhouse/issues)中提交 issue。
:::

<div id="creating-a-vector-similarity-index">
  #### 创建向量相似度索引
</div>

可以在新表上创建向量相似度索引，如下所示：

```sql
CREATE TABLE table
(
  [...],
  vectors Array(Float*),
  INDEX <index_name> vectors TYPE vector_similarity(<type>, <distance_function>, <dimensions>) [GRANULARITY <N>]
)
ENGINE = MergeTree
ORDER BY [...]
```

或者，若要向现有表添加向量相似度索引：

```sql
ALTER TABLE table ADD INDEX <index_name> vectors TYPE vector_similarity(<type>, <distance_function>, <dimensions>) [GRANULARITY <N>];
```

向量相似度索引是一种特殊的跳过索引 (参见[此处](mergetree.md#table_engine-mergetree-data_skipping-indexes)和[此处](../../../optimize/skipping-indexes)) 。
因此，上述 `ALTER TABLE` 语句仅会对之后插入表中的新数据构建索引。
若要同时为现有数据构建索引，需要对其进行物化：

```sql
ALTER TABLE table MATERIALIZE INDEX <index_name> SETTINGS mutations_sync = 2;
```

函数 `<distance_function>` 必须是

* `L2Distance`，[欧几里得距离](https://en.wikipedia.org/wiki/Euclidean_distance)，表示欧几里得空间中两点间连线的长度，
* `cosineDistance`，[余弦距离](https://en.wikipedia.org/wiki/Cosine_similarity#Cosine_distance)，表示两个非零向量之间的夹角，或
* `dotProduct`，[点积](https://en.wikipedia.org/wiki/Dot_product) (内积) ，表示两个向量按元素相乘后所得结果之和。在归一化数据上，它等价于 `cosineDistance`。

对于归一化数据，`L2Distance` 通常是最佳选择；否则建议使用 `cosineDistance` 以弥补量纲差异。

:::note
对于距离函数 `L2Distance` 和 `cosineDistance`，值越小表示相似度越高；而对于 `dotProduct`，值越大表示相似度越高。
因此，基于 `L2Distance` 和 `cosineDistance` 构建的向量索引只能用于 `SELECT [...] ORDER BY [...] ASC` 查询 (`ASC` 是 `ORDER BY` 的默认排序方向) ，而基于 `dotProduct` 构建的向量索引只能用于 `SELECT [...] ORDER BY [...] DESC` 查询。
:::

`<dimensions>` 指定底层列中数组的基数 (元素数量) 。
如果 ClickHouse 在创建索引时发现某个数组的基数与此不符，该索引将被丢弃并返回错误。

可选的 GRANULARITY 参数 `<N>` 指定索引粒度的大小 (参见[此处](../../../optimize/skipping-indexes)) 。
与默认索引粒度为 1 的常规跳过索引不同，向量相似度索引的默认索引粒度为 1 亿。
该值可确保即使对于较大的 parts，内部构建的索引数量也很少。
我们建议仅由充分了解相关影响的高级用户修改索引粒度 (参见[下文](#differences-to-regular-skipping-indexes)) 。

向量相似度索引具有通用性，可支持不同的近似搜索方法。
实际使用的方法由参数 `<type>` 指定。
目前，唯一可用的方法是 HNSW ([学术论文](https://arxiv.org/abs/1603.09320)) ，这是一种基于层次化近邻图的近似向量搜索技术，目前广泛使用且处于业界前沿。
若将 HNSW 用作类型，用户可选择性地指定更多 HNSW 专用参数：

```sql
CREATE TABLE table
(
  [...],
  vectors Array(Float*),
  INDEX index_name vectors TYPE vector_similarity('hnsw', <distance_function>, <dimensions>[, <quantization>, <hnsw_max_connections_per_layer>, <hnsw_candidate_list_size_for_construction>]) [GRANULARITY N]
)
ENGINE = MergeTree
ORDER BY [...]
```

以下 HNSW 专用参数可用：

* `<quantization>` 控制邻近图中向量的量化。可选值为 `f64`、`f32`、`f16`、`bf16`、`i8` 或 `b1`。默认值为 `bf16`。请注意，此参数不会影响底层列中向量的表示形式。
* `<hnsw_max_connections_per_layer>` 控制每个图节点的邻居数量，也称为 HNSW 超参数 `M`。默认值为 `32`。值 `0` 表示使用默认值。
* `<hnsw_candidate_list_size_for_construction>` 控制构建 HNSW 图时动态候选列表的大小，也称为 HNSW 超参数 `ef_construction`。默认值为 `128`。值 `0` 表示使用默认值。

所有 HNSW 专用参数的默认值在大多数用例中都表现良好。
因此，我们不建议自定义 HNSW 专用参数。

此外，还有以下限制：

* 向量相似度索引只能构建在类型为 [Array(Float32)](../../../sql-reference/data-types/array.md)、[Array(Float64)](../../../sql-reference/data-types/array.md) 或 [Array(BFloat16)](../../../sql-reference/data-types/array.md) 的列上。不允许使用可空浮点数组和低基数浮点数组，例如 `Array(Nullable(Float32))` 和 `Array(LowCardinality(Float32))`。
* 向量相似度索引必须构建在单列上。
* 向量相似度索引可以构建在计算表达式上 (例如 `INDEX index_name arraySort(vectors) TYPE vector_similarity([...])`) ，但这类索引后续不能用于近似邻居搜索。
* 向量相似度索引要求底层列中的所有数组都包含 `<dimension>` 个元素——这一点会在创建索引时检查。为了尽早发现不满足这一要求的情况，用户可以为向量列添加一个 [约束](/zh/sql-reference/statements/create/table.md#constraints)，例如 `CONSTRAINT same_length CHECK length(vectors) = 256`。
* 同样，底层列中的数组值不能为空 (`[]`) ，也不能为默认值 (同样是 `[]`) 。

**估算存储和内存消耗**

为典型 AI 模型 (例如大语言模型 [LLM](https://en.wikipedia.org/wiki/Large_language_model)) 生成的向量通常由数百到数千个浮点值组成。
因此，单个向量值的内存占用可能达到数 KB。
如果想估算表中底层向量列所需的存储空间，以及向量相似度索引所需的主内存，可以使用下面两个公式：

表中向量列的存储消耗 (未压缩) ：

```text
Storage consumption = Number of vectors * Dimension * Size of column data type
```

以 [dbpedia 数据集](https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M) 为例：

```text
Storage consumption = 1 million * 1536 * 4 (for Float32) = 6.1 GB
```

要执行搜索，必须将向量相似度索引从磁盘完整加载到主内存中。
同样，向量索引也是先在内存中完整构建，再保存到磁盘。

加载向量索引所需的内存消耗：

```text
Memory for vectors in the index (mv) = Number of vectors * Dimension * Size of quantized data type
Memory for in-memory graph (mg) = Number of vectors * hnsw_max_connections_per_layer * Bytes_per_node_id (= 4) * Layer_node_repetition_factor (= 2)

Memory consumption: mv + mg
```

以 [dbpedia 数据集](https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M) 为例：

```text
Memory for vectors in the index (mv) = 1 million * 1536 * 2 (for BFloat16) = 3072 MB
Memory for in-memory graph (mg) = 1 million * 64 * 2 * 4 = 512 MB

Memory consumption = 3072 + 512 = 3584 MB
```

上述公式未考虑向量相似度索引为分配运行时数据结构 (如预分配缓冲区和缓存) 所需的额外内存。

<div id="using-a-vector-similarity-index">
  #### 使用向量相似度索引
</div>

:::note
要使用向量相似度索引，[compatibility](../../../operations/settings/settings.md) 设置必须为 `''` (默认值) ，或 `'25.1'` 及以上版本。
:::

向量相似度索引支持以下形式的 SELECT 查询：

```sql
WITH [...] AS reference_vector
SELECT [...]
FROM table
WHERE [...] -- a WHERE clause is optional
ORDER BY <DistanceFunction>(vectors, reference_vector)
LIMIT <N>
```

ClickHouse 的查询优化器会尝试匹配上述查询模板，并利用可用的向量相似度索引。
只有当 SELECT 查询中的距离函数与索引定义中的距离函数一致时，查询才能使用向量相似度索引。

高级用户可以为设置 [hnsw&#95;candidate&#95;list&#95;size&#95;for&#95;search](../../../operations/settings/settings.md#hnsw_candidate_list_size_for_search) 指定自定义值 (也称为 HNSW 超参数 &quot;ef&#95;search&quot;) ，以调整搜索过程中候选列表的大小 (例如 `SELECT [...] SETTINGS hnsw_candidate_list_size_for_search = <value>`) 。
该设置的默认值 256 在大多数用例中都能取得良好效果。
更高的设置值意味着更高的准确性，但代价是性能更慢。

如果查询可以使用向量相似度索引，ClickHouse 会检查 SELECT 查询中指定的 LIMIT `<N>` 是否在合理范围内。
更具体地说，如果 `<N>` 大于设置 [max&#95;limit&#95;for&#95;vector&#95;search&#95;queries](../../../operations/settings/settings.md#max_limit_for_vector_search_queries) 的值，则会返回错误；该设置的默认值为 100。
过大的 LIMIT 值会拖慢搜索速度，通常也意味着使用方式有误。

要检查 SELECT 查询是否使用了向量相似度索引，可以在查询前加上 `EXPLAIN indexes = 1`。

例如，查询

```sql
EXPLAIN indexes = 1
WITH [0.462, 0.084, ..., -0.110] AS reference_vec
SELECT id, vec
FROM tab
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 10;
```

可能返回

```result
    ┌─explain─────────────────────────────────────────────────────────────────────────────────────────┐
 1. │ Expression (Project names)                                                                      │
 2. │   Limit (preliminary LIMIT (without OFFSET))                                                    │
 3. │     Sorting (Sorting for ORDER BY)                                                              │
 4. │       Expression ((Before ORDER BY + (Projection + Change column names to column identifiers))) │
 5. │         ReadFromMergeTree (default.tab)                                                         │
 6. │         Indexes:                                                                                │
 7. │           PrimaryKey                                                                            │
 8. │             Condition: true                                                                     │
 9. │             Parts: 1/1                                                                          │
10. │             Granules: 575/575                                                                   │
11. │           Skip                                                                                  │
12. │             Name: idx                                                                           │
13. │             Description: vector_similarity GRANULARITY 100000000                                │
14. │             Parts: 1/1                                                                          │
15. │             Granules: 10/575                                                                    │
    └─────────────────────────────────────────────────────────────────────────────────────────────────┘
```

在此示例中，[dbpedia 数据集](https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M) 中的 100 万个向量 (每个向量的维度为 1536) 存储在 575 个粒度中，即每个粒度约 1.7k 行。
该查询请求 10 个近邻，向量相似度索引会在 10 个不同的粒度中找到这 10 个近邻。
这 10 个粒度会在查询执行期间被读取。

如果输出中包含 `Skip` 以及向量索引的名称和类型 (本例中为 `idx` 和 `vector_similarity`) ，则说明使用了向量相似度索引。
在这种情况下，向量相似度索引跳过了 4 个粒度中的 2 个，也就是 50% 的数据。
能跳过的粒度越多，索引的使用效果就越好。

:::tip
要强制使用索引，可以在运行 SELECT 查询时设置 [force&#95;data&#95;skipping&#95;indexes](../../../operations/settings/settings#force_data_skipping_indices) (将索引名称作为设置值) 。
:::

**后过滤与前过滤**

用户也可以为 SELECT 查询额外指定一个带过滤条件的 `WHERE` 子句。
ClickHouse 会使用后过滤或前过滤策略来评估这些过滤条件。
简而言之，这两种策略决定了过滤条件的评估顺序：

* 后过滤表示先评估向量相似度索引，然后 ClickHouse 再评估 `WHERE` 子句中指定的附加过滤条件。
* 前过滤表示过滤条件的评估顺序正好相反。

这两种策略各有不同的权衡：

* 后过滤普遍存在一个问题：返回的行数可能少于 `LIMIT <N>` 子句中要求的行数。当向量相似度索引返回的一行或多行结果不满足附加过滤器时，就会出现这种情况。
* 前过滤通常仍是一个尚未解决的问题。某些专用向量数据库提供了前过滤算法，但大多数关系型数据库 (包括 ClickHouse) 都会回退到精确近邻搜索，即不使用索引的穷举扫描。

采用哪种策略取决于过滤条件。

*附加过滤器是分区键的一部分*

如果附加过滤条件是分区键的一部分，ClickHouse 就会进行分区剪枝。
例如，某个表按列 `year` 进行范围分区，并执行以下查询：

```sql
WITH [0., 2.] AS reference_vec
SELECT id, vec
FROM tab
WHERE year = 2025
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 3;
```

ClickHouse 会裁剪掉除 2025 年分区之外的所有分区。

*无法使用索引评估额外过滤条件*

如果额外的过滤条件无法通过索引 (主键索引、跳过索引) 来评估，ClickHouse 将应用后过滤。

*可以使用主键索引评估额外过滤条件*

如果额外的过滤条件可以通过[主键](mergetree.md#primary-key)来评估 (即它们构成主键的前缀) ，并且

* 如果过滤条件在某个 part 内至少排除了一行，ClickHouse 将对该 part 中“保留下来”的范围改用前过滤，
* 如果过滤条件在某个 part 内没有排除任何行，ClickHouse 将对该 part 执行后过滤。

在实际用例中，后一种情况其实不太常见。

*可以使用跳过索引评估额外过滤条件*

如果额外的过滤条件可以通过[跳过索引](mergetree.md#table_engine-mergetree-data_skipping-indexes) (minmax 索引、set 索引等) 来评估，ClickHouse 会执行后过滤。
在这种情况下，会先评估向量相似度索引，因为预计与其他跳过索引相比，它能过滤掉更多的行。

为了更细致地控制后过滤和前过滤，可以使用两个设置：

可将设置 [vector&#95;search&#95;filter&#95;strategy](../../../operations/settings/settings#vector_search_filter_strategy) (默认值：`auto`，会实现上述启发式策略) 设为 `prefilter`。
这在额外过滤条件具有极高选择性时，可用于强制启用前过滤。
例如，下面这个查询可能会从前过滤中受益：

```sql
SELECT bookid, author, title
FROM books
WHERE price < 2.00
ORDER BY cosineDistance(book_vector, getEmbedding('Books on ancient Asian empires'))
LIMIT 10
```

假设价格低于 2 美元的图书只有极少数，后过滤可能会返回零行，因为向量索引返回的前 10 个匹配结果的价格都可能高于 2 美元。
通过强制使用前过滤 (在查询中添加 `SETTINGS vector_search_filter_strategy = 'prefilter'`) ，ClickHouse 会先找出所有价格低于 2 美元的图书，然后对这些图书执行穷举向量搜索。

作为解决上述问题的另一种方法，可以将 [vector&#95;search&#95;index&#95;fetch&#95;multiplier](../../../operations/settings/settings#vector_search_index_fetch_multiplier) (默认值：`1.0`，最大值：`1000.0`) 配置为大于 `1.0` 的值 (例如 `2.0`) 。
从向量索引中拉取的最近邻数量会按该设置值成倍增加，然后再对这些行应用额外的过滤器，以返回 LIMIT 指定数量的行。
例如，我们可以再次执行查询，但这次使用乘数 `3.0`：

```sql
SELECT bookid, author, title
FROM books
WHERE price < 2.00
ORDER BY cosineDistance(book_vector, getEmbedding('Books on ancient Asian empires'))
LIMIT 10
SETTING vector_search_index_fetch_multiplier = 3.0;
```

ClickHouse 会从每个分片中的向量索引拉取 3.0 x 10 = 30 个最近邻，然后再评估额外的过滤条件。
最终只会返回距离最近的 10 个邻居。
请注意，设置 `vector_search_index_fetch_multiplier` 可以缓解这个问题，但在极端情况下 (WHERE 条件选择性非常高) ，返回的行数仍可能少于请求的 N 行。

**重新评分**

ClickHouse 中的跳过索引通常是在粒度级别进行过滤，也就是说，对跳过索引的一次查找 (在内部) 会返回一个可能匹配的粒度列表，从而减少后续扫描中需要读取的数据量。
这对跳过索引通常都很有效，但对于向量相似度索引来说，会产生一种“粒度不匹配”。
更具体地说，向量相似度索引会针对给定的参考向量确定最相似的 N 个向量的行号，但随后需要将这些行号推算为粒度编号。
随后 ClickHouse 会从磁盘加载这些粒度，并对这些粒度中的所有向量再次执行距离计算。
这一步称为重新评分。虽然它理论上可以提高准确性——请记住，向量相似度索引返回的只是*近似*结果——但很明显，这在性能方面并不是最优的。

因此，ClickHouse 提供了一项优化，可以禁用重新评分，并直接从索引中返回最相似的向量及其距离。
该优化默认启用，参见设置项 [vector&#95;search&#95;with&#95;rescoring](../../../operations/settings/settings#vector_search_with_rescoring)。
从高层来看，其工作方式是 ClickHouse 将最相似的向量及其距离作为虚拟列 `_distances` 提供出来。
要查看这一点，请使用 `EXPLAIN header = 1` 运行向量搜索查询：

```sql
EXPLAIN header = 1
WITH [0., 2.] AS reference_vec
SELECT id
FROM tab
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 3
SETTINGS vector_search_with_rescoring = 0
```

```result
Query id: a2a9d0c8-a525-45c1-96ca-c5a11fa66f47

    ┌─explain─────────────────────────────────────────────────────────────────────────────────────────────────┐
 1. │ Expression (Project names)                                                                              │
 2. │ Header: id Int32                                                                                        │
 3. │   Limit (preliminary LIMIT (without OFFSET))                                                            │
 4. │   Header: L2Distance(__table1.vec, _CAST([0., 2.]_Array(Float64), 'Array(Float64)'_String)) Float64     │
 5. │           __table1.id Int32                                                                             │
 6. │     Sorting (Sorting for ORDER BY)                                                                      │
 7. │     Header: L2Distance(__table1.vec, _CAST([0., 2.]_Array(Float64), 'Array(Float64)'_String)) Float64   │
 8. │             __table1.id Int32                                                                           │
 9. │       Expression ((Before ORDER BY + (Projection + Change column names to column identifiers)))         │
10. │       Header: L2Distance(__table1.vec, _CAST([0., 2.]_Array(Float64), 'Array(Float64)'_String)) Float64 │
11. │               __table1.id Int32                                                                         │
12. │         ReadFromMergeTree (default.tab)                                                                 │
13. │         Header: id Int32                                                                                │
14. │                 _distance Float32                                                                       │
    └─────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

:::note
在未使用重新评分 (`vector_search_with_rescoring = 0`) 且启用了并行副本的情况下，查询可能会自动改用重新评分。
:::

<div id="performance-tuning">
  #### 性能调优
</div>

**压缩调优**

在几乎所有用例中，底层列中的向量都是稠密的，因此通常难以获得良好的压缩效果。
因此，[压缩](/zh/sql-reference/statements/create/table.md#column_compression_codec)会降低对向量列的写入和读取性能。
所以我们建议禁用压缩。
为此，请像下面这样为向量列指定 `CODEC(NONE)`：

```sql
CREATE TABLE tab(id Int32, vec Array(Float32) CODEC(NONE), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = MergeTree ORDER BY id;
```

**调优索引创建**

向量相似度索引的生命周期与 parts 的生命周期相关联。
换句话说，每当创建一个定义了向量相似度索引的新 part 时，相应的索引也会同时创建。
这通常发生在数据被[插入](https://clickhouse.com/docs/guides/inserting-data)时，或在[合并](https://clickhouse.com/docs/merges)过程中。
遗憾的是，HNSW 的索引创建时间通常较长，可能会显著拖慢插入和合并操作。
因此，向量相似度索引更适合用于不可变或很少变更的数据。

为了加快索引创建速度，可以采用以下方法：

首先，可以并行执行索引创建。
索引创建线程的最大数量可通过服务器设置 [max&#95;build&#95;vector&#95;similarity&#95;index&#95;thread&#95;pool&#95;size](/zh/operations/server-configuration-parameters/settings#max_build_vector_similarity_index_thread_pool_size) 进行配置。
为获得最佳性能，该设置值应配置为 CPU 核心数。

其次，为了加快 INSERT 语句的执行，用户可以使用会话设置 [materialize&#95;skip&#95;indexes&#95;on&#95;insert](../../../operations/settings/settings.md#materialize_skip_indexes_on_insert) 禁止在新插入的 parts 上创建跳过索引。
对此类 parts 执行 SELECT 查询时，将回退到精确搜索。
由于新插入的 parts 相比整个表通常较小，因此预计这对性能的影响可以忽略不计。

第三，为了加快合并，用户可以使用会话设置 [materialize&#95;skip&#95;indexes&#95;on&#95;merge](../../../operations/settings/merge-tree-settings.md#materialize_skip_indexes_on_merge) 禁止在 merged parts 上创建跳过索引。
配合语句 [ALTER TABLE [...] MATERIALIZE INDEX [...]](../../../sql-reference/statements/alter/skipping-index.md#materialize-index)，这可以显式控制向量相似度索引的生命周期。
例如，可以将索引创建延后到所有数据都已摄取完成之后，或延后到系统负载较低的时段，例如周末。

**调优索引使用**

SELECT 查询若要使用向量相似度索引，需要先将其加载到主内存中。
为避免同一个向量相似度索引被反复加载到主内存，ClickHouse 为这类索引提供了专用的内存缓存。
缓存越大，不必要的加载就越少。
最大缓存大小可通过服务器设置 [vector&#95;similarity&#95;index&#95;cache&#95;size](../../../operations/server-configuration-parameters/settings.md#vector_similarity_index_cache_size) 进行配置。
默认情况下，该缓存最大可增长到 5 GB。

以下日志消息 (`system.text_log`) 表示向量相似度索引正在加载。
如果这类消息在不同的向量搜索查询中反复出现，则说明缓存大小过小。

```text
2026-02-03 07:39:10.351635 [1386] f0ac5c85-1b1c-4f35-8848-87a1d1aa00ba : VectorSimilarityIndex Start loading vector similarity index

<...>

2026-02-03 07:40:25.217603 [1386] f0ac5c85-1b1c-4f35-8848-87a1d1aa00ba : VectorSimilarityIndex Loaded vector similarity index: max_level = 2, connectivity = 64, size = 1808111, capacity = 1808111, memory_usage = 8.00 GiB, bytes_per_vector = 4096, scalar_words = 1024, nodes = 1808111, edges = 51356964, max_edges = 233395072
```

:::note
向量相似度索引缓存用于存储向量索引粒度。
如果单个向量索引粒度大于缓存容量，则不会被缓存。
因此，请务必先计算向量索引的大小 (根据“估算存储和内存消耗”中的公式或 [system.data&#95;skipping&#95;indices](../../../operations/system-tables/data_skipping_indices)) ，再据此设置合适的缓存大小。
:::

*我们再次强调：在排查向量搜索查询缓慢的问题时，首先应检查向量索引缓存，并在必要时增大其容量。*

当前向量相似度索引缓存的大小可在 [system.metrics](../../../operations/system-tables/metrics.md) 中查看：

```sql
SELECT metric, value
FROM system.metrics
WHERE metric = 'VectorSimilarityIndexCacheBytes'
```

可从 [system.query&#95;log](../../../operations/system-tables/query_log.md) 中获取某个查询 id 对应查询的缓存命中和未命中情况：

```sql
SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['VectorSimilarityIndexCacheHits'], ProfileEvents['VectorSimilarityIndexCacheMisses']
FROM system.query_log
WHERE type = 'QueryFinish' AND query_id = '<...>'
ORDER BY event_time_microseconds;
```

对于生产环境的使用场景，我们建议将缓存设置得足够大，以确保所有向量索引始终保留在内存中。

**量化调优**

[量化](https://huggingface.co/blog/embedding-quantization)是一种用于减少向量内存占用，并降低构建和遍历向量索引计算开销的技术。
ClickHouse 向量索引支持以下量化选项：

| Quantization   | Name               | Storage per dimension |
| -------------- | ------------------ | --------------------- |
| f32            | 单精度                | 4 字节                  |
| f16            | 半精度                | 2 字节                  |
| bf16 (default) | 半精度 (brain float)  | 2 字节                  |
| i8             | 四分之一精度             | 1 字节                  |
| b1             | 二进制                | 1 比特                  |

与搜索原始全精度浮点值 (`f32`) 相比，量化会降低向量搜索的精度。
不过，在大多数数据集上，半精度 brain float 量化 (`bf16`) 带来的精度损失微乎其微，因此向量相似度索引默认使用这种量化技术。
四分之一精度 (`i8`) 和二进制 (`b1`) 量化会给向量搜索带来较明显的精度损失。
只有在向量相似度索引的大小显著超过可用 DRAM 容量时，我们才建议使用这两种量化方式。
在这种情况下，我们还建议启用重评分 ([vector&#95;search&#95;index&#95;fetch&#95;multiplier](../../../operations/settings/settings#vector_search_index_fetch_multiplier)、[vector&#95;search&#95;with&#95;rescoring](../../../operations/settings/settings#vector_search_with_rescoring)) 以提高准确性。
仅在以下情况下推荐使用二进制量化：1) 嵌入向量已归一化 (即向量长度 = 1，OpenAI 模型通常已归一化) ；2) 使用余弦距离作为距离函数。
二进制量化在内部使用 Hamming 距离来构建和搜索近邻图。
重评分步骤会使用存储在表中的原始全精度向量，通过余弦距离识别最近邻。

**数据传输调优**

向量搜索查询中的参考向量由用户提供，通常通过调用大语言模型 (LLM) 获取。
在 ClickHouse 中执行向量搜索的典型 Python 代码可能如下所示

```python
search_v = openai_client.embeddings.create(input = "[Good Books]", model='text-embedding-3-large', dimensions=1536).data[0].embedding

params = {'search_v': search_v}
result = chclient.query(
   "SELECT id FROM items
    ORDER BY cosineDistance(vector, %(search_v)s)
    LIMIT 10",
    parameters = params)
```

嵌入向量 (上面代码片段中的 `search_v`) 的维度可能非常大。
例如，OpenAI 提供的模型可生成维度为 1536 甚至 3072 的嵌入向量。
在上述代码中，ClickHouse Python 驱动程序会将嵌入向量替换为人类可读的字符串，随后把整个 SELECT 查询作为字符串发送。
假设该嵌入向量由 1536 个单精度浮点值组成，那么发送的字符串长度将达到 20 kB。
这会在标记化、解析以及执行数千次字符串到浮点数的转换时带来很高的 CPU 占用。
此外，ClickHouse server 日志文件还需要占用大量空间，也会导致 `system.query_log` 膨胀。

请注意，大多数 LLM 模型都会以原生浮点数列表或 NumPy 数组的形式返回嵌入向量。
因此，我们建议 Python 应用程序使用以下方式，以二进制形式绑定参考向量参数：

```python
search_v = openai_client.embeddings.create(input = "[Good Books]", model='text-embedding-3-large', dimensions=1536).data[0].embedding

params = {'$search_v_binary$': np.array(search_v, dtype=np.float32).tobytes()}
result = chclient.query(
   "SELECT id FROM items
    ORDER BY cosineDistance(vector, reinterpret($search_v_binary$, 'Array(Float32)'))
    LIMIT 10"
    parameters = params)
```

在该示例中，参考向量会按原样以二进制形式发送，并在服务器端重新解释为 Float 数组。
这样既能节省服务器端的 CPU 时间，也可避免服务器日志和 `system.query_log` 膨胀过大。

<div id="administration">
  #### 管理与监控
</div>

向量相似度索引的磁盘占用大小可从 [system.data&#95;skipping&#95;indices](../../../operations/system-tables/data_skipping_indices) 获取：

```sql
SELECT database, table, name, formatReadableSize(data_compressed_bytes)
FROM system.data_skipping_indices
WHERE type = 'vector_similarity';
```

示例输出：

```result
┌─database─┬─table─┬─name─┬─formatReadab⋯ssed_bytes)─┐
│ default  │ tab   │ idx  │ 348.00 MB                │
└──────────┴───────┴──────┴──────────────────────────┘
```

<div id="differences-to-regular-skipping-indexes">
  #### 与常规跳过索引的差异
</div>

与常规[跳过索引](/zh/optimize/skipping-indexes)一样，向量相似度索引也是基于粒度构建的，并且每个已建立索引的块由 `GRANULARITY = [N]` 个粒度组成 (普通跳过索引中，`[N]` 默认为 1) 。
例如，如果表的主索引粒度为 8192 (设置 `index_granularity = 8192`) ，且 `GRANULARITY = 2`，那么每个已建立索引的块将包含 16384 行。
不过，用于近似邻居搜索的数据结构和算法本质上是按行组织的。
它们存储的是一组行的紧凑表示，并且返回的也是向量搜索查询对应的行。
因此，与普通跳过索引相比，向量相似度索引在行为上会表现出一些不太直观的差异。

当用户在某个列上定义向量相似度索引时，ClickHouse 会在内部为每个索引块创建一个向量相似度“子索引”。
这里的子索引之所以称为“局部”索引，是因为它只知道其所属索引块中的行。
沿用前面的示例，假设某个列有 65536 行，那么会得到四个索引块 (跨越八个粒度) ，并且每个索引块都会有一个向量相似度子索引。
理论上，子索引可以直接返回其索引块内距离最近的 N 个点所在的行。
但是，由于 ClickHouse 以粒度为单位将数据从磁盘加载到内存中，子索引会将匹配行映射到粒度级别。
这不同于常规跳过索引，后者是在索引块粒度上跳过数据。

`GRANULARITY` 参数决定会创建多少个向量相似度子索引。
`GRANULARITY` 值越大，向量相似度子索引越少，但每个子索引也越大，直到某个列 (或该列的数据分区片段) 只剩下一个子索引。
在这种情况下，该子索引对列中的所有行都具有“全局”视角，并且可以直接返回该列 (分区片段) 中所有包含相关行的粒度 (这样的粒度最多有 `LIMIT [N]` 个) 。
第二步中，ClickHouse 会加载这些粒度，并通过对这些粒度中的所有行执行穷举距离计算，找出实际最优的行。
当 `GRANULARITY` 值较小时，每个子索引最多会返回 `LIMIT N` 个粒度。
因此，需要加载并进行后过滤的粒度会更多。
请注意，这两种情况下的搜索精度同样高，区别只在于处理性能。
通常建议向量相似度索引使用较大的 `GRANULARITY`，只有在出现向量相似度结构内存消耗过高等问题时，才退回使用较小的 `GRANULARITY` 值。
如果未为向量相似度索引指定 `GRANULARITY`，默认值为 1 亿。

<div id="approximate-nearest-neighbor-search-example">
  #### 示例
</div>

查询：

```sql title="Query"
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [1.5, 0.0]), (6, [0.0, 2.0]), (7, [0.0, 2.1]), (8, [0.0, 2.2]), (9, [0.0, 2.3]), (10, [0.0, 2.4]), (11, [0.0, 2.5]);

WITH [0., 2.] AS reference_vec
SELECT id, vec
FROM tab
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 3;
```

```result title="Response"
   ┌─id─┬─vec─────┐
1. │  6 │ [0,2]   │
2. │  7 │ [0,2.1] │
3. │  8 │ [0,2.2] │
   └────┴─────────┘
```

更多使用近似向量搜索的示例数据集：

* [LAION-400M](../../../getting-started/example-datasets/laion-400m-dataset)
* [LAION-5B](../../../getting-started/example-datasets/laion-5b-dataset)
* [dbpedia](../../../getting-started/example-datasets/dbpedia-dataset)
* [hackernews](../../../getting-started/example-datasets/hackernews-vector-search-dataset)

<div id="approximate-nearest-neighbor-search-qbit">
  ### Quantized Bit (QBit)
</div>

加速精确向量搜索的一种常见方法是使用较低精度的[浮点数据类型](../../../sql-reference/data-types/float.md)。
例如，如果向量存储为 `Array(BFloat16)` 而不是 `Array(Float32)`，数据大小会减半，查询运行时间通常也会相应缩短。
这种方法称为量化。虽然它可以加快计算速度，但即使对所有向量执行穷尽扫描，结果准确性仍可能有所下降。

采用传统量化时，我们会在搜索和数据存储这两个阶段都损失精度。在上面的示例中，我们存储的是 `BFloat16` 而不是 `Float32`，这意味着即使后续有需要，也无法再执行精度更高的搜索。一种替代方案是同时存储两份数据：量化后的版本和全精度版本。虽然这种方法可行，但会带来额外的存储冗余。设想这样一种场景：原始数据为 `Float64`，并且希望以不同精度 (16 位、32 位或完整 64 位) 执行搜索。那么，我们就需要存储三份独立的数据副本。

ClickHouse 提供了 Quantized Bit (`QBit`) 数据类型，可通过以下方式解决这些限制：

1. 存储原始全精度数据。
2. 允许在查询时指定量化精度。

这是通过以按位分组的格式存储数据实现的 (也就是说，所有向量的第 i 位都存储在一起) ，从而只按所请求的精度级别读取数据。这样既能获得量化带来的 I/O 和计算量降低所带来的速度优势，又能在需要时保留所有原始数据可用。当选择最大精度时，搜索就是精确搜索。

要声明 `QBit` 类型的列，请使用以下语法：

```sql
column_name QBit(element_type, dimension[, stride])
```

其中：

* `element_type` – 每个向量元素的类型。支持的类型包括 `Int8`、`BFloat16`、`Float32` 和 `Float64`
* `dimension` – 每个向量中的元素个数
* `stride` – 可选。`dimension` 的一个除数，用于将各维度划分为 `dimension / stride` 个连续分组，并分别存储在独立的流中，这样只对前几个维度进行搜索时，读取的流会更少 (对 Matryoshka 嵌入向量很有用) 。默认值为 `dimension`；在这种情况下，该类型在字节级别上与非跨步 `QBit` 完全相同。详见 [`QBit` 数据类型页面](/zh/sql-reference/data-types/qbit)。

<div id="qbit-create">
  #### 创建 `QBit` 表并插入数据
</div>

```sql
CREATE TABLE fruit_animal (
    word String,
    vec QBit(Float64, 5)
) ENGINE = MergeTree
ORDER BY word;

INSERT INTO fruit_animal VALUES
    ('apple', [-0.99105519, 1.28887844, -0.43526649, -0.98520696, 0.66154391]),
    ('banana', [-0.69372815, 0.25587061, -0.88226235, -2.54593015, 0.05300475]),
    ('orange', [0.93338752, 2.06571317, -0.54612565, -1.51625717, 0.69775337]),
    ('dog', [0.72138876, 1.55757105, 2.10953259, -0.33961248, -0.62217325]),
    ('cat', [-0.56611276, 0.52267331, 1.27839863, -0.59809804, -1.26721048]),
    ('horse', [-0.61435682, 0.48542571, 1.21091247, -0.62530446, -1.33082533]);
```

<div id="qbit-search">
  #### 使用 `QBit` 进行向量搜索
</div>

下面我们使用 L2 距离，查找与表示单词 &#39;lemon&#39; 的向量最近的邻居。距离函数中的第三个参数用于指定精度 (单位为位) ——值越高，精度越高，但所需计算量也越大。

你可以在[这里](../../../sql-reference/data-types/qbit.md#vector-search-functions)查看 `QBit` 支持的所有距离函数。

**全精度搜索 (64 位) ：**

```sql
SELECT
    word,
    L2DistanceTransposed(vec, [-0.88693672, 1.31532824, -0.51182908, -0.99652702, 0.59907770], 64) AS distance
FROM fruit_animal
ORDER BY distance;
```

```text
   ┌─word───┬────────────distance─┐
1. │ apple  │ 0.14639757188169716 │
2. │ banana │   1.998961369007679 │
3. │ orange │   2.039041552613732 │
4. │ cat    │   2.752802631487914 │
5. │ horse  │  2.7555776805484813 │
6. │ dog    │   3.382295083120104 │
   └────────┴─────────────────────┘
```

**降精度搜索：**

```sql
SELECT
    word,
    L2DistanceTransposed(vec, [-0.88693672, 1.31532824, -0.51182908, -0.99652702, 0.59907770], 12) AS distance
FROM fruit_animal
ORDER BY distance;
```

```text
   ┌─word───┬───────────distance─┐
1. │ apple  │  0.757668703053566 │
2. │ orange │ 1.5499475034938677 │
3. │ banana │ 1.6168396735102937 │
4. │ cat    │  2.429752230904804 │
5. │ horse  │  2.524650475528617 │
6. │ dog    │   3.17766975527459 │
   └────────┴────────────────────┘
```

请注意，使用 12 比特量化后，我们在加快查询执行速度的同时，仍能较准确地逼近这些距离。相对排序基本保持一致，&#39;apple&#39; 仍然是最接近的匹配项。

<div id="qbit-performance">
  #### 性能注意事项
</div>

`QBit` 的性能优势来自 I/O 操作的减少：使用较低精度时，需要从存储中读取的数据更少。此外，当 `QBit` 包含 `Float32` 数据且精度参数为 16 或更低时，由于计算量减少，还能获得额外的性能收益。精度参数直接决定了准确性与速度之间的权衡：

* **更高精度** (更接近原始数据宽度) ：结果更准确，但查询更慢
* **更低精度**：查询更快，但结果是近似值，且内存占用更低

<div id="references">
  ### 参考资料
</div>

博客文章：

* [ClickHouse 向量搜索 - 第 1 部分](https://clickhouse.com/blog/vector-search-clickhouse-p1)
* [ClickHouse 向量搜索 - 第 2 部分](https://clickhouse.com/blog/vector-search-clickhouse-p2)
* [我们构建了一个可让你在查询时选择精度的向量搜索引擎](https://clickhouse.com/blog/qbit-vector-search)