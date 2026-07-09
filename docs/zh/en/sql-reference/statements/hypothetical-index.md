---
description: '假设（what-if）索引文档'
sidebar_label: '假设索引'
sidebar_position: 47
slug: /sql-reference/statements/hypothetical-index
title: '假设索引'
doc_type: 'reference'
---

<div id="hypothetical-indexes">
  # 假设索引
</div>

假设索引是虚拟的、会话级的跳过索引，你可以将其附加到 `MergeTree` 家族的表，而无需实际构建或存储它们。它们仅存在于当前会话中，并由 [`EXPLAIN WHATIF`](/zh/sql-reference/statements/explain#explain-whatif) 用来估算真实的跳过索引会如何影响查询——通常包括跳过率 (可跳过的标记占比) 以及以标记数和字节数表示的粗略成本。

使用假设索引可以先评估候选索引，再决定是否承担将其物化并存储到磁盘上的成本。

<div id="create-hypothetical-index">
  ## CREATE 假设索引
</div>

```sql
CREATE HYPOTHETICAL INDEX [IF NOT EXISTS] name
    ON [db.]table_name (expression) TYPE type[(args)] [GRANULARITY value]
```

该语法与 `ALTER TABLE ... ADD INDEX` 相同，但不会构建或写入任何索引——只会在当前会话中存储索引描述。

* `name` — 索引名称；在此会话中，必须在 `(database, table)` 范围内唯一。
* `expression` — 要建立索引的列或表达式。
* `TYPE type` — `minmax`、`set(N)`、`bloom_filter(p)`、`ngrambf_v1(...)`、`tokenbf_v1(...)`。`text` 和 `vector_similarity` 不受支持，并会在 `CREATE` 时被拒绝，因为它们实际的 `ALTER TABLE ... ADD INDEX` 校验依赖表级设置，而会话级存储无法复制这些设置。
* `GRANULARITY value` — 每个索引粒度对应的数据粒度数。默认为 1。

目标表必须是 `Atomic` 数据库中的 `MergeTree` 家族表 (即必须具有 UUID) 。没有 UUID 的表——例如 legacy `Ordinary` 数据库中的表，或使用旧语法的 `MergeTree` 表——会被拒绝，因为会话存储是以表 UUID 作为键来保存假设索引的。

**示例**

```sql
CREATE HYPOTHETICAL INDEX idx_b ON t (b) TYPE minmax GRANULARITY 1;
```

<div id="evaluating-a-hypothetical-index-with-explain-whatif">
  ## 使用 EXPLAIN WHATIF 评估假设索引
</div>

单独定义假设索引并不会有任何效果——要查看它会如何影响查询，请对一个具有代表性的 `SELECT` 运行 [`EXPLAIN WHATIF`](/zh/sql-reference/statements/explain#explain-whatif)。估算器会报告每个候选索引的适用性、将读取的标记、最终的跳过率，以及该估算是如何得出的 (`empirical`、`statistical` 或 `applicability_only`) 。

```sql
CREATE TABLE t (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 100;

INSERT INTO t SELECT number, number FROM numbers(10000);

CREATE HYPOTHETICAL INDEX idx_b ON t (b) TYPE minmax GRANULARITY 1;

EXPLAIN WHATIF SELECT * FROM t WHERE b = 42;
```

结果：

```text
Baseline (after PK + partition + existing indexes):
  table:       default.t
  parts:       1
  marks:       100
  est_bytes:   85.52 KiB

With idx_b (minmax, hypothetical):
  status:       applicable
  marks:        1
  est_bytes:    875.00 B
  skip_ratio:   99.0%

Estimation:
  source:           empirical
  empirical_status: ok
  sampled_parts:    1 / 1
  sampled_marks:    100 / 100
  elapsed_us:       631
```

`est_bytes` 是根据表的平均行大小估算得出的，因此精确数值会因存储方式和压缩情况而有所不同。

若要跳过基于内存的经验扫描，改为根据[列统计信息](/zh/engines/table-engines/mergetree-family/mergetree#column-statistics)进行估算，请先为相关列定义这些统计信息 (默认未启用) ，等待物化变更完成，然后禁用经验估算路径：

```sql
ALTER TABLE t ADD STATISTICS b TYPE TDigest;
ALTER TABLE t MATERIALIZE STATISTICS b SETTINGS mutations_sync = 1;

EXPLAIN WHATIF empirical = 0 SELECT * FROM t WHERE b < 10;
```

```text
With idx_b (minmax, hypothetical):
  status:       applicable
  marks:        1
  est_bytes:    1.66 KiB
  skip_ratio:   99.9%

Estimation:
  source:           statistical
  empirical_status: disabled
```

有关完整的输出 schema 和设置，请参阅 [`EXPLAIN WHATIF`](/zh/sql-reference/statements/explain#explain-whatif) 参考。

<div id="drop-hypothetical-index">
  ## DROP HYPOTHETICAL INDEX
</div>

```sql
DROP HYPOTHETICAL INDEX [IF EXISTS] name ON [db.]table_name
```

从当前会话中删除一个假设索引。

<div id="drop-all-hypothetical-indexes">
  ## DROP ALL HYPOTHETICAL INDEXES
</div>

```sql
DROP ALL HYPOTHETICAL INDEXES
```

清除当前会话中定义的所有假设索引，无论属于哪个表。

<div id="scope-and-lifetime">
  ## 范围和生命周期
</div>

* 假设索引仅存在于**当前会话**中——对其他会话不可见，并会在会话结束时丢弃。
* 定义或删除假设索引都不会构建任何实际索引，也绝不会影响针对该表的常规查询。经验型 `EXPLAIN WHATIF` 确实会读取表数据，以便在内存中构建候选索引，而该扫描会计入会话的读取限制和配额。
* 可通过 [`system.hypothetical_indexes`](/zh/operations/system-tables/hypothetical_indexes) 查看当前会话中的假设索引。

<div id="limitations">
  ## 限制
</div>

`text` 和 `vector_similarity` 候选项会在 `CREATE HYPOTHETICAL INDEX` 时被拒绝，因为它们的实际校验依赖表级设置，而会话级存储无法复制这些设置。

对于带有 `FINAL` 的查询，`EXPLAIN WHATIF` 会报告 `status: not_applicable` (跳过索引裁剪会与 `PrimaryKeyExpand` 相互影响) ；而当查询命中投影时，则会报 `NOT_IMPLEMENTED` 错误 (父表索引不会在 projection parts 上 materialized) 。

经验性的 `skip_ratio` 是一个**上界**：它会独立统计每个保留下来的粒度，而不会对寻道间隙合并 (`merge_tree_min_rows_for_seek` / `merge_tree_min_bytes_for_seek`) 进行建模，也不会对析取 (`OR`) 谓词下候选项与现有跳过索引的组合进行建模。因此，真实的 materialized 索引读取量可能会略高一些，或者在某些估算未体现的情况下完成裁剪。

<div id="required-privileges">
  ## 所需特权
</div>

`CREATE HYPOTHETICAL INDEX` 要求对索引表达式引用的列具有 `SELECT` 权限——列级 `SELECT` (例如 `GRANT SELECT(b)`) 即可——因为经验型 `EXPLAIN WHATIF` 会读取这些列。

`DROP HYPOTHETICAL INDEX` 和 `DROP ALL HYPOTHETICAL INDEXES` 不需要额外特权；它们只会从会话本地存储中删除条目。

<div id="see-also">
  ## 另请参阅
</div>

* [`EXPLAIN WHATIF`](/zh/sql-reference/statements/explain#explain-whatif)
* [`system.hypothetical_indexes`](/zh/operations/system-tables/hypothetical_indexes)
* [数据跳过索引](/zh/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes)