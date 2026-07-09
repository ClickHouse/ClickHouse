---
description: '执行查询时，ClickHouse 会使用不同的缓存。'
sidebar_label: '缓存'
sidebar_position: 65
slug: /operations/caches
title: '缓存类型'
keywords: ['cache']
doc_type: 'reference'
---

执行查询时，ClickHouse 会使用不同的缓存来加速查询，
并减少对磁盘读写的需求。

主要的缓存类型包括：

* `mark_cache` — [`MergeTree`](../engines/table-engines/mergetree-family/mergetree.md) 家族表引擎使用的[标记](/zh/development/architecture#merge-tree)缓存。
* `uncompressed_cache` — [`MergeTree`](../engines/table-engines/mergetree-family/mergetree.md) 家族表引擎使用的未压缩数据缓存。
* 操作系统页缓存 (间接使用，用于存放实际数据的文件) 。

此外，还有许多其他类型的缓存：

* DNS 缓存。
* [Regexp](/zh/interfaces/formats/Regexp) 缓存。
* 已编译表达式缓存。
* [向量相似度索引](../engines/table-engines/mergetree-family/annindexes.md)缓存。
* [文本索引](../engines/table-engines/mergetree-family/textindexes.md#caching)缓存。
* [Avro 格式](/zh/interfaces/formats/Avro) schema 缓存。
* [字典](../sql-reference/statements/create/dictionary/overview.md)数据缓存。
* schema 推断缓存。
* 基于 S3、Azure、Local 和其他磁盘的[文件系统缓存](storing-data.md)。
* [用户态页缓存](/zh/operations/userspace-page-cache)
* [查询缓存](query-cache.md)。
* [查询条件缓存](query-condition-cache.md)。
* 格式 schema 缓存。

如果您出于性能调优、故障排查或数据一致性方面的原因希望清除某个缓存，
可以使用 [`SYSTEM CLEAR ... CACHE`](../sql-reference/statements/system.md) 语句。