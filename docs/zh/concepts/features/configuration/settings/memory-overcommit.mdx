---
description: '一种 Experimental 技术，旨在为查询设置更灵活的内存限制。'
slug: /operations/settings/memory-overcommit
title: '内存 overcommit'
doc_type: 'reference'
---

内存 overcommit 是一种 Experimental 技术，旨在为查询设置更灵活的内存限制。

这项技术的核心思路是引入一组设置，用于表示查询可使用的受保障内存量。
启用内存 overcommit 后，一旦达到内存限制，ClickHouse 会选择 overcommit 程度最高的查询，并尝试通过终止该查询来释放内存。

当达到内存限制时，任何查询在尝试分配新内存时都会等待一段时间。
如果在 timeout 到期前内存已被释放，查询将继续执行。
否则会抛出异常，并终止该查询。

根据达到的是哪种内存限制，选择要停止或终止的查询这一操作由全局或用户级 overcommit 跟踪器执行。
如果 overcommit 跟踪器无法选择要停止的查询，则会抛出 MEMORY&#95;LIMIT&#95;EXCEEDED 异常。

<div id="user-overcommit-tracker">
  ## 用户 overcommit 跟踪器
</div>

用户 overcommit 跟踪器会在该用户的查询列表中找出 overcommit 比率 最大的查询。
查询的 overcommit 比率 计算方式为：已分配的字节数除以设置 `memory_overcommit_ratio_denominator_for_user` 的值。

如果该查询的 `memory_overcommit_ratio_denominator_for_user` 等于零，overcommit 跟踪器不会选择该查询。

等待超时由 `memory_usage_overcommit_max_wait_microseconds` 设置。

**示例**

```sql
SELECT number FROM numbers(1000) GROUP BY number SETTINGS memory_overcommit_ratio_denominator_for_user=4000, memory_usage_overcommit_max_wait_microseconds=500
```

<div id="global-overcommit-tracker">
  ## 全局 overcommit 跟踪器
</div>

全局 overcommit 跟踪器会在所有查询中找出 overcommit 比率最大的查询。
这里，overcommit 比率的计算方式为：已分配的字节数除以设置 `memory_overcommit_ratio_denominator` 的值。

如果某个查询的 `memory_overcommit_ratio_denominator` 等于 0，overcommit 跟踪器就不会选择该查询。

等待超时时间由配置文件中的 `memory_usage_overcommit_max_wait_microseconds` 参数指定。