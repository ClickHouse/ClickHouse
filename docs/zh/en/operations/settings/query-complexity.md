---
description: '用于限制查询复杂度的设置。'
sidebar_label: '查询复杂度限制'
sidebar_position: 59
slug: /operations/settings/query-complexity
title: '查询复杂度限制'
doc_type: '参考'
---

<div id="overview">
  ## 概述
</div>

作为[设置](/zh/operations/settings/overview)的一部分，ClickHouse 提供了
对查询复杂度设置限制的功能。这有助于防止
可能大量消耗资源的查询，从而确保执行过程
更安全、更可预测，尤其是在使用用户界面时。

这些限制几乎都只适用于 `SELECT` 查询；而对于分布式
查询处理，限制会分别在每台服务器上生效。

ClickHouse 通常只会在数据分区片段
完成处理后才检查这些限制，而不是对每一行都进行检查。这可能
导致在处理数据分区片段的过程中
出现违反限制的情况。

<div id="overflow_mode_setting">
  ## `overflow_mode` 设置
</div>

大多数限制也都有一个 `overflow_mode` 设置，用于定义
超出限制时的处理方式，并且可以取以下两个值之一：

* `throw`：抛出异常 (默认) 。
* `break`：停止执行查询并返回部分结果，就像
  源数据已经耗尽了一样。

<div id="group_by_overflow_mode_settings">
  ## `group_by_overflow_mode` 设置
</div>

`group_by_overflow_mode` 设置还有一个
值 `any`：

* `any`：继续对已进入集合的键进行聚合，但不再
  将新键添加到集合中。

<div id="relevant-settings">
  ## 设置列表
</div>

以下设置用于施加查询复杂度限制。

:::note
对于“某项内容最大数量”的限制，取值可以为 `0`，
这表示“不受限制”。
:::

| 设置                                                                                                                     | 简要说明                                                                             |
| ---------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| [`max_memory_usage`](/zh/operations/settings/settings#max_memory_usage)                                                   | 在单台服务器上运行查询时可使用的最大 RAM 量。                                                        |
| [`max_memory_usage_for_user`](/zh/operations/settings/settings#max_memory_usage_for_user)                                 | 在单台服务器上运行某个用户的查询时可使用的最大 RAM 量。                                                   |
| [`max_rows_to_read`](/zh/operations/settings/settings#max_rows_to_read)                                                   | 运行查询时可从表中读取的最大行数。                                                                |
| [`max_bytes_to_read`](/zh/operations/settings/settings#max_bytes_to_read)                                                 | 运行查询时可从表中读取的最大字节数 (未压缩数据) 。                                                      |
| [`read_overflow_mode_leaf`](/zh/operations/settings/settings#read_overflow_mode_leaf)                                     | 设置读取数据量超过某个叶子节点限制时的处理方式。                                                         |
| [`max_rows_to_read_leaf`](/zh/operations/settings/settings#max_rows_to_read_leaf)                                         | 运行分布式查询时，可从叶子节点上的本地表中读取的最大行数。                                                    |
| [`max_bytes_to_read_leaf`](/zh/operations/settings/settings#max_bytes_to_read_leaf)                                       | 运行分布式查询时，可从叶子节点上的本地表中读取的最大字节数 (未压缩数据) 。                                          |
| [`read_overflow_mode_leaf`](/zh/docs/operations/settings/settings#read_overflow_mode_leaf)                                | 设置读取数据量超过某个叶子节点限制时的处理方式。                                                         |
| [`max_rows_to_group_by`](/zh/operations/settings/settings#max_rows_to_group_by)                                           | 聚合过程中接收的唯一键最大数量。                                                                 |
| [`group_by_overflow_mode`](/zh/operations/settings/settings#group_by_overflow_mode)                                       | 设置聚合的唯一键数量超过限制时的处理方式。                                                            |
| [`max_bytes_before_external_group_by`](/zh/operations/settings/settings#max_bytes_before_external_group_by)               | 启用或禁用在外部内存中执行 `GROUP BY` 子句。                                                     |
| [`max_bytes_ratio_before_external_group_by`](/zh/operations/settings/settings#max_bytes_ratio_before_external_group_by)   | `GROUP BY` 可使用的可用内存比例。达到该比例后，将使用外部内存进行聚合。                                        |
| [`max_bytes_before_external_sort`](/zh/operations/settings/settings#max_bytes_before_external_sort)                       | 启用或禁用在外部内存中执行 `ORDER BY` 子句。                                                     |
| [`max_bytes_ratio_before_external_sort`](/zh/operations/settings/settings#max_bytes_ratio_before_external_sort)           | `ORDER BY` 可使用的可用内存比例。达到该比例后，将使用外部排序。                                            |
| [`max_rows_to_sort`](/zh/operations/settings/settings#max_rows_to_sort)                                                   | 排序前允许的最大行数。可用于限制排序时的内存消耗。                                                        |
| [`max_bytes_to_sort`](/zh/operations/settings/settings#max_rows_to_sort)                                                  | 排序前允许的最大字节数。                                                                     |
| [`sort_overflow_mode`](/zh/operations/settings/settings#sort_overflow_mode)                                               | 设置排序前接收的行数超过某个限制时的处理方式。                                                          |
| [`max_result_rows`](/zh/operations/settings/settings#max_result_rows)                                                     | 限制结果中的行数。                                                                        |
| [`max_result_bytes`](/zh/operations/settings/settings#max_result_bytes)                                                   | 限制结果大小 (按字节计，未压缩) 。                                                              |
| [`result_overflow_mode`](/zh/operations/settings/settings#result_overflow_mode)                                           | 设置结果数据量超过某个限制时的处理方式。                                                             |
| [`max_execution_time`](/zh/operations/settings/settings#max_execution_time)                                               | 查询的最大执行时间 (秒) 。                                                                  |
| [`timeout_overflow_mode`](/zh/operations/settings/settings#timeout_overflow_mode)                                         | 设置查询运行时间超过 `max_execution_time`，或估计运行时间超过 `max_estimated_execution_time` 时的处理方式。 |
| [`max_execution_time_leaf`](/zh/operations/settings/settings#max_execution_time_leaf)                                     | 语义上与 `max_execution_time` 类似，但仅应用于分布式查询或远程查询的叶子节点。                               |
| [`timeout_overflow_mode_leaf`](/zh/operations/settings/settings#timeout_overflow_mode_leaf)                               | 设置叶子节点上的查询运行时间超过 `max_execution_time_leaf` 时的处理方式。                               |
| [`min_execution_speed`](/zh/operations/settings/settings#min_execution_speed)                                             | 最低执行速度，以每秒行数计。                                                                   |
| [`min_execution_speed_bytes`](/zh/operations/settings/settings#min_execution_speed_bytes)                                 | 最低执行速度，以每秒字节数计。                                                                  |
| [`max_execution_speed`](/zh/operations/settings/settings#max_execution_speed)                                             | 最高执行速度，以每秒行数计。                                                                   |
| [`max_execution_speed_bytes`](/zh/operations/settings/settings#max_execution_speed_bytes)                                 | 最高执行速度，以每秒字节数计。                                                                  |
| [`timeout_before_checking_execution_speed`](/zh/operations/settings/settings#timeout_before_checking_execution_speed)     | 在经过指定秒数后，检查执行速度是否过慢 (不低于 `min_execution_speed`) 。                                |
| [`max_estimated_execution_time`](/zh/operations/settings/settings#max_estimated_execution_time)                           | 查询估计执行时间的最大值 (秒) 。                                                               |
| [`max_columns_to_read`](/zh/operations/settings/settings#max_columns_to_read)                                             | 单个查询可从表中读取的最大列数。                                                                 |
| [`max_temporary_columns`](/zh/operations/settings/settings#max_temporary_columns)                                         | 运行查询时，必须同时保存在 RAM 中的临时列最大数量，包括常量列。                                               |
| [`max_temporary_non_const_columns`](/zh/operations/settings/settings#max_temporary_non_const_columns)                     | 运行查询时，必须同时保存在 RAM 中的临时列最大数量，但不包括常量列。                                             |
| [`max_subquery_depth`](/zh/operations/settings/settings#max_subquery_depth)                                               | 设置查询中的嵌套子查询超过指定数量时的处理方式。                                                         |
| [`max_ast_depth`](/zh/operations/settings/settings#max_ast_depth)                                                         | 查询语法树的最大嵌套深度。                                                                    |
| [`max_ast_elements`](/zh/operations/settings/settings#max_ast_elements)                                                   | 查询语法树中的最大元素数。                                                                    |
| [`max_rows_in_set`](/zh/operations/settings/settings#max_rows_in_set)                                                     | 由子查询在 IN 子句中创建的数据集的最大行数。                                                         |
| [`max_bytes_in_set`](/zh/operations/settings/settings#max_bytes_in_set)                                                   | 由子查询在 IN 子句中创建的集合可使用的最大字节数 (未压缩数据) 。                                             |
| [`set_overflow_mode`](/zh/operations/settings/settings#max_bytes_in_set)                                                  | 设置数据量超过某个限制时的处理方式。                                                               |
| [`max_rows_in_distinct`](/zh/operations/settings/settings#max_rows_in_distinct)                                           | 使用 DISTINCT 时，不同行的最大数量。                                                          |
| [`max_bytes_in_distinct`](/zh/operations/settings/settings#max_bytes_in_distinct)                                         | 使用 DISTINCT 时，哈希表在内存中使用的状态的最大字节数 (按未压缩字节计算) 。                                    |
| [`distinct_overflow_mode`](/zh/operations/settings/settings#distinct_overflow_mode)                                       | 设置数据量超过某个限制时的处理方式。                                                               |
| [`max_rows_to_transfer`](/zh/operations/settings/settings#max_rows_to_transfer)                                           | 执行 GLOBAL IN/JOIN 部分时，可传递到远程服务器或保存到 temporary table 中的最大大小 (按行数计) 。              |
| [`max_bytes_to_transfer`](/zh/operations/settings/settings#max_bytes_to_transfer)                                         | 执行 GLOBAL IN/JOIN 部分时，可传递到远程服务器或保存到 temporary table 中的最大字节数 (未压缩数据) 。            |
| [`transfer_overflow_mode`](/zh/operations/settings/settings#transfer_overflow_mode)                                       | 设置数据量超过某个限制时的处理方式。                                                               |
| [`max_rows_in_join`](/zh/operations/settings/settings#max_rows_in_join)                                                   | 限制连接表时所用哈希表中的最大行数。                                                               |
| [`max_bytes_in_join`](/zh/operations/settings/settings#max_bytes_in_join)                                                 | 连接表时所用哈希表的最大字节大小。                                                                |
| [`join_overflow_mode`](/zh/operations/settings/settings#join_overflow_mode)                                               | 定义达到以下任一 join 限制时 ClickHouse 执行的操作。                                              |
| [`max_partitions_per_insert_block`](/zh/operations/settings/settings#max_partitions_per_insert_block)                     | 限制单个插入块中的最大分区数；如果块中包含过多分区，则会抛出异常。                                                |
| [`throw_on_max_partitions_per_insert_block`](/zh/operations/settings/settings#throw_on_max_partitions_per_insert_block)   | 允许你控制达到 `max_partitions_per_insert_block` 时的行为。                                  |
| [`max_temporary_data_on_disk_size_for_user`](/zh/operations/settings/settings#throw_on_max_partitions_per_insert_block)   | 所有并发运行的用户查询在磁盘上的临时 File 所消耗的最大数据量 (以字节为单位) 。                                     |
| [`max_temporary_data_on_disk_size_for_query`](/zh/operations/settings/settings#max_temporary_data_on_disk_size_for_query) | 所有并发运行的查询在磁盘上的临时 File 所消耗的最大数据量 (以字节为单位) 。                                       |
| [`max_sessions_for_user`](/zh/operations/settings/settings#max_sessions_for_user)                                         | 每个已认证用户连接到 ClickHouse 服务器的最大并发会话数。                                               |
| [`max_partitions_to_read`](/zh/operations/settings/settings#max_partitions_to_read)                                       | 限制单个查询可访问的最大分区数。                                                                 |

<div id="obsolete-settings">
  ## 已废弃设置
</div>

:::note
以下设置已废弃
:::

<div id="max-pipeline-depth">
  ### max_pipeline_depth
</div>

最大管道深度。它表示每个
数据块在查询处理过程中经历的转换次数。该值是在
单个 服务器 的限制范围内计算的。如果管道深度超过该值，则会抛出异常。