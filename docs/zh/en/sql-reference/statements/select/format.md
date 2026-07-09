---
description: 'FORMAT 子句文档'
sidebar_label: 'FORMAT'
slug: /sql-reference/statements/select/format
title: 'FORMAT 子句'
doc_type: 'reference'
---

ClickHouse 支持多种[序列化格式](../../../interfaces/formats.md)，可用于查询结果等多种场景。为 `SELECT` 输出选择格式有多种方式，其中一种是在查询末尾指定 `FORMAT format`，以特定格式返回结果数据。

特定格式可用于提升使用便利性、与其他系统集成，或提高性能。

<div id="default-format">
  ## 默认格式
</div>

如果省略 `FORMAT` 子句，则会使用默认格式；默认格式取决于相关设置以及访问 ClickHouse server 时使用的接口。对于 [HTTP interface](/zh/interfaces/http) 和处于批次模式的[命令行客户端](../../../interfaces/client.md)，默认格式为 `TabSeparated`。对于处于交互模式的命令行客户端，默认格式为 `PrettyCompact` (它会生成紧凑且便于阅读的表) 。

<div id="implementation-details">
  ## 实现细节
</div>

使用命令行客户端时，数据始终以内部的高效格式 (`Native`) 通过网络传输。客户端会独立解析查询中的 `FORMAT` 子句，并自行完成数据格式化 (从而减轻网络和服务器的额外开销) 。