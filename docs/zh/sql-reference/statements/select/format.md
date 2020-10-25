---
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
toc_title: FORMAT
---

# 格式子句 {#format-clause}

ClickHouse支持广泛的 [序列化格式](../../../interfaces/formats.md) 可用于查询结果等。 有多种方法可以选择以下格式 `SELECT` 输出，其中之一是指定 `FORMAT format` 在查询结束时以任何特定格式获取结果数据。

特定的格式可以用于方便使用，与其他系统集成或性能增益。

## 默认格式 {#default-format}

如果 `FORMAT` 省略子句，使用默认格式，这取决于用于访问ClickHouse服务器的设置和接口。 为 [HTTP接口](../../../interfaces/http.md) 和 [命令行客户端](../../../interfaces/cli.md) 在批处理模式下，默认格式为 `TabSeparated`. 对于交互模式下的命令行客户端，默认格式为 `PrettyCompact` （它生成紧凑的人类可读表）。

## 实施细节 {#implementation-details}

使用命令行客户端时，数据始终以内部高效格式通过网络传递 (`Native`). 客户端独立解释 `FORMAT` 查询子句并格式化数据本身（从而减轻网络和服务器的额外负载）。
