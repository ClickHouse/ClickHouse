---
toc_title: FORMAT
---

# 格式化子句 {#format-clause}

ClickHouse支持广泛的 [序列化格式](../../../interfaces/formats.md) 可用于查询结果等。 有多种方法可以选择格式化 `SELECT` 的输出，其中之一是指定 `FORMAT format` 在查询结束时以任何特定格式获取结果集。

特定的格式方便使用，与其他系统集成或增强性能。

## 默认格式 {#default-format}

如果 `FORMAT` 被省略则使用默认格式，这取决于用于访问ClickHouse服务器的设置和接口。 为 [HTTP接口](../../../interfaces/http.md) 和 [命令行客户端](../../../interfaces/cli.md) 在批处理模式下，默认格式为 `TabSeparated`. 对于交互模式下的命令行客户端，默认格式为 `PrettyCompact` （它生成紧凑的人类可读表）。

## 实现细节 {#implementation-details}

使用命令行客户端时，数据始终以内部高效格式通过网络传递 (`Native`). 客户端独立解释 `FORMAT` 查询子句并格式化数据本身（以减轻网络和服务器的额外负担）。
