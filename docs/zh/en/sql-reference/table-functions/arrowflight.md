---
description: '允许读取和写入通过 Apache Arrow Flight 服务器公开的数据。'
sidebar_label: 'arrowFlight'
sidebar_position: 186
slug: /sql-reference/table-functions/arrowflight
title: 'arrowFlight'
doc_type: 'reference'
---

允许读取和写入通过 [Apache Arrow Flight](/zh/interfaces/arrowflight) 服务器公开的数据。

**语法**

```sql
arrowFlight('host:port', 'dataset_name' [, 'username', 'password'])
```

**参数**

* `host:port` — Arrow Flight 服务器的地址。如果省略端口，将使用默认端口 `8815`。[String](../../sql-reference/data-types/string.md)。
* `dataset_name` — Arrow Flight 服务器上可用的数据集或描述符名称。[String](../../sql-reference/data-types/string.md)。
* `username` — 用于 HTTP 基本身份验证的用户名。[String](../../sql-reference/data-types/string.md)。
* `password` — 用于 HTTP 基本身份验证的密码。[String](../../sql-reference/data-types/string.md)。

如果未指定 `username` 和 `password`，则不使用身份验证 (仅当 Arrow Flight 服务器允许匿名访问时才有效) 。

该函数还支持 [named collections](/zh/operations/named-collections) —— 支持的参数列表请参见 [ArrowFlight 表引擎](/zh/engines/table-engines/integrations/arrowflight#named-collections)。

**返回值**

表示远程数据集的表对象。schema 会根据 Arrow Flight 服务器自动推断。

**设置**

* `arrow_flight_request_descriptor_type` — 控制将数据集名称发送到 Flight 服务器的方式。取值：`path` (默认) 或 `command`。详情请参见 [ArrowFlight 表引擎](/zh/engines/table-engines/integrations/arrowflight#settings)。

**示例**

从远程 Arrow Flight 服务器读取：

```sql title="Query"
SELECT * FROM arrowFlight('127.0.0.1:9005', 'sample_dataset') ORDER BY id;
```

```text title="Response"
┌─id─┬─name────┬─value─┐
│  1 │ foo     │ 42.1  │
│  2 │ bar     │ 13.3  │
│  3 │ baz     │ 77.0  │
└────┴─────────┴───────┘
```

将数据插入远程 Arrow Flight 服务器：

```sql
INSERT INTO FUNCTION arrowFlight('127.0.0.1:9005', 'sample_dataset') VALUES (4, 'qux', 99.9);
```

使用 named collection：

```sql
SELECT * FROM arrowFlight(named_collection_name);
```

**另请参阅**

* [ArrowFlight 表引擎](/zh/engines/table-engines/integrations/arrowflight)
* [Arrow Flight 接口](/zh/interfaces/arrowflight)
* [Apache Arrow Flight SQL 规范](https://arrow.apache.org/docs/format/FlightSql.html)