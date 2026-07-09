---
description: 'ClickHouse IPv4 数据类型文档'
sidebar_label: 'IPv4'
sidebar_position: 28
slug: /sql-reference/data-types/ipv4
title: 'IPv4'
doc_type: 'reference'
---

<div id="ipv4">
  ## IPv4
</div>

IPv4 地址。以 UInt32 类型存储，占 4 个字节。

<div id="basic-usage">
  ### 基础用法
</div>

```sql
CREATE TABLE hits (url String, from IPv4) ENGINE = MergeTree() ORDER BY url;

DESCRIBE TABLE hits;
```

```text
┌─name─┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┐
│ url  │ String │              │                    │         │                  │
│ from │ IPv4   │              │                    │         │                  │
└──────┴────────┴──────────────┴────────────────────┴─────────┴──────────────────┘
```

或者，也可以将 IPv4 域作为键使用：

```sql
CREATE TABLE hits (url String, from IPv4) ENGINE = MergeTree() ORDER BY from;
```

`IPv4` 域支持自定义输入格式，即 IPv4 字符串：

```sql
INSERT INTO hits (url, from) VALUES ('https://wikipedia.org', '116.253.40.133')('https://clickhouse.com', '183.247.232.58')('https://clickhouse.com/docs/en/', '116.106.34.242');

SELECT * FROM hits;
```

```text
┌─url────────────────────────────────┬───────────from─┐
│ https://clickhouse.com/docs/en/ │ 116.106.34.242 │
│ https://wikipedia.org              │ 116.253.40.133 │
│ https://clickhouse.com          │ 183.247.232.58 │
└────────────────────────────────────┴────────────────┘
```

值以紧凑的二进制形式存储：

```sql
SELECT toTypeName(from), hex(from) FROM hits LIMIT 1;
```

```text
┌─toTypeName(from)─┬─hex(from)─┐
│ IPv4             │ B7F7E83A  │
└──────────────────┴───────────┘
```

IPv4 地址可直接与 IPv6 地址比较：

```sql
SELECT toIPv4('127.0.0.1') = toIPv6('::ffff:127.0.0.1');
```

```text
┌─equals(toIPv4('127.0.0.1'), toIPv6('::ffff:127.0.0.1'))─┐
│                                                       1 │
└─────────────────────────────────────────────────────────┘
```

**另请参阅**

* [IPv4 和 IPv6 地址函数](../functions/ip-address-functions.md)