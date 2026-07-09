---
description: 'IPv6アドレスを16バイトの値として格納するClickHouseのIPv6データ型に関するドキュメント'
sidebar_label: 'IPv6'
sidebar_position: 30
slug: /sql-reference/data-types/ipv6
title: 'IPv6'
doc_type: 'reference'
---

<div id="ipv6">
  ## IPv6
</div>

IPv6アドレス。16バイトのビッグエンディアンのUInt128として格納されます。

<div id="basic-usage">
  ### 基本的な使い方
</div>

```sql
CREATE TABLE hits (url String, from IPv6) ENGINE = MergeTree() ORDER BY url;

DESCRIBE TABLE hits;
```

```text
┌─name─┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┐
│ url  │ String │              │                    │         │                  │
│ from │ IPv6   │              │                    │         │                  │
└──────┴────────┴──────────────┴────────────────────┴─────────┴──────────────────┘
```

または、キーとして `IPv6` ドメインを使用できます：

```sql
CREATE TABLE hits (url String, from IPv6) ENGINE = MergeTree() ORDER BY from;
```

`IPv6` domain は、IPv6文字列によるカスタム入力をサポートしています:

```sql
INSERT INTO hits (url, from) VALUES ('https://wikipedia.org', '2a02:aa08:e000:3100::2')('https://clickhouse.com', '2001:44c8:129:2632:33:0:252:2')('https://clickhouse.com/docs/en/', '2a02:e980:1e::1');

SELECT * FROM hits;
```

```text
┌─url────────────────────────────────┬─from──────────────────────────┐
│ https://clickhouse.com          │ 2001:44c8:129:2632:33:0:252:2 │
│ https://clickhouse.com/docs/en/ │ 2a02:e980:1e::1               │
│ https://wikipedia.org              │ 2a02:aa08:e000:3100::2        │
└────────────────────────────────────┴───────────────────────────────┘
```

値はコンパクトなバイナリ形式で格納されます。

```sql
SELECT toTypeName(from), hex(from) FROM hits LIMIT 1;
```

```text
┌─toTypeName(from)─┬─hex(from)────────────────────────┐
│ IPv6             │ 200144C8012926320033000002520002 │
└──────────────────┴──────────────────────────────────┘
```

IPv6アドレスはIPv4アドレスと直接比較できます:

```sql
SELECT toIPv4('127.0.0.1') = toIPv6('::ffff:127.0.0.1');
```

```text
┌─equals(toIPv4('127.0.0.1'), toIPv6('::ffff:127.0.0.1'))─┐
│                                                       1 │
└─────────────────────────────────────────────────────────┘
```

**関連項目**

* [IPv4 および IPv6 アドレスを扱う関数](../functions/ip-address-functions.md)