---
description: 'توثيق نوع البيانات IPv4 في ClickHouse'
sidebar_label: 'IPv4'
sidebar_position: 28
slug: /sql-reference/data-types/ipv4
title: 'IPv4'
doc_type: 'مرجع'
---

<div id="ipv4">
  ## IPv4
</div>

عناوين IPv4. تُخزَّن في 4 بايت على هيئة UInt32.

<div id="basic-usage">
  ### الاستخدام الأساسي
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

أو يمكنك استخدام نطاق IPv4 كمفتاح:

```sql
CREATE TABLE hits (url String, from IPv4) ENGINE = MergeTree() ORDER BY from;
```

يدعم نطاق `IPv4` تنسيقَ إدخالٍ مخصصًا على هيئة سلاسل IPv4:

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

تُخزَّن القيم بتنسيق ثنائي مضغوط:

```sql
SELECT toTypeName(from), hex(from) FROM hits LIMIT 1;
```

```text
┌─toTypeName(from)─┬─hex(from)─┐
│ IPv4             │ B7F7E83A  │
└──────────────────┴───────────┘
```

يمكن مقارنة عناوين IPv4 مباشرةً بعناوين IPv6:

```sql
SELECT toIPv4('127.0.0.1') = toIPv6('::ffff:127.0.0.1');
```

```text
┌─equals(toIPv4('127.0.0.1'), toIPv6('::ffff:127.0.0.1'))─┐
│                                                       1 │
└─────────────────────────────────────────────────────────┘
```

**راجع أيضًا**

* [الدوال الخاصة بالتعامل مع عناوين IPv4 وIPv6](../functions/ip-address-functions.md)