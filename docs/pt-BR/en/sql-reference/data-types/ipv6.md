---
description: 'Documentação sobre o tipo de dado IPv6 no ClickHouse, que armazena
  endereços IPv6 em valores de 16 bytes'
sidebar_label: 'IPv6'
sidebar_position: 30
slug: /sql-reference/data-types/ipv6
title: 'IPv6'
doc_type: 'reference'
---

<div id="ipv6">
  ## IPv6
</div>

Endereços IPv6. Armazenados em 16 bytes como UInt128 em big-endian.

<div id="basic-usage">
  ### Uso básico
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

Ou você também pode usar o domínio `IPv6` como chave:

```sql
CREATE TABLE hits (url String, from IPv6) ENGINE = MergeTree() ORDER BY from;
```

O domínio `IPv6` aceita entradas personalizadas como strings IPv6:

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

Os valores são armazenados em formato binário compacto:

```sql
SELECT toTypeName(from), hex(from) FROM hits LIMIT 1;
```

```text
┌─toTypeName(from)─┬─hex(from)────────────────────────┐
│ IPv6             │ 200144C8012926320033000002520002 │
└──────────────────┴──────────────────────────────────┘
```

Endereços IPv6 podem ser comparados diretamente com endereços IPv4:

```sql
SELECT toIPv4('127.0.0.1') = toIPv6('::ffff:127.0.0.1');
```

```text
┌─equals(toIPv4('127.0.0.1'), toIPv6('::ffff:127.0.0.1'))─┐
│                                                       1 │
└─────────────────────────────────────────────────────────┘
```

**Veja também**

* [Funções para trabalhar com endereços IPv4 e IPv6](../functions/ip-address-functions.md)