---
description: 'Documentation for the MacAddress data type in ClickHouse'
sidebar_label: 'MacAddress'
sidebar_position: 31
slug: /sql-reference/data-types/macaddress
title: 'MacAddress'
doc_type: 'reference'
---

# MacAddress

A MAC (Media Access Control) address is a 48-bit (6-byte) identifier used to uniquely identify network interfaces. ClickHouse stores MAC addresses efficiently in 8 bytes internally, using only the lower 48 bits.

## Basic Usage {#basic-usage}

```sql
CREATE TABLE devices (
    device_id UInt32,
    device_name String,
    mac MacAddress
) ENGINE = MergeTree()
ORDER BY device_id;

DESCRIBE TABLE devices;
```

```text
┌─name────────┬─type───────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┐
│ device_id   │ UInt32     │              │                    │         │                  │
│ device_name │ String     │              │                    │         │                  │
│ mac         │ MacAddress │              │                    │         │                  │
└─────────────┴────────────┴──────────────┴────────────────────┴─────────┴──────────────────┘
```

## Input Formats {#input-formats}

The `MacAddress` type supports multiple input formats for flexibility:

```sql
INSERT INTO devices VALUES (1, 'Router', toMacAddress('00:1A:2B:3C:4D:5E')); -- Colon-Separated
INSERT INTO devices VALUES (3, 'Server', toMacAddress('00-1A-2B-3C-4D-5E')); -- Hyphen-Separated
INSERT INTO devices VALUES (4, 'Cisco Router', toMacAddress('001A.2B3C.4D5E')); -- Cisco Format
INSERT INTO devices VALUES (5, 'Access Point', toMacAddress('001A2B3C4D5E')); -- Raw Hexadecimal
```

## Output Format {#output-format}

MAC addresses are always displayed in lowercase colon-separated format:

```sql
SELECT * FROM devices ORDER BY device_id;
```

```text
┌─device_id─┬─device_name──┬─mac───────────────┐
│         1 │ Router       │ 00:1a:2b:3c:4d:5e │
│         2 │ Switch       │ aa:bb:cc:dd:ee:ff │
│         3 │ Server       │ 00:1a:2b:3c:4d:5e │
│         4 │ Cisco Router │ 00:1a:2b:3c:4d:5e │
│         5 │ Access Point │ 00:1a:2b:3c:4d:5e │
└───────────┴──────────────┴───────────────────┘
```

## PostgreSQL Compatibility {#postgresql-compatibility}

For compatibility with PostgreSQL, ClickHouse supports the `MACADDR` alias:

```sql
CREATE TABLE devices_pg (
    id UInt32,
    mac MACADDR
) ENGINE = Memory;

DESCRIBE TABLE devices_pg;
```

```text
┌─name─┬─type───────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┐
│ id   │ UInt32     │              │                    │         │                  │
│ mac  │ MacAddress │              │                    │         │                  │
└──────┴────────────┴──────────────┴────────────────────┴─────────┴──────────────────┘
```

**See Also**

- [Functions for Working with MAC Addresses](../functions/mac-address-functions.md)
