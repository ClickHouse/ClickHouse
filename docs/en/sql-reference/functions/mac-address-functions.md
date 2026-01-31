---
description: 'Documentation for functions used for working with MAC addresses.'
sidebar_label: 'MAC Addresses'
slug: /sql-reference/functions/mac-address-functions
title: 'Functions for working with MAC addresses'
doc_type: 'reference'
---

# Functions for Working with MAC Addresses

## toMacAddress

Converts a string to [MacAddress](../data-types/macaddress.md) data type. Throws an exception if the input string is not a valid MAC address.

**Syntax**

```sql
toMacAddress(string)
```

**Arguments**

- `string` — String representation of a MAC address. [String](../data-types/string.md).

**Supported Formats**

The function accepts MAC addresses in the following formats:

- **Colon-separated**: `00:1A:2B:3C:4D:5E`
- **Hyphen-separated**: `00-1A-2B-3C-4D-5E`
- **Cisco format (dot-separated)**: `001A.2B3C.4D5E`
- **Raw hexadecimal**: `001A2B3C4D5E`

All formats are case-insensitive.

**Returned value**

- MAC address. [MacAddress](../data-types/macaddress.md).

**Examples**

Query:

```sql
SELECT toMacAddress('00:1A:2B:3C:4D:5E') AS mac;
```

Result:

```text
┌─mac───────────────┐
│ 00:1a:2b:3c:4d:5e │
└───────────────────┘
```

Query with different formats:

```sql
SELECT 
    toMacAddress('00:1A:2B:3C:4D:5E') AS colon,
    toMacAddress('00-1A-2B-3C-4D-5E') AS hyphen,
    toMacAddress('001A.2B3C.4D5E') AS cisco,
    toMacAddress('001A2B3C4D5E') AS raw;
```

Result:

```text
┌─colon─────────────┬─hyphen────────────┬─cisco─────────────┬─raw───────────────┐
│ 00:1a:2b:3c:4d:5e │ 00:1a:2b:3c:4d:5e │ 00:1a:2b:3c:4d:5e │ 00:1a:2b:3c:4d:5e │
└───────────────────┴───────────────────┴───────────────────┴───────────────────┘
```

## toMacAddressOrNull

Converts a string to [MacAddress](../data-types/macaddress.md) data type. Returns `NULL` if the input string is not a valid MAC address.

**Syntax**

```sql
toMacAddressOrNull(string)
```

**Arguments**

- `string` — String representation of a MAC address. [String](../data-types/string.md).

**Returned value**

- MAC address if the input string is valid, otherwise `NULL`.

**Examples**

Query:

```sql
SELECT 
    toMacAddressOrNull('00:1A:2B:3C:4D:5E') AS valid,
    toMacAddressOrNull('invalid') AS invalid,
    toMacAddressOrNull('ZZ:ZZ:ZZ:ZZ:ZZ:ZZ') AS invalid_hex;
```

Result:

```text
┌─valid─────────────┬─invalid─┬─invalid_hex─┐
│ 00:1a:2b:3c:4d:5e │ NULL    │ NULL        │
└───────────────────┴─────────┴─────────────┘
```

## toMacAddressOrZero

Converts a string to [MacAddress](../data-types/macaddress.md) data type. Returns zero MAC address (`00:00:00:00:00:00`) if the input string is not a valid MAC address.

**Syntax**

```sql
toMacAddressOrZero(string)
```

**Arguments**

- `string` — String representation of a MAC address. [String](../data-types/string.md).

**Returned value**

- MAC address if the input string is valid, otherwise `00:00:00:00:00:00`. [MacAddress](../data-types/macaddress.md).

**Examples**

Query:

```sql
SELECT 
    toMacAddressOrZero('00:1A:2B:3C:4D:5E') AS valid,
    toMacAddressOrZero('invalid') AS invalid;
```

Result:

```text
┌─valid─────────────┬─invalid───────────┐
│ 00:1a:2b:3c:4d:5e │ 00:00:00:00:00:00 │
└───────────────────┴───────────────────┘
```

**See Also**

- [MacAddress Data Type](../data-types/macaddress.md)
