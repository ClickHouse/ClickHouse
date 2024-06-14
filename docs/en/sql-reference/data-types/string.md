---
slug: /en/sql-reference/data-types/string
sidebar_position: 8
sidebar_label: String
---

# String

Strings of an arbitrary length. The length is not limited. The value can contain an arbitrary set of bytes, including null bytes.
The String type replaces the types VARCHAR, BLOB, CLOB, and others from other DBMSs.

When creating tables, numeric parameters for string fields can be set (e.g. `VARCHAR(255)`), but ClickHouse ignores them.

Aliases:

- `String` — `LONGTEXT`, `MEDIUMTEXT`, `TINYTEXT`, `TEXT`, `LONGBLOB`, `MEDIUMBLOB`, `TINYBLOB`, `BLOB`, `VARCHAR`, `CHAR`, `CHAR LARGE OBJECT`, `CHAR VARYING`, `CHARACTER LARGE OBJECT`, `CHARACTER VARYING`, `NCHAR LARGE OBJECT`, `NCHAR VARYING`, `NATIONAL CHARACTER LARGE OBJECT`, `NATIONAL CHARACTER VARYING`, `NATIONAL CHAR VARYING`, `NATIONAL CHARACTER`, `NATIONAL CHAR`, `BINARY LARGE OBJECT`, `BINARY VARYING`,

## Encodings

ClickHouse does not have the concept of encodings. Strings can contain an arbitrary set of bytes, which are stored and output as-is.
If you need to store texts, we recommend using UTF-8 encoding. At the very least, if your terminal uses UTF-8 (as recommended), you can read and write your values without making conversions.
Similarly, certain functions for working with strings have separate variations that work under the assumption that the string contains a set of bytes representing a UTF-8 encoded text.
For example, the [length](../functions/string-functions.md#length) function calculates the string length in bytes, while the [lengthUTF8](../functions/string-functions.md#lengthutf8) function calculates the string length in Unicode code points, assuming that the value is UTF-8 encoded.
