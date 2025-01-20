---
title : RowBinary
slug : /en/interfaces/formats/RowBinary
keywords : [RowBinary]
---

## Description

Formats and parses data by row in binary format. Rows and values are listed consecutively, without separators. Because data is in the binary format the delimiter after `FORMAT RowBinary` is strictly specified as next: any number of whitespaces (`' '` - space, code `0x20`; `'\t'` - tab, code `0x09`; `'\f'` - form feed, code `0x0C`) followed by exactly one new line sequence (Windows style `"\r\n"` or Unix style `'\n'`), immediately followed by binary data.
This format is less efficient than the Native format since it is row-based.

Integers use fixed-length little-endian representation. For example, UInt64 uses 8 bytes.
DateTime is represented as UInt32 containing the Unix timestamp as the value.
Date is represented as a UInt16 object that contains the number of days since 1970-01-01 as the value.
String is represented as a varint length (unsigned [LEB128](https://en.wikipedia.org/wiki/LEB128)), followed by the bytes of the string.
FixedString is represented simply as a sequence of bytes.

Array is represented as a varint length (unsigned [LEB128](https://en.wikipedia.org/wiki/LEB128)), followed by successive elements of the array.

For [NULL](/docs/en/sql-reference/syntax.md/#null-literal) support, an additional byte containing 1 or 0 is added before each [Nullable](/docs/en/sql-reference/data-types/nullable.md) value. If 1, then the value is `NULL` and this byte is interpreted as a separate value. If 0, the value after the byte is not `NULL`.

## Example Usage

## Format Settings