# RowBinary

Formats and parses data by row in binary format. Rows and values are listed consecutively, without separators.
This format is less efficient than the Native format, since it is row-based.

Integers use fixed-length little endian representation. For example, UInt64 uses 8 bytes.
DateTime is represented as UInt32 containing the Unix timestamp as the value.
Date is represented as a UInt16 object that contains the number of days since 1970-01-01 as the value.
String is represented as a varint length (unsigned [LEB128](https://en.wikipedia.org/wiki/LEB128)), followed by the bytes of the string.
FixedString is represented simply as a sequence of bytes.

Arrays are represented as a varint length (unsigned [LEB128](https://en.wikipedia.org/wiki/LEB128)), followed by the array elements in order.

