RowBinary
---------

Writes data by row in binary format. Rows and values are listed consecutively, without separators.
This format is less efficient than the Native format, since it is row-based.

Numbers is written in little endian, fixed width. For example, UInt64 takes 8 bytes.
DateTime is written as UInt32 with unix timestamp value.
Date is written as UInt16 with number of days since 1970-01-01 in value.
String is written as length in varint (unsigned `LEB128 <https://en.wikipedia.org/wiki/LEB128>`_) format and then bytes of string.
FixedString is written as just its bytes.
Array is written as length in varint (unsigned `LEB128 <https://en.wikipedia.org/wiki/LEB128>`_) format and then all elements, contiguously

