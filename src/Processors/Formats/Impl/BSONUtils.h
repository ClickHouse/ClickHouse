#pragma once

#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBuffer.h>


namespace DB
{

enum BSONDataTypeIndex
{
    DOUBLE = 0x01,
    STRING = 0x02,
    EMB_DOCUMENT = 0x03,
    ARRAY = 0x04,
    BINARY = 0x05,
    UNDEFINED = 0x06,
    OID = 0x07,
    BOOL = 0x08,
    DATETIME = 0x09,
    NULLVALUE = 0x0A,
    REGEXP = 0x0B,
    DBPOINTER = 0x0C,
    JAVASCRIPT_CODE = 0x0D,
    SYMBOL = 0x0E,
    JAVASCRIPT_CODE_WITH_SCOPE = 0x0F,
    INT32 = 0x10,
    TIMESTAMP = 0x11, // The same as UINT64
    UINT64 = 0x11, // The same as TIMESTAMP
    INT64 = 0x12,
    DECIMAL128 = 0x13,
    MIN_KEY = 0xFF,
    MAX_KEY = 0x7F,
};

enum
{
    BSON_TYPE = 1,
    BSON_ZERO = 1,
    BSON_8 = 1,
    BSON_32 = 4,
    BSON_64 = 8,
    BSON_128 = 16,
};

namespace BSONUtils
{
    bool readField(ReadBuffer & in, IColumn & column, char type, UInt32 & already_read);
}

}
