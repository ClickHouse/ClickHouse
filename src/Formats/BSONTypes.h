#pragma once

#include <cstdint>
#include <string>

namespace DB
{

static const uint8_t BSON_DOCUMENT_END = 0x00;
static const size_t BSON_OBJECT_ID_SIZE = 12;
static const size_t BSON_DB_POINTER_SIZE = 12;
using BSONSizeT = uint32_t;
static const BSONSizeT MAX_BSON_SIZE = std::numeric_limits<BSONSizeT>::max();

/// See details on https://bsonspec.org/spec.html
enum class BSONType : uint8_t
{
    DOUBLE = 0x01,
    STRING = 0x02,
    DOCUMENT = 0x03,
    ARRAY = 0x04,
    BINARY = 0x05,
    UNDEFINED = 0x06,
    OBJECT_ID = 0x07,
    BOOL = 0x08,
    DATETIME = 0x09,
    NULL_VALUE = 0x0A,
    REGEXP = 0x0B,
    DB_POINTER = 0x0C,
    JAVA_SCRIPT_CODE = 0x0D,
    SYMBOL = 0x0E,
    JAVA_SCRIPT_CODE_W_SCOPE = 0x0F,
    INT32 = 0x10,
    TIMESTAMP = 0x11,
    INT64 = 0x12,
    DECIMAL128 = 0x13,
    MIN_KEY = 0xFF,
    MAX_KEY = 0x7F,
};

enum class BSONBinarySubtype : uint8_t
{
    BINARY = 0x00,
    FUNCTION = 0x01,
    BINARY_OLD = 0x02,
    UUID_OLD = 0x03,
    UUID = 0x04,
    MD5 = 0x05,
    ENCRYPTED_BSON_VALUE = 0x06,
    COMPRESSED_BSON_COLUMN = 0x07,
};

BSONType getBSONType(uint8_t value);
std::string getBSONTypeName(BSONType type);

BSONBinarySubtype getBSONBinarySubtype(uint8_t value);
std::string getBSONBinarySubtypeName(BSONBinarySubtype subtype);

}
