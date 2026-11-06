#include <Formats/BSONTypes.h>
#include <Common/Exception.h>
#include <base/hex.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
}

static std::string byteToHexString(uint8_t byte)
{
    return "0x" + getHexUIntUppercase(byte);
}

BSONType getBSONType(uint8_t value)
{
    if ((value >= 0x01 && value <= 0x13) || value == 0xFF || value == 0x7f)
        return BSONType(value);

    throw Exception(ErrorCodes::UNKNOWN_TYPE, "Unknown BSON type: {}", byteToHexString(value));
}

BSONBinarySubtype getBSONBinarySubtype(uint8_t value)
{
    if (value <= 0x07)
        return BSONBinarySubtype(value);

    throw Exception(ErrorCodes::UNKNOWN_TYPE, "Unknown BSON binary subtype: {}", byteToHexString(value));
}

std::string getBSONTypeName(BSONType type)
{
    switch (type)
    {
        case BSONType::BINARY:
            return "Binary";
        case BSONType::SYMBOL:
            return "Symbol";
        case BSONType::ARRAY:
            return "Array";
        case BSONType::DOCUMENT:
            return "Document";
        case BSONType::TIMESTAMP:
            return "Timestamp";
        case BSONType::INT64:
            return "Int64";
        case BSONType::INT32:
            return "Int32";
        case BSONType::BOOL:
            return "Bool";
        case BSONType::DOUBLE:
            return "Double";
        case BSONType::STRING:
            return "String";
        case BSONType::DECIMAL128:
            return "Decimal128";
        case BSONType::JAVA_SCRIPT_CODE_W_SCOPE:
            return "JavaScript code w/ scope";
        case BSONType::JAVA_SCRIPT_CODE:
            return "JavaScript code";
        case BSONType::DB_POINTER:
            return "DBPointer";
        case BSONType::REGEXP:
            return "Regexp";
        case BSONType::DATETIME:
            return "Datetime";
        case BSONType::OBJECT_ID:
            return "ObjectId";
        case BSONType::UNDEFINED:
            return "Undefined";
        case BSONType::NULL_VALUE:
            return "Null";
        case BSONType::MAX_KEY:
            return "Max key";
        case BSONType::MIN_KEY:
            return "Min key";
    }
}

std::string getBSONBinarySubtypeName(BSONBinarySubtype subtype)
{
    switch (subtype)
    {
        case BSONBinarySubtype::BINARY:
            return "Binary";
        case BSONBinarySubtype::FUNCTION:
            return "Function";
        case BSONBinarySubtype::BINARY_OLD:
            return "Binary (Old)";
        case BSONBinarySubtype::UUID_OLD:
            return "UUID (Old)";
        case BSONBinarySubtype::UUID:
            return "UUID";
        case BSONBinarySubtype::MD5:
            return "MD5";
        case BSONBinarySubtype::ENCRYPTED_BSON_VALUE:
            return "Encrypted BSON value";
        case BSONBinarySubtype::COMPRESSED_BSON_COLUMN:
            return "Compressed BSON column";
    }
}

}
