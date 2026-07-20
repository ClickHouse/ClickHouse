#include <Formats/ParseError.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int CANNOT_PARSE_QUOTED_STRING;
    extern const int CANNOT_PARSE_DATE;
    extern const int CANNOT_PARSE_DATETIME;
    extern const int CANNOT_PARSE_NUMBER;
    extern const int CANNOT_PARSE_BOOL;
    extern const int CANNOT_PARSE_UUID;
    extern const int CANNOT_READ_ARRAY_FROM_TEXT;
    extern const int CANNOT_READ_MAP_FROM_TEXT;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int TOO_LARGE_STRING_SIZE;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int INCORRECT_DATA;
    extern const int CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING;
    extern const int CANNOT_PARSE_IPV4;
    extern const int CANNOT_PARSE_IPV6;
    extern const int UNKNOWN_ELEMENT_OF_ENUM;
    extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
    extern const int UNEXPECTED_DATA_AFTER_PARSED_VALUE;
}

bool isParseError(int code)
{
    return code == ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED
        || code == ErrorCodes::CANNOT_PARSE_QUOTED_STRING
        || code == ErrorCodes::CANNOT_PARSE_DATE
        || code == ErrorCodes::CANNOT_PARSE_DATETIME
        || code == ErrorCodes::CANNOT_PARSE_NUMBER
        || code == ErrorCodes::CANNOT_PARSE_UUID
        || code == ErrorCodes::CANNOT_PARSE_BOOL
        || code == ErrorCodes::CANNOT_READ_ARRAY_FROM_TEXT
        || code == ErrorCodes::CANNOT_READ_MAP_FROM_TEXT
        || code == ErrorCodes::CANNOT_READ_ALL_DATA
        || code == ErrorCodes::TOO_LARGE_STRING_SIZE
        || code == ErrorCodes::ARGUMENT_OUT_OF_BOUND       /// For Decimals
        || code == ErrorCodes::INCORRECT_DATA              /// For some ReadHelpers
        || code == ErrorCodes::CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING
        || code == ErrorCodes::CANNOT_PARSE_IPV4
        || code == ErrorCodes::CANNOT_PARSE_IPV6
        || code == ErrorCodes::UNKNOWN_ELEMENT_OF_ENUM
        || code == ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE
        || code == ErrorCodes::UNEXPECTED_DATA_AFTER_PARSED_VALUE;
}

void rethrowIfNotParseError()
{
    if (!isParseError(getCurrentExceptionCode()))
        throw;
}

}
