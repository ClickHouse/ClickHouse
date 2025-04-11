#include <Server/HTTP/exceptionCodeToHTTPStatus.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_PARSE_TEXT;
    extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
    extern const int CANNOT_PARSE_QUOTED_STRING;
    extern const int CANNOT_PARSE_DATE;
    extern const int CANNOT_PARSE_DATETIME;
    extern const int CANNOT_PARSE_NUMBER;
    extern const int CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING;
    extern const int CANNOT_PARSE_IPV4;
    extern const int CANNOT_PARSE_IPV6;
    extern const int CANNOT_PARSE_UUID;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int CANNOT_SCHEDULE_TASK;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_COMPILE_REGEXP;
    extern const int DUPLICATE_COLUMN;
    extern const int ILLEGAL_COLUMN;
    extern const int THERE_IS_NO_COLUMN;
    extern const int UNKNOWN_ELEMENT_IN_AST;
    extern const int UNKNOWN_TYPE_OF_AST_NODE;
    extern const int TOO_DEEP_AST;
    extern const int TOO_BIG_AST;
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;

    extern const int SYNTAX_ERROR;

    extern const int INCORRECT_DATA;
    extern const int TYPE_MISMATCH;

    extern const int UNKNOWN_TABLE;
    extern const int UNKNOWN_FUNCTION;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int UNKNOWN_TYPE;
    extern const int UNKNOWN_STORAGE;
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_SETTING;
    extern const int UNKNOWN_DIRECTION_OF_SORTING;
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
    extern const int UNKNOWN_FORMAT;
    extern const int UNKNOWN_DATABASE_ENGINE;
    extern const int UNKNOWN_TYPE_OF_QUERY;
    extern const int UNKNOWN_ROLE;

    extern const int QUERY_IS_TOO_LARGE;

    extern const int NOT_IMPLEMENTED;
    extern const int SOCKET_TIMEOUT;

    extern const int UNKNOWN_USER;
    extern const int WRONG_PASSWORD;
    extern const int REQUIRED_PASSWORD;
    extern const int AUTHENTICATION_FAILED;
    extern const int SET_NON_GRANTED_ROLE;

    extern const int HTTP_LENGTH_REQUIRED;

    extern const int TIMEOUT_EXCEEDED;
}


Poco::Net::HTTPResponse::HTTPStatus exceptionCodeToHTTPStatus(int exception_code)
{
    using namespace Poco::Net;

    if (exception_code == ErrorCodes::REQUIRED_PASSWORD)
    {
        return HTTPResponse::HTTP_UNAUTHORIZED;
    }
    if (exception_code == ErrorCodes::UNKNOWN_USER || exception_code == ErrorCodes::WRONG_PASSWORD
        || exception_code == ErrorCodes::AUTHENTICATION_FAILED || exception_code == ErrorCodes::SET_NON_GRANTED_ROLE)
    {
        return HTTPResponse::HTTP_FORBIDDEN;
    }
    if (exception_code == ErrorCodes::BAD_ARGUMENTS || exception_code == ErrorCodes::CANNOT_COMPILE_REGEXP
        || exception_code == ErrorCodes::CANNOT_PARSE_TEXT || exception_code == ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE
        || exception_code == ErrorCodes::CANNOT_PARSE_QUOTED_STRING || exception_code == ErrorCodes::CANNOT_PARSE_DATE
        || exception_code == ErrorCodes::CANNOT_PARSE_DATETIME || exception_code == ErrorCodes::CANNOT_PARSE_NUMBER
        || exception_code == ErrorCodes::CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING || exception_code == ErrorCodes::CANNOT_PARSE_IPV4
        || exception_code == ErrorCodes::CANNOT_PARSE_IPV6 || exception_code == ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED
        || exception_code == ErrorCodes::CANNOT_PARSE_UUID || exception_code == ErrorCodes::DUPLICATE_COLUMN
        || exception_code == ErrorCodes::ILLEGAL_COLUMN || exception_code == ErrorCodes::UNKNOWN_ELEMENT_IN_AST
        || exception_code == ErrorCodes::UNKNOWN_TYPE_OF_AST_NODE || exception_code == ErrorCodes::THERE_IS_NO_COLUMN
        || exception_code == ErrorCodes::TOO_DEEP_AST || exception_code == ErrorCodes::TOO_BIG_AST
        || exception_code == ErrorCodes::UNEXPECTED_AST_STRUCTURE || exception_code == ErrorCodes::SYNTAX_ERROR
        || exception_code == ErrorCodes::INCORRECT_DATA || exception_code == ErrorCodes::TYPE_MISMATCH
        || exception_code == ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE)
    {
        return HTTPResponse::HTTP_BAD_REQUEST;
    }
    if (exception_code == ErrorCodes::UNKNOWN_TABLE || exception_code == ErrorCodes::UNKNOWN_FUNCTION
        || exception_code == ErrorCodes::UNKNOWN_IDENTIFIER || exception_code == ErrorCodes::UNKNOWN_TYPE
        || exception_code == ErrorCodes::UNKNOWN_STORAGE || exception_code == ErrorCodes::UNKNOWN_DATABASE
        || exception_code == ErrorCodes::UNKNOWN_SETTING || exception_code == ErrorCodes::UNKNOWN_DIRECTION_OF_SORTING
        || exception_code == ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION || exception_code == ErrorCodes::UNKNOWN_FORMAT
        || exception_code == ErrorCodes::UNKNOWN_DATABASE_ENGINE || exception_code == ErrorCodes::UNKNOWN_TYPE_OF_QUERY
        || exception_code == ErrorCodes::UNKNOWN_ROLE)
    {
        return HTTPResponse::HTTP_NOT_FOUND;
    }
    if (exception_code == ErrorCodes::QUERY_IS_TOO_LARGE)
    {
        return HTTPResponse::HTTP_REQUESTENTITYTOOLARGE;
    }
    if (exception_code == ErrorCodes::NOT_IMPLEMENTED)
    {
        return HTTPResponse::HTTP_NOT_IMPLEMENTED;
    }
    if (exception_code == ErrorCodes::SOCKET_TIMEOUT || exception_code == ErrorCodes::CANNOT_OPEN_FILE)
    {
        return HTTPResponse::HTTP_SERVICE_UNAVAILABLE;
    }
    if (exception_code == ErrorCodes::HTTP_LENGTH_REQUIRED)
    {
        return HTTPResponse::HTTP_LENGTH_REQUIRED;
    }
    if (exception_code == ErrorCodes::TIMEOUT_EXCEEDED)
    {
        return HTTPResponse::HTTP_REQUEST_TIMEOUT;
    }
    if (exception_code == ErrorCodes::CANNOT_SCHEDULE_TASK)
    {
        return HTTPResponse::HTTP_SERVICE_UNAVAILABLE;
    }

    return HTTPResponse::HTTP_INTERNAL_SERVER_ERROR;
}

}
