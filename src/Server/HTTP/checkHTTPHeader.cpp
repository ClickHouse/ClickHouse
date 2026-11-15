#include <Server/HTTP/checkHTTPHeader.h>

#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_HTTP_HEADERS;
}

void checkHTTPHeader(const HTTPRequest & request, const String & header_name, const String & expected_value)
{
    if (!request.has(header_name))
        throw Exception(ErrorCodes::UNEXPECTED_HTTP_HEADERS, "No HTTP header {}", header_name);
    if (request.get(header_name) != expected_value)
        throw Exception(ErrorCodes::UNEXPECTED_HTTP_HEADERS, "HTTP header {} has unexpected value '{}' (instead of '{}')", header_name, request.get(header_name), expected_value);
}

}
