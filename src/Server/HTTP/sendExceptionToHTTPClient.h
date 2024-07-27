#pragma once

#include <Common/logger_useful.h>
#include <base/types.h>


namespace DB
{
class HTTPServerRequest;
class HTTPServerResponse;
class WriteBufferFromHTTPServerResponse;

/// Sends an exception to HTTP client. This function doesn't handle its own exceptions so it needs to be wrapped in try-catch.
/// Argument `out` may be either created from `response` or be nullptr (if it wasn't created before the exception).
void sendExceptionToHTTPClient(
    const String & exception_message,
    int exception_code,
    HTTPServerRequest & request,
    HTTPServerResponse & response,
    WriteBufferFromHTTPServerResponse * out,
    LoggerPtr log);

/// Sets "X-ClickHouse-Exception-Code" header and the correspondent HTTP status in the response for an exception.
/// This is a part of what sendExceptionToHTTPClient() does.
void setHTTPResponseStatusAndHeadersForException(
    int exception_code, HTTPServerRequest & request, HTTPServerResponse & response, WriteBufferFromHTTPServerResponse * out, LoggerPtr log);
}
