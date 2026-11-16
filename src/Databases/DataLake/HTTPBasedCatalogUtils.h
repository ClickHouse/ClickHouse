#pragma once

#include <IO/ReadWriteBufferFromHTTP.h>
#include <functional>
#include <Poco/JSON/Parser.h>

namespace DataLake
{

DB::ReadWriteBufferFromHTTPPtr createReadBuffer(
    const std::string & endpoint,
    DB::ContextPtr context,
    const std::string & bearer_token,
    const Poco::URI::QueryParameters & params = {},
    const DB::HTTPHeaderEntries & headers = {},
    const std::string & method = Poco::Net::HTTPRequest::HTTP_GET,
    std::function<void(std::ostream &)> out_stream_callaback = {});

std::pair<Poco::Dynamic::Var, std::string> makeHTTPRequestAndReadJSON(
    const std::string & endpoint,
    DB::ContextPtr context,
    const std::string & bearer_token,
    const Poco::URI::QueryParameters & params = {},
    const DB::HTTPHeaderEntries & headers = {},
    const std::string & method = Poco::Net::HTTPRequest::HTTP_GET,
    std::function<void(std::ostream &)> out_stream_callaback = {});

/// Validate a bearer token as the `Authorization: Bearer <token>` header that
/// `createWithBearerToken` synthesizes, applying the same `http_forbid_headers` and
/// control-character checks as a user-supplied `auth_header`. Throws BAD_ARGUMENTS on a forbidden
/// or malformed token; an empty token is a no-op (no header is sent).
void validateBearerToken(const DB::ContextPtr & context, const std::string & bearer_token);

}
