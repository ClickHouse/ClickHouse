#pragma once

#include <base/types.h>
#include <Core/Names.h>
#include <Common/re2.h>

#include <map>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>


namespace DB
{

/// A description of an HTTP handler defined from SQL (CREATE HANDLER ...).
/// It is an immutable value, ready to be matched against incoming HTTP requests.
struct SQLDefinedHandler
{
    enum class URLMatchType
    {
        Exact,
        Prefix,
        Regexp,
    };

    String name;

    /// If set, the handler is active only for the composable protocol with this name.
    /// If not set, the handler is active on all HTTP endpoints: the built-in http/https
    /// ports and every HTTP-type composable protocol listener.
    std::optional<String> protocol;

    URLMatchType url_match_type = URLMatchType::Exact;
    String url;
    /// Compiled regular expression, set only when url_match_type == Regexp.
    std::shared_ptr<const re2::RE2> url_regex;

    /// Allowed HTTP methods (uppercase), never empty. Defaults to {"GET"}.
    std::vector<String> methods;

    /// The handler type. Only "query" is supported for now.
    String type = "query";

    /// The SQL query to be executed by the handler (the part after AS).
    String query;

    /// Query parameters expected by `query` (URL params, form variables, headers, regexp capture groups),
    /// precomputed at build time so the request path does not re-parse the query on every matching request.
    NameSet receive_params;

    /// The canonical CREATE HANDLER statement, used for persistence and introspection.
    String create_statement;

    /// Whether a request to this handler can consume the HTTP request body: an `INSERT` takes the body as its
    /// data, and a query using the `_request_body` parameter reads it explicitly. Only for such handlers must a
    /// non-chunked request declare `Content-Length` up front, otherwise the body would be read until EOF and a
    /// dropped connection would be accepted as a complete request (see `HTTPHandler::handleRequest`). A handler
    /// that never looks at the body must not impose that requirement on ordinary clients.
    bool consumes_request_body = false;

    /// Returns true if the request path (without query string and fragment) matches this handler's URL.
    bool matchesURL(const String & path) const;

    /// Whether `path` matches the base path `prefix` on a path-segment boundary: the base path itself or
    /// anything below it after a '/'. E.g. "/api/v1" matches "/api/v1", "/api/v1/" and "/api/v1/write",
    /// but not "/api/v1beta". A trailing '/' in `prefix` is ignored, so "/api/v1/" and "/api/v1" behave
    /// the same. These are the semantics of the configuration-defined `url_prefix` rule
    /// (see `HTTPHandlerRequestFilter`), which `URL PREFIX` handlers mirror.
    static bool urlPrefixMatches(std::string_view prefix, std::string_view path);

    /// Returns true if the given HTTP method (uppercase) is allowed by this handler.
    bool matchesMethod(const String & method) const;

    /// Returns true if the handler is active for the given protocol name.
    /// Empty protocol_name means a legacy http/https port (matches only protocol-less handlers).
    bool matchesProtocol(const String & protocol_name) const;
};

using SQLDefinedHandlerPtr = std::shared_ptr<const SQLDefinedHandler>;

/// Ordered by name: this defines the matching priority of SQL-defined handlers (lexicographic by name).
using SQLDefinedHandlers = std::map<String, SQLDefinedHandlerPtr>;
using SQLDefinedHandlersPtr = std::shared_ptr<const SQLDefinedHandlers>;

}
