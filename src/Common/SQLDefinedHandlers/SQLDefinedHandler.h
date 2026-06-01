#pragma once

#include <base/types.h>
#include <Common/re2.h>

#include <map>
#include <memory>
#include <optional>
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
    /// If not set, the handler is active for all of http/https protocols.
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

    /// The canonical CREATE HANDLER statement, used for persistence and introspection.
    String create_statement;

    /// Returns true if the request path (without query string and fragment) matches this handler's URL.
    bool matchesURL(const String & path) const;

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
