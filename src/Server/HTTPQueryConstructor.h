#pragma once

#include <base/types.h>
#include <vector>


namespace DB
{

class HTMLForm;

/// Information extracted from the request URL path according to
/// `http_allow_database_as_path`, `http_allow_table_as_file`, `http_allow_filters_as_path` settings.
struct HTTPPathInfo
{
    String database;
    String table;
    String format;
    String compression;
    /// Filter expressions parsed from /name=value/ and /name>value/ etc. components.
    /// Each entry is a valid SQL expression already wrapped in parentheses.
    std::vector<String> path_filters;
    /// Filename to use in `Content-Disposition: attachment` header, if any.
    String filename_for_disposition;
};

/// Parse the path (without the query string) into HTTPPathInfo.
/// The result honors the three `allow_*` flags. Behavior:
///   - If allow_table is true, the LAST path component that is not a filter is parsed
///     as `table[.format[.compression]]`.
///   - If allow_database is true, exactly one preceding non-filter component is treated as the database.
///   - If allow_filters is true, components matching `name<op>value` (with op in =, !=, <>, <, >, <=, >=)
///     are parsed as filter expressions.
/// Throws on conflicts or ambiguity (e.g., two database components, or compression without format).
HTTPPathInfo parseHTTPPath(const String & path, bool allow_database, bool allow_table, bool allow_filters);

/// Parse a URL parameter as a filter expression for the `http_allow_filters_as_unrecognized_url_parameters` mode.
/// Returns the constructed SQL filter expression. The value must contain a comparison operator or
/// be the right-hand side of `name = value`. Returns empty string if the input cannot be interpreted.
String parseURLParameterAsFilter(const String & name, const String & value);

/// Build the wrapped SQL query.
/// `base_query` is the inner query expression. If `select_expr`, `filter_expr`, or `order_expr`
/// is non-empty, the query is wrapped as `SELECT [select_expr|*] FROM (base_query) [WHERE filter_expr] [ORDER BY order_expr]`.
/// Note: limit/offset/page are handled by the regular `limit`/`offset` settings.
String wrapHTTPQuery(
    const String & base_query,
    const String & select_expr,
    const String & filter_expr,
    const String & order_expr);

/// Convert a comma-separated list of identifiers with optional +/- prefix into an ORDER BY clause.
/// E.g., "a,-b,+c" -> "a ASC, b DESC, c ASC".
/// Throws on invalid input (e.g., complex expressions). The "+" prefix means ASC; bare identifiers default to ASC.
String convertSortToOrderBy(const String & sort);

/// Returns true if the given format is considered binary based on its registered content type.
/// Used to decide whether to set Content-Disposition: attachment.
bool isBinaryOutputFormat(const String & format_name);

/// Map a known compression extension (gz, br, zstd, xz, lz4, deflate, snappy) to a canonical name.
/// Returns empty if unrecognized.
String canonicalizeCompressionExtension(const String & ext);

}
