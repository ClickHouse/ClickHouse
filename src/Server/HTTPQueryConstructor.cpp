#include <Server/HTTPQueryConstructor.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/quoteString.h>
#include <Formats/FormatFactory.h>
#include <IO/CompressionMethod.h>
#include <Poco/Exception.h>
#include <Poco/String.h>
#include <Poco/URI.h>

#include <array>
#include <optional>
#include <utility>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_FORMAT;
    extern const int UNKNOWN_TABLE;
}


namespace
{

/// Returns the canonical (registered) format name from a case-insensitive lookup.
String findFormatCaseInsensitive(const String & candidate)
{
    String lower = Poco::toLower(candidate);
    for (const auto & [name, _] : FormatFactory::instance().getAllFormats())
        if (Poco::toLower(name) == lower)
            return name;
    return {};
}

/// Split a *raw* (percent-encoded) path on '/' producing non-empty components, percent-decoding each
/// component only after the split. Decoding after splitting keeps an encoded slash (`%2F`) as data inside
/// a single component (e.g. a filter value like `a=foo%2Fbar`, or a back-quoted name `` `a%2Fb` ``),
/// instead of turning it into a component boundary.
Strings splitPathComponents(const String & path)
{
    Strings result;
    String current;
    auto flush = [&]()
    {
        if (!current.empty())
        {
            String decoded;
            try
            {
                Poco::URI::decode(current, decoded);
            }
            catch (const Poco::Exception &)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Malformed percent-encoded component in HTTP URL path");
            }
            result.push_back(decoded);
            current.clear();
        }
    };
    for (char c : path)
    {
        if (c == '/')
            flush();
        else
            current += c;
    }
    flush();
    return result;
}

/// If the component contains one of the supported comparison operators, return the parsed filter
/// as a SQL expression (with the identifier quoted). Returns empty string if not a filter.
/// Operators in order of attempt (longer first): `>=`, `<=`, `!=`, `<>`, `>`, `<`, `=`.
String tryParseFilterComponent(const String & component)
{
    static constexpr std::array<const char *, 7> ops = {">=", "<=", "!=", "<>", ">", "<", "="};
    for (const char * op : ops)
    {
        auto pos = component.find(op);
        if (pos == String::npos)
            continue;
        /// Don't match if op starts at 0 (no name) or extends to end (no value).
        size_t op_len = strlen(op);
        if (pos == 0 || pos + op_len >= component.size())
            continue;

        String name = component.substr(0, pos);
        String value = component.substr(pos + op_len);
        String sql_op(op);
        /// Translate `<>` to `!=` for consistency.
        if (sql_op == "<>")
            sql_op = "!=";

        return "(" + backQuoteIfNeed(name) + " " + sql_op + " " + quoteString(value) + ")";
    }
    return {};
}

/// A fully back-quoted component (`` `name` ``) is an explicit identifier (a database/table name), so it
/// must be treated as a name — never a filter — even when it contains characters that look like a filter
/// operator, e.g. `` `a=1` `` or `` `a>1` ``. Otherwise such a table name is misparsed as a filter.
bool isFullyBackQuotedComponent(const String & component)
{
    return component.size() >= 2 && component.front() == '`' && component.back() == '`';
}

/// If `component` is fully back-quoted (`` `name` ``), return the identifier with the surrounding
/// back-quotes removed and doubled back-quotes unescaped (`` `` `` -> `` ` ``), as in SQL identifier
/// quoting. Otherwise return it unchanged.
String unquoteBackQuotedComponent(const String & component)
{
    if (!isFullyBackQuotedComponent(component))
        return component;
    const String inner = component.substr(1, component.size() - 2);
    String unquoted;
    unquoted.reserve(inner.size());
    for (size_t i = 0; i < inner.size(); ++i)
    {
        if (inner[i] == '`' && i + 1 < inner.size() && inner[i + 1] == '`')
        {
            unquoted += '`';
            ++i;
        }
        else
            unquoted += inner[i];
    }
    return unquoted;
}

std::optional<size_t> findBackQuotedComponentEnd(const String & component)
{
    if (component.size() < 2 || component.front() != '`')
        return {};

    for (size_t i = 1; i < component.size(); ++i)
    {
        if (component[i] != '`')
            continue;

        if (i + 1 < component.size() && component[i + 1] == '`')
        {
            ++i;
            continue;
        }

        return i;
    }

    return {};
}

std::optional<size_t> findBackQuotedComponentEndWithSuffix(const String & component)
{
    const auto closing_backquote = findBackQuotedComponentEnd(component);
    if (!closing_backquote || *closing_backquote + 2 >= component.size() || component[*closing_backquote + 1] != '.')
        return {};

    return closing_backquote;
}

struct BackQuotedComponentWithSuffix
{
    size_t closing_backquote;
    String format;
    String compression;
};

/// Recognize a quoted identifier followed by a format and optional compression suffix, for example
/// `` `a=1`.CSV `` or `` `a=1`.CSV.gz ``. This must happen before filter parsing because the quoted table
/// identifier itself may contain comparison operators. Once the quoted-identifier-plus-suffix shape is
/// present, an unknown suffix is rejected instead of being treated as part of the table name.
std::optional<BackQuotedComponentWithSuffix> tryParseBackQuotedComponentWithSuffix(const String & component)
{
    const auto closing_backquote = findBackQuotedComponentEndWithSuffix(component);
    if (!closing_backquote)
        return {};

    const String suffix = component.substr(*closing_backquote + 2);

    String format_candidate = suffix;
    String compression;
    const auto last_dot = suffix.rfind('.');
    if (last_dot != String::npos)
    {
        compression = canonicalizeCompressionExtension(suffix.substr(last_dot + 1));
        if (compression.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Unknown compression extension '{}' in URL path.", suffix.substr(last_dot + 1));
        format_candidate = suffix.substr(0, last_dot);
    }

    String format = findFormatCaseInsensitive(format_candidate);
    if (format.empty())
        throw Exception(ErrorCodes::UNKNOWN_FORMAT, "Unknown format '{}' in URL path.", format_candidate);

    return BackQuotedComponentWithSuffix{*closing_backquote, std::move(format), std::move(compression)};
}

bool isLiteralTableComponent(const String & component)
{
    /// Keep any syntactically valid backquoted identifier with a suffix out of filter parsing. Format and
    /// compression validation happens later when the table component is converted into a query.
    return isFullyBackQuotedComponent(component) || findBackQuotedComponentEndWithSuffix(component).has_value();
}

}


HTTPPathInfo parseHTTPPath(
    const String & path,
    bool allow_database,
    bool allow_table,
    bool allow_filters,
    bool allow_table_after_database)
{
    HTTPPathInfo result;
    if (path.empty() || path == "/")
        return result;

    Strings components = splitPathComponents(path);
    if (components.empty())
        return result;

    /// Walk components in order. Last non-filter component (if any) may be the table.
    /// Preceding non-filter components include at most one database.
    /// Filter components can be intermixed.

    int last_non_filter_index = -1;
    /// First pass: identify filters and non-filter components.
    std::vector<int> non_filter_indices;
    std::vector<String> per_component_filter; // for each index, parsed filter or empty
    per_component_filter.resize(components.size());

    for (size_t i = 0; i < components.size(); ++i)
    {
        String filter_expr;
        if (allow_filters && !isLiteralTableComponent(components[i]))
            filter_expr = tryParseFilterComponent(components[i]);

        if (!filter_expr.empty())
        {
            per_component_filter[i] = filter_expr;
        }
        else
        {
            non_filter_indices.push_back(static_cast<int>(i));
            last_non_filter_index = static_cast<int>(i);
        }
    }

    /// Determine table component
    int table_index = -1;
    if (allow_table && last_non_filter_index >= 0)
    {
        table_index = last_non_filter_index;
    }
    else if (allow_table_after_database && allow_database && non_filter_indices.size() >= 2)
    {
        /// PUT path uploads may opt into the final table component only after a database component.
        /// Keep the ordinary table-as-file setting scoped to unqualified paths.
        table_index = last_non_filter_index;
    }

    /// Determine database component (everything else before the table among non-filter indices)
    std::vector<int> db_indices;
    for (int idx : non_filter_indices)
        if (idx != table_index)
            db_indices.push_back(idx);

    if (!allow_database && !db_indices.empty())
    {
        /// Non-filter components other than the table cannot be claimed when `http_allow_database_as_path`
        /// is off — leave them unclaimed and return an empty result. The path is effectively ignored
        /// and the request proceeds as if it had hit the root URL.
        return {};
    }
    if (db_indices.size() > 1)
    {
        /// A path this deep names no resource: the path form is `/database/table[.format[.compression]]`,
        /// so there is nothing for a third component to be. Report it as "not found" rather than as a
        /// malformed request, so that enabling `http_allow_path_requests` does not turn the plain 404 an
        /// unmatched URL used to get into a 400 (`UNKNOWN_TABLE` maps to HTTP 404, `BAD_ARGUMENTS` to 400).
        throw Exception(ErrorCodes::UNKNOWN_TABLE,
            "There is no table at the HTTP URL path: it has more than one database component "
            "('{}' and '{}'). The path form is /database/table[.format[.compression]].",
            components[db_indices[0]], components[db_indices[1]]);
    }
    /// Special case: if there is exactly one non-filter component and allow_database is on
    /// but allow_table is off, that single component is the database (not the table).
    if (!allow_table && allow_database && non_filter_indices.size() == 1)
    {
        result.database = unquoteBackQuotedComponent(components[non_filter_indices[0]]);
    }
    else
    {
        if (!db_indices.empty())
            result.database = unquoteBackQuotedComponent(components[db_indices[0]]);
        if (table_index >= 0)
        {
            /// Parse table[.format[.compression]] from the last component.
            const String & raw = components[table_index];

            /// A fully back-quoted component with no suffix is a *literal* table name: its dots are part of the
            /// name and no format/compression suffix is stripped from it. This mirrors SQL identifier quoting
            /// (where `db.table` is always `database.identifier`) and is the escape hatch for a table whose name
            /// ends in (or contains) a registered format or compression token. A known format suffix outside the
            /// closing backquote is also supported, so `` /db/`a=1`.CSV `` means table `a=1` with format `CSV`.
            /// For example `` /db/`events.JSON` `` reads the table named `events.JSON`, whereas the unquoted
            /// `/db/events.JSON` reads table `events` with format `JSON`. When the name is fully back-quoted,
            /// specify the format/compression via the `format` / `compression` URL parameters (or the `format`
            /// setting) when no path suffix is present.
            /// A backtick travels in a URL percent-encoded as `%60`; `parseHTTPPath` splits the raw path and
            /// percent-decodes each component afterwards, so `/db/%60events.JSON%60` arrives here as
            /// `` `events.JSON` ``.
            if (isFullyBackQuotedComponent(raw))
            {
                const String unquoted = unquoteBackQuotedComponent(raw);
                result.table = unquoted;
                result.format = {};
                result.compression = {};
                result.filename_for_disposition = unquoted;
            }
            else if (const auto quoted_with_suffix = tryParseBackQuotedComponentWithSuffix(raw))
            {
                result.table = unquoteBackQuotedComponent(raw.substr(0, quoted_with_suffix->closing_backquote + 1));
                result.format = quoted_with_suffix->format;
                result.compression = quoted_with_suffix->compression;
                result.filename_for_disposition = result.table + "." + result.format;
                if (!result.compression.empty())
                    result.filename_for_disposition += "." + result.compression;
            }
            else
            {
                /// Try splitting from the right.
                String table_part = raw;
                String format_part;
                String compression_part;
                String disposition_filename = raw;

                auto last_dot = table_part.rfind('.');
                if (last_dot != String::npos)
                {
                    String maybe_extension = table_part.substr(last_dot + 1);
                    String maybe_compression_name = canonicalizeCompressionExtension(maybe_extension);
                    if (!maybe_compression_name.empty())
                    {
                        compression_part = maybe_compression_name;
                        /// Canonicalize the compression extension in the disposition filename so an accepted
                        /// alias (`.zstd` / `.gzip` / `.lzma` / `.bzip2`) is not duplicated when `HTTPHandler`
                        /// appends the canonical suffix (`.zst` / `.gz` / `.xz` / `.bz2`). For example
                        /// `/db/hits.Native.zstd` yields the filename `hits.Native.zst`, not `hits.Native.zstd.zst`.
                        if (maybe_compression_name != maybe_extension)
                            disposition_filename = raw.substr(0, last_dot + 1) + maybe_compression_name;
                        table_part = table_part.substr(0, last_dot);
                        last_dot = table_part.rfind('.');
                        if (last_dot != String::npos)
                        {
                            String fmt_candidate = table_part.substr(last_dot + 1);
                            String canonical_format = findFormatCaseInsensitive(fmt_candidate);
                            if (canonical_format.empty())
                            {
                                throw Exception(ErrorCodes::UNKNOWN_FORMAT,
                                    "Unknown format '{}' in URL path. Compression cannot be specified without a known format.", fmt_candidate);
                            }
                            format_part = canonical_format;
                            table_part = table_part.substr(0, last_dot);
                        }
                        else
                        {
                            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Compression extension '{}' specified without a format in URL path.", compression_part);
                        }
                    }
                    else
                    {
                        /// Maybe just a format extension (no compression).
                        String canonical_format = findFormatCaseInsensitive(maybe_extension);
                        if (!canonical_format.empty())
                        {
                            format_part = canonical_format;
                            table_part = table_part.substr(0, last_dot);
                        }
                        /// Otherwise leave it as part of the table name.
                    }
                }

                result.table = table_part;
                result.format = format_part;
                result.compression = compression_part;
                result.filename_for_disposition = disposition_filename;
            }
        }
    }

    /// Collect filters in their path order.
    for (size_t i = 0; i < components.size(); ++i)
        if (!per_component_filter[i].empty())
            result.path_filters.push_back(per_component_filter[i]);

    return result;
}


bool hasHTTPPathFormatSuffix(const String & path)
{
    String path_only = path;
    if (const auto query_start = path_only.find('?'); query_start != String::npos)
        path_only.resize(query_start);

    /// Validate every path component before looking only at the final component. The routing filter runs
    /// before the catch-all handler, so accepting a path with a malformed database component would route it
    /// into parseHTTPPath and turn a client-side URL error into an internal server error.
    try
    {
        String decoded_path;
        Poco::URI::decode(path_only, decoded_path);
    }
    catch (const Poco::Exception &)
    {
        return false;
    }

    const auto last_slash = path_only.rfind('/');
    const size_t component_start = last_slash == String::npos ? 0 : last_slash + 1;
    if (component_start >= path_only.size())
        return false;

    String decoded_component;
    try
    {
        Poco::URI::decode(path_only.substr(component_start), decoded_component);
    }
    catch (const Poco::Exception &)
    {
        /// A malformed percent-encoded path cannot be a valid path upload. Do not let routing turn it into a
        /// server error, especially when this helper is used before the request reaches the path handler.
        return false;
    }

    /// Dots inside a quoted identifier are part of the table name, not a format suffix. A dot after the
    /// closing quote is a suffix even when the format is unknown, so the request reaches the path handler
    /// and receives its normal unknown-format error.
    if (const auto closing_backquote = findBackQuotedComponentEnd(decoded_component))
    {
        if (*closing_backquote + 1 >= decoded_component.size() || decoded_component[*closing_backquote + 1] != '.')
            return false;
        return *closing_backquote + 2 < decoded_component.size();
    }

    const auto last_dot = decoded_component.rfind('.');
    return last_dot != String::npos && last_dot + 1 < decoded_component.size();
}


String parseURLParameterAsFilter(const String & name, const String & value)
{
    if (name.empty())
        return {};

    /// Case 1: HTMLForm splits a URL parameter on the first `=`. For two-character operators
    /// that end in `=` (`!=`, `>=`, `<=`), the operator's `=` ends up as that separator, leaving
    /// the leading character of the operator stuck to the end of the name and the literal in the value.
    /// Examples:
    ///   `?a!=2` -> name `a!` and value `2` -> `a != 2`
    ///   `?a>=2` -> name `a>` and value `2` -> `a >= 2`
    ///   `?a<=2` -> name `a<` and value `2` -> `a <= 2`
    if (name.size() > 1
        && (name.back() == '!' || name.back() == '>' || name.back() == '<'))
    {
        char op_char = name.back();
        String identifier = name.substr(0, name.size() - 1);
        if (!identifier.empty())
        {
            String op;
            if (op_char == '!')
                op = "!=";
            else if (op_char == '>')
                op = ">=";
            else /* '<' */
                op = "<=";
            return "(" + backQuoteIfNeed(identifier) + " " + op + " " + quoteString(value) + ")";
        }
    }

    /// Case 2: The full operator survived inside `name` because the URL had no `=` to split on
    /// (e.g. `?a>2`, `?a<>2`, `?f(x)>3`). Treat the reassembled `name[=value]` as a SQL expression.
    static constexpr std::array<const char *, 6> compare_ops = {">=", "<=", "!=", "<>", ">", "<"};
    auto has_compare_op = [&](const String & s)
    {
        for (const char * op : compare_ops)
            if (s.contains(op))
                return true;
        return false;
    };

    if (has_compare_op(name))
    {
        String full = value.empty() ? name : name + "=" + value;
        return "(" + full + ")";
    }

    /// Case 3: Plain `name=value` -> `name = value` with quoted literal.
    return "(" + backQuoteIfNeed(name) + " = " + quoteString(value) + ")";
}


bool isBinaryOutputFormat(const String & format_name)
{
    if (format_name.empty())
        return false;
    try
    {
        String content_type = FormatFactory::instance().getContentType(format_name, {});
        /// Common binary content types.
        if (startsWith(content_type, "application/octet-stream"))
            return true;
        if (startsWith(content_type, "application/x-parquet"))
            return true;
        /// Heuristic: any content type that starts with "application/" but isn't json/xml/x-www-form is binary-ish.
        if (startsWith(content_type, "application/"))
        {
            if (content_type.contains("json"))
                return false;
            if (content_type.contains("xml"))
                return false;
            return true;
        }
        return false;
    }
    catch (...) /// Ok: unknown / malformed format name — fall back to "not binary".
    {
        return false;
    }
}


String canonicalizeCompressionExtension(const String & ext)
{
    String lower = Poco::toLower(ext);
    /// Supported compression methods recognized by `wrapWriteBufferWithCompressionMethod`.
    /// Map common file extensions to the canonical name expected by `chooseCompressionMethod`.
    if (lower == "gz" || lower == "gzip")
        return "gz";
    if (lower == "br")
        return "br";
    if (lower == "zst" || lower == "zstd")
        return "zst";
    if (lower == "xz" || lower == "lzma")
        return "xz";
    if (lower == "lz4")
        return "lz4";
    if (lower == "bz2" || lower == "bzip2")
        return "bz2";
    if (lower == "deflate")
        return "deflate";
    if (lower == "snappy")
        return "snappy";
    return {};
}

}
