#include <Server/HTTPQueryConstructor.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/quoteString.h>
#include <Formats/FormatFactory.h>
#include <IO/CompressionMethod.h>
#include <Poco/String.h>

#include <array>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_FORMAT;
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

/// Split a path on '/' producing non-empty components.
Strings splitPathComponents(const String & path)
{
    Strings result;
    String current;
    for (char c : path)
    {
        if (c == '/')
        {
            if (!current.empty())
            {
                result.push_back(current);
                current.clear();
            }
        }
        else
        {
            current += c;
        }
    }
    if (!current.empty())
        result.push_back(current);
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

}


HTTPPathInfo parseHTTPPath(const String & path, bool allow_database, bool allow_table, bool allow_filters)
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
        if (allow_filters)
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
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Multiple database components in HTTP URL path: '{}' and '{}'. At most one is allowed.",
            components[db_indices[0]], components[db_indices[1]]);
    }
    /// Special case: if there is exactly one non-filter component and allow_database is on
    /// but allow_table is off, that single component is the database (not the table).
    if (!allow_table && allow_database && non_filter_indices.size() == 1)
    {
        result.database = components[non_filter_indices[0]];
    }
    else
    {
        if (!db_indices.empty())
            result.database = components[db_indices[0]];
        if (table_index >= 0)
        {
            /// Parse table[.format[.compression]] from the last component.
            const String & raw = components[table_index];

            /// Try splitting from the right.
            String table_part = raw;
            String format_part;
            String compression_part;

            auto last_dot = table_part.rfind('.');
            if (last_dot != String::npos)
            {
                String maybe_extension = table_part.substr(last_dot + 1);
                String maybe_compression_name = canonicalizeCompressionExtension(maybe_extension);
                if (!maybe_compression_name.empty())
                {
                    compression_part = maybe_compression_name;
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
            result.filename_for_disposition = raw;
        }
    }

    /// Collect filters in their path order.
    for (size_t i = 0; i < components.size(); ++i)
        if (!per_component_filter[i].empty())
            result.path_filters.push_back(per_component_filter[i]);

    return result;
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
            if (s.find(op) != String::npos)
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


String wrapHTTPQuery(
    const String & base_query,
    const String & select_expr,
    const String & filter_expr,
    const String & order_expr)
{
    if (select_expr.empty() && filter_expr.empty() && order_expr.empty())
        return base_query;

    String result = "SELECT ";
    result += select_expr.empty() ? "*" : select_expr;
    result += " FROM (";
    result += base_query;
    result += ")";
    if (!filter_expr.empty())
    {
        result += " WHERE ";
        result += filter_expr;
    }
    if (!order_expr.empty())
    {
        result += " ORDER BY ";
        result += order_expr;
    }
    return result;
}


String convertSortToOrderBy(const String & sort)
{
    String result;
    String current;
    auto flush_one = [&](const String & item)
    {
        if (item.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty identifier in 'sort' setting");

        String direction = " ASC";
        String name = item;
        if (item[0] == '-')
        {
            direction = " DESC";
            name = item.substr(1);
        }
        else if (item[0] == '+')
        {
            direction = " ASC";
            name = item.substr(1);
        }
        /// Validate that name is a simple identifier (no spaces or operators).
        if (name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty identifier in 'sort' setting");
        for (char c : name)
        {
            if (!isAlphaNumericASCII(c) && c != '_')
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Invalid character '{}' in identifier '{}' in 'sort' setting. Use 'order' for complex expressions.", c, name);
        }
        if (!result.empty())
            result += ", ";
        result += backQuoteIfNeed(name);
        result += direction;
    };

    for (char c : sort)
    {
        if (c == ',')
        {
            String trimmed = current;
            while (!trimmed.empty() && (trimmed.front() == ' ' || trimmed.front() == '\t'))
                trimmed.erase(0, 1);
            while (!trimmed.empty() && (trimmed.back() == ' ' || trimmed.back() == '\t'))
                trimmed.pop_back();
            if (!trimmed.empty())
                flush_one(trimmed);
            current.clear();
        }
        else
        {
            current += c;
        }
    }
    {
        String trimmed = current;
        while (!trimmed.empty() && (trimmed.front() == ' ' || trimmed.front() == '\t'))
            trimmed.erase(0, 1);
        while (!trimmed.empty() && (trimmed.back() == ' ' || trimmed.back() == '\t'))
            trimmed.pop_back();
        if (!trimmed.empty())
            flush_one(trimmed);
    }
    return result;
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
            if (content_type.find("json") != String::npos)
                return false;
            if (content_type.find("xml") != String::npos)
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
