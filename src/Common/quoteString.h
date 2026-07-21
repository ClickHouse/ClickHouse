#pragma once

#include <base/types.h>


namespace DB
{
[[nodiscard]] String quoteString(std::string_view x);

// Prefer string_view over std::string_view for implicit conversions
[[nodiscard]] inline String quoteString(std::same_as<std::string_view> auto x)
{
    return quoteString(std::string_view{x.data, x.size});
}

[[nodiscard]] String quoteStringSingleQuoteWithSingleQuote(std::string_view x);

[[nodiscard]] inline String quoteStringPostgreSQL(std::string_view x)
{
    return quoteStringSingleQuoteWithSingleQuote(x);
}

/// Double quote the string.
String doubleQuoteString(std::string_view x);

/// Double quote the string with standard SQL identifier escaping: an embedded double quote is doubled,
/// a backslash stays literal (the rules of SQLite and PostgreSQL).
String doubleQuoteStringStandard(std::string_view x);

/// Quote the identifier with backquotes.
String backQuote(std::string_view x);

/// Quote the identifier with backquotes, if required.
String backQuoteIfNeed(std::string_view x);

/// Quote the identifier with backquotes, for use in MySQL queries.
String backQuoteMySQL(std::string_view x);

}
