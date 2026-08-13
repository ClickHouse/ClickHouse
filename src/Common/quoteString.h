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

/// Quote a string for embedding in a query sent to a PostgreSQL server. Emits an `E'...'`
/// escape-string constant (doubling both the quote and the backslash), which is safe regardless of
/// the remote server's `standard_conforming_strings` — unlike a plain `'...'` literal.
[[nodiscard]] String quoteStringPostgreSQL(std::string_view x);

[[nodiscard]] inline String quoteStringSQLite(std::string_view x)
{
    return quoteStringSingleQuoteWithSingleQuote(x);
}

/// Double quote the string.
String doubleQuoteString(std::string_view x);

/// Quote the identifier with backquotes.
String backQuote(std::string_view x);

/// Quote the identifier with backquotes, if required.
String backQuoteIfNeed(std::string_view x);

/// Quote the identifier with backquotes, for use in MySQL queries.
String backQuoteMySQL(std::string_view x);

}
