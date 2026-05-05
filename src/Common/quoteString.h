#pragma once

#include <base/types.h>
#include <base/StringRef.h>


namespace DB
{
[[nodiscard]] String quoteString(std::string_view x);

// Prefer string_view over StringRef for implicit conversions
[[nodiscard]] inline String quoteString(std::same_as<StringRef> auto x)
{
    return quoteString(std::string_view{x.data, x.size});
}

[[nodiscard]] String quoteStringSingleQuoteWithSingleQuote(std::string_view x);

[[nodiscard]] inline String quoteStringPostgreSQL(std::string_view x)
{
    return quoteStringSingleQuoteWithSingleQuote(x);
}

[[nodiscard]] inline String quoteStringSQLite(std::string_view x)
{
    return quoteStringSingleQuoteWithSingleQuote(x);
}

/// Double quote the string.
String doubleQuoteString(StringRef x);

/// Quote the identifier with backquotes.
String backQuote(StringRef x);

/// Quote the identifier with backquotes, if required.
String backQuoteIfNeed(StringRef x);

/// Quote the identifier with backquotes, for use in MySQL queries.
String backQuoteMySQL(StringRef x);

}
