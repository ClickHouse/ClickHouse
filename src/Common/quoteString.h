#pragma once

#include <base/types.h>
#include <base/StringRef.h>
#include <concepts>


namespace DB
{
[[nodiscard]] String quoteString(std::string_view x);

// Prefer string_view over StringRef for implicit conversions
[[nodiscard]] inline String quoteString(std::same_as<StringRef> auto x)
{
    return quoteString(std::string_view{x.data, x.size});
}

/// Double quote the string.
String doubleQuoteString(StringRef x);

/// Quote the identifier with backquotes.
String backQuote(StringRef x);

/// Quote the identifier with backquotes, if required.
String backQuoteIfNeed(StringRef x);

}
