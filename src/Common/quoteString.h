#pragma once

#include <common/types.h>
#include <common/StringRef.h>
#include <concepts>


namespace DB
{
String quoteString(std::string_view x);

String quoteString(std::same_as<StringRef> auto x) { quoteString(std::string_view{x.data, x.size}); }

/// Double quote the string.
String doubleQuoteString(const StringRef & x);

/// Quote the identifier with backquotes.
String backQuote(const StringRef & x);

/// Quote the identifier with backquotes, if required.
String backQuoteIfNeed(const StringRef & x);
}
