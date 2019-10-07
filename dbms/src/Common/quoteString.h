#pragma once

#include <Core/Types.h>


namespace DB
{
/// Quote the string.
String quoteString(const String & x);

/// Quote the identifier with backquotes, if required.
String backQuoteIfNeed(const String & x);

/// Quote the identifier with backquotes.
String backQuote(const String & x);
}
