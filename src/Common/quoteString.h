#pragma once

#include <common/types.h>
#include <common/StringRef.h>


namespace DB
{
/// Quote the string.
String quoteString(const StringRef & x);

/// Double quote the string.
String doubleQuoteString(const StringRef & x);

/// Quote the identifier with backquotes.
String backQuote(const StringRef & x);

/// Quote the identifier with backquotes, if required.
String backQuoteIfNeed(const StringRef & x);
}
