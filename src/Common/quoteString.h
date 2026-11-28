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

/// Quote a string for embedding in a query sent to a PostgreSQL server: the value is emitted so
/// that PostgreSQL reads back exactly these bytes on every server configuration
/// (`writeQuotedStringPostgreSQLLossless`). Prefer this over `quoteString` for PostgreSQL, whose
/// backslash escaping does not escape the quote under the default `standard_conforming_strings`,
/// so an embedded `'` still terminates the literal.
[[nodiscard]] String quoteStringPostgreSQL(std::string_view x);

/// Double quote the string.
String doubleQuoteString(std::string_view x);

/// Quote an identifier for a query sent to a PostgreSQL server. Prefer this over `doubleQuoteString`,
/// whose `\"` does not escape the quote there: the identifier ends and the rest is parsed as SQL.
String doubleQuoteStringPostgreSQL(std::string_view x);

String doubleQuoteStringSQLite(std::string_view x);

/// Quote the identifier with backquotes.
String backQuote(std::string_view x);

/// Quote the identifier with backquotes, if required.
String backQuoteIfNeed(std::string_view x);

/// Quote the identifier with backquotes, unless it is one bare word: a letter or an underscore,
/// then letters, digits, and underscores. Unlike `backQuoteIfNeed`, it does not quote keywords.
/// Use it only where the parser accepts any bare word as an identifier, mostly to keep old
/// formatting unchanged. Shouldn't be used for new syntax.
String backQuoteIfNotBareWord(std::string_view x);

/// Quote the identifier with backquotes, for use in MySQL queries.
String backQuoteMySQL(std::string_view x);

/// Quote the identifier with backquotes, for use in SQLite queries.
String backQuoteSQLite(std::string_view x);
}
