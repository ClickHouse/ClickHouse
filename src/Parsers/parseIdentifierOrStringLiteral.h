#pragma once

#include <Core/Names.h>
#include <Core/Types.h>
#include <Parsers/IParser.h>

#include <string_view>


namespace DB
{

/** Parses a name of an object which could be written in the following forms:
  * name / `name` / "name" (identifier) or 'name'.
  * Note that empty strings are not allowed.
  */
bool parseIdentifierOrStringLiteral(IParser::Pos & pos, Expected & expected, String & result);

/// Parse a list of identifiers or string literals.
bool parseIdentifiersOrStringLiterals(IParser::Pos & pos, Expected & expected, Strings & result);

/// The overloads that parse a whole string, taking the parser limits from the settings, live in
/// `Interpreters/parseIdentifiersOrStringLiteralsWithSettings.h` - the parser does not depend on
/// the settings schema.

/** Parses the whole string as a comma-separated list of column names, such as: a, n.x, `c,ol`, 'd'.
  * A name is an identifier, a sequence of identifiers joined by dots (column `x` of a Nested structure
  * `n` is named `n.x`) or a non-empty string literal. An empty or whitespace-only string gives an empty
  * set. Unlike the functions above, it throws CANNOT_PARSE_TEXT if the string is not such a list, so
  * that `n.x` or `a b` are not silently read as `n` or `a`. It uses the default parser limits, so that
  * the result does not depend on the query settings.
  */
NameSet parseColumnNameList(std::string_view str);

}
