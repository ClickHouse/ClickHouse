#pragma once

#include <Core/Types.h>
#include <Parsers/IParser.h>


namespace DB
{

struct Settings;

/** Parses a name of an object which could be written in the following forms:
  * name / `name` / "name" (identifier) or 'name'.
  * Note that empty strings are not allowed.
  */
bool parseIdentifierOrStringLiteral(IParser::Pos & pos, Expected & expected, String & result);

/** Parse a list of identifiers or string literals. */
bool parseIdentifiersOrStringLiterals(IParser::Pos & pos, Expected & expected, Strings & result);

/** Parse a list of identifiers or string literals into vector of strings. */
std::vector<String> parseIdentifiersOrStringLiterals(const String & str, const Settings & settings);

}
