#pragma once

#include <Parsers/IParser.h>

namespace DB
{

/** Parses a name of an object which could be written in 3 forms:
  * name, `name` or 'name' */
bool parseIdentifierOrStringLiteral(IParser::Pos & pos, Expected & expected, String & result);

/** Parse a list of identifiers or string literals. */
bool parseIdentifiersOrStringLiterals(IParser::Pos & pos, Expected & expected, Strings & result);

}
