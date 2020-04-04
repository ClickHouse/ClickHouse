#pragma once

#include <Parsers/IParser.h>

namespace DB
{

/** Parses a name of an object which could be written in 3 forms:
  * name, `name` or 'name' */
bool parseIdentifierOrStringLiteral(IParser::Pos & pos, Expected & expected, String & result);

}
