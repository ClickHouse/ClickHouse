#pragma once

#include <Parsers/IParser.h>


namespace DB
{
class AllowedHosts;


/// Parses a string like
/// name[@host]
bool parseUserName(IParser::Pos & pos, Expected & expected, String & result);
bool parseUserName(IParser::Pos & pos, Expected & expected, String & result, AllowedHosts & allowed_hosts);
bool parseRoleName(IParser::Pos & pos, Expected & expected, String & result);
}
