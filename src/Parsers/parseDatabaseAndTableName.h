#pragma once
#include <Parsers/IParser.h>

namespace DB
{

/// Parses [db].name
bool parseDatabaseAndTableName(IParser::Pos & pos, Expected & expected, IParser::Ranges * ranges, String & database_str, String & table_str);

}
