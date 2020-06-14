#pragma once
#include <Parsers/IParser.h>

namespace DB
{

/// Parses [db].name
bool parseDatabaseAndTableName(IParser::Pos & pos, Expected & expected, String & database_str, String & table_str);

}
