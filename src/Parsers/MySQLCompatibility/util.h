#pragma once

#include <Parsers/MySQLCompatibility/types.h>

namespace MySQLCompatibility
{
String removeQuotes(const String & quoted);
Poco::Logger * getLogger();

bool tryExtractIdentifier(MySQLPtr node, String & value);
bool tryExtractTableName(MySQLPtr node, String & table_name, String & db_name);
}
