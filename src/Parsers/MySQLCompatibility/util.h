#pragma once

#include <Parsers/MySQLCompatibility/types.h>

namespace MySQLCompatibility
{
String removeQuotes(const String & quoted);
Poco::Logger * getLogger();
}
