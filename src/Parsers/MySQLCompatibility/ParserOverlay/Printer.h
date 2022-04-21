#pragma once

#include <string>

namespace MySQLParserOverlay
{
class ParseTreePrinter
{
public:
	std::string Print(const std::string & query) const;
};
}
