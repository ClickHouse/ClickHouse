#pragma once

#include <Parsers/IAST.h>

namespace MySQLCompatibility
{
class Converter
{
public:
	String dumpAST(const String & query);
	DB::ASTPtr toClickHouseAST(const String & query);
};
}
