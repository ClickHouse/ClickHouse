#pragma once

#include <Parsers/IAST.h>

namespace MySQLCompatibility
{
class Converter
{
public:
	String dumpAST(const String & query);
	void toClickHouseAST(const String & query, DB::ASTPtr & ch_tree);
};
}
