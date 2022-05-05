#pragma once

#include <Parsers/IAST.h>

namespace MySQLCompatibility
{
class Converter
{
public:
    String dumpAST(const String & query) const;
    String dumpTerminals(const String & query) const;
    bool toClickHouseAST(const String & query, DB::ASTPtr & ch_tree, String & error) const;
    static String extractQuery(const char *& pos, const char * end);
};
}
