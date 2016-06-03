#pragma once

#include <DB/Parsers/ASTQueryWithTableAndOutput.h>


namespace DB
{

DEFINE_AST_QUERY_WITH_TABLE_AND_OUTPUT(ASTExistsQuery, "ExistsQuery", "EXISTS TABLE")
DEFINE_AST_QUERY_WITH_TABLE_AND_OUTPUT(ASTShowCreateQuery, "ShowCreateQuery", "SHOW CREATE TABLE")
DEFINE_AST_QUERY_WITH_TABLE_AND_OUTPUT(ASTDescribeQuery, "DescribeQuery", "DESCRIBE TABLE")

}
