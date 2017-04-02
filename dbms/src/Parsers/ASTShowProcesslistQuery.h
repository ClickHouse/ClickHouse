#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{

DEFINE_AST_QUERY_WITH_OUTPUT(ASTShowProcesslistQuery, "ShowProcesslistQuery", "SHOW PROCESSLIST")

}
