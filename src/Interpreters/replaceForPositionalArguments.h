#pragma once
#include <Parsers/ASTSelectQuery.h>

namespace DB
{

bool replaceForPositionalArguments(ASTPtr & argument, const ASTSelectQuery * select_query, ASTSelectQuery::Expression expression);

}
