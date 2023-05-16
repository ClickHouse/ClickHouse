#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTExpressionList.h>

namespace DB
{

ASTExpressionList * extractTableFunctionArgumentsFromSelectQuery(ASTPtr & query);

/// Add structure argument for queries with s3Cluster/hdfsCluster table function.
void addColumnsStructureToQueryWithClusterEngine(ASTPtr & query, const String & structure, size_t max_arguments, const String & function_name);

}
