#pragma once

#include <Parsers/IAST.h>

namespace DB
{

/// Add structure argument for queries with s3Cluster/hdfsCluster table function.
void addColumnsStructureToQueryWithClusterEngine(ASTPtr & query, const String & structure, size_t max_arguments, const String & function_name);

}
