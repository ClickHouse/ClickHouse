#pragma once

#include <Parsers/IAST.h>

namespace DB
{

/// Add structure argument for queries with urlCluster/hdfsCluster/s3CLuster table function.
void addColumnsStructureToQueryWithHDFSClusterEngine(ASTPtr & query, const String & structure);
void addColumnsStructureToQueryWithURLClusterEngine(ASTPtr & query, const String & structure);
void addColumnsStructureToQueryWithS3ClusterEngine(ASTPtr & query, const String & structure);

}
