#pragma once

#include <Core/Types.h>

#include <vector>

namespace DB
{

/// Parsed body of a SQL `CREATE SHARD` row in the shard catalog (memory image after load / before persist).
struct ShardCatalogDefinition
{
    std::vector<String> replica_collections;
    UInt32 weight = 1;
    bool internal_replication = false;
};

/// Parsed body of a SQL `CREATE CLUSTER` row in the cluster catalog.
struct ClusterCatalogDefinition
{
    std::vector<String> members;
};

}
