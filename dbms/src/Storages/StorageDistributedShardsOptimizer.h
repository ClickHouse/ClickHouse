#pragma once

#include <Interpreters/Cluster.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/SelectQueryInfo.h>

#include <boost/noncopyable.hpp>

namespace Poco
{
class Logger;
}

namespace DB
{
class StorageDistributedShardsOptimizer : private boost::noncopyable
{
public:
    StorageDistributedShardsOptimizer();
    ClusterPtr skipUnusedShards(ClusterPtr cluster,
        const SelectQueryInfo & query_info,
        ExpressionActionsPtr sharding_key_expr,
        std::string sharding_key_column_name);
};
}
