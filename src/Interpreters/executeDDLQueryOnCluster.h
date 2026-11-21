#pragma once

#include <Access/Common/AccessRightsElement.h>
#include <QueryPipeline/BlockIO.h>
#include <Processors/ISource.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Common/ZooKeeper/ZooKeeperRetries.h>


namespace zkutil
{
    class ZooKeeper;
}

namespace DB
{

struct DDLLogEntry;
class Cluster;
using ClusterPtr = std::shared_ptr<Cluster>;

/// Returns true if provided ALTER type can be executed ON CLUSTER
bool isSupportedAlterTypeForOnClusterDDLQuery(int type);

struct DDLQueryOnClusterParams
{
    /// A cluster to execute a distributed query.
    /// If not set, executeDDLQueryOnCluster() will use `query->cluster` to determine a cluster to execute the query.
    ClusterPtr cluster;

    /// 1-bases index of a shard to execute a query on, 0 means all shards.
    size_t only_shard_num = 0;

    /// 1-bases index of a replica to execute a query on, 0 means all replicas.
    size_t only_replica_num = 0;

    /// Privileges which the current user should have to execute a query.
    AccessRightsElements access_to_check;

    /// Use retries when creating nodes "query-0000000000", "query-0000000001", "query-0000000002" in ZooKeeper.
    ZooKeeperRetriesInfo retries_info;
};

/// Pushes distributed DDL query to the queue.
/// Returns DDLQueryStatusSource, which reads results of query execution on each host in the cluster.
BlockIO executeDDLQueryOnCluster(const ASTPtr & query_ptr, ContextPtr context, const DDLQueryOnClusterParams & params = {});

BlockIO getDDLOnClusterStatus(const String & node_path, const String & replicas_path, const DDLLogEntry & entry, ContextPtr context);

bool maybeRemoveOnCluster(const ASTPtr & query_ptr, ContextPtr context);

}
