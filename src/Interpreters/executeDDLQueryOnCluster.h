#pragma once

#include <Access/Common/AccessRightsElement.h>
#include <QueryPipeline/BlockIO.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>


namespace zkutil
{
    class ZooKeeper;
}

namespace DB
{

class AccessRightsElements;
struct DDLLogEntry;


/// Returns true if provided ALTER type can be executed ON CLUSTER
bool isSupportedAlterType(int type);

struct DDLQueryOnClusterParams
{
    /// 1-bases index of a shard to execute a query on, 0 means all shards.
    size_t shard_index = 0;

    /// 1-bases index of a replica to execute a query on, 0 means all replicas (see also allow_storing_multiple_replicas).
    size_t replica_index = 0;

    /// Allows executing a query on multiple replicas.
    bool allow_multiple_replicas = true;

    /// Privileges which the current user should have to execute a query.
    AccessRightsElements access_to_check;
};

/// Pushes distributed DDL query to the queue.
/// Returns DDLQueryStatusSource, which reads results of query execution on each host in the cluster.
BlockIO executeDDLQueryOnCluster(const ASTPtr & query_ptr, ContextPtr context, const DDLQueryOnClusterParams & params = {});

BlockIO getDistributedDDLStatus(
    const String & node_path, const DDLLogEntry & entry, ContextPtr context, const std::optional<Strings> & hosts_to_wait = {});

}
