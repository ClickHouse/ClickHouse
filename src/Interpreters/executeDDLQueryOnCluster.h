#pragma once

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

/// Pushes distributed DDL query to the queue.
/// Returns DDLQueryStatusSource, which reads results of query execution on each host in the cluster.
BlockIO executeDDLQueryOnCluster(const ASTPtr & query_ptr, ContextPtr context);
BlockIO executeDDLQueryOnCluster(const ASTPtr & query_ptr, ContextPtr context, const AccessRightsElements & query_requires_access);
BlockIO executeDDLQueryOnCluster(const ASTPtr & query_ptr, ContextPtr context, AccessRightsElements && query_requires_access);

BlockIO getDistributedDDLStatus(
    const String & node_path, const DDLLogEntry & entry, ContextPtr context, const std::optional<Strings> & hosts_to_wait = {});

}
