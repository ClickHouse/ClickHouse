#pragma once

#include <Storages/ColumnsDescription.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/Cluster.h>


namespace DB
{

class Context;
struct StorageID;

/// Find the names and types of the table columns on any server in the cluster.
/// Used to implement the `remote` table function and others.
/// `for_query_execution` selects the privilege that authorizes resolving a local shard's table:
/// the privilege on its data (`SELECT`) for a read, or the privilege on its schema
/// (`SHOW COLUMNS`) for introspection and for persisting the inferred columns in a table definition.
ColumnsDescription getStructureOfRemoteTable(
    const Cluster & cluster,
    const StorageID & table_id,
    ContextPtr context,
    const ASTPtr & table_func_ptr = nullptr,
    bool for_query_execution = false);

}
