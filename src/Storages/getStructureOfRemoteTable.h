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
/// `is_insert_query` is only consulted for a table-function target resolved on a local shard: it is passed
/// to `getActualTableStructureWithAccess`, where it selects the access flags and, for object storages, a
/// writable rather than a read-only client. Pass `false` when the structure is needed only for reading.
ColumnsDescription getStructureOfRemoteTable(
    const Cluster & cluster,
    const StorageID & table_id,
    ContextPtr context,
    const ASTPtr & table_func_ptr = nullptr,
    bool is_insert_query = true);

}
