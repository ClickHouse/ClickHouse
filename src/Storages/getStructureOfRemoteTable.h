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
/// `is_replayed_definition` says whether the target comes from a definition read back from the metadata
/// rather than from a fresh `CREATE`: the inner table function has to know, because a combination it
/// refuses while a definition is written must still load.
ColumnsDescription getStructureOfRemoteTable(
    const Cluster & cluster,
    const StorageID & table_id,
    ContextPtr context,
    const ASTPtr & table_func_ptr = nullptr,
    bool is_replayed_definition = false);

}
