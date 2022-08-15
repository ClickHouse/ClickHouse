#pragma once

#include <Storages/ColumnsDescription.h>
#include <Parsers/IAST.h>
#include <Parsers/queryToString.h>
#include <Interpreters/Cluster.h>


namespace DB
{
class Context;
struct StorageID;

/// Find the names and types of the table columns on any server in the cluster.
/// Used to implement the `remote` table function and others.
ColumnsDescription getStructureOfRemoteTable(
    const Cluster & cluster,
    const StorageID & table_id,
    ContextPtr context,
    const ASTPtr & table_func_ptr = nullptr);

}
