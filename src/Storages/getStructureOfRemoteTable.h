#pragma once

#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage_fwd.h>
#include <Parsers/IAST_fwd.h>
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

/// Throws unless `context` may describe `table_id`: `SHOW_COLUMNS` on it and, for an `Alias`, on its target
/// (an `Alias` reports its target's columns). `storage` is the table if it exists on this server, and may be null.
/// The caller looks it up with a non-throwing method: the `SHOW_COLUMNS` check must come before any error that
/// tells whether the table exists.
void checkDescribeTableAccess(const StoragePtr & storage, const StorageID & table_id, const ContextPtr & context);

}
