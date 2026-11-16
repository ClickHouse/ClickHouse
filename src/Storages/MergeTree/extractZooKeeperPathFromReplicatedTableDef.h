#pragma once

#include <Databases/LoadingStrictnessLevel.h>
#include <Storages/TableZnodeInfo.h>
#include <base/types.h>
#include <memory>
#include <optional>


namespace DB
{
class ASTCreateQuery;
class Context;
struct StorageID;
using ContextPtr = std::shared_ptr<const Context>;

/// Extracts a zookeeper path from a specified CREATE TABLE query.
/// The function checks the table engine and if it is Replicated*MergeTree then it takes the first argument and expands macros in it.
/// Returns std::nullopt if the specified CREATE query doesn't describe a Replicated table or its arguments can't be evaluated.
std::optional<String> extractZooKeeperPathFromReplicatedTableDef(const ASTCreateQuery & create_query, const ContextPtr & local_context);

/// Resolves the path, the replica name and the Keeper name of a Replicated*MergeTree table from its `CREATE TABLE`
/// query the same way creating the storage with strictness `mode` does, without creating it.
/// Returns std::nullopt if the query doesn't describe a Replicated table; throws if its arguments can't be evaluated.
std::optional<TableZnodeInfo> extractZnodeInfoFromReplicatedTableDef(
    const ASTCreateQuery & create_query, const StorageID & table_id, LoadingStrictnessLevel mode, const ContextPtr & local_context);

}
