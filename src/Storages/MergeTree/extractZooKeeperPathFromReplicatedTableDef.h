#pragma once

#include <base/types.h>
#include <memory>
#include <optional>


namespace DB
{
class ASTCreateQuery;
class Context;
using ContextPtr = std::shared_ptr<const Context>;

/// Extracts a zookeeper path from a specified CREATE TABLE query.
/// The function checks the table engine and if it is Replicated*MergeTree then it takes the first argument and expands macros in it.
/// Returns std::nullopt if the specified CREATE query doesn't describe a Replicated table or its arguments can't be evaluated.
std::optional<String> extractZooKeeperPathFromReplicatedTableDef(const ASTCreateQuery & create_query, const ContextPtr & local_context);

}
