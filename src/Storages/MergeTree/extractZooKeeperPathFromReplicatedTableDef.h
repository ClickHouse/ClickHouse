#pragma once

#include <base/types.h>
#include <memory>
#include <optional>


namespace DB
{
class ASTCreateQuery;
class Context;
using ContextPtr = std::shared_ptr<const Context>;

/// Extracts a zookeeper path from a specified CREATE TABLE query. Returns std::nullopt if fails.
/// The function checks the table engine and if it is Replicated*MergeTree then it takes the first argument and expands macros in it.
std::optional<String> extractZooKeeperPathFromReplicatedTableDef(const ASTCreateQuery & create_query, const ContextPtr & context);

}
