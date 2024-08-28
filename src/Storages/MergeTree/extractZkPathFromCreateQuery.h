#pragma once

#include <base/types.h>
#include <memory>
#include <optional>


namespace DB
{
class IAST;
class Context;
using ContextPtr = std::shared_ptr<const Context>;

/// Extracts a zookeeper path from a specified CREATE TABLE query. Returns std::nullopt if fails.
/// The function takes the first argument of the ReplicatedMergeTree table engine and expands macros in it.
/// It works like a part of what the create() function in registerStorageMergeTree.cpp does but in a simpler manner.
std::optional<String> tryExtractZkPathFromCreateQuery(const IAST & create_query, const ContextPtr & global_context);

}
