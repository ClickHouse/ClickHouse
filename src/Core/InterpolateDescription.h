#pragma once

#include <unordered_map>
#include <memory>
#include <string>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/ActionsDAG.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>


namespace DB
{

using Aliases = std::unordered_map<String, ASTPtr>;

/// Interpolate description
struct InterpolateDescription
{
    explicit InterpolateDescription(ActionsDAG actions, const Aliases & aliases);

    ActionsDAG actions;

    UnorderedMapWithMemoryTracking<std::string, NameAndTypePair> required_columns_map; /// input column name -> {alias, type}
    UnorderedSetWithMemoryTracking<std::string> result_columns_set; /// result block columns
    VectorWithMemoryTracking<std::string> result_columns_order; /// result block columns order

    /// Executed-output column name -> index within `result_columns_order`. Several outputs may collapse to one
    /// destination, so the executed interpolate block must be routed to destinations by name, not by position.
    UnorderedMapWithMemoryTracking<std::string, size_t> output_to_result_index;
};

using InterpolateDescriptionPtr = std::shared_ptr<InterpolateDescription>;

}
