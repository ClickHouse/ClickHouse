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
};

using InterpolateDescriptionPtr = std::shared_ptr<InterpolateDescription>;

}
