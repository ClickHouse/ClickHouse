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

    /// Input column name -> {alias, type}. With `enable_analyzer = 0` the block the filling transform
    /// works on is the one before the final projection, so a single input column can back several of
    /// the columns the INTERPOLATE expressions are written in terms of (e.g. `SELECT x AS a, x AS b`
    /// with `INTERPOLATE (a AS a + b)`). Hence a multimap.
    UnorderedMultiMapWithMemoryTracking<std::string, NameAndTypePair> required_columns_map;
    UnorderedSetWithMemoryTracking<std::string> result_columns_set; /// result block columns
    VectorWithMemoryTracking<std::string> result_columns_order; /// result block columns order
};

using InterpolateDescriptionPtr = std::shared_ptr<InterpolateDescription>;

}
