#pragma once

#include <memory>
#include <unordered_map>
#include <Parsers/StringRange.h>

namespace DB
{

class IAST;
using ASTPtr = std::shared_ptr<IAST>;

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class Set;
using SetPtr = std::shared_ptr<Set>;

/// Information about calculated sets in right hand side of IN.
using PreparedSets = std::unordered_map<StringRange, SetPtr, StringRangePointersHash, StringRangePointersEqualTo>;

struct PrewhereInfo
{
    /// Ections which are executed in order to alias columns are used for prewhere actions.
    ExpressionActionsPtr alias_actions;
    /// Actions which are executed on block in order to get filter column for prewhere step.
    ExpressionActionsPtr prewhere_actions;
    String prewhere_column_name;
    bool remove_prewhere_column = false;

    PrewhereInfo() = default;
    explicit PrewhereInfo(ExpressionActionsPtr prewhere_actions_, String prewhere_column_name_)
        : prewhere_actions(std::move(prewhere_actions_)), prewhere_column_name(std::move(prewhere_column_name_)) {}
};

using PrewhereInfoPtr = std::shared_ptr<PrewhereInfo>;


/** Query along with some additional data,
  *  that can be used during query processing
  *  inside storage engines.
  */
struct SelectQueryInfo
{
    ASTPtr query;

    PrewhereInfoPtr prewhere_info;

    /// Prepared sets are used for indices by storage engine.
    /// Example: x IN (1, 2, 3)
    PreparedSets sets;
};

}
