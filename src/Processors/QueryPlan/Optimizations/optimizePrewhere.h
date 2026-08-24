#pragma once

#include <Interpreters/ActionsDAG.h>

namespace DB
{

struct PrewhereInfo;
using PrewhereInfoPtr = std::shared_ptr<PrewhereInfo>;

namespace QueryPlanOptimizations
{

/// Splits a filter expression into PREWHERE and WHERE parts, and fills the given PrewhereInfo.
///
/// This function analyzes the given filter expression and identifies parts that can be evaluated
/// early during the PREWHERE stage, based on the provided set of nodes. It modifies the
/// `PrewhereInfo` object by populating it with a corresponding PREWHERE expression and column name.
///
/// It also handles edge cases where default values may depend on columns used in PREWHERE,
/// ensuring such dependencies are preserved to avoid incorrect results.
///
/// If multiple conditions are used for PREWHERE, they will be combined using `AND`.
/// The remaining part of the filter expression is returned as a new ActionsDAG.
ActionsDAG splitAndFillPrewhereInfo(
    PrewhereInfoPtr & prewhere_info,
    bool remove_prewhere_column,
    ActionsDAG filter_expression,
    const String & filter_column_name,
    const std::unordered_set<const ActionsDAG::Node *> & prewhere_nodes,
    const std::list<const ActionsDAG::Node *> & prewhere_nodes_list);

}

}
