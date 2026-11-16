#pragma once

#include <Functions/CastOverloadResolver.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/IFunctionAdaptors.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/ExpressionActions.h>

#include <map>

namespace DB
{

struct IndexReadTask;
using IndexReadTasks = std::map<String, IndexReadTask>;

bool tryBuildPrewhereSteps(
    PrewhereInfoPtr prewhere_info,
    const ExpressionActionsSettings & actions_settings,
    PrewhereExprInfo & prewhere,
    bool force_short_circuit_execution,
    bool read_ahead_columns = false,
    const IndexReadTasks * index_read_tasks = nullptr);

/// A PREWHERE condition an optimizer synthesizes as a dynamic/runtime filter, as opposed to a part of the
/// user's query predicate: the `__topKFilter` of `ORDER BY ... LIMIT` dynamic filtering, and the
/// `__applyFilter` join runtime filter (alone, or inside the `OR isNull(key)` an ANTI join adds). The
/// PREWHERE-effectiveness ProfileEvents exclude these so a query without a user `PREWHERE`/`WHERE`
/// predicate reports nothing; see `PrewhereExprStep::is_query_prewhere`.
bool isDynamicRuntimePrewhereFilter(const ActionsDAG::Node * node);

/// Whether every conjunct of the PREWHERE actions is a user query predicate, i.e. none is a synthesized
/// dynamic filter. `filter_column_name` is the PREWHERE result column (the conjunction root). A single fused
/// step (used when the step splitter is disabled or inapplicable) evaluates all its conjuncts together, so a
/// step mixing a user predicate with a dynamic filter cannot separate their rejections and is not attributed
/// to PREWHERE; only an entirely user-predicate step is.
bool prewhereIsEntirelyQueryPredicate(const ActionsDAG & prewhere_actions, const String & filter_column_name);

}
