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

/// Whether the PREWHERE actions evaluate at least one user query-predicate conjunct, as opposed to only
/// synthesized dynamic filters. `filter_column_name` is the PREWHERE result column (the conjunction root).
bool prewhereEvaluatesQueryPredicate(const ActionsDAG & prewhere_actions, const String & filter_column_name);

}
