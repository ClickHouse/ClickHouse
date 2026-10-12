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

class ColumnsDescription;
struct IndexReadTask;
using IndexReadTasks = std::map<String, IndexReadTask>;

bool tryBuildPrewhereSteps(
    PrewhereInfoPtr prewhere_info,
    const ExpressionActionsSettings & actions_settings,
    PrewhereExprInfo & prewhere,
    bool force_short_circuit_execution,
    const ColumnsDescription * columns = nullptr,
    bool read_ahead_columns = false,
    /// Adjacent conditions on the virtual columns of one index may share a step, like conditions on subcolumns of one column,
    /// so that the index reader fills these columns in one pass.
    const IndexReadTasks * index_read_tasks = nullptr);

}
