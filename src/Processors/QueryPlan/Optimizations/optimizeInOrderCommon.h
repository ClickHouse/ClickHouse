#pragma once

#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Storages/SelectQueryInfo.h>

#include <Common/logger_useful.h>

#include <stack>


/*
 * Common functions for join/aggregation/sorting read in order optimizations.
 */
namespace DB::QueryPlanOptimizations
{

using StepStack = std::vector<IQueryPlanStep *>;

using Positions = std::set<size_t>;
using Permutation = std::vector<size_t>;

/// Sort description for step that requires sorting (aggregation or sorting JOIN).
/// Note: We really do need three different sort descriptions here.
///
/// For example, aggregation query:
///
///   create table tab (a Int32, b Int32, c Int32, d Int32) engine = MergeTree order by (a, b, c);
///   select a, any(b), c, d from tab where b = 1 group by a, c, d order by c, d;
///
/// We would like to have:
/// (a, b, c) - a sort description for reading from table (it's into input_order)
/// (a, c) - a sort description for merging (an input of AggregatingInOrderTransform is sorted by this GROUP BY keys)
/// (a, c, d) - a target sort description (GROUP BY sort description, an input of FinishAggregatingInOrderTransform is sorted by all GROUP BY keys)
///
/// Sort description from input_order is not actually used. ReadFromMergeTree reads only PK prefix size.
/// We should remove it later.
struct StepInputOrder
{
    InputOrderInfoPtr input_order;
    SortDescription sort_description_for_merging;
    SortDescription target_sort_description;

    /// Map indices from target_sort_description original positions
    std::vector<Positions> permutation;
};

/// FixedColumns are columns which values become constants after filtering.
/// In a query "SELECT x, y, z FROM table WHERE x = 1 AND y = 'a' ORDER BY x, y, z"
/// Fixed columns are 'x' and 'y'.
using FixedColumns = std::unordered_set<const ActionsDAG::Node *>;

QueryPlan::Node * findReadingStep(QueryPlan::Node & node, StepStack & backward_path);

StepInputOrder buildInputOrderInfo(const Names & keys, QueryPlan::Node & node, QueryPlan::Node * reading_node);

void requestInputOrderInfo(const InputOrderInfoPtr & input_order_info, QueryPlanStepPtr & reading_step);

void updateStepsDataStreams(StepStack & steps_to_update);


Poco::Logger * getLogger();

}
