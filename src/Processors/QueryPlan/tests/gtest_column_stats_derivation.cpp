#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/Optimizations/joinOrder.h>

using namespace DB;
using namespace DB::QueryPlanOptimizations;

namespace
{

/// Build statistics for a single column with the given distinct-value count.
std::unordered_map<String, ColumnStats> statsOf(const String & name, UInt64 num_distinct_values)
{
    std::unordered_map<String, ColumnStats> stats;
    stats[name] = ColumnStats{.num_distinct_values = num_distinct_values};
    return stats;
}

/// Add `function(children)` to the DAG as an output named `result_name` and return its node.
const ActionsDAG::Node & addOutputFunction(
    ActionsDAG & dag,
    const String & function_name,
    ActionsDAG::NodeRawConstPtrs children,
    const String & result_name)
{
    auto resolver = FunctionFactory::instance().get(function_name, getContext().context);
    const auto & node = dag.addFunction(resolver, std::move(children), result_name);
    dag.addOrReplaceInOutputs(node);
    return node;
}

}

/// A deterministic single-argument function inherits its argument's distinct count as an upper
/// bound, since it cannot produce more distinct values than its argument.
TEST(ColumnStatsDerivation, DeterministicSingleArgFunctionBoundsDistinctValues)
{
    tryRegisterFunctions();

    const UInt64 distinct_dates = 2556;

    auto date_type = std::make_shared<DataTypeDate>();
    ActionsDAG dag;
    const auto & date_input = dag.addInput("d", date_type);

    /// Single-argument deterministic functions of a date column, each bounded by the date's NDV.
    addOutputFunction(dag, "toYear", {&date_input}, "year");
    addOutputFunction(dag, "toMonth", {&date_input}, "month");
    addOutputFunction(dag, "toDayOfWeek", {&date_input}, "day_of_week");
    addOutputFunction(dag, "toStartOfMonth", {&date_input}, "start_of_month");
    addOutputFunction(dag, "toString", {&date_input}, "as_string");
    /// `materialize` is in the explicit pass-through list and must keep working.
    addOutputFunction(dag, "materialize", {&date_input}, "materialized");
    /// The column itself passed through as an output keeps its stats.
    dag.addOrReplaceInOutputs(date_input);

    auto stats = statsOf("d", distinct_dates);
    remapColumnStats(stats, dag);

    EXPECT_EQ(stats["year"].num_distinct_values, distinct_dates);
    EXPECT_EQ(stats["month"].num_distinct_values, distinct_dates);
    EXPECT_EQ(stats["day_of_week"].num_distinct_values, distinct_dates);
    EXPECT_EQ(stats["start_of_month"].num_distinct_values, distinct_dates);
    EXPECT_EQ(stats["as_string"].num_distinct_values, distinct_dates);
    EXPECT_EQ(stats["materialized"].num_distinct_values, distinct_dates);
    EXPECT_EQ(stats["d"].num_distinct_values, distinct_dates);
}

/// Single-argument functions of a numeric column behave the same way.
TEST(ColumnStatsDerivation, SingleArgFunctionOfNumericColumn)
{
    tryRegisterFunctions();

    const UInt64 distinct_values = 1000;

    auto int_type = std::make_shared<DataTypeInt64>();
    ActionsDAG dag;
    const auto & int_input = dag.addInput("n", int_type);

    addOutputFunction(dag, "negate", {&int_input}, "neg");
    addOutputFunction(dag, "abs", {&int_input}, "magnitude");
    addOutputFunction(dag, "toString", {&int_input}, "as_string");

    auto stats = statsOf("n", distinct_values);
    remapColumnStats(stats, dag);

    EXPECT_EQ(stats["neg"].num_distinct_values, distinct_values);
    EXPECT_EQ(stats["magnitude"].num_distinct_values, distinct_values);
    EXPECT_EQ(stats["as_string"].num_distinct_values, distinct_values);
}

/// A multi-argument function is not bounded by a single argument's distinct count, so the bound
/// is not propagated: the derived column gets no stats.
TEST(ColumnStatsDerivation, MultiArgFunctionDoesNotPropagateBound)
{
    tryRegisterFunctions();

    auto int_type = std::make_shared<DataTypeInt64>();
    ActionsDAG dag;
    const auto & int_input = dag.addInput("n", int_type);

    addOutputFunction(dag, "plus", {&int_input, &int_input}, "doubled");

    auto stats = statsOf("n", 1000);
    remapColumnStats(stats, dag);

    EXPECT_EQ(stats.count("doubled"), 0u);
}

/// A derived column is attributed to the right source column when several inputs carry stats.
TEST(ColumnStatsDerivation, AttributesDerivedColumnToCorrectSource)
{
    tryRegisterFunctions();

    auto date_type = std::make_shared<DataTypeDate>();
    auto int_type = std::make_shared<DataTypeInt64>();
    ActionsDAG dag;
    const auto & date_input = dag.addInput("d", date_type);
    const auto & int_input = dag.addInput("n", int_type);

    addOutputFunction(dag, "toYear", {&date_input}, "year");
    addOutputFunction(dag, "negate", {&int_input}, "neg");

    std::unordered_map<String, ColumnStats> stats;
    stats["d"] = ColumnStats{.num_distinct_values = 2556};
    stats["n"] = ColumnStats{.num_distinct_values = 1000};
    remapColumnStats(stats, dag);

    EXPECT_EQ(stats["year"].num_distinct_values, 2556u);
    EXPECT_EQ(stats["neg"].num_distinct_values, 1000u);
}
