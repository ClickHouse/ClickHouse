#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeNullable.h>
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

    /// Single-argument deterministic functions whose distinct count is genuinely bounded by the
    /// date's NDV, with no smaller fixed codomain (unlike `toMonth`, which is always <= 12 and would
    /// warrant a tighter constant bound instead).
    addOutputFunction(dag, "toYear", {&date_input}, "year");
    addOutputFunction(dag, "toStartOfMonth", {&date_input}, "start_of_month");
    addOutputFunction(dag, "toString", {&date_input}, "as_string");
    /// `materialize` is in the explicit pass-through list and must keep working.
    addOutputFunction(dag, "materialize", {&date_input}, "materialized");
    /// The column itself passed through as an output keeps its stats.
    dag.addOrReplaceInOutputs(date_input);

    auto stats = statsOf("d", distinct_dates);
    remapColumnStats(stats, dag);

    EXPECT_EQ(stats["year"].num_distinct_values, distinct_dates);
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

/// NDV counts only non-null values. A null-aware function such as `isNull` maps NULL to one extra
/// counted value, so its bound is the argument's NDV plus one. A null-preserving function over the
/// same Nullable argument keeps the exact bound, since NULL stays NULL and is uncounted on both sides.
TEST(ColumnStatsDerivation, NullAwareFunctionAddsOneToBound)
{
    tryRegisterFunctions();

    const UInt64 distinct_dates = 2556;

    auto nullable_date_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDate>());
    ActionsDAG dag;
    const auto & date_input = dag.addInput("d", nullable_date_type);

    /// Nullable -> non-Nullable UInt8: NULL maps to one counted value, so the bound gains one.
    addOutputFunction(dag, "isNull", {&date_input}, "is_null");
    addOutputFunction(dag, "isNotNull", {&date_input}, "is_not_null");
    /// Nullable -> Nullable: NULL stays NULL, so the exact bound still holds.
    addOutputFunction(dag, "toYear", {&date_input}, "year");

    auto stats = statsOf("d", distinct_dates);
    remapColumnStats(stats, dag);

    EXPECT_EQ(stats["is_null"].num_distinct_values, distinct_dates + 1);
    EXPECT_EQ(stats["is_not_null"].num_distinct_values, distinct_dates + 1);
    EXPECT_EQ(stats["year"].num_distinct_values, distinct_dates);
}

/// The +1 from a null-aware hop is carried along the rest of the chain, regardless of its position:
/// a downstream null-preserving function keeps it, and an upstream one does not consume it.
TEST(ColumnStatsDerivation, NullAwareFunctionInChainCarriesOffset)
{
    tryRegisterFunctions();

    const UInt64 distinct_dates = 2556;

    auto nullable_date_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDate>());
    ActionsDAG dag;
    const auto & date_input = dag.addInput("d", nullable_date_type);

    /// toString(isNull(d)): isNull adds one, the downstream toString preserves it.
    const auto & is_null = dag.addFunction(
        FunctionFactory::instance().get("isNull", getContext().context), {&date_input}, "is_null");
    addOutputFunction(dag, "toString", {&is_null}, "is_null_string");

    /// isNull(toStartOfMonth(d)): the upstream null-preserving hop keeps the bound exact, then isNull adds one.
    const auto & start_of_month = dag.addFunction(
        FunctionFactory::instance().get("toStartOfMonth", getContext().context), {&date_input}, "start_of_month");
    addOutputFunction(dag, "isNull", {&start_of_month}, "month_is_null");

    auto stats = statsOf("d", distinct_dates);
    remapColumnStats(stats, dag);

    EXPECT_EQ(stats["is_null_string"].num_distinct_values, distinct_dates + 1);
    EXPECT_EQ(stats["month_is_null"].num_distinct_values, distinct_dates + 1);
}

/// A `CAST` that drops nullability collapses NULL into a counted value, like a null-aware function,
/// so its bound is the source NDV plus one even though `CAST` is a pass-through hop.
TEST(ColumnStatsDerivation, CastDroppingNullabilityAddsOneToBound)
{
    tryRegisterFunctions();

    const UInt64 distinct_values = 1000;

    auto nullable_int_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    ActionsDAG dag;
    const auto & int_input = dag.addInput("n", nullable_int_type);

    /// Nullable(Int32) -> Int32: NULL maps to the default value, gaining one distinct value.
    const auto & casted = dag.addCast(int_input, std::make_shared<DataTypeInt32>(), "n_int32", getContext().context);
    dag.addOrReplaceInOutputs(casted);

    /// Casting to Nullable(Int32) preserves nulls, so the exact bound holds.
    const auto & casted_nullable = dag.addCast(int_input, nullable_int_type, "n_nullable", getContext().context);
    dag.addOrReplaceInOutputs(casted_nullable);

    auto stats = statsOf("n", distinct_values);
    remapColumnStats(stats, dag);

    EXPECT_EQ(stats["n_int32"].num_distinct_values, distinct_values + 1);
    EXPECT_EQ(stats["n_nullable"].num_distinct_values, distinct_values);
}

/// Two outputs consuming the same internal node both inherit the source bound: resolving one output
/// must not stop the other from resolving through the shared node, regardless of output order.
TEST(ColumnStatsDerivation, SharedIntermediateNodeResolvesForAllOutputs)
{
    tryRegisterFunctions();

    const UInt64 distinct_dates = 2556;

    auto date_type = std::make_shared<DataTypeDate>();
    ActionsDAG dag;
    const auto & date_input = dag.addInput("d", date_type);

    /// Internal node year = toYear(d), consumed by two separate outputs but not an output itself.
    const auto & year = dag.addFunction(
        FunctionFactory::instance().get("toYear", getContext().context), {&date_input}, "year");
    addOutputFunction(dag, "toString", {&year}, "year_string");
    addOutputFunction(dag, "toUInt32", {&year}, "year_uint");

    auto stats = statsOf("d", distinct_dates);
    remapColumnStats(stats, dag);

    EXPECT_EQ(stats["year_string"].num_distinct_values, distinct_dates);
    EXPECT_EQ(stats["year_uint"].num_distinct_values, distinct_dates);
}

/// `firstNonDefault(a, b)` (the JOIN USING merged key) draws values from both arguments, so a single
/// source's NDV is not a valid upper bound for it. No stats are propagated for such multi-source hops.
TEST(ColumnStatsDerivation, FirstNonDefaultDoesNotPropagateBound)
{
    tryRegisterFunctions();

    auto int_type = std::make_shared<DataTypeInt64>();
    ActionsDAG dag;
    const auto & left = dag.addInput("a", int_type);
    const auto & right = dag.addInput("b", int_type);

    addOutputFunction(dag, "firstNonDefault", {&left, &right}, "merged");

    std::unordered_map<String, ColumnStats> stats;
    stats["a"] = ColumnStats{.num_distinct_values = 10};
    stats["b"] = ColumnStats{.num_distinct_values = 1000};
    remapColumnStats(stats, dag);

    EXPECT_EQ(stats.count("merged"), 0u);
}

/// A very long chain of single-argument functions resolves without hitting a recursion-depth limit,
/// and the source bound still reaches the final output.
TEST(ColumnStatsDerivation, DeepChainOfFunctionsResolves)
{
    tryRegisterFunctions();

    const UInt64 distinct_values = 1000;
    const size_t chain_length = 20000;

    auto int_type = std::make_shared<DataTypeInt64>();
    ActionsDAG dag;
    const auto & int_input = dag.addInput("n", int_type);

    const ActionsDAG::Node * current = &int_input;
    for (size_t i = 0; i < chain_length; ++i)
        current = &dag.addFunction(
            FunctionFactory::instance().get("negate", getContext().context), {current}, "v" + std::to_string(i));
    dag.addOrReplaceInOutputs(*current);

    auto stats = statsOf("n", distinct_values);
    remapColumnStats(stats, dag);

    EXPECT_EQ(stats[current->result_name].num_distinct_values, distinct_values);
}

/// The bound propagates through a chain of deterministic single-argument functions: no link can
/// increase the distinct count, so the final output is still bounded by the source column. A
/// multi-argument link anywhere in the chain breaks the propagation.
TEST(ColumnStatsDerivation, ChainOfFunctionsPropagatesBound)
{
    tryRegisterFunctions();

    auto date_type = std::make_shared<DataTypeDate>();
    auto int_type = std::make_shared<DataTypeInt64>();
    ActionsDAG dag;
    const auto & date_input = dag.addInput("d", date_type);
    const auto & int_input = dag.addInput("n", int_type);

    /// toString(toStartOfMonth(d)) -- two single-argument functions in a row.
    const auto & start_of_month = dag.addFunction(
        FunctionFactory::instance().get("toStartOfMonth", getContext().context), {&date_input}, "start_of_month");
    addOutputFunction(dag, "toString", {&start_of_month}, "formatted_month");

    /// toString(abs(negate(n))) -- three single-argument functions in a row.
    const auto & neg = dag.addFunction(
        FunctionFactory::instance().get("negate", getContext().context), {&int_input}, "neg");
    const auto & magnitude = dag.addFunction(
        FunctionFactory::instance().get("abs", getContext().context), {&neg}, "magnitude");
    addOutputFunction(dag, "toString", {&magnitude}, "formatted_magnitude");

    /// negate(plus(n, n)) -- a multi-argument link breaks the chain, so the bound stops there.
    const auto & doubled = dag.addFunction(
        FunctionFactory::instance().get("plus", getContext().context), {&int_input, &int_input}, "doubled");
    addOutputFunction(dag, "negate", {&doubled}, "neg_doubled");

    std::unordered_map<String, ColumnStats> stats;
    stats["d"] = ColumnStats{.num_distinct_values = 2556};
    stats["n"] = ColumnStats{.num_distinct_values = 1000};
    remapColumnStats(stats, dag);

    EXPECT_EQ(stats["formatted_month"].num_distinct_values, 2556u);
    EXPECT_EQ(stats["formatted_magnitude"].num_distinct_values, 1000u);
    EXPECT_EQ(stats.count("neg_doubled"), 0u);
}
