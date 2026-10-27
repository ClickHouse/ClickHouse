#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/Optimizations/Cascades/StatisticsDerivation.h>

using namespace DB;

namespace
{

/// A DAG over inputs `k` (NDV 1000), `v` (NDV 10), `u`/`w` (no statistics) and `a`/`b`
/// (one equivalence class), plus an alias `k_alias` of `k`.
struct Fixture
{
    ActionsDAG dag;
    ExpressionStatistics stats;
    std::unordered_map<String, const ActionsDAG::Node *> inputs;

    Fixture()
    {
        tryRegisterFunctions();
        auto type = std::make_shared<DataTypeUInt64>();
        for (const auto * name : {"k", "v", "u", "w", "a", "b"})
            inputs[name] = &dag.addInput(name, type);
        inputs["k_alias"] = &dag.addAlias(*inputs["k"], "k_alias");

        stats.estimated_row_count = 1000000;
        stats.column_statistics["k"].num_distinct_values = 1000;
        stats.column_statistics["v"].num_distinct_values = 10;
        stats.equivalences.add("a", "b");
    }

    const ActionsDAG::Node * constant(UInt64 value)
    {
        auto type = std::make_shared<DataTypeUInt64>();
        return &dag.addColumn(type->createColumnConst(1, Field(value)), type, "const_" + std::to_string(value));
    }

    const ActionsDAG::Node * function(const String & name, ActionsDAG::NodeRawConstPtrs children)
    {
        auto resolver = FunctionFactory::instance().get(name, getContext().context);
        return &dag.addFunction(resolver, std::move(children), {});
    }

    Float64 estimate(const ActionsDAG::Node * node) const { return estimatePredicateSelectivity(node, stats); }
};

}

TEST(CascadesFilterSelectivity, ComparisonWithConstant)
{
    Fixture f;
    /// An equality on a column with a known NDV counts as 1/NDV; `<>` as the complement.
    EXPECT_DOUBLE_EQ(f.estimate(f.function("equals", {f.inputs["k"], f.constant(42)})), 1.0 / 1000);
    EXPECT_DOUBLE_EQ(f.estimate(f.function("notEquals", {f.inputs["k"], f.constant(42)})), 1.0 - 1.0 / 1000);
    /// An alias resolves to its input column.
    EXPECT_DOUBLE_EQ(f.estimate(f.function("equals", {f.inputs["k_alias"], f.constant(42)})), 1.0 / 1000);
    /// Without an NDV the equality default applies.
    EXPECT_DOUBLE_EQ(f.estimate(f.function("equals", {f.inputs["u"], f.constant(42)})), 0.01);
    /// The column statistics carry no value ranges, so ranges keep the default factor.
    EXPECT_DOUBLE_EQ(f.estimate(f.function("greater", {f.inputs["k"], f.constant(42)})), 0.33);
}

TEST(CascadesFilterSelectivity, TwoColumns)
{
    Fixture f;
    /// Two columns with NDVs: the join-equality formula 1 / max(NDV).
    EXPECT_DOUBLE_EQ(f.estimate(f.function("equals", {f.inputs["k"], f.inputs["v"]})), 1.0 / 1000);
    /// An equality the plan below already enforces removes nothing; its negation removes everything.
    EXPECT_DOUBLE_EQ(f.estimate(f.function("equals", {f.inputs["a"], f.inputs["b"]})), 1.0);
    EXPECT_DOUBLE_EQ(f.estimate(f.function("notEquals", {f.inputs["a"], f.inputs["b"]})), 0.0);
    /// Two columns without NDVs: the equality default.
    EXPECT_DOUBLE_EQ(f.estimate(f.function("equals", {f.inputs["u"], f.inputs["w"]})), 0.01);
}

TEST(CascadesFilterSelectivity, NdvThatRowsWithoutStatisticsCouldDouble)
{
    Fixture f;
    /// `u` has one value and 100 rows without statistics, which could add 100 more; `w` has one value on all rows.
    f.stats.column_statistics["u"] = ColumnStats{.num_distinct_values = 1, .rows_without_statistics = 100};
    f.stats.column_statistics["w"] = ColumnStats{.num_distinct_values = 1};
    /// Such an NDV counts as unknown: the defaults apply, also next to a column whose NDV is usable, in either order.
    for (const auto & [left, right] : {std::pair{"u", "w"}, std::pair{"w", "u"}})
    {
        bool used_default = false;
        EXPECT_DOUBLE_EQ(estimatePredicateSelectivity(f.function("notEquals", {f.inputs[left], f.inputs[right]}), f.stats, used_default), 0.99);
        EXPECT_TRUE(used_default);
    }
    bool used_default = false;
    EXPECT_DOUBLE_EQ(estimatePredicateSelectivity(f.function("equals", {f.inputs["u"], f.constant(7)}), f.stats, used_default), 0.01);
    EXPECT_TRUE(used_default);
    /// The precise column alone gives its NDV.
    used_default = true;
    EXPECT_DOUBLE_EQ(estimatePredicateSelectivity(f.function("equals", {f.inputs["w"], f.constant(7)}), f.stats, used_default), 1.0);
    EXPECT_FALSE(used_default);
}

TEST(CascadesFilterSelectivity, BooleanCombinations)
{
    Fixture f;
    const auto * range = f.function("greater", {f.inputs["k"], f.constant(42)});
    const auto * equality = f.function("equals", {f.inputs["k"], f.constant(42)});

    EXPECT_DOUBLE_EQ(f.estimate(f.function("and", {range, equality})), 0.33 * (1.0 / 1000));
    EXPECT_DOUBLE_EQ(f.estimate(f.function("or", {range, equality})), 1.0 - (1.0 - 0.33) * (1.0 - 1.0 / 1000));
    EXPECT_DOUBLE_EQ(f.estimate(f.function("not", {range})), 1.0 - 0.33);
    /// A predicate the estimator cannot classify keeps the unknown default.
    EXPECT_DOUBLE_EQ(f.estimate(f.function("modulo", {f.inputs["k"], f.constant(7)})), 0.33);
}
