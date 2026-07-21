#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/Optimizations/Cascades/DagNameTranslation.h>

using namespace DB;

namespace
{

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

/// A DAG with input `x` renamed to `y`, `materialize(x)` as `m`, and `x + 1` as `computed`.
ActionsDAG makeDag()
{
    tryRegisterFunctions();
    ActionsDAG dag;
    const auto & x = dag.addInput("x", std::make_shared<DataTypeUInt64>());
    dag.addOrReplaceInOutputs(dag.addAlias(x, "y"));
    addOutputFunction(dag, "materialize", {&x}, "m");
    auto one_type = std::make_shared<DataTypeUInt64>();
    const auto & one = dag.addColumn(one_type->createColumnConst(1, Field(1ULL)), one_type, "1");
    addOutputFunction(dag, "plus", {&x, &one}, "computed");
    return dag;
}

SortDescription sortBy(const String & column)
{
    SortDescription description;
    description.emplace_back(column, 1, 1);
    return description;
}

}

TEST(CascadesDagNameTranslation, ClassifyOutputName)
{
    auto dag = makeDag();
    String input_name;

    EXPECT_EQ(classifyOutputName(dag, "y", input_name), TranslatedName::Traced);
    EXPECT_EQ(input_name, "x");

    EXPECT_EQ(classifyOutputName(dag, "m", input_name), TranslatedName::Traced);
    EXPECT_EQ(input_name, "x");

    EXPECT_EQ(classifyOutputName(dag, "computed", input_name), TranslatedName::Computed);

    /// A name the DAG does not produce may be an input column carried around the step.
    EXPECT_EQ(classifyOutputName(dag, "not_in_dag", input_name), TranslatedName::Passthrough);
}

TEST(CascadesDagNameTranslation, DistributionColumns)
{
    auto dag = makeDag();

    /// Renamed and materialized columns translate to the input name; unknown names are kept.
    std::vector<NameSet> columns = {{"y"}, {"m", "not_in_dag"}};
    ASSERT_TRUE(translateDistributionColumns(dag, columns));
    EXPECT_EQ(columns[0], NameSet{"x"});
    EXPECT_EQ(columns[1], (NameSet{"x", "not_in_dag"}));

    /// In a mixed equivalence set only the computed name drops out.
    std::vector<NameSet> mixed = {{"y", "computed"}};
    ASSERT_TRUE(translateDistributionColumns(dag, mixed));
    EXPECT_EQ(mixed[0], NameSet{"x"});

    /// A set of only computed columns makes the distribution underivable from the input.
    std::vector<NameSet> computed_only = {{"computed"}};
    EXPECT_FALSE(translateDistributionColumns(dag, computed_only));
}

TEST(CascadesDagNameTranslation, SortDescription)
{
    auto dag = makeDag();

    auto renamed = sortBy("y");
    ASSERT_TRUE(translateSortDescription(dag, renamed));
    EXPECT_EQ(renamed[0].column_name, "x");

    auto passthrough = sortBy("not_in_dag");
    ASSERT_TRUE(translateSortDescription(dag, passthrough));
    EXPECT_EQ(passthrough[0].column_name, "not_in_dag");

    /// A computed sort key's order is not derivable from the input.
    auto computed = sortBy("computed");
    EXPECT_FALSE(translateSortDescription(dag, computed));
}
