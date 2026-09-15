#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>

using namespace DB;

namespace
{

/// Builds a single-output DAG holding `<function_name>(b, <value>)` under the given result name,
/// optionally with an ALIAS node between the function and its `b` argument.
ActionsDAG makeFilterDAG(
    const std::string & condition_name,
    UInt32 value,
    bool alias_the_argument,
    const std::string & function_name = "equals")
{
    ActionsDAG dag;

    const auto & input = dag.addInput("b", std::make_shared<DataTypeUInt32>());
    const ActionsDAG::Node * argument = &input;
    if (alias_the_argument)
        argument = &dag.addAlias(input, "b_renamed");

    const auto & constant = dag.addColumn(
        ColumnConst::create(ColumnVector<UInt32>::create(1, value), 1), std::make_shared<DataTypeUInt32>(), "value");

    auto function = FunctionFactory::instance().get(function_name, getContext().context);
    const auto & condition = dag.addFunction(function, {argument, &constant}, condition_name);

    dag.getOutputs().clear();
    dag.getOutputs().push_back(&condition);
    return dag;
}

}

TEST(ActionsDAGNodeHashAliases, AliasedArgumentHashesLikeTheArgument)
{
    tryRegisterFunctions();

    auto plain = makeFilterDAG("cond", 19999, /* alias_the_argument= */ false);
    auto aliased = makeFilterDAG("cond", 19999, /* alias_the_argument= */ true);

    const auto * plain_node = plain.getOutputs().front();
    const auto * aliased_node = aliased.getOutputs().front();

    EXPECT_EQ(aliased_node->getHash(/* skip_aliases= */ true), plain_node->getHash(/* skip_aliases= */ true));
    EXPECT_NE(aliased_node->getHash(/* skip_aliases= */ false), plain_node->getHash(/* skip_aliases= */ false));
}

TEST(ActionsDAGNodeHashAliases, AliasChainAboveConditionHashesLikeTheCondition)
{
    tryRegisterFunctions();

    auto dag = makeFilterDAG("cond", 19999, /* alias_the_argument= */ false);
    const auto * condition = dag.getOutputs().front();
    const auto expected = condition->getHash(/* skip_aliases= */ true);

    const auto & renamed = dag.addAlias(*condition, "_projection_filter");
    EXPECT_EQ(renamed.getHash(/* skip_aliases= */ true), expected);
    EXPECT_NE(renamed.getHash(/* skip_aliases= */ false), condition->getHash(/* skip_aliases= */ false));

    const auto & renamed_twice = dag.addAlias(renamed, "cond_outer");
    EXPECT_EQ(renamed_twice.getHash(/* skip_aliases= */ true), expected);
}

TEST(ActionsDAGNodeHashAliases, AliasInsideConjunctionHashesLikeTheConjunction)
{
    tryRegisterFunctions();

    auto build = [](bool alias_the_conjunct)
    {
        auto dag = makeFilterDAG("cond", 19999, /* alias_the_argument= */ false);

        const ActionsDAG::Node * conjunct = dag.getOutputs().front();
        if (alias_the_conjunct)
            conjunct = &dag.addAlias(*conjunct, "cond_renamed");

        const auto & other = dag.addColumn(
            ColumnConst::create(ColumnVector<UInt8>::create(1, static_cast<UInt8>(1)), 1),
            std::make_shared<DataTypeUInt8>(),
            "other");

        auto and_function = FunctionFactory::instance().get("and", getContext().context);
        const auto & conjunction = dag.addFunction(and_function, {&other, conjunct}, "conjunction");

        dag.getOutputs().clear();
        dag.getOutputs().push_back(&conjunction);
        return dag;
    };

    auto plain = build(/* alias_the_conjunct= */ false);
    auto aliased = build(/* alias_the_conjunct= */ true);

    const auto * plain_node = plain.getOutputs().front();
    const auto * aliased_node = aliased.getOutputs().front();

    EXPECT_EQ(aliased_node->getHash(/* skip_aliases= */ true), plain_node->getHash(/* skip_aliases= */ true));
    EXPECT_NE(aliased_node->getHash(/* skip_aliases= */ false), plain_node->getHash(/* skip_aliases= */ false));
}

TEST(ActionsDAGNodeHashAliases, DifferentConditionsStillHashDifferently)
{
    tryRegisterFunctions();

    auto reference = makeFilterDAG("cond", 19999, /* alias_the_argument= */ true);
    const auto expected = reference.getOutputs().front()->getHash(/* skip_aliases= */ true);

    auto other_value = makeFilterDAG("cond", 19998, /* alias_the_argument= */ true);
    EXPECT_NE(other_value.getOutputs().front()->getHash(/* skip_aliases= */ true), expected);

    auto other_function = makeFilterDAG("cond", 19999, /* alias_the_argument= */ true, "notEquals");
    EXPECT_NE(other_function.getOutputs().front()->getHash(/* skip_aliases= */ true), expected);

    ActionsDAG input_only;
    const auto & input = input_only.addInput("cond", std::make_shared<DataTypeUInt8>());
    EXPECT_NE(input.getHash(/* skip_aliases= */ true), expected);
}
