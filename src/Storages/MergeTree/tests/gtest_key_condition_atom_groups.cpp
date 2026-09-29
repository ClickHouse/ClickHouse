#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/MergeTree/KeyCondition.h>

namespace DB
{
namespace
{

class KeyConditionAtomGroups : public ::testing::Test
{
protected:
    void SetUp() override
    {
        tryRegisterFunctions();
        context = Context::createCopy(getContext().context);
        context->setSetting("analyze_index_with_multiple_key_columns_per_condition", Field(true));
    }

    const ActionsDAG::Node & addConstant(UInt64 value)
    {
        auto type = std::make_shared<DataTypeUInt64>();
        return dag.addColumn(type->createColumnConst(1, value), type, std::to_string(value));
    }

    const ActionsDAG::Node & addFunction(const String & name, ActionsDAG::NodeRawConstPtrs arguments)
    {
        return dag.addFunction(FunctionFactory::instance().get(name, context), std::move(arguments), {});
    }

    KeyCondition makeCondition(const ActionsDAG::Node & predicate, ActionsDAG::NodeRawConstPtrs key_nodes)
    {
        dag.getOutputs() = std::move(key_nodes);
        auto key_dag = dag.clone();
        key_dag.removeUnusedActions();
        auto key_names = key_dag.getNames();
        auto key_actions = std::make_shared<ExpressionActions>(std::move(key_dag));
        return KeyCondition(
            ActionsDAGWithInversionPushDown(&predicate, context, true), context, key_names, key_actions);
    }

    static void expectRangeMaskWithExactness(
        const KeyCondition & condition,
        const std::vector<FieldRef> & left,
        const std::vector<FieldRef> & right,
        const DataTypes & types,
        BoolMask expected)
    {
        const auto indices = condition.getUsedColumnsInOrder();
        std::vector<FieldRef> sparse_left;
        std::vector<FieldRef> sparse_right;
        DataTypes sparse_types;
        for (size_t index : indices)
        {
            sparse_left.push_back(left[index]);
            sparse_right.push_back(right[index]);
            sparse_types.push_back(types[index]);
        }

        std::vector<UInt8> equal_boundaries(left.size());
        for (size_t i = 0; i < left.size(); ++i)
            equal_boundaries[i] = Range::equals(left[i], right[i]);

        /// Saturated components are ignored by the caller and need not retain their initial value.
        for (BoolMask initial : {BoolMask(false, false), BoolMask::consider_only_can_be_true, BoolMask::consider_only_can_be_false})
        {
            SCOPED_TRACE(fmt::format("Initial mask: ({}, {})", initial.can_be_true, initial.can_be_false));
            const auto dense = condition.checkInRangeWithExactness(left.size(), left.data(), right.data(), types, initial);
            const auto sparse = condition.checkInRangeWithExactness(
                indices, sparse_left.data(), sparse_right.data(), sparse_types, equal_boundaries, initial);
            if (!initial.can_be_true)
            {
                EXPECT_EQ(dense.can_be_true, expected.can_be_true);
                EXPECT_EQ(sparse.can_be_true, expected.can_be_true);
            }
            if (!initial.can_be_false)
            {
                EXPECT_EQ(dense.can_be_false, expected.can_be_false);
                EXPECT_EQ(sparse.can_be_false, expected.can_be_false);
            }
        }
    }

    ContextMutablePtr context;
    ActionsDAG dag;
};

TEST_F(KeyConditionAtomGroups, AddedBoundUpdatesExactnessWithoutChangingCopies)
{
    const auto type = std::make_shared<DataTypeUInt64>();
    const auto & x = dag.addInput("x", type);
    const auto & remainder = addFunction("modulo", {&x, &addConstant(3)});
    const auto & predicate = addFunction("equals", {&x, &addConstant(5)});
    auto condition = makeCondition(predicate, {&remainder, &x});
    const auto copy = condition;

    ASSERT_TRUE(condition.addCondition(x.result_name, Range::createLeftBounded(UInt64(6), true)));
    ASSERT_TRUE(condition.canCheckExactness());
    ASSERT_TRUE(copy.canCheckExactness());
    expectRangeMaskWithExactness(condition, {UInt64(2), UInt64(5)}, {UInt64(2), UInt64(5)}, {type, type}, BoolMask(false, true));
    expectRangeMaskWithExactness(copy, {UInt64(2), UInt64(5)}, {UInt64(2), UInt64(5)}, {type, type}, BoolMask(true, false));
}

}
}
