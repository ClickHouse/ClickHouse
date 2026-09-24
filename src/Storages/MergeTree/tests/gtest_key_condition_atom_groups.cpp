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

TEST_F(KeyConditionAtomGroups, SparseEvaluatorPreservesRangeMasks)
{
    const auto type = std::make_shared<DataTypeUInt64>();
    const auto & y = dag.addInput("y", type);
    const auto & x = dag.addInput("x", type);
    const auto & successor = addFunction("plus", {&x, &addConstant(1)});
    const auto & remainder = addFunction("modulo", {&x, &addConstant(3)});
    const auto & equals = addFunction("equals", {&x, &addConstant(5)});
    const auto & not_equals = addFunction("notEquals", {&x, &addConstant(5)});
    const auto & less = addFunction("less", {&y, &addConstant(3)});
    const auto & conjunction = addFunction("and", {&less, &equals});
    const auto & disjunction = addFunction("or", {&less, &equals});
    const auto & negation = addFunction("not", {&conjunction});

    for (const auto * predicate : {&equals, &not_equals, &conjunction, &disjunction, &negation})
    {
        auto condition = makeCondition(*predicate, {&y, &x, &successor, &remainder});
        const auto original_description = condition.toString();
        SCOPED_TRACE(original_description);

        /// Each layout models a different part. Suffix entries beyond the loaded prefix are
        /// supplied by partition bounds, so they must remain available to the evaluator.
        for (const auto & indices : std::vector<std::vector<size_t>>{{}, {0}, {0, 1}, {0, 2}, {0, 3}, {0, 1, 2, 3}})
        {
            for (bool exactness : {false, true})
            {
                KeyCondition::SparseRangeEvaluator evaluator(condition, indices, exactness);
                for (size_t loaded_prefix : {1, 4})
                {
                    for (UInt64 value : {4, 5, 6})
                    {
                        for (UInt64 first_key : {0, 3})
                        {
                            const std::vector<FieldRef> left{Field(first_key), Field(value), Field(value + 1), Field(value % 3)};
                            const std::vector<FieldRef> right{Field(first_key + 1), Field(value), Field(value + 1), Field(value % 3)};
                            Hyperrectangle bounds{Range::createWholeUniverseTypeAware(type), Range(left[1]), Range(left[2]), Range(left[3])};
                            std::vector<UInt8> equal_boundaries(loaded_prefix, true);
                            equal_boundaries[0] = false;
                            std::vector<FieldRef> sparse_left;
                            std::vector<FieldRef> sparse_right;
                            DataTypes sparse_types;
                            for (size_t index : indices)
                            {
                                sparse_left.push_back(left[index]);
                                sparse_right.push_back(right[index]);
                                sparse_types.push_back(type);
                            }

                            for (BoolMask initial : {BoolMask(false, false), BoolMask::consider_only_can_be_true, BoolMask::consider_only_can_be_false})
                            {
                                const auto expected = exactness
                                    ? condition.checkInRangeWithExactness(
                                        indices, sparse_left.data(), sparse_right.data(), sparse_types, equal_boundaries, initial, &bounds)
                                    : condition.checkInRange(
                                        indices, sparse_left.data(), sparse_right.data(), sparse_types, equal_boundaries, initial, &bounds);
                                const auto actual = evaluator.checkInRange(
                                    sparse_left.data(), sparse_right.data(), sparse_types, equal_boundaries, initial, &bounds);
                                EXPECT_EQ(actual, expected);
                            }
                        }
                    }
                }
            }
        }
        EXPECT_EQ(condition.toString(), original_description);
    }
}

TEST_F(KeyConditionAtomGroups, ExtractedLaterAtomStartsItsOwnGroup)
{
    const auto type = std::make_shared<DataTypeUInt64>();
    const auto & x = dag.addInput("x", type);
    const auto & remainder = addFunction("modulo", {&x, &addConstant(3)});
    const auto & predicate = addFunction("equals", {&x, &addConstant(5)});
    auto condition = makeCondition(predicate, {&x, &remainder});

    ASSERT_EQ(condition.getRPN().size(), 3);
    ASSERT_TRUE(condition.getRPN()[1].continues_multi_atom_group);
    ASSERT_TRUE(condition.getRPN()[1].relaxed);

    std::vector<std::pair<size_t, std::shared_ptr<KeyCondition>>> columns;
    condition.extractSingleColumnConditions(columns, nullptr);
    ASSERT_EQ(columns.size(), 2);
    ASSERT_EQ(columns[1].first, 1);
    const auto & extracted = *columns[1].second;
    ASSERT_EQ(extracted.getRPN().size(), 1);
    EXPECT_FALSE(extracted.getRPN().front().continues_multi_atom_group);
    EXPECT_TRUE(extracted.isRelaxed());
    EXPECT_FALSE(extracted.canCheckExactness());

    /// The remaining necessary condition still prunes, but cannot prove that the leaf matches.
    const auto matching = extracted.checkInHyperrectangle(
        {Range(Field(UInt64(5))), Range(Field(UInt64(2)))}, {type, type});
    EXPECT_TRUE(matching.can_be_true);
    EXPECT_TRUE(matching.can_be_false);
    const auto excluded = extracted.checkInHyperrectangle(
        {Range(Field(UInt64(5))), Range(Field(UInt64(1)))}, {type, type});
    EXPECT_FALSE(excluded.can_be_true);
}

TEST_F(KeyConditionAtomGroups, IndependentConjunctsDoNotCoverEachOthersRelaxedAtoms)
{
    const auto type = std::make_shared<DataTypeUInt64>();
    const auto & x = dag.addInput("x", type);
    const auto & y = dag.addInput("y", type);
    const auto & remainder = addFunction("modulo", {&x, &addConstant(3)});
    const auto & remainder_predicate = addFunction("greater", {&remainder, &addConstant(0)});
    const auto & y_predicate = addFunction("equals", {&y, &addConstant(1)});
    const auto & x_predicate = addFunction("equals", {&x, &addConstant(5)});
    /// Equal-sized conjunctions retain their order during decomposition, placing the independent
    /// remainder predicate before the relaxed remainder atom generated by `x = 5`.
    const auto & first_conjunct = addFunction("and", {&remainder_predicate, &y_predicate});
    const auto & predicate = addFunction("and", {&first_conjunct, &x_predicate});
    auto condition = makeCondition(predicate, {&x, &remainder, &y});

    std::vector<std::pair<size_t, std::shared_ptr<KeyCondition>>> columns;
    condition.extractSingleColumnConditions(columns, nullptr);
    ASSERT_EQ(columns.size(), 3);
    ASSERT_EQ(columns[1].first, 1);
    const auto & extracted = *columns[1].second;
    ASSERT_EQ(extracted.getRPN().size(), 3);
    for (const auto & element : extracted.getRPN())
        EXPECT_FALSE(element.continues_multi_atom_group);

    /// The exact remainder predicate comes from a different leaf than the relaxed equality atom.
    /// It cannot supply that leaf's falsity information when the original `x` atom is absent.
    EXPECT_TRUE(extracted.isRelaxed());
    ASSERT_FALSE(extracted.canCheckExactness());
    const auto matching = extracted.checkInHyperrectangle(
        {Range(Field(UInt64(5))), Range(Field(UInt64(2))), Range(Field(UInt64(1)))}, {type, type, type});
    EXPECT_TRUE(matching.can_be_true);
    EXPECT_TRUE(matching.can_be_false);
    const auto excluded = extracted.checkInHyperrectangle(
        {Range(Field(UInt64(5))), Range(Field(UInt64(1))), Range(Field(UInt64(1)))}, {type, type, type});
    EXPECT_FALSE(excluded.can_be_true);
}

TEST_F(KeyConditionAtomGroups, ExtractedDisjunctionPreservesNestedGroups)
{
    const auto & x = dag.addInput("x", std::make_shared<DataTypeUInt64>());
    const auto & tuple_key = addFunction("tuple", {&x, &x});
    const auto & first_tuple = addFunction("tuple", {&addConstant(5), &addConstant(5)});
    const auto & second_tuple = addFunction("tuple", {&addConstant(6), &addConstant(6)});
    const auto & first_array = addFunction("array", {&first_tuple});
    const auto & second_array = addFunction("array", {&second_tuple});
    const auto & first_leaf = addFunction("has", {&first_array, &tuple_key});
    const auto & second_leaf = addFunction("has", {&second_array, &tuple_key});
    const auto & predicate = addFunction("or", {&first_leaf, &second_leaf});
    auto condition = makeCondition(predicate, {&tuple_key});

    /// Each membership leaf has a relaxed component atom and an exact packed-tuple atom for one key.
    const std::vector<bool> expected_markers{false, true, true, false, true, true, false};
    ASSERT_EQ(condition.getRPN().size(), expected_markers.size());
    for (size_t i = 0; i < expected_markers.size(); ++i)
        ASSERT_EQ(condition.getRPN()[i].continues_multi_atom_group, expected_markers[i]);

    std::vector<std::pair<size_t, std::shared_ptr<KeyCondition>>> columns;
    condition.extractSingleColumnConditions(columns, nullptr);
    ASSERT_EQ(columns.size(), 1);
    const auto & extracted = *columns.front().second;
    ASSERT_EQ(extracted.getRPN().size(), expected_markers.size());
    for (size_t i = 0; i < expected_markers.size(); ++i)
        EXPECT_EQ(extracted.getRPN()[i].continues_multi_atom_group, expected_markers[i]);

    EXPECT_TRUE(extracted.canCheckExactness());
    for (UInt64 value : {5, 6})
        expectRangeMaskWithExactness(
            extracted, {Tuple{value, value}}, {Tuple{value, value}}, {tuple_key.result_type}, BoolMask(true, false));
    expectRangeMaskWithExactness(
        extracted, {Tuple{UInt64(7), UInt64(7)}}, {Tuple{UInt64(7), UInt64(7)}}, {tuple_key.result_type}, BoolMask(false, true));
}

TEST_F(KeyConditionAtomGroups, ExactSiblingSuppliesFalsityWithoutLosingPruning)
{
    const auto type = std::make_shared<DataTypeUInt64>();
    const auto & unused = dag.addInput("unused", type);
    const auto & x = dag.addInput("x", type);
    const auto & remainder = addFunction("modulo", {&x, &addConstant(3)});
    const auto & predicate = addFunction("equals", {&x, &addConstant(5)});
    auto condition = makeCondition(predicate, {&unused, &remainder, &x});

    ASSERT_TRUE(condition.isRelaxed());
    ASSERT_TRUE(condition.canCheckExactness());
    ASSERT_EQ(condition.getUsedColumnsInOrder(), (std::vector<size_t>{1, 2}));

    /// The exact equality proves the point matches even though the remainder atom is relaxed.
    const std::vector<FieldRef> point{UInt64(0), UInt64(2), UInt64(5)};
    EXPECT_EQ(condition.checkInRange(point.size(), point.data(), point.data(), {type, type, type}), BoolMask(true, true));
    expectRangeMaskWithExactness(condition, point, point, {type, type, type}, BoolMask(true, false));

    /// The remainder excludes this range even though the exact equality overlaps its `x` bounds.
    expectRangeMaskWithExactness(condition,
        {UInt64(0), UInt64(1), NEGATIVE_INFINITY}, {UInt64(0), UInt64(1), POSITIVE_INFINITY},
        {type, type, type}, BoolMask(false, true));
}

TEST_F(KeyConditionAtomGroups, UncoveredRelaxedLeafPreventsExactness)
{
    const auto type = std::make_shared<DataTypeUInt64>();
    const auto & x = dag.addInput("x", type);
    const auto & y = dag.addInput("y", type);
    const auto & remainder_x = addFunction("modulo", {&x, &addConstant(3)});
    const auto & remainder_y = addFunction("modulo", {&y, &addConstant(3)});
    const auto & x_predicate = addFunction("equals", {&x, &addConstant(5)});
    const auto & y_predicate = addFunction("equals", {&y, &addConstant(2)});
    const auto & predicate = addFunction("and", {&x_predicate, &y_predicate});
    auto condition = makeCondition(predicate, {&remainder_x, &x, &remainder_y});

    /// The exact atom for `x` covers its remainder sibling, but cannot prove the predicate on `y`.
    ASSERT_FALSE(condition.canCheckExactness());
    expectRangeMaskWithExactness(condition,
        {UInt64(2), UInt64(5), UInt64(2)}, {UInt64(2), UInt64(5), UInt64(2)},
        {type, type, type}, BoolMask(true, true));
    expectRangeMaskWithExactness(condition,
        {UInt64(2), UInt64(5), UInt64(1)}, {UInt64(2), UInt64(5), UInt64(1)},
        {type, type, type}, BoolMask(false, true));
}

TEST_F(KeyConditionAtomGroups, MultiValueSetKeepsExactnessConservative)
{
    const auto & x = dag.addInput("x", std::make_shared<DataTypeUInt64>());
    const auto & tuple_key = addFunction("tuple", {&x, &x});
    const auto & first_tuple = addFunction("tuple", {&addConstant(5), &addConstant(5)});
    const auto & second_tuple = addFunction("tuple", {&addConstant(6), &addConstant(6)});
    const auto & values = addFunction("array", {&first_tuple, &second_tuple});
    const auto & predicate = addFunction("has", {&values, &tuple_key});
    auto condition = makeCondition(predicate, {&tuple_key});

    /// The packed-tuple atom covers the relaxed component atom, but its two-element set keeps
    /// the condition ineligible for exactness under the conservative `isRelaxed` contract.
    ASSERT_EQ(condition.getRPN().size(), 3);
    ASSERT_TRUE(condition.getRPN()[0].relaxed);
    ASSERT_FALSE(condition.getRPN()[1].relaxed);
    ASSERT_EQ(condition.getRPN()[1].function, KeyCondition::RPNElement::FUNCTION_IN_SET);
    ASSERT_EQ(condition.getRPN()[1].set_index->size(), 2);
    ASSERT_FALSE(condition.canCheckExactness());

    for (UInt64 value : {5, 6})
        expectRangeMaskWithExactness(
            condition, {Tuple{value, value}}, {Tuple{value, value}}, {tuple_key.result_type}, BoolMask(true, true));
    expectRangeMaskWithExactness(
        condition, {Tuple{UInt64(7), UInt64(7)}}, {Tuple{UInt64(7), UInt64(7)}}, {tuple_key.result_type}, BoolMask(false, true));
}

TEST_F(KeyConditionAtomGroups, ExactSingleAtomNeedsNoDerivedCondition)
{
    const auto type = std::make_shared<DataTypeUInt64>();
    const auto & x = dag.addInput("x", type);
    const auto & predicate = addFunction("equals", {&x, &addConstant(5)});
    auto condition = makeCondition(predicate, {&x});

    ASSERT_FALSE(condition.isRelaxed());
    ASSERT_TRUE(condition.canCheckExactness());
    expectRangeMaskWithExactness(condition, {UInt64(5)}, {UInt64(5)}, {type}, BoolMask(true, false));
    expectRangeMaskWithExactness(condition, {UInt64(6)}, {UInt64(6)}, {type}, BoolMask(false, true));
}

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
