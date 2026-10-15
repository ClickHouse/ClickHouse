#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/PreparedSets.h>
#include <Storages/MergeTree/MergeTreeIndexBloomFilter.h>
#include <Storages/MergeTree/RPNBuilder.h>

using namespace DB;

namespace
{

struct MatchOptions
{
    UInt64 query_value = 17;
    UInt64 index_value = 17;
    bool fold_index = false;
    bool non_deterministic_index_constant = false;
    bool input_array_element = false;
    bool alias_array_constant = false;
    bool mark_alias_secret = false;
    bool mark_alias_runtime_filter = false;
    size_t constant_array_concat_arguments = 0;
};

struct MatchResult
{
    bool can_prune = false;
    bool folded_constant_is_non_deterministic = false;
};

const ActionsDAG::Node & addArrayLiteral(
    ActionsDAG & dag,
    const ContextPtr & context,
    const DataTypePtr & uint64_type,
    UInt64 value,
    bool non_deterministic,
    bool input_element,
    bool alias,
    bool mark_secret,
    bool mark_runtime_filter)
{
    const ActionsDAG::Node * element = nullptr;
    if (input_element)
        element = &dag.addInput("needle", uint64_type);
    else
    {
        const auto & constant = dag.addColumn(
            uint64_type->createColumnConst(0, Field(value)), uint64_type, "needle", !non_deterministic);
        element = &constant;
    }

    const auto & array = dag.addFunction(FunctionFactory::instance().get("array", context), {element}, "literal_array");
    if (!alias)
        return array;

    const auto & alias_node = dag.addAlias(array, "array_alias");
    auto & mutable_alias = const_cast<ActionsDAG::Node &>(alias_node);
    mutable_alias.is_masked_secret = mark_secret;
    mutable_alias.is_runtime_filter_id = mark_runtime_filter;
    return alias_node;
}

MatchResult canPruneArrayJoinIn(const MatchOptions & options)
{
    tryRegisterFunctions();
    const auto context = getContext().context;
    const auto uint64_type = std::make_shared<DataTypeUInt64>();
    const auto array_type = std::make_shared<DataTypeArray>(uint64_type);

    ActionsDAG query_dag;
    ActionsDAG index_dag;

    auto add_index_expression = [&](ActionsDAG & dag, UInt64 value, bool is_query) -> const ActionsDAG::Node &
    {
        const ActionsDAG::Node * array_argument = nullptr;
        if (options.constant_array_concat_arguments)
        {
            const auto & empty_array = dag.addColumn(
                array_type->createColumnConst(0, Field(Array{})), array_type, "empty_array");
            std::vector<const ActionsDAG::Node *> children(options.constant_array_concat_arguments, &empty_array);
            return dag.addFunction(
                FunctionFactory::instance().get("arrayConcat", context), std::move(children), "wide_constant_array");
        }

        const auto & input = dag.addInput("a", array_type);
        array_argument = &addArrayLiteral(
            dag,
            context,
            uint64_type,
            value,
            !is_query && options.non_deterministic_index_constant,
            options.input_array_element,
            options.alias_array_constant,
            is_query && options.mark_alias_secret,
            is_query && options.mark_alias_runtime_filter);
        return dag.addFunction(
            FunctionFactory::instance().get("arrayConcat", context), {&input, array_argument}, "array_index_expression");
    };

    const auto & query_array = add_index_expression(query_dag, options.query_value, true);
    const auto & index_array = add_index_expression(index_dag, options.index_value, false);
    index_dag.getOutputs().push_back(&index_array);

    MatchResult result;
    if (options.fold_index)
    {
        index_dag.removeUnusedActions(false);
        if (options.non_deterministic_index_constant && !options.constant_array_concat_arguments)
        {
            const auto * folded = index_array.children.at(1);
            result.folded_constant_is_non_deterministic = folded->type == ActionsDAG::ActionType::COLUMN
                && !folded->is_deterministic_constant;
        }
    }

    const auto column_name = RPNBuilderTreeNode(&index_array, context).getColumnName();
    Block header{{array_type->createColumn(), array_type, column_name}};

    const auto & array_join = query_dag.addArrayJoin(query_array, "array_join");
    auto set_values = ColumnUInt64::create();
    set_values->insertValue(17);
    auto future_set = std::make_shared<FutureSetFromTuple>(
        FutureSet::Hash{}, nullptr, ColumnsWithTypeAndName{{std::move(set_values), uint64_type, "set_value"}}, false, SizeLimits{});
    const auto & set_node = query_dag.addColumn(
        ColumnConst::create(ColumnSet::create(1, std::move(future_set)), 0), std::make_shared<DataTypeSet>(), "set");
    const auto & predicate = query_dag.addFunction(
        FunctionFactory::instance().get("in", context), {&array_join, &set_node}, "predicate");

    MergeTreeIndexConditionBloomFilter condition(
        &predicate, context, header, index_dag, 4, {}, {});
    result.can_prune = !condition.alwaysUnknownOrTrue();
    return result;
}

}

TEST(MergeTreeIndexBloomFilter, MatchesSafeFixedWidthArrayConstantsOnly)
{
    /// Two live FUNCTION leaves compare by their cached fixed-width array payloads.
    EXPECT_TRUE(canPruneArrayJoinIn({}).can_prune);

    /// A normally folded index-side FUNCTION becomes a COLUMN with the same payload.
    MatchOptions folded;
    folded.fold_index = true;
    EXPECT_TRUE(canPruneArrayJoinIn(folded).can_prune);

    /// Different payloads, including the FUNCTION/COLUMN shape, must remain unknown.
    MatchOptions mismatch;
    mismatch.index_value = 18;
    mismatch.fold_index = true;
    EXPECT_FALSE(canPruneArrayJoinIn(mismatch).can_prune);

    /// Folding preserves a non-deterministic descendant marker on the resulting COLUMN.
    MatchOptions non_deterministic;
    non_deterministic.fold_index = true;
    non_deterministic.non_deterministic_index_constant = true;
    const auto non_deterministic_result = canPruneArrayJoinIn(non_deterministic);
    EXPECT_TRUE(non_deterministic_result.folded_constant_is_non_deterministic);
    EXPECT_FALSE(non_deterministic_result.can_prune);

    /// Visible alias tags are checked before aliases are normalized away.
    MatchOptions secret_alias;
    secret_alias.alias_array_constant = true;
    secret_alias.mark_alias_secret = true;
    EXPECT_FALSE(canPruneArrayJoinIn(secret_alias).can_prune);

    MatchOptions runtime_filter_alias;
    runtime_filter_alias.alias_array_constant = true;
    runtime_filter_alias.mark_alias_runtime_filter = true;
    EXPECT_FALSE(canPruneArrayJoinIn(runtime_filter_alias).can_prune);

    /// A live input child has no cached constant payload, so generic function matching stays strict.
    MatchOptions input_child;
    input_child.input_array_element = true;
    EXPECT_FALSE(canPruneArrayJoinIn(input_child).can_prune);

    /// Repeated empty fixed-width arrays exceed the shared comparison budget without exceeding payload limits.
    MatchOptions over_budget;
    over_budget.constant_array_concat_arguments = 1500;
    EXPECT_FALSE(canPruneArrayJoinIn(over_budget).can_prune);
}
