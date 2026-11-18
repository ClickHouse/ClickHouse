#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/hasNullable.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/tuple.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/PreparedSets.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Storages/StorageValues.h>
#include <Common/SipHash.h>

namespace DB::QueryPlanOptimizations
{

/// Filters a `MergeTree` side of an equi-join by the join keys of the other side when the other side
/// is a block that is in memory while the query is planned, e.g. the block inserted into the source
/// table of a materialized view. `block JOIN t ON block.k = t.k` reads `t` with `k IN (keys of the block)`,
/// as if the keys were written as a literal list, so `t` is pruned by its primary key.

namespace
{

/// A column of a source step, which is the child `child_idx` of `parent`
struct SourceColumn
{
    QueryPlan::Node * parent = nullptr;
    size_t child_idx = 0;
    String name;

    [[nodiscard]] QueryPlan::Node * node() const { return parent->children[child_idx]; }
};

/// The name that the output `name` of a step with `dag` has in the step's input, if the step only passes it on
std::optional<String> findNameBelow(const ActionsDAG & dag, const String & name)
{
    const auto * node = dag.tryFindInOutputs(name);
    /// The step does not compute the column, it comes from the input as is
    if (!node)
        return name;
    while (node->type == ActionsDAG::ActionType::ALIAS)
        node = node->children.front();
    if (node->type != ActionsDAG::ActionType::INPUT)
        return {};
    return node->result_name;
}

/// Follows the column `name` of a child of `parent` down to the step that reads it, through the steps that
/// pass its values on unchanged. Such a step may drop or repeat rows, but every value above it is a value below it.
/// Expression and filter steps are the ones the index analysis walks up through from a read.
std::optional<SourceColumn> findSourceColumn(QueryPlan::Node & parent, size_t child_idx, String name, bool through_joins)
{
    SourceColumn source{&parent, child_idx, std::move(name)};
    while (!source.node()->children.empty())
    {
        const auto * step = source.node()->step.get();
        std::optional<String> name_below;
        size_t next_child_idx = 0;

        if (const auto * expression_step = typeid_cast<const ExpressionStep *>(step))
        {
            if (!expression_step->getExpression().hasArrayJoin())
                name_below = findNameBelow(expression_step->getExpression(), source.name);
        }
        else if (const auto * filter_step = typeid_cast<const FilterStep *>(step))
        {
            name_below = findNameBelow(filter_step->getExpression(), source.name);
        }
        else if (const auto * join_step = typeid_cast<const JoinStepLogical *>(step); join_step && through_joins)
        {
            const auto kind = join_step->getJoinOperator().kind;
            for (const auto & output : join_step->getOutputActions())
            {
                if (output.getColumnName() != source.name)
                    continue;

                /// An outer join fills the rows that have no match on the other side with default values, which are not in the input
                const auto input = output.resolveAliases();
                if (input.getNode()->type != ActionsDAG::ActionType::INPUT)
                    break;
                if (input.fromLeft() && (isInnerOrLeft(kind) || isCrossOrComma(kind)))
                    next_child_idx = 0;
                else if (input.fromRight() && (isInnerOrRight(kind) || isCrossOrComma(kind)))
                    next_child_idx = 1;
                else
                    break;

                name_below = input.getColumnName();
                break;
            }
        }

        if (!name_below)
            return {};

        source = SourceColumn{source.node(), next_child_idx, std::move(*name_below)};
    }

    return source;
}

/// `IN` finds the values that are identical. A join may compare its keys instead, depending on the algorithm,
/// and that is not the same for floating-point numbers: `0` and `-0`, `NaN`.
bool isIdentityTheSameAsEquality(const DataTypePtr & type)
{
    return type->isValueUnambiguouslyRepresentedInContiguousMemoryRegion() && !hasTypeThatCanContainFloat(type);
}

DataTypePtr removeNullableAndLowCardinality(const DataTypePtr & type) { return removeNullable(removeLowCardinality(type)); }

bool filterSideByKeysOfBlock(QueryPlan::Node & join_node, size_t block_side, QueryPlan::Nodes & nodes, size_t max_rows_in_block)
{
    const auto & join_step = typeid_cast<const JoinStepLogical &>(*join_node.step);
    const size_t filtered_side = 1 - block_side;

    const Block * block = nullptr;
    ColumnsWithTypeAndName block_keys;
    std::optional<SourceColumn> filtered_read;
    Names filtered_keys;

    for (const auto & predicate : join_step.getJoinOperator().expression)
    {
        auto [predicate_op, lhs, rhs] = predicate.asBinaryPredicate();
        if (predicate_op != JoinConditionOperator::Equals)
            continue;

        if (lhs.fromRight() && rhs.fromLeft())
            std::swap(lhs, rhs);
        else if (!lhs.fromLeft() || !rhs.fromRight())
            continue;

        /// Only the keys that are columns of the inputs: nothing is calculated from the block here
        const auto block_key = (block_side == 0 ? lhs : rhs).resolveAliases();
        const auto filtered_key = (block_side == 0 ? rhs : lhs).resolveAliases();
        if (block_key.getNode()->type != ActionsDAG::ActionType::INPUT || filtered_key.getNode()->type != ActionsDAG::ActionType::INPUT)
            continue;

        const auto block_column = findSourceColumn(join_node, block_side, block_key.getColumnName(), /*through_joins=*/true);
        const auto * read_from_storage
            = block_column ? typeid_cast<const ReadFromStorageStep *>(block_column->node()->step.get()) : nullptr;
        const auto * storage_values
            = read_from_storage ? typeid_cast<const StorageValues *>(read_from_storage->getStorage().get()) : nullptr;
        const auto * key_block = storage_values ? storage_values->tryGetBlock() : nullptr;
        if (!key_block || key_block->rows() > max_rows_in_block)
            continue;

        /// All keys are taken from one block, so that its rows make the tuples of a multi-column set
        if (block && block != key_block)
            continue;

        auto block_key_column = key_block->findColumnOrSubcolumnByName(block_column->name);
        if (!block_key_column)
            continue;

        /// The join converts keys of different types to a common type, which `IN` does differently
        const auto key_type = removeNullableAndLowCardinality(filtered_key.getType());
        if (!key_type->equals(*removeNullableAndLowCardinality(block_key_column->type)) || !isIdentityTheSameAsEquality(key_type))
            continue;

        auto filtered_column = findSourceColumn(join_node, filtered_side, filtered_key.getColumnName(), /*through_joins=*/false);
        const auto * read_from_merge_tree
            = filtered_column ? typeid_cast<const ReadFromMergeTree *>(filtered_column->node()->step.get()) : nullptr;
        if (!read_from_merge_tree)
            continue;

        const auto & primary_key_columns = read_from_merge_tree->getStorageMetadata()->getPrimaryKey().column_names;
        if (!std::ranges::contains(primary_key_columns, filtered_column->name)
            || std::ranges::contains(filtered_keys, filtered_column->name))
            continue;

        block = key_block;
        block_keys.push_back(std::move(*block_key_column));
        filtered_keys.push_back(filtered_column->name);
        filtered_read = std::move(filtered_column);
    }

    if (block_keys.empty())
        return false;

    /// The set is named by its content, like the set of a literal list is named by the literals
    SipHash hash;
    for (const auto & key : block_keys)
    {
        hash.update(key.type->getName());
        key.column->updateHashFast(hash);
    }
    const auto set_hash = getSipHash128AsPair(hash);

    /// An equality of join keys is never true for NULL, and a set without `transform_null_in` skips the rows with NULL
    auto future_set
        = std::make_shared<FutureSetFromTuple>(set_hash, nullptr, std::move(block_keys), /*transform_null_in_=*/false, SizeLimits{});

    /// The filter is put right above the read, as a filter on the table written in the query would be after it is pushed down
    auto * read_node = filtered_read->node();
    const auto & read_header = read_node->step->getOutputHeader();
    ActionsDAG filter_dag(read_header->getColumnsWithTypeAndName());

    ActionsDAG::NodeRawConstPtrs key_nodes;
    for (const auto & key : filtered_keys)
        key_nodes.push_back(&filter_dag.findInOutputs(key));

    const auto * in_lhs_arg = key_nodes.front();
    if (key_nodes.size() > 1)
    {
        FunctionOverloadResolverPtr tuple_resolver = std::make_shared<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionTuple>());
        in_lhs_arg = &filter_dag.addFunction(tuple_resolver, std::move(key_nodes), {});
    }

    const auto & in_rhs_arg = filter_dag.addColumn(
        ColumnConst::create(ColumnSet::create(1, std::move(future_set)), 0),
        std::make_shared<DataTypeSet>(),
        "__set_" + toString(set_hash));

    const auto & in_node = filter_dag.addFunction(FunctionFactory::instance().get("in", nullptr), {in_lhs_arg, &in_rhs_arg}, {});
    filter_dag.getOutputs().push_back(&in_node);
    auto filter_column_name = in_node.result_name;

    auto & filter_node = nodes.emplace_back();
    filter_node.step
        = std::make_unique<FilterStep>(read_header, std::move(filter_dag), std::move(filter_column_name), /*remove_filter_column_=*/true);
    filter_node.step->setStepDescription("Filter by JOIN keys of the other side");
    filter_node.children = {read_node};
    filtered_read->parent->children[filtered_read->child_idx] = &filter_node;
    return true;
}

}

bool tryFilterJoinByKeysOfBlock(QueryPlan::Node & node, QueryPlan::Nodes & nodes, size_t max_rows_in_block)
{
    const auto * join_step = typeid_cast<const JoinStepLogical *>(node.step.get());
    if (!join_step || node.children.size() != 2)
        return false;

    /// The filter takes whole groups of rows with the same key, so it changes neither the row that `Any`
    /// picks nor whether `Semi` finds a match, see also `tryPropagatePredicateAcrossEquiJoin`
    const auto & join_operator = join_step->getJoinOperator();
    if (join_operator.strictness != JoinStrictness::All && join_operator.strictness != JoinStrictness::Any
        && join_operator.strictness != JoinStrictness::Semi && join_operator.strictness != JoinStrictness::RightAny)
        return false;

    /// A side whose rows the join keeps without a match cannot be filtered
    bool filtered = false;
    if (isInnerOrLeft(join_operator.kind))
        filtered |= filterSideByKeysOfBlock(node, /*block_side=*/0, nodes, max_rows_in_block);
    if (isInnerOrRight(join_operator.kind))
        filtered |= filterSideByKeysOfBlock(node, /*block_side=*/1, nodes, max_rows_in_block);
    return filtered;
}

}
