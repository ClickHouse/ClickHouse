#include <Processors/QueryPlan/QueryPlanShapeHash.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/Collator.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFunction.h>
#include <Common/SipHash.h>
#include <Common/StackWithMemoryTracking.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/WindowDescription.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/IntersectOrExceptStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageLog.h>
#include <Storages/StorageSnapshot.h>

#include <fmt/format.h>

namespace DB
{

namespace
{

/// Stands in for a column a step names but its input header does not have.
constexpr size_t COLUMN_NOT_IN_HEADER = std::numeric_limits<size_t>::max();

/// Every string goes in with its length, so that consecutive strings cannot run together: without it
/// the table `ab`.`c` and the table `a`.`bc` would hash alike.
void hashString(std::string_view value, SipHash & hash)
{
    hash.update(value.size());
    hash.update(value.data(), value.size());
}

void hashStorageID(const StorageID & storage_id, SipHash & hash)
{
    hashString(storage_id.database_name, hash);
    hashString(storage_id.table_name, hash);
}

/// The result type of a function, without the width of its numbers. A literal is typed by its value
/// -- `5` is a `UInt8` and `500` a `UInt16` -- and the width carries over into the result of every
/// function above it, so `a + 5` is a `UInt16` and `a + 500` a `UInt32`. Only whether a number is
/// signed, unsigned or floating point is kept, which is enough to keep `CAST(x, 'Int32')` and
/// `CAST(x, 'String')` apart once their constant argument is erased, though not `CAST(x, 'Int32')`
/// and `CAST(x, 'Int64')`.
void hashResultType(const IDataType & type, SipHash & hash)
{
    const WhichDataType which(type);
    if (which.isUInt())
    {
        hashString("UInt", hash);
    }
    else if (which.isInt())
    {
        hashString("Int", hash);
    }
    else if (which.isFloat())
    {
        hashString("Float", hash);
    }
    else if (const auto * nullable = typeid_cast<const DataTypeNullable *>(&type))
    {
        hashString("Nullable", hash);
        hashResultType(*nullable->getNestedType(), hash);
    }
    else if (const auto * low_cardinality = typeid_cast<const DataTypeLowCardinality *>(&type))
    {
        hashString("LowCardinality", hash);
        hashResultType(*low_cardinality->getDictionaryType(), hash);
    }
    else if (const auto * array = typeid_cast<const DataTypeArray *>(&type))
    {
        hashString("Array", hash);
        hashResultType(*array->getNestedType(), hash);
    }
    else if (const auto * tuple = typeid_cast<const DataTypeTuple *>(&type))
    {
        hashString("Tuple", hash);
        hash.update(tuple->getElements().size());
        for (const auto & element : tuple->getElements())
            hashResultType(*element, hash);
    }
    else
    {
        hashString(type.getName(), hash);
    }
}

/// A column a step refers to by name. Within a plan the name is the result name of the expression
/// that produced the column, which carries the literals of that expression, so the column is
/// identified by its position in the header instead.
///
/// Without a header the names are hashed as they are, which is only right where they are names of
/// storage columns -- the filters a source step applies while reading.
void hashColumn(const String & name, const Block * header, SipHash & hash)
{
    if (!header)
    {
        hashString(name, hash);
        return;
    }

    hash.update(header->findPositionByName(name).value_or(COLUMN_NOT_IN_HEADER));
}

void hashColumns(const Names & names, const Block * header, SipHash & hash)
{
    hash.update(names.size());
    for (const auto & name : names)
        hashColumn(name, header, hash);
}

void hashSortDescription(const SortDescription & description, const Block * header, SipHash & hash)
{
    hash.update(description.size());
    for (const auto & column : description)
    {
        hashColumn(column.column_name, header, hash);
        hash.update(column.direction);
        hash.update(column.nulls_direction);
        hash.update(column.with_fill);
        if (column.collator)
            hashString(column.collator->getLocale(), hash);
    }
}

/// The shape of an expression: which operations, in what arrangement, over which input columns.
///
/// Unlike `ActionsDAG::updateHash`, which has to tell two constants apart, this hashes no constant
/// and no name:
///  - a `COLUMN` node hashes as "a constant", without its value and without its type, since `5` and
///    `500` are a `UInt8` and a `UInt16`;
///  - no `result_name` is hashed, because a constant's name is its value and is part of the name of
///    every node above it;
///  - an `INPUT` is identified by its position in `input_header`.
/// The result types of functions are kept, without the width of their numbers, see `hashResultType`.
/// The body of a lambda is hashed as well, see `hashLambda`.
void hashActionsDAG(const ActionsDAG & dag, const Block * input_header, SipHash & hash);

/// The body of a lambda, which is not among the children of the node that holds it. Its inputs are
/// the lambda's arguments and the columns it captures, identified by their position among the
/// body's inputs, since the captured columns are named after their expressions.
void hashLambda(const ActionsDAG & body, SipHash & hash)
{
    Block body_header;
    for (const auto * input : body.getInputs())
        body_header.insert({nullptr, input->result_type, input->result_name});
    hashActionsDAG(body, &body_header, hash);
}

void hashActionsDAG(const ActionsDAG & dag, const Block * input_header, SipHash & hash)
{
    using Node = ActionsDAG::Node;
    using ActionType = ActionsDAG::ActionType;

    /// Nodes are numbered in the order they are first finished, which depends only on the shape, so
    /// a node shared by several parents hashes as a reference to its number rather than again.
    UnorderedMapWithMemoryTracking<const Node *, size_t> numbers;

    const auto hash_node = [&](const Node & node)
    {
        hash.update(node.type);
        switch (node.type)
        {
            case ActionType::INPUT:
                hashColumn(node.result_name, input_header, hash);
                break;
            case ActionType::FUNCTION:
                hashString(node.function_base->getName(), hash);
                hashResultType(*node.result_type, hash);
                /// A lambda with a captured column, whose children are the captured columns.
                if (const auto * function_capture = typeid_cast<const FunctionCapture *>(node.function_base.get()))
                    hashLambda(function_capture->getAcionsDAG(), hash);
                break;
            case ActionType::PLACEHOLDER:
                hashString(node.result_type->getName(), hash);
                break;
            case ActionType::COLUMN:
                /// A lambda that captures no column is folded into a constant, which holds the
                /// lambda rather than a literal.
                if (node.column && node.result_type && WhichDataType(node.result_type).isFunction())
                {
                    const IColumn * column = node.column.get();
                    if (const auto * column_const = typeid_cast<const ColumnConst *>(column))
                        column = &column_const->getDataColumn();
                    if (const auto * column_function = typeid_cast<const ColumnFunction *>(column))
                    {
                        if (const auto * function_expression = typeid_cast<const FunctionExpression *>(column_function->getFunction().get()))
                            hashLambda(function_expression->getAcionsDAG(), hash);
                    }
                }
                break;
            case ActionType::ALIAS:
            case ActionType::ARRAY_JOIN:
                break;
        }

        hash.update(node.children.size());
        for (const auto * child : node.children)
            hash.update(numbers.at(child));
    };

    struct Frame
    {
        const Node * node = nullptr;
        size_t next_child = 0;
    };

    hash.update(dag.getOutputs().size());
    for (const auto * output : dag.getOutputs())
    {
        StackWithMemoryTracking<Frame> stack;
        if (!numbers.contains(output))
            stack.push({.node = output});

        while (!stack.empty())
        {
            auto & frame = stack.top();
            if (frame.next_child == frame.node->children.size())
            {
                hash_node(*frame.node);
                numbers.emplace(frame.node, numbers.size());
                stack.pop();
                continue;
            }

            const auto * child = frame.node->children[frame.next_child++];
            if (!numbers.contains(child))
                stack.push({.node = child});
        }

        hash.update(numbers.at(output));
    }
}

/// The position of `name` among the outputs of `dag`: the filter column, named after its own
/// expression.
void hashOutputPosition(const ActionsDAG & dag, const String & name, SipHash & hash)
{
    const auto & outputs = dag.getOutputs();
    for (size_t i = 0; i < outputs.size(); ++i)
    {
        if (outputs[i]->result_name == name)
        {
            hash.update(i);
            return;
        }
    }
    hash.update(COLUMN_NOT_IN_HEADER);
}

void hashFilter(const ActionsDAG & dag, const String & filter_column_name, bool removes_filter_column, const Block * header, SipHash & hash)
{
    hashActionsDAG(dag, header, hash);
    hashOutputPosition(dag, filter_column_name, hash);
    hash.update(removes_filter_column);
}

void hashAggregates(const AggregateDescriptions & aggregates, const Block * header, SipHash & hash)
{
    hash.update(aggregates.size());
    for (const auto & aggregate : aggregates)
    {
        /// The name without the parameters, which are literals: `quantile(0.9)` and
        /// `quantile(0.5)` are the same choice.
        hashString(aggregate.function->getName(), hash);
        hashColumns(aggregate.argument_names, header, hash);
    }
}

void hashAggregatorParams(const Aggregator::Params & params, const Block * header, SipHash & hash)
{
    hashColumns(params.keys, header, hash);
    hashAggregates(params.aggregates, header, hash);
    hash.update(params.overflow_row);
    hash.update(params.only_merge);

    /// The strategies the plan optimization picks for an aggregation that feeds an `ORDER BY ...
    /// LIMIT` or a `HAVING`. Which one is used, and on which keys or aggregates, is a choice; the
    /// limits and the thresholds are literals.
    hash.update(params.bucket_top_k != 0);
    if (params.bucket_top_k)
    {
        hash.update(params.bucket_top_k_ascending);
        hash.update(params.bucket_top_k_rank_index);
    }

    hash.update(params.threshold_top_k.has_value());
    if (params.threshold_top_k)
    {
        hash.update(params.threshold_top_k->ascending);
        hash.update(params.threshold_top_k->aggregate_index);
        hash.update(params.threshold_top_k->bound);
    }

    hash.update(params.top_k.has_value());
    if (params.top_k)
    {
        hash.update(params.top_k->key_columns);
        hash.update(params.top_k->directions.size());
        for (int direction : params.top_k->directions)
            hash.update(direction);
        hash.update(params.top_k->nulls_directions.size());
        for (int nulls_direction : params.top_k->nulls_directions)
            hash.update(nulls_direction);
        hash.update(params.top_k->threshold_tracker != nullptr);
    }

    hash.update(params.having_prefilter_op);
    if (params.having_prefilter_op != Aggregator::Params::HavingPrefilterOp::Disabled)
        hash.update(params.having_prefilter_count_index);
}

void hashGroupingSets(const GroupingSetsParamsList & grouping_sets, const Block * header, SipHash & hash)
{
    hash.update(grouping_sets.size());
    for (const auto & grouping_set : grouping_sets)
    {
        hashColumns(grouping_set.used_keys, header, hash);
        hashColumns(grouping_set.missing_keys, header, hash);
    }
}

void hashJoin(const IJoin & join, const Block * left_header, const Block * right_header, SipHash & hash)
{
    const auto & table_join = join.getTableJoin();
    hashString(join.getName(), hash);
    hash.update(table_join.kind());
    hash.update(table_join.strictness());
    if (table_join.strictness() == JoinStrictness::Asof)
        hash.update(table_join.getAsofInequality());

    hash.update(table_join.getClauses().size());
    for (const auto & clause : table_join.getClauses())
    {
        hashColumns(clause.key_names_left, left_header, hash);
        hashColumns(clause.key_names_right, right_header, hash);
        VectorWithMemoryTracking<size_t> null_safe(clause.nullsafe_compare_key_indexes.begin(), clause.nullsafe_compare_key_indexes.end());
        std::ranges::sort(null_safe);
        for (size_t index : null_safe)
            hash.update(index);

        /// The columns holding the conditions of the clause that read one side only.
        const auto [left_condition, right_condition] = clause.condColumnNames();
        hash.update(!left_condition.empty());
        if (!left_condition.empty())
            hashColumn(left_condition, left_header, hash);
        hash.update(!right_condition.empty());
        if (!right_condition.empty())
            hashColumn(right_condition, right_header, hash);
    }

    /// The condition that reads both sides, such as `l.a < r.b` in an outer join, is evaluated by
    /// the join itself, over the columns of both sides.
    if (const auto & mixed_expression = table_join.getMixedJoinExpression())
    {
        hash.update(1);
        Block both_sides;
        for (const auto * side : {left_header, right_header})
        {
            if (side)
            {
                for (const auto & column : *side)
                    both_sides.insert(column);
            }
        }
        hashActionsDAG(mixed_expression->getActionsDAG(), &both_sides, hash);
    }
    else
        hash.update(0);
}

/// What a source step reads and filters on while reading. The filters run over storage columns, so
/// their inputs are hashed by name.
void hashSource(const SourceStepWithFilter & source, SipHash & hash)
{
    if (const auto & snapshot = source.getStorageSnapshot())
        hashStorageID(snapshot->storage.getStorageID(), hash);

    /// The storage columns it reads. Above the source a column is known by its position only, so
    /// this is what tells `sum(a)` from `sum(b)` over two columns of the same type.
    hashColumns(source.requiredSourceColumns(), nullptr, hash);

    hash.update(source.isQueryWithFinal());

    if (const auto & prewhere = source.getPrewhereInfo())
        hashFilter(prewhere->prewhere_actions, prewhere->prewhere_column_name, prewhere->remove_prewhere_column, nullptr, hash);
    else
        hash.update(0);

    if (const auto & row_level = source.getRowLevelFilter())
        hashFilter(row_level->actions, row_level->column_name, row_level->do_remove_column, nullptr, hash);
    else
        hash.update(0);
}

void hashReadFromMergeTree(const ReadFromMergeTree & read, SipHash & hash)
{
    hashColumns(read.getAllColumnNames(), nullptr, hash);
    hash.update(read.isQueryWithSampling());

    /// A projection is read through the projection's own metadata, which is how the read tells which
    /// projection was chosen.
    const auto metadata = read.getStorageMetadata();
    const auto table_metadata = read.getMergeTreeData().getInMemoryMetadataPtr(read.getContext(), /*bypass_metadata_cache=*/ false);
    for (const auto & projection : table_metadata->projections)
    {
        if (projection.metadata == metadata)
        {
            hashString(projection.name, hash);
            break;
        }
    }

    if (const auto & input_order = read.getInputOrder())
        hash.update(input_order->direction);
    else
        hash.update(0);

    if (const auto & prewhere = read.getDeferredPrewhereInfo())
        hashFilter(prewhere->prewhere_actions, prewhere->prewhere_column_name, prewhere->remove_prewhere_column, nullptr, hash);
    else
        hash.update(0);

    if (const auto & row_level = read.getDeferredRowLevelFilter())
        hashFilter(row_level->actions, row_level->column_name, row_level->do_remove_column, nullptr, hash);
    else
        hash.update(0);

    /// Which indexes the filter can use. What they selected depends on the data and on the
    /// constants, so it is left out, as are the conditions themselves, which hold the constants.
    if (const auto & indexes = read.getIndexes())
    {
        hash.update(indexes->key_condition != nullptr);
        hash.update(indexes->minmax_idx_condition != nullptr);
        hash.update(indexes->partition_pruner && !indexes->partition_pruner->isUseless());
        hash.update(indexes->use_skip_indexes);
        hash.update(indexes->skip_indexes.useful_indices.size());
        for (const auto & skip_index : indexes->skip_indexes.useful_indices)
            hashString(skip_index.index->index.name, hash);
        if (indexes->skip_indexes.skip_index_for_top_k_filtering)
            hashString(indexes->skip_indexes.skip_index_for_top_k_filtering->index.name, hash);
    }
    else
        hash.update(0);
}

/// The choices made for one step, apart from its children.
void hashStep(const IQueryPlanStep & step, SipHash & hash)
{
    hashString(step.getName(), hash);

    const auto & input_headers = step.getInputHeaders();
    const Block * header = input_headers.empty() ? nullptr : input_headers.front().get();

    if (const auto * expression = typeid_cast<const ExpressionStep *>(&step))
    {
        hashActionsDAG(expression->getExpression(), header, hash);
    }
    else if (const auto * filter = typeid_cast<const FilterStep *>(&step))
    {
        hashFilter(filter->getExpression(), filter->getFilterColumnName(), filter->removesFilterColumn(), header, hash);
    }
    else if (const auto * totals_having = typeid_cast<const TotalsHavingStep *>(&step))
    {
        if (const auto * dag = totals_having->getActions())
            hashFilter(*dag, totals_having->getFilterColumnName(), false, header, hash);
        else
            hash.update(0);
    }
    else if (const auto * array_join = typeid_cast<const ArrayJoinStep *>(&step))
    {
        hashColumns(array_join->getColumns(), header, hash);
        hash.update(array_join->isLeft());
        hash.update(array_join->isUnaligned());
        if (const auto & element_filter = array_join->getElementFilter())
            hashFilter(*element_filter, array_join->getElementFilterColumnName(), false, header, hash);
        else
            hash.update(0);
    }
    else if (const auto * aggregating = typeid_cast<const AggregatingStep *>(&step))
    {
        hashAggregatorParams(aggregating->getParams(), header, hash);
        hashGroupingSets(aggregating->getGroupingSetsParamsList(), header, hash);
        hash.update(aggregating->isFinal());
        hash.update(aggregating->isGroupByUseNulls());
        hash.update(aggregating->inOrder());
        hash.update(aggregating->explicitSortingRequired());
        /// `isMergingSkipped` is left out: it follows from the parts the index analysis selected, so
        /// a filter whose constant selects no part turns it on, as the selected parts themselves are
        /// left out (see `hashReadFromMergeTree`).
    }
    else if (const auto * merging_aggregated = typeid_cast<const MergingAggregatedStep *>(&step))
    {
        hashAggregatorParams(merging_aggregated->getParams(), header, hash);
        hashGroupingSets(merging_aggregated->getGroupingSetsParamsList(), header, hash);
    }
    else if (const auto * sorting = typeid_cast<const SortingStep *>(&step))
    {
        /// The limit pushed into the sort is a literal, but whether there is one is a choice.
        hash.update(sorting->getType());
        hashSortDescription(sorting->getSortDescription(), header, hash);
        hashSortDescription(sorting->getPartitionByDescription(), header, hash);
        hash.update(sorting->isPartialTopN());
    }
    else if (const auto * limit = typeid_cast<const LimitStep *>(&step))
    {
        /// The limit and the offset are literals.
        hash.update(limit->withTies());
        hash.update(limit->alwaysReadTillEnd());
    }
    else if (const auto * limit_by = typeid_cast<const LimitByStep *>(&step))
    {
        hashColumns(limit_by->getColumns(), header, hash);
    }
    else if (const auto * distinct = typeid_cast<const DistinctStep *>(&step))
    {
        hashColumns(distinct->getColumnNames(), header, hash);
        hash.update(distinct->isPreliminary());
        hashSortDescription(distinct->getSortDescription(), header, hash);
    }
    else if (const auto * window = typeid_cast<const WindowStep *>(&step))
    {
        const auto & description = window->getWindowDescription();
        hashSortDescription(description.partition_by, header, hash);
        hashSortDescription(description.order_by, header, hash);
        /// The boundaries' offsets are literals; their kinds are not.
        hash.update(description.frame.type);
        hash.update(description.frame.begin_type);
        hash.update(description.frame.begin_preceding);
        hash.update(description.frame.end_type);
        hash.update(description.frame.end_preceding);

        hash.update(window->getWindowFunctions().size());
        for (const auto & function : window->getWindowFunctions())
        {
            hashString(function.getFunctionName(), hash);
            hashColumns(function.argument_names, header, hash);
        }
    }
    else if (const auto * intersect_or_except = typeid_cast<const IntersectOrExceptStep *>(&step))
    {
        hash.update(intersect_or_except->getOperator());
    }
    else if (const auto * join = typeid_cast<const JoinStep *>(&step))
    {
        const Block * right_header = input_headers.size() > 1 ? input_headers[1].get() : nullptr;
        hashJoin(*join->getJoin(), header, right_header, hash);
        hash.update(join->swap_streams);
    }
    else if (const auto * filled_join = typeid_cast<const FilledJoinStep *>(&step))
    {
        /// The right side is a table that is already filled, such as a `Join` engine table, and
        /// its keys are names of its columns.
        hashJoin(*filled_join->getJoin(), header, nullptr, hash);
    }

    if (const auto * source = dynamic_cast<const SourceStepWithFilter *>(&step))
    {
        hashSource(*source, hash);
        if (const auto * read_from_merge_tree = typeid_cast<const ReadFromMergeTree *>(&step))
            hashReadFromMergeTree(*read_from_merge_tree, hash);
    }
    else if (input_headers.empty())
    {
        /// Any other source: the columns it produces, which are storage columns that no literal
        /// names, and the table, for the steps that know theirs.
        const auto & output_header = step.getOutputHeader();
        hash.update(output_header ? output_header->columns() : 0);
        if (output_header)
        {
            for (const auto & column : *output_header)
                hashString(column.name, hash);
        }

        if (const auto * read_from_storage = typeid_cast<const ReadFromStorageStep *>(&step))
            hashStorageID(read_from_storage->getStorage()->getStorageID(), hash);
        else if (const auto * read_from_log = typeid_cast<const ReadFromStorageLogStep *>(&step))
            hashStorageID(read_from_log->getStorageSnapshot()->storage.getStorageID(), hash);
    }
}

UInt64 hashNode(const QueryPlan::Node & node, QueryPlanShapeHashes & result)
{
    SipHash hash;
    hashStep(*node.step, hash);

    hash.update(node.children.size());
    for (const auto * child : node.children)
        hash.update(hashNode(*child, result));

    const auto child_plans = node.step->getBuiltChildPlans();
    hash.update(child_plans.size());
    for (const auto * child_plan : child_plans)
    {
        if (child_plan && child_plan->getRootNode())
            hash.update(hashNode(*child_plan->getRootNode(), result));
        else
            hash.update(0);
    }

    const UInt64 value = hash.get64();
    result.steps.emplace(node.step->getUniqID(), value);
    return value;
}

}

QueryPlanShapeHashes hashQueryPlanShape(const QueryPlan & plan)
{
    QueryPlanShapeHashes result;
    if (const auto * root = plan.getRootNode())
        result.root = hashNode(*root, result);
    return result;
}

String formatQueryPlanShapeHash(UInt64 hash)
{
    return fmt::format("{:016x}", hash);
}

}
