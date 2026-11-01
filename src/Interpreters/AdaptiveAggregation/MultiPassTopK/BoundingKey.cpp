#include <Interpreters/AdaptiveAggregation/MultiPassTopK/BoundingKey.h>

#include <Interpreters/AdaptiveAggregation/MultiPassTopK/KeySketch.h>
#include <Interpreters/AdaptiveAggregation/MultiPassTopK/MultiPassTopK.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeSet.h>
#include <Functions/FunctionFactory.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <unordered_set>

namespace DB::MultiPassTopKImpl
{

using QueryPlanOptimizations::hasResultNameClash;

namespace
{

constexpr auto set_column_name = "__top_k_set";
constexpr auto in_column_name = "__top_k_in";

/// The column of the read that `name` passes through unchanged, from the top expression down, if any.
std::optional<String> traceToRead(const std::vector<const ActionsDAG *> & expressions, String name)
{
    for (const auto * expression : expressions)
    {
        const auto * node = expression->tryFindInOutputs(name);
        if (!node)
            return {};
        while (node->type == ActionsDAG::ActionType::ALIAS)
            node = node->children.front();
        if (node->type != ActionsDAG::ActionType::INPUT)
            return {};
        name = node->result_name;
    }
    return name;
}

/// Whether a value of the type takes at most 16 bytes.
bool isNarrow(const DataTypePtr & type)
{
    const auto nested = removeNullable(type);
    return nested->haveMaximumSizeOfValue() && nested->getMaximumSizeOfValueInMemory() <= 16;
}

/// Whether the filter `filter_column` of `dag` reads only narrow columns.
bool filterReadsNarrowColumns(const ActionsDAG & dag, const String & filter_column)
{
    const auto sub_dag = ActionsDAG::cloneSubDAG({&dag.findInOutputs(filter_column)}, /*remove_aliases=*/false);
    return std::ranges::all_of(sub_dag.getInputs(), [](const auto * input) { return isNarrow(input->result_type); });
}

/// Whether the filters of the input read only narrow columns of the table: `PREWHERE`, the row policy, and the filters
/// among `steps`, whose expressions `expressions` are, from the top down, traced to the columns of the read.
bool filtersReadNarrowColumns(
    const std::vector<const IQueryPlanStep *> & steps, const std::vector<const ActionsDAG *> & expressions, const ReadFromMergeTree & read)
{
    if (const auto & prewhere = read.getPrewhereInfo(); prewhere && !filterReadsNarrowColumns(prewhere->prewhere_actions, prewhere->prewhere_column_name))
        return false;
    if (const auto & filter = read.getRowLevelFilter(); filter && !filterReadsNarrowColumns(filter->actions, filter->column_name))
        return false;

    const auto & read_header = *read.getOutputHeader();
    for (size_t i = 0; i < steps.size(); ++i)
    {
        const auto * filter = typeid_cast<const FilterStep *>(steps[i]);
        if (!filter)
            continue;
        Names names{filter->getFilterColumnName()};
        for (size_t level = i; level < expressions.size(); ++level)
        {
            ActionsDAG::NodeRawConstPtrs outputs;
            for (const auto & name : names)
                outputs.push_back(&expressions[level]->findInOutputs(name));
            names = ActionsDAG::cloneSubDAG(outputs, /*remove_aliases=*/false).getRequiredColumnsNames();
        }
        if (!std::ranges::all_of(names, [&](const String & name) { return isNarrow(read_header.getByName(name).type); }))
            return false;
    }
    return true;
}

}

FutureSetPtr makeSet(const TopKBoundingKey & key, ColumnPtr values)
{
    SipHash hash;
    hash.update(key.type->getName());
    hash.update(values->size());
    values->updateHashFast(hash);
    ColumnsWithTypeAndName block{ColumnWithTypeAndName(values, key.type, key.column)};
    return std::make_shared<FutureSetFromTuple>(
        getSipHash128AsPair(hash), nullptr, std::move(block), /*transform_null_in=*/false, SizeLimits{});
}

void insertKeyValue(IColumn & column, UInt64 bits)
{
    if (auto * uint64_column = typeid_cast<ColumnUInt64 *>(&column))
        uint64_column->insertValue(bits);
    else if (auto * int64_column = typeid_cast<ColumnInt64 *>(&column))
        int64_column->insertValue(bit_cast<Int64>(bits));
    else if (auto * uint32_column = typeid_cast<ColumnUInt32 *>(&column))
        uint32_column->insertValue(bit_cast<UInt32>(bits));
    else
        assert_cast<ColumnInt32 &>(column).insertValue(bit_cast<Int32>(bits));
}

TopKKeyFilter makeInFilter(const TopKBoundingKey & key, const FutureSetPtr & set, const ContextPtr & context)
{
    ActionsDAG dag;
    const auto & column = dag.addInput(key.column, key.type);
    ColumnConst::Ptr set_column = ColumnConst::create(ColumnSet::create(1, set), 0);
    const auto & set_node = dag.addColumn(std::move(set_column), std::make_shared<DataTypeSet>(), set_column_name);
    const auto & in = dag.addFunction(FunctionFactory::instance().get("in", context), {&column, &set_node}, in_column_name);
    dag.getOutputs() = {&column, &in};
    return {.dag = std::move(dag), .column = in_column_name};
}

PrewhereInfoPtr addToPrewhere(
    const PrewhereInfoPtr & existing, TopKKeyFilter filter, const TopKBoundingKey & key, const Block & read_header, const ContextPtr & context)
{
    auto result = std::make_shared<PrewhereInfo>();
    result->need_filter = true;
    result->remove_prewhere_column = true;

    const auto is_new_filter = [&](const ActionsDAG & dag, const ActionsDAG::Node * node)
    {
        if (node->type != ActionsDAG::ActionType::FUNCTION || node->children.empty() || hasResultNameClash(dag, node))
            return false;
        const auto * input = node->children.front();
        if (input->type != ActionsDAG::ActionType::INPUT || input->result_name != key.column || !input->result_type->equals(*key.type)
            || hasResultNameClash(dag, input))
            return false;
        return std::all_of(node->children.begin() + 1, node->children.end(), [&](const ActionsDAG::Node * child)
            { return child->type == ActionsDAG::ActionType::COLUMN && !hasResultNameClash(dag, child); });
    };

    if (!existing)
    {
        const auto * filter_node = &filter.dag.findInOutputs(filter.column);
        if (!is_new_filter(filter.dag, filter_node) || read_header.has(filter.column))
            return nullptr;
        result->prewhere_actions = std::move(filter.dag);
        result->prewhere_column_name = filter.column;
        return result;
    }

    ActionsDAG combined = existing->prewhere_actions.clone();
    const auto * existing_filter = &combined.findInOutputs(existing->prewhere_column_name);
    ActionsDAG::NodeRawConstPtrs new_outputs;
    combined.mergeNodes(std::move(filter.dag), &new_outputs);
    const ActionsDAG::Node * filter_node = nullptr;
    for (const auto * node : new_outputs)
        if (node->result_name == filter.column)
            filter_node = node;
    if (!filter_node || !is_new_filter(combined, filter_node))
        return nullptr;

    const auto * and_node = &combined.addFunction(FunctionFactory::instance().get("and", context), {existing_filter, filter_node}, {});
    if (hasResultNameClash(combined, and_node) || read_header.has(and_node->result_name))
        return nullptr;
    auto & outputs = combined.getOutputs();
    for (const auto * node : new_outputs)
        if (node != filter_node && std::ranges::find(outputs, node) == outputs.end())
            outputs.push_back(node);
    if (existing->remove_prewhere_column)
        std::erase(outputs, existing_filter);
    outputs.push_back(and_node);

    result->prewhere_actions = std::move(combined);
    result->prewhere_column_name = and_node->result_name;
    return result;
}

namespace
{

/// Whether the filters by the values of `key` merge into the `PREWHERE` of `input.read`: by a set of values, and by the
/// hot fine cells of the sketch. Their names depend only on the kind of the filter, so empty ones decide for all.
bool canFilterByKey(const MultiPassTopKInput & input, const TopKBoundingKey & key)
{
    const auto & prewhere = input.read->getPrewhereInfo();
    const auto & header = *input.read->getOutputHeader();
    const auto set = makeSet(key, key.type->createColumn());
    const auto no_cells = std::make_shared<const TopKCellSet>(key_sketch_fine_cells);
    const auto no_values = std::make_shared<const std::unordered_set<UInt64>>();
    return addToPrewhere(prewhere, makeInFilter(key, set, input.context), key, header, input.context)
        && addToPrewhere(prewhere, makeHotFineCellsFilter(key, no_cells, no_values), key, header, input.context);
}

}

std::optional<TopKBoundingKey> findBoundingKey(const AggregatingStep & aggregating, const MultiPassTopKInput & input)
{
    const auto & params = aggregating.getParams();
    const auto & input_header = *aggregating.getInputHeaders().front();
    const auto & read = *input.read;

    /// The steps of the input and their expressions from the top down, the order in which a key traces to the read.
    std::vector<const IQueryPlanStep *> steps;
    std::vector<const ActionsDAG *> expressions;
    for (auto it = input.input_steps.rbegin(); it != input.input_steps.rend(); ++it)
    {
        steps.push_back(it->get());
        expressions.push_back(expressionOfInputStep(**it));
    }

    if (params.keys_size < 2
        || std::ranges::all_of(params.keys, [&](const String & key) { return isNarrow(input_header.getByName(key).type); })
        || !filtersReadNarrowColumns(steps, expressions, read))
        return {};

    const auto & read_header = *read.getOutputHeader();
    const auto metadata = read.getStorageMetadata();
    const auto primary_key = metadata->getColumnsRequiredForPrimaryKey();
    const auto partition_key = metadata->getColumnsRequiredForPartitionKey();
    const auto & prewhere = read.getPrewhereInfo();
    const auto & row_level_filter = read.getRowLevelFilter();

    std::optional<TopKBoundingKey> result;
    for (const auto & key : params.keys)
    {
        const auto & type = input_header.getByName(key).type;
        const WhichDataType which(type);
        if (!which.isInt32() && !which.isUInt32() && !which.isInt64() && !which.isUInt64())
            continue;
        const auto column = traceToRead(expressions, key);
        if (!column || !read_header.has(*column) || !read_header.getByName(*column).type->equals(*type))
            continue;
        /// A column that `PREWHERE` or the row policy computes is not the column of the table.
        const auto is_computed = [&](const ActionsDAG & dag)
        {
            const auto * node = dag.tryFindInOutputs(*column);
            return node && node->type != ActionsDAG::ActionType::INPUT;
        };
        if ((prewhere && is_computed(prewhere->prewhere_actions)) || (row_level_filter && is_computed(row_level_filter->actions)))
            continue;

        const bool prunes_granules = std::ranges::find(primary_key, *column) != primary_key.end()
            || std::ranges::find(partition_key, *column) != partition_key.end();
        if (!result || (prunes_granules && !result->prunes_granules))
            result = TopKBoundingKey{.name = key, .column = *column, .type = type, .prunes_granules = prunes_granules};
    }
    if (result && !canFilterByKey(input, *result))
        return {};
    return result;
}

}
