#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/PartsSplitter.h>
#include <Processors/QueryPlan/PromQLRangeRateStep.h>
#include <Processors/QueryPlan/PromQLRangeSumByStep.h>
#include <Processors/QueryPlan/PromQLTwoRangeRatesStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnConst.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
#include <DataTypes/DataTypeTuple.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

#include <algorithm>
#include <array>
#include <optional>
#include <string_view>
#include <type_traits>
#include <unordered_set>

#include <fmt/format.h>


namespace DB
{
namespace QueryPlanOptimizations
{
namespace
{

constexpr size_t default_fused_two_range_rates_parallel_lanes = 4;

struct PromQLRangeSumBySource
{
    SortingStep * sorting = nullptr;
    ReadFromMergeTree * reading = nullptr;
};

bool canSplitByID(const ReadFromMergeTree & reading);

enum class SelectorColumn : uint8_t
{
    Unknown,
    ID,
    Bucket,
    Samples,
    TimeSeries,
};

SelectorColumn getSelectorColumn(const String & actual_name)
{
    std::string_view name = actual_name;
    const auto separator = actual_name.rfind('.');
    if (actual_name.starts_with("__table") && separator != String::npos)
        name = std::string_view(actual_name).substr(separator + 1);

    if (name == TimeSeriesColumnNames::ID)
        return SelectorColumn::ID;
    if (name == TimeSeriesColumnNames::Bucket)
        return SelectorColumn::Bucket;
    if (name == TimeSeriesColumnNames::Samples)
        return SelectorColumn::Samples;
    if (name == TimeSeriesColumnNames::TimeSeries)
        return SelectorColumn::TimeSeries;
    return SelectorColumn::Unknown;
}

bool isIDBucketSort(const SortDescription & description)
{
    return description.size() == 2
        && getSelectorColumn(description[0].column_name) == SelectorColumn::ID
        && description[0].direction == 1
        && getSelectorColumn(description[1].column_name) == SelectorColumn::Bucket
        && description[1].direction == 1;
}

bool isExactSimpleAggregateSamplesType(const DataTypePtr & type)
{
    const auto * array = typeid_cast<const DataTypeArray *>(type.get());
    const auto * simple = typeid_cast<const DataTypeCustomSimpleAggregateFunction *>(type->getCustomName());
    if (!array || !simple)
        return false;

    const auto & arguments = simple->getArgumentsDataTypes();
    return simple->getFunctionName() == "timeSeriesGroupArray"
        && simple->getParameters().empty()
        && arguments.size() == 1
        && arguments.front()->equals(*type)
        && simple->getFunction()->getResultType()->equals(*type);
}

struct RawSelectorInputs
{
    const ActionsDAG::Node * id = nullptr;
    const ActionsDAG::Node * bucket = nullptr;
    const ActionsDAG::Node * samples = nullptr;
};

std::optional<RawSelectorInputs> getExactRawSelectorInputs(const ActionsDAG::NodeRawConstPtrs & nodes)
{
    if (nodes.size() != 3)
        return std::nullopt;

    RawSelectorInputs result;
    for (const auto * node : nodes)
    {
        switch (getSelectorColumn(node->result_name))
        {
            case SelectorColumn::ID:
                if (result.id)
                    return std::nullopt;
                result.id = node;
                break;
            case SelectorColumn::Bucket:
                if (result.bucket)
                    return std::nullopt;
                result.bucket = node;
                break;
            case SelectorColumn::Samples:
                if (result.samples)
                    return std::nullopt;
                result.samples = node;
                break;
            default:
                return std::nullopt;
        }
    }

    if (!result.id || !result.bucket || !result.samples || !isExactSimpleAggregateSamplesType(result.samples->result_type))
        return std::nullopt;
    return result;
}

const ActionsDAG::Node * unwrapAliases(
    const ActionsDAG::Node * node,
    std::unordered_set<const ActionsDAG::Node *> & visited)
{
    std::unordered_set<const ActionsDAG::Node *> aliases;
    while (node && node->type == ActionsDAG::ActionType::ALIAS)
    {
        if (node->children.size() != 1 || !aliases.insert(node).second)
            return nullptr;
        visited.insert(node);
        node = node->children.front();
    }

    return node;
}

bool markExactAliasToInput(
    const ActionsDAG::Node * output,
    const ActionsDAG::Node * expected_input,
    std::unordered_set<const ActionsDAG::Node *> & visited)
{
    const auto * input = unwrapAliases(output, visited);
    if (!input
        || input != expected_input
        || input->type != ActionsDAG::ActionType::INPUT
        || !output->result_type->equals(*input->result_type))
        return false;

    visited.insert(input);
    return true;
}

bool isExactCastTypeLiteral(const ActionsDAG::Node & node, const IDataType & result_type)
{
    return node.type == ActionsDAG::ActionType::COLUMN
        && node.children.empty()
        && node.column
        && node.column->getField().getType() == Field::Types::String
        && node.column->getField().safeGet<String>() == result_type.getName();
}

bool haveSameValueTypeIgnoringTupleNames(const IDataType & lhs, const IDataType & rhs)
{
    if (lhs.equals(rhs))
        return true;

    const auto * lhs_array = typeid_cast<const DataTypeArray *>(&lhs);
    const auto * rhs_array = typeid_cast<const DataTypeArray *>(&rhs);
    if (lhs_array || rhs_array)
    {
        if (!lhs_array || !rhs_array)
            return false;
        return haveSameValueTypeIgnoringTupleNames(*lhs_array->getNestedType(), *rhs_array->getNestedType());
    }

    const auto * lhs_tuple = typeid_cast<const DataTypeTuple *>(&lhs);
    const auto * rhs_tuple = typeid_cast<const DataTypeTuple *>(&rhs);
    if (!lhs_tuple || !rhs_tuple || lhs_tuple->getElements().size() != rhs_tuple->getElements().size())
        return false;

    for (size_t i = 0; i != lhs_tuple->getElements().size(); ++i)
    {
        if (!haveSameValueTypeIgnoringTupleNames(*lhs_tuple->getElement(i), *rhs_tuple->getElement(i)))
            return false;
    }
    return true;
}

const ActionsDAG::Node * matchExactNoOpCast(
    const ActionsDAG::Node * node,
    bool allow_tuple_name_changes,
    std::unordered_set<const ActionsDAG::Node *> & visited)
{
    node = unwrapAliases(node, visited);
    if (!node
        || node->type != ActionsDAG::ActionType::FUNCTION
        || !node->function_base
        || node->function_base->getName() != "_CAST"
        || node->children.size() != 2)
        return nullptr;

    const auto * input = unwrapAliases(node->children[0], visited);
    const auto * type_literal = node->children[1];
    if (!input
        || !isExactCastTypeLiteral(*type_literal, *node->result_type)
        || (allow_tuple_name_changes
            ? !haveSameValueTypeIgnoringTupleNames(*input->result_type, *node->result_type)
            : !input->result_type->equals(*node->result_type)))
        return nullptr;

    visited.insert(node);
    visited.insert(input);
    visited.insert(type_literal);
    return input;
}

bool isExactSelectorCarrierProjection(const ExpressionStep & expression)
{
    const auto & dag = expression.getExpression();
    if (dag.getInputs().size() != 3 || dag.getOutputs().size() != 3)
        return false;

    const ActionsDAG::Node * id_input = nullptr;
    const ActionsDAG::Node * bucket_input = nullptr;
    const ActionsDAG::Node * time_series_input = nullptr;
    for (const auto * input : dag.getInputs())
    {
        switch (getSelectorColumn(input->result_name))
        {
            case SelectorColumn::ID:
                if (id_input)
                    return false;
                id_input = input;
                break;
            case SelectorColumn::Bucket:
                if (bucket_input)
                    return false;
                bucket_input = input;
                break;
            case SelectorColumn::Samples:
                return false;
            default:
                if (time_series_input)
                    return false;
                time_series_input = input;
                break;
        }
    }

    if (!id_input
        || !bucket_input
        || !time_series_input
        || !typeid_cast<const DataTypeArray *>(time_series_input->result_type.get())
        || time_series_input->result_type->getCustomName())
        return false;

    std::unordered_set<const ActionsDAG::Node *> visited;
    std::array<bool, 3> seen{};
    for (const auto * output : dag.getOutputs())
    {
        switch (getSelectorColumn(output->result_name))
        {
            case SelectorColumn::ID:
            {
                if (seen[0] || output->result_name != TimeSeriesColumnNames::ID)
                    return false;
                const auto * input = matchExactNoOpCast(output, /*allow_tuple_name_changes=*/ false, visited);
                if (input != id_input || input->type != ActionsDAG::ActionType::INPUT)
                    return false;
                seen[0] = true;
                break;
            }
            case SelectorColumn::Bucket:
            {
                if (seen[1] || output->result_name != TimeSeriesColumnNames::Bucket)
                    return false;
                const auto * input = matchExactNoOpCast(output, /*allow_tuple_name_changes=*/ false, visited);
                if (input != bucket_input || input->type != ActionsDAG::ActionType::INPUT)
                    return false;
                seen[1] = true;
                break;
            }
            case SelectorColumn::TimeSeries:
                if (seen[2]
                    || output->result_name != TimeSeriesColumnNames::TimeSeries
                    || !markExactAliasToInput(output, time_series_input, visited))
                    return false;
                seen[2] = true;
                break;
            default:
                return false;
        }
    }

    return seen[0] && seen[1] && seen[2] && visited.size() == dag.getNodes().size();
}

struct ExactRawSelectorProjection
{
    DataTypePtr timestamp_type;
    DataTypePtr value_type;
};

std::optional<ExactRawSelectorProjection> getExactRawSelectorProjection(const ExpressionStep & expression)
{
    const auto & dag = expression.getExpression();
    const auto inputs = getExactRawSelectorInputs(dag.getInputs());
    if (!inputs || dag.getOutputs().size() != 3)
        return std::nullopt;

    const auto * raw_array = typeid_cast<const DataTypeArray *>(inputs->samples->result_type.get());
    const auto * raw_tuple = raw_array ? typeid_cast<const DataTypeTuple *>(raw_array->getNestedType().get()) : nullptr;
    if (!raw_tuple || raw_tuple->getElements().size() != 2)
        return std::nullopt;

    std::unordered_set<const ActionsDAG::Node *> visited;
    std::array<bool, 3> seen{};
    for (const auto * output : dag.getOutputs())
    {
        switch (getSelectorColumn(output->result_name))
        {
            case SelectorColumn::ID:
            {
                if (seen[0] || output->result_name != TimeSeriesColumnNames::ID)
                    return std::nullopt;
                const auto * input = matchExactNoOpCast(output, /*allow_tuple_name_changes=*/ false, visited);
                if (input != inputs->id || input->type != ActionsDAG::ActionType::INPUT)
                    return std::nullopt;
                seen[0] = true;
                break;
            }
            case SelectorColumn::Bucket:
            {
                if (seen[1] || output->result_name != TimeSeriesColumnNames::Bucket)
                    return std::nullopt;
                const auto * input = matchExactNoOpCast(output, /*allow_tuple_name_changes=*/ false, visited);
                if (input != inputs->bucket || input->type != ActionsDAG::ActionType::INPUT)
                    return std::nullopt;
                seen[1] = true;
                break;
            }
            case SelectorColumn::Samples:
            {
                if (seen[2]
                    || output->result_name != TimeSeriesColumnNames::Samples
                    || output->result_type->getCustomName())
                    return std::nullopt;
                const auto * input = matchExactNoOpCast(output, /*allow_tuple_name_changes=*/ true, visited);
                if (input != inputs->samples || input->type != ActionsDAG::ActionType::INPUT)
                    return std::nullopt;
                seen[2] = true;
                break;
            }
            default:
                return std::nullopt;
        }
    }

    if (!std::ranges::all_of(seen, [](bool value) { return value; }) || visited.size() != dag.getNodes().size())
        return std::nullopt;

    return ExactRawSelectorProjection{raw_tuple->getElement(0), raw_tuple->getElement(1)};
}

bool isExactRawSelectorAliasProjection(const ExpressionStep & expression)
{
    const auto & dag = expression.getExpression();
    const auto inputs = getExactRawSelectorInputs(dag.getInputs());
    if (!inputs || dag.getOutputs().size() != 3)
        return false;

    std::unordered_set<const ActionsDAG::Node *> visited;
    std::array<bool, 3> seen{};
    for (const auto * output : dag.getOutputs())
    {
        switch (getSelectorColumn(output->result_name))
        {
            case SelectorColumn::ID:
                if (seen[0] || !markExactAliasToInput(output, inputs->id, visited))
                    return false;
                seen[0] = true;
                break;
            case SelectorColumn::Bucket:
                if (seen[1] || !markExactAliasToInput(output, inputs->bucket, visited))
                    return false;
                seen[1] = true;
                break;
            case SelectorColumn::Samples:
                if (seen[2] || !markExactAliasToInput(output, inputs->samples, visited))
                    return false;
                seen[2] = true;
                break;
            default:
                return false;
        }
    }

    return std::ranges::all_of(seen, [](bool value) { return value; }) && visited.size() == dag.getNodes().size();
}

std::optional<Field> getExactBoundLiteral(const ActionsDAG::Node & node, const IDataType & timestamp_type)
{
    if (node.type == ActionsDAG::ActionType::COLUMN
        && node.children.empty()
        && node.column
        && node.result_type->equals(timestamp_type))
        return node.column->getField();

    return std::nullopt;
}

struct ExactRawSlice
{
    DataTypePtr timestamp_type;
    DataTypePtr value_type;
    DataTypePtr result_type;
    Field min_time;
    Field max_time;
};

std::optional<ExactRawSlice> matchExactRawSliceCast(
    const ActionsDAG::Node * output,
    const RawSelectorInputs & inputs,
    std::unordered_set<const ActionsDAG::Node *> & visited)
{
    const auto * slice = matchExactNoOpCast(output, /*allow_tuple_name_changes=*/ true, visited);
    if (!slice
        || slice->type != ActionsDAG::ActionType::FUNCTION
        || !slice->function_base
        || slice->function_base->getName() != "timeSeriesSliceSortedArray"
        || slice->children.size() != 3
        || slice->children[0] != inputs.samples
        || slice->children[0]->type != ActionsDAG::ActionType::INPUT)
        return std::nullopt;

    const auto * raw_array = typeid_cast<const DataTypeArray *>(inputs.samples->result_type.get());
    const auto * raw_tuple = raw_array ? typeid_cast<const DataTypeTuple *>(raw_array->getNestedType().get()) : nullptr;
    if (!raw_tuple
        || raw_tuple->getElements().size() != 2
        || !haveSameValueTypeIgnoringTupleNames(*inputs.samples->result_type, *slice->result_type)
        || slice->result_type->getCustomName())
        return std::nullopt;

    auto min_time = getExactBoundLiteral(*slice->children[1], *raw_tuple->getElement(0));
    auto max_time = getExactBoundLiteral(*slice->children[2], *raw_tuple->getElement(0));
    if (!min_time || !max_time)
        return std::nullopt;

    visited.insert(slice);
    visited.insert(inputs.samples);
    visited.insert(slice->children[1]);
    visited.insert(slice->children[2]);
    return ExactRawSlice{
        raw_tuple->getElement(0),
        raw_tuple->getElement(1),
        slice->result_type,
        std::move(*min_time),
        std::move(*max_time)};
}

bool haveSameExactRawSlice(const ExactRawSlice & lhs, const ExactRawSlice & rhs)
{
    return lhs.timestamp_type->equals(*rhs.timestamp_type)
        && lhs.value_type->equals(*rhs.value_type)
        && haveSameValueTypeIgnoringTupleNames(*lhs.result_type, *rhs.result_type)
        && lhs.min_time == rhs.min_time
        && lhs.max_time == rhs.max_time;
}

std::optional<ExactRawSlice> getExactRawSliceProjection(const ExpressionStep & expression)
{
    const auto & dag = expression.getExpression();
    const auto inputs = getExactRawSelectorInputs(dag.getInputs());
    if (!inputs || dag.getOutputs().size() != 3)
        return std::nullopt;

    std::unordered_set<const ActionsDAG::Node *> visited;
    std::array<bool, 3> seen{};
    std::optional<ExactRawSlice> exact_slice;
    for (const auto * output : dag.getOutputs())
    {
        switch (getSelectorColumn(output->result_name))
        {
            case SelectorColumn::ID:
                if (seen[0] || !markExactAliasToInput(output, inputs->id, visited))
                    return std::nullopt;
                seen[0] = true;
                break;
            case SelectorColumn::Bucket:
                if (seen[1] || !markExactAliasToInput(output, inputs->bucket, visited))
                    return std::nullopt;
                seen[1] = true;
                break;
            case SelectorColumn::Unknown:
                if (seen[2])
                    return std::nullopt;
                exact_slice = matchExactRawSliceCast(output, *inputs, visited);
                if (!exact_slice)
                    return std::nullopt;
                seen[2] = true;
                break;
            default:
                return std::nullopt;
        }
    }

    if (!seen[0] || !seen[1] || !seen[2] || visited.size() != dag.getNodes().size())
        return std::nullopt;
    return exact_slice;
}

bool isExactRawSliceNotEmptyFilter(
    const FilterStep & filter,
    const ExactRawSlice & expected_slice)
{
    const auto & dag = filter.getExpression();
    const auto inputs = getExactRawSelectorInputs(dag.getInputs());
    const auto * predicate = dag.tryFindInOutputs(filter.getFilterColumnName());
    if (!filter.removesFilterColumn()
        || !inputs
        || !predicate
        || predicate->type != ActionsDAG::ActionType::FUNCTION
        || !predicate->function_base
        || predicate->function_base->getName() != "notEmpty"
        || predicate->children.size() != 1
        || predicate->result_type->getName() != "UInt8"
        || dag.getOutputs().size() != 4)
        return false;

    std::unordered_set<const ActionsDAG::Node *> visited{predicate};
    auto exact_slice = matchExactRawSliceCast(predicate->children.front(), *inputs, visited);
    if (!exact_slice || !haveSameExactRawSlice(*exact_slice, expected_slice))
        return false;

    std::array<bool, 4> seen{};
    for (const auto * output : dag.getOutputs())
    {
        if (output == predicate)
        {
            if (seen[0])
                return false;
            seen[0] = true;
            continue;
        }

        switch (getSelectorColumn(output->result_name))
        {
            case SelectorColumn::ID:
                if (seen[1] || !markExactAliasToInput(output, inputs->id, visited))
                    return false;
                seen[1] = true;
                break;
            case SelectorColumn::Bucket:
                if (seen[2] || !markExactAliasToInput(output, inputs->bucket, visited))
                    return false;
                seen[2] = true;
                break;
            case SelectorColumn::Samples:
                if (seen[3] || !markExactAliasToInput(output, inputs->samples, visited))
                    return false;
                seen[3] = true;
                break;
            default:
                return false;
        }
    }

    return std::ranges::all_of(seen, [](bool value) { return value; }) && visited.size() == dag.getNodes().size();
}

bool isExactUnaryLink(const QueryPlan::Node & parent, const QueryPlan::Node & child)
{
    const auto & input_headers = parent.step->getInputHeaders();
    return parent.children.size() == 1
        && parent.children.front() == &child
        && input_headers.size() == 1
        && child.step->hasOutputHeader()
        && blocksHaveEqualStructure(*input_headers.front(), *child.step->getOutputHeader());
}

struct ExactTwoRangeRatesSource
{
    QueryPlan::Node * read_node = nullptr;
    QueryPlan::Node * delayed_sets_node = nullptr;
    SortingStep * sorting = nullptr;
    ReadFromMergeTree * reading = nullptr;
    PromQLTwoRangeRatesFusionConfigPtr fusion_config;
};

PromQLTwoRangeRatesFusionConfigPtr makeRawFusionConfig(
    const PromQLTwoRangeRatesFusionConfig & config,
    const ExactRawSlice & exact_slice,
    const LoggerPtr & log)
{
    const auto & function = config.rate_function;
    const auto & argument_types = function->getArgumentTypes();
    if (function->getName() != "timeSeriesRateToGrid"
        || argument_types.size() != 1
        || !haveSameValueTypeIgnoringTupleNames(*argument_types.front(), *exact_slice.result_type))
        return {};

    AggregateFunctionProperties properties;
    AggregateFunctionPtr raw_function;
    try
    {
        raw_function = AggregateFunctionFactory::instance().get(
            function->getName(),
            NullsAction::EMPTY,
            DataTypes{exact_slice.timestamp_type, exact_slice.value_type},
            function->getParameters(),
            properties);
    }
    catch (const Exception & exception)
    {
        LOG_DEBUG(log, "PromQL two-range-rate storage fusion could not create the exact raw aggregate: {}", exception.message());
        return {};
    }

    if (!raw_function->getResultType()->equals(*function->getResultType()))
        return {};

    return std::make_shared<const PromQLTwoRangeRatesFusionConfig>(
        config.collector,
        std::move(raw_function),
        config.first_metric_name,
        config.second_metric_name,
        config.max_samples_per_series,
        config.max_output_block_size,
        config.max_join_groups,
        config.max_grid_cells,
        exact_slice.min_time,
        exact_slice.max_time,
        config.output_header);
}

bool isExactRawFusionConfig(
    const PromQLTwoRangeRatesFusionConfig & config,
    const ExactRawSelectorProjection & raw_projection)
{
    const auto & function = config.rate_function;
    if (!function || function->getName() != "timeSeriesRateToGrid")
        return false;

    const auto & argument_types = function->getArgumentTypes();
    return argument_types.size() == 2
        && argument_types[0]->equals(*raw_projection.timestamp_type)
        && argument_types[1]->equals(*raw_projection.value_type)
        && config.raw_min_time.has_value()
        && config.raw_max_time.has_value();
}

std::optional<ExactTwoRangeRatesSource> findExactTwoRangeRatesSource(
    QueryPlan::Node & promql_node,
    PromQLTwoRangeRatesFusionConfigPtr config,
    const LoggerPtr & log)
{
    const auto reject = [&](const char * reason)
    {
        LOG_DEBUG(log, "PromQL two-range-rate storage fusion rejected the D06 source island: {}", reason);
        return std::optional<ExactTwoRangeRatesSource>{};
    };

    if (!config)
        return reject("fusion configuration is null");
    if (promql_node.children.size() != 1)
        return reject("PromQLTwoRangeRates does not have exactly one input");

    auto * top_expression_node = promql_node.children.front();
    auto * top_expression = typeid_cast<ExpressionStep *>(top_expression_node->step.get());
    if (!top_expression)
        return reject("top carrier projection is not an expression");

    const bool reads_sliced_samples = isExactSelectorCarrierProjection(*top_expression);
    auto raw_projection = reads_sliced_samples ? std::nullopt : getExactRawSelectorProjection(*top_expression);
    if (!reads_sliced_samples && !raw_projection)
        return reject("top projection is neither the exact sliced nor raw selector carrier");

    if (top_expression_node->children.size() != 1)
        return reject("top carrier projection does not have exactly one input");
    auto * delayed_sets_node = top_expression_node->children.front();
    auto * delayed_sets = typeid_cast<DelayedCreatingSetsStep *>(delayed_sets_node->step.get());
    if (!delayed_sets || delayed_sets_node->children.size() != 1)
        return reject("DelayedCreatingSets does not have exactly one main input");

    QueryPlan::Node * raw_slice_node = nullptr;
    QueryPlan::Node * raw_alias_node = nullptr;
    QueryPlan::Node * raw_read_alias_node = nullptr;
    std::optional<ExactRawSlice> exact_slice;
    auto * sorting_node = delayed_sets_node->children.front();
    if (reads_sliced_samples)
    {
        raw_slice_node = sorting_node;
        auto * raw_slice = typeid_cast<ExpressionStep *>(raw_slice_node->step.get());
        exact_slice = raw_slice ? getExactRawSliceProjection(*raw_slice) : std::nullopt;
        if (!exact_slice)
            return reject("raw sample projection is not the exact bounded slice and value-preserving cast");
        if (raw_slice_node->children.size() != 1)
            return reject("raw sample projection does not have exactly one input");
        sorting_node = raw_slice_node->children.front();
    }
    else if (!isExactRawFusionConfig(*config, *raw_projection))
    {
        return reject("raw selector types do not match the exact two-scalar rate aggregate and bounds");
    }
    else if (const auto * raw_alias = typeid_cast<const ExpressionStep *>(sorting_node->step.get()))
    {
        if (!isExactRawSelectorAliasProjection(*raw_alias))
            return reject("expression below DelayedCreatingSets is not the exact raw-selector alias projection");
        raw_alias_node = sorting_node;
        if (raw_alias_node->children.size() != 1)
            return reject("raw-selector alias projection does not have exactly one input");
        sorting_node = raw_alias_node->children.front();
    }

    auto * sorting = typeid_cast<SortingStep *>(sorting_node->step.get());
    if (!sorting)
    {
        LOG_DEBUG(
            log,
            "PromQL two-range-rate storage fusion found {} with {} children at the ordered selector boundary",
            sorting_node->step->getName(),
            sorting_node->children.size());
        return reject("ordered selector boundary is not a SortingStep");
    }
    if (sorting->getType() != SortingStep::Type::FinishSorting
        || !sorting->canReplaceWithOrderedConsumer(/*may_disable_virtual_rows=*/ true)
        || !isIDBucketSort(sorting->getSortDescription()))
    {
        LOG_DEBUG(
            log,
            "PromQL two-range-rate storage fusion found a non-replaceable sort "
            "(type={}, limit={}, partitions={}, scatter_partitions={}, merge_join={}, partial_top_n={}, ordered_consumer={})",
            static_cast<unsigned int>(sorting->getType()),
            sorting->getLimit(),
            sorting->hasPartitions(),
            sorting->getScatterPartitions(),
            sorting->isSortingForMergeJoin(),
            sorting->isPartialTopN(),
            sorting->canReplaceWithOrderedConsumer(/*may_disable_virtual_rows=*/ true));
        return reject("sorting is not a replaceable FinishSorting on ascending (id, bucket)");
    }

    if (sorting_node->children.size() != 1)
        return reject("FinishSorting does not have exactly one input");
    QueryPlan::Node * filter_node = nullptr;
    auto * read_node = sorting_node->children.front();
    if (reads_sliced_samples)
    {
        filter_node = read_node;
        auto * filter = typeid_cast<FilterStep *>(filter_node->step.get());
        if (!filter || !isExactRawSliceNotEmptyFilter(*filter, *exact_slice))
            return reject("filter is not exact notEmpty of the same bounded raw-sample slice and cast");
        if (filter_node->children.size() != 1)
            return reject("raw sample filter does not have exactly one input");
        read_node = filter_node->children.front();
    }
    else if (const auto * raw_read_alias = typeid_cast<const ExpressionStep *>(read_node->step.get()))
    {
        if (!isExactRawSelectorAliasProjection(*raw_read_alias))
            return reject("expression below sorting is not the exact raw-selector alias projection");
        raw_read_alias_node = read_node;
        if (raw_read_alias_node->children.size() != 1)
            return reject("raw-selector alias projection below sorting does not have exactly one input");
        read_node = raw_read_alias_node->children.front();
    }

    auto * reading = typeid_cast<ReadFromMergeTree *>(read_node->step.get());
    if (!reading)
    {
        LOG_DEBUG(
            log,
            "PromQL two-range-rate storage fusion found {} with {} children where a MergeTree read was expected",
            read_node->step->getName(),
            read_node->children.size());
        return reject("MergeTree read is absent");
    }

    const bool splittable = canSplitByID(*reading);
    const bool has_deferred_row_filter = reading->getDeferredRowLevelFilter() != nullptr;
    const bool has_deferred_prewhere = reading->getDeferredPrewhereInfo() != nullptr;
    if (!read_node->children.empty() || !splittable || has_deferred_row_filter || has_deferred_prewhere)
    {
        const auto & input_order = reading->getInputOrder();
        LOG_DEBUG(
            log,
            "PromQL two-range-rate storage fusion rejected MergeTree read "
            "(children={}, splittable={}, streams={}, final={}, sampling={}, parallel={}, parallel_replicas={}, "
            "partition_ports={}, input_order={}, direction={}, prefix={}, deferred_row_filter={}, deferred_prewhere={})",
            read_node->children.size(),
            splittable,
            reading->getNumStreams(),
            reading->isQueryWithFinal(),
            reading->isQueryWithSampling(),
            reading->isParallelReadingEnabled(),
            reading->isParallelReadingFromReplicas(),
            reading->willOutputEachPartitionThroughSeparatePort(),
            static_cast<bool>(input_order),
            input_order ? input_order->direction : 0,
            input_order ? input_order->used_prefix_of_sorting_key_size : 0,
            has_deferred_row_filter,
            has_deferred_prewhere);
        return reject("MergeTree read is absent, non-leaf, unsplittable, or has a deferred filter");
    }

    if (!isExactUnaryLink(promql_node, *top_expression_node)
        || !isExactUnaryLink(*top_expression_node, *delayed_sets_node)
        || (reads_sliced_samples
            && (!isExactUnaryLink(*delayed_sets_node, *raw_slice_node)
                || !isExactUnaryLink(*raw_slice_node, *sorting_node)
                || !isExactUnaryLink(*sorting_node, *filter_node)
                || !isExactUnaryLink(*filter_node, *read_node)))
        || (!reads_sliced_samples
            && ((raw_alias_node
                    && (!isExactUnaryLink(*delayed_sets_node, *raw_alias_node)
                        || !isExactUnaryLink(*raw_alias_node, *sorting_node)))
                || (!raw_alias_node && !isExactUnaryLink(*delayed_sets_node, *sorting_node))
                || (raw_read_alias_node
                    && (!isExactUnaryLink(*sorting_node, *raw_read_alias_node)
                        || !isExactUnaryLink(*raw_read_alias_node, *read_node)))
                || (!raw_read_alias_node && !isExactUnaryLink(*sorting_node, *read_node)))))
        return reject("adjacent island headers do not match exactly");

    const auto metadata = reading->getStorageMetadata();
    const auto & primary_key = metadata->getPrimaryKey();
    if (primary_key.column_names.size() != 2
        || primary_key.data_types.size() != 2
        || primary_key.column_names[0] != TimeSeriesColumnNames::ID
        || primary_key.column_names[1] != TimeSeriesColumnNames::Bucket
        || (!primary_key.reverse_flags.empty()
            && (primary_key.reverse_flags.size() != 2 || primary_key.reverse_flags[0] || primary_key.reverse_flags[1])))
        return reject("primary key is not exactly ascending (id, bucket)");

    if (reads_sliced_samples)
    {
        config = makeRawFusionConfig(*config, *exact_slice, log);
        if (!config)
            return reject("the sliced rate aggregate cannot be reproduced exactly for raw timestamp/value arguments");
    }

    LOG_DEBUG(log, "PromQL two-range-rate storage fusion admitted the exact D06 source island");
    return ExactTwoRangeRatesSource{read_node, delayed_sets_node, sorting, reading, std::move(config)};
}

bool isTransparentUnaryStep(const IQueryPlanStep * step)
{
    return typeid_cast<const ExpressionStep *>(step)
        || typeid_cast<const FilterStep *>(step)
        || typeid_cast<const CreatingSetsStep *>(step)
        || typeid_cast<const DelayedCreatingSetsStep *>(step);
}

std::optional<PromQLRangeSumBySource> findSource(QueryPlan::Node & promql_node, const LoggerPtr & log)
{
    PromQLRangeSumBySource result;
    QueryPlan::Node * node = &promql_node;

    while (node->children.size() == 1)
    {
        node = node->children.front();
        auto * step = node->step.get();

        if (auto * sorting = typeid_cast<SortingStep *>(step))
        {
            if (result.sorting)
            {
                LOG_DEBUG(log, "PromQL source matcher rejected a second SortingStep");
                return std::nullopt;
            }

            if (sorting->getType() != SortingStep::Type::FinishSorting || !isIDBucketSort(sorting->getSortDescription()))
            {
                String description;
                for (const auto & column : sorting->getSortDescription())
                {
                    if (!description.empty())
                        description += ", ";
                    description += fmt::format("{}:{}", column.column_name, column.direction);
                }
                LOG_DEBUG(
                    log,
                    "PromQL source matcher rejected SortingStep (type={}, sort=[{}])",
                    static_cast<unsigned int>(sorting->getType()),
                    description);
                return std::nullopt;
            }
            result.sorting = sorting;
            continue;
        }

        if (auto * reading = typeid_cast<ReadFromMergeTree *>(step))
        {
            if (!result.sorting)
            {
                LOG_DEBUG(log, "PromQL source matcher reached ReadFromMergeTree before a supported SortingStep");
                return std::nullopt;
            }
            result.reading = reading;
            return result;
        }

        if (!isTransparentUnaryStep(step))
        {
            LOG_DEBUG(log, "PromQL source matcher rejected step {}", step->getName());
            return std::nullopt;
        }
    }

    return std::nullopt;
}

bool canSplitByID(const ReadFromMergeTree & reading)
{
    if (reading.isQueryWithFinal()
        || reading.isQueryWithSampling()
        || reading.isParallelReadingEnabled()
        || reading.isParallelReadingFromReplicas()
        || reading.willOutputEachPartitionThroughSeparatePort()
        || reading.getNumStreams() <= 1)
        return false;

    const auto & input_order = reading.getInputOrder();
    if (!input_order
        || input_order->direction != 1
        || input_order->used_prefix_of_sorting_key_size < 2)
        return false;

    /// `requestReadingInOrder` stores an empty `sort_description_for_merging` and only retains the
    /// matched sorting-key prefix and direction. The `FinishSorting` step matched by `findSource`
    /// owns the actual `(id, bucket)` description after that request.

    const auto metadata = reading.getStorageMetadata();
    const auto & primary_key = metadata->getPrimaryKey();
    if (primary_key.column_names.empty()
        || primary_key.data_types.empty()
        || primary_key.column_names.front() != TimeSeriesColumnNames::ID
        || !isSafePrimaryDataKeyType(*primary_key.data_types.front()))
        return false;

    if (!primary_key.reverse_flags.empty() && primary_key.reverse_flags.front())
        return false;

    return true;
}

template <typename PromQLStep>
bool tryOptimize(QueryPlan::Node & node, PromQLStep & promql_step)
{
    const auto log = getLogger("optimizePromQLRangeSumByShards");
    if (!promql_step.isParallelProcessingRequested() || promql_step.isParallelProcessingEnabled())
    {
        LOG_DEBUG(
            log,
            "PromQL primary-key range sharding skipped before source analysis (requested={}, enabled={})",
            promql_step.isParallelProcessingRequested(),
            promql_step.isParallelProcessingEnabled());
        return false;
    }

    PromQLTwoRangeRatesFusionConfigPtr fusion_config;
    std::optional<ExactTwoRangeRatesSource> exact_two_rates_source;
    if constexpr (std::is_same_v<PromQLStep, PromQLTwoRangeRatesStep>)
    {
        if (promql_step.isStorageFusionRequested())
        {
            fusion_config = promql_step.getFusionConfig();
            exact_two_rates_source = findExactTwoRangeRatesSource(node, fusion_config, log);
        }
    }

    std::optional<PromQLRangeSumBySource> source;
    if constexpr (std::is_same_v<PromQLStep, PromQLTwoRangeRatesStep>)
    {
        if (exact_two_rates_source)
            source = PromQLRangeSumBySource{exact_two_rates_source->sorting, exact_two_rates_source->reading};
    }
    if (!source)
        source = findSource(node, log);

    if (!source || !canSplitByID(*source->reading))
    {
        if (!source)
        {
            String path;
            const QueryPlan::Node * current = &node;
            for (size_t depth = 0; current && depth < 16; ++depth)
            {
                if (!path.empty())
                    path += " -> ";
                path += fmt::format("{}[{}]", current->step->getName(), current->children.size());
                current = current->children.size() == 1 ? current->children.front() : nullptr;
            }
            LOG_DEBUG(log, "PromQL primary-key range sharding skipped: source shape is not supported ({})", path);
        }
        else
        {
            const auto & input_order = source->reading->getInputOrder();
            const auto metadata = source->reading->getStorageMetadata();
            const auto & primary_key = metadata->getPrimaryKey();
            LOG_DEBUG(
                log,
                "PromQL primary-key range sharding skipped: read preconditions are not met "
                "(streams={}, final={}, sampling={}, parallel={}, parallel_replicas={}, partition_ports={}, "
                "input_order={}, direction={}, prefix={}, merge_columns={}, primary_key_columns={}, primary_key_first={})",
                source->reading->getNumStreams(),
                source->reading->isQueryWithFinal(),
                source->reading->isQueryWithSampling(),
                source->reading->isParallelReadingEnabled(),
                source->reading->isParallelReadingFromReplicas(),
                source->reading->willOutputEachPartitionThroughSeparatePort(),
                static_cast<bool>(input_order),
                input_order ? input_order->direction : 0,
                input_order ? input_order->used_prefix_of_sorting_key_size : 0,
                input_order ? input_order->sort_description_for_merging.size() : 0,
                primary_key.column_names.size(),
                primary_key.column_names.empty() ? String{"<none>"} : primary_key.column_names.front());
        }
        return false;
    }

    auto analysis_result = source->reading->getAnalyzedResult();
    if (!analysis_result)
        analysis_result = source->reading->selectRangesToRead();

    if (!analysis_result
        || analysis_result->parts_with_ranges.empty()
        || !analysis_result->split_parts.layers.empty())
    {
        LOG_DEBUG(
            log,
            "PromQL primary-key range sharding skipped: selected ranges are unavailable, empty, or already split "
            "(analysis={}, parts={}, existing_layers={})",
            static_cast<bool>(analysis_result),
            analysis_result ? analysis_result->parts_with_ranges.size() : 0,
            analysis_result ? analysis_result->split_parts.layers.size() : 0);
        return false;
    }

    const size_t read_streams = source->reading->getNumStreams();
    const size_t rows_to_read = analysis_result->parts_with_ranges.getRowsCountAllParts();
    const size_t rows_per_read_block = std::max<size_t>(source->reading->getMaxBlockSize(), 1);
    size_t automatically_selected_max_layers = read_streams;
    if constexpr (std::is_same_v<PromQLStep, PromQLTwoRangeRatesStep>)
    {
        if (exact_two_rates_source)
        {
            /// Fusion produces one ordered storage read, so its lane budget must not be multiplied
            /// by the two logical rate inputs. Keep enough lanes to feed every read stream, but avoid
            /// creating lanes smaller than one ordinary read block. The range splitter may reduce the
            /// result further when ID boundaries are sparse.
            const size_t work_limited_layers
                = rows_to_read / rows_per_read_block + (rows_to_read % rows_per_read_block != 0);
            automatically_selected_max_layers = std::min(read_streams, std::max<size_t>(work_limited_layers, 1));
        }
    }

    const size_t configured_max_layers = promql_step.getMaxParallelLanes();
    size_t automatic_policy_max_layers = automatically_selected_max_layers;
    if constexpr (std::is_same_v<PromQLStep, PromQLTwoRangeRatesStep>)
    {
        if (exact_two_rates_source)
        {
            /// A fused two-rate read already shares storage work across both logical inputs.
            /// Additional primary-key layers duplicate boundary reads and own independent
            /// transform state, so keep the automatic policy conservative. A positive setting
            /// remains an explicit query-scoped override of this plan-specific default.
            automatic_policy_max_layers
                = std::min(automatic_policy_max_layers, default_fused_two_range_rates_parallel_lanes);
        }
    }

    const size_t max_layers = configured_max_layers
        ? std::min(automatically_selected_max_layers, configured_max_layers)
        : automatic_policy_max_layers;
    LOG_DEBUG(
        log,
        "PromQL primary-key range sharding selected at most {} layers "
        "(read_streams={}, selected_rows={}, read_block_rows={}, automatic_layers={}, automatic_policy_layers={}, configured_cap={})",
        max_layers,
        read_streams,
        rows_to_read,
        rows_per_read_block,
        automatically_selected_max_layers,
        automatic_policy_max_layers,
        configured_max_layers);
    auto split = splitIntersectingPartsRangesIntoLayers(
        analysis_result->parts_with_ranges,
        max_layers,
        /*max_columns_in_index=*/1,
        /*in_reverse_order=*/false,
        log);

    if (split.layers.size() <= 1)
    {
        LOG_DEBUG(
            log,
            "PromQL primary-key range sharding skipped: {} selected parts produced only {} layer",
            analysis_result->parts_with_ranges.size(),
            split.layers.size());
        return false;
    }

    LOG_DEBUG(
        log,
        "PromQL primary-key range sharding enabled with {} layers from {} selected parts",
        split.layers.size(),
        analysis_result->parts_with_ranges.size());

    if constexpr (std::is_same_v<PromQLStep, PromQLTwoRangeRatesStep>)
    {
        if (exact_two_rates_source)
        {
            /// Every shape and execution gate, the split, and the immutable configuration have
            /// succeeded. Mutate the source and fold the admitted island only after that point.
            /// Virtual rows are scheduling hints consumed by the removed `FinishSorting` step,
            /// not data rows. Prevent the source from producing them for the fused ordered consumer.
            exact_two_rates_source->reading->resetVirtualRowConversions();
            analysis_result->split_parts = std::move(split);
            exact_two_rates_source->reading->enablePromQLTwoRangeRatesFusion(
                std::move(exact_two_rates_source->fusion_config));
            exact_two_rates_source->delayed_sets_node->step->updateInputHeader(
                exact_two_rates_source->reading->getOutputHeader());
            exact_two_rates_source->delayed_sets_node->children.front() = exact_two_rates_source->read_node;
            node.step = std::move(exact_two_rates_source->delayed_sets_node->step);
            node.children = std::move(exact_two_rates_source->delayed_sets_node->children);
            node.cost_estimation.reset();

            LOG_DEBUG(
                log,
                "PromQL two-range-rate storage fusion replaced the exact D06 main island with a {}-layer fused read "
                "while preserving DelayedCreatingSets",
                analysis_result->split_parts.layers.size());
            return true;
        }
    }

    analysis_result->split_parts = std::move(split);
    source->sorting->convertToPartitionedFinishSorting();
    promql_step.enableParallelProcessing();
    return true;
}

}

void optimizePromQLRangeSumByShards(QueryPlan::Node & root)
{
    std::vector<QueryPlan::Node *> stack{&root};
    while (!stack.empty())
    {
        QueryPlan::Node * node = stack.back();
        stack.pop_back();

        if (auto * promql_step = typeid_cast<PromQLRangeSumByStep *>(node->step.get()))
            tryOptimize(*node, *promql_step);
        else if (auto * promql_rate_step = typeid_cast<PromQLRangeRateStep *>(node->step.get()))
            tryOptimize(*node, *promql_rate_step);
        else if (auto * promql_two_rates_step = typeid_cast<PromQLTwoRangeRatesStep *>(node->step.get()))
            tryOptimize(*node, *promql_two_rates_step);

        stack.insert(stack.end(), node->children.begin(), node->children.end());
    }
}

}
}
