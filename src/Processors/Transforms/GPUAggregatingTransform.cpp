#include <Processors/Transforms/GPUAggregatingTransform.h>

#if USE_GPU

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <Processors/QueryPlan/GPUAggregatingStep.h>

#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

GPUAggregatingTransform::GPUAggregatingTransform(
    const SharedHeader & input_header_,
    const SharedHeader & output_header_,
    const Aggregator::Params & params,
    size_t batch_bytes,
    bool input_grouped_)
    : IAccumulatingTransform(input_header_, output_header_)
    , empty_result_for_empty_set(params.empty_result_for_aggregation_by_empty_set)
    , input_grouped(input_grouped_)
{
    const auto aggregations = gpuAggregationsOf(params);
    if (!aggregations)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "A GPU aggregation of aggregate functions the device does not have reached the pipeline");

    argument_positions.reserve(params.aggregates.size());

    DataTypes argument_types;
    DataTypes result_types;
    argument_types.reserve(params.aggregates.size());
    result_types.reserve(params.aggregates.size());

    for (const auto & aggregate : params.aggregates)
    {
        const String & argument_name = aggregate.argument_names.front();

        argument_positions.push_back(input_header_->getPositionByName(argument_name));
        argument_types.push_back(input_header_->getByName(argument_name).type);
        result_types.push_back(aggregate.function->getResultType());
    }

    if (input_grouped)
    {
        if (params.keys.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU aggregation without keys was told its input is grouped already");

        for (const auto & key : params.keys)
            key_positions.push_back(input_header_->getPositionByName(key));

        for (size_t i = 0; i < params.aggregates.size(); ++i)
        {
            const auto & argument = input_header_->getByPosition(argument_positions[i]);
            const auto & result = output_header_->getByPosition(key_positions.size() + i);
            if (!argument.type->equals(*result.type))
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "A grouped input passes column {} of type {} on as {} of type {}",
                    argument.name,
                    argument.type->getName(),
                    result.name,
                    result.type->getName());
        }

        return;
    }

    if (params.keys.empty())
    {
        accumulators.reserve(params.aggregates.size());
        for (size_t i = 0; i < argument_types.size(); ++i)
            accumulators.emplace_back(*argument_types[i], *result_types[i], (*aggregations)[i], batch_bytes);

        return;
    }

    DataTypes key_types;
    key_positions.reserve(params.keys.size());
    key_types.reserve(params.keys.size());

    for (const auto & key : params.keys)
    {
        key_positions.push_back(input_header_->getPositionByName(key));
        key_types.push_back(input_header_->getByName(key).type);
    }

    group_by_accumulator.emplace(key_types, argument_types, result_types, *aggregations, batch_bytes);
}

void GPUAggregatingTransform::consume(Chunk chunk)
{
    const size_t num_rows = chunk.getNumRows();
    if (num_rows == 0)
        return;

    total_rows += num_rows;

    const Columns & columns = chunk.getColumns();

    if (input_grouped)
    {
        Columns grouped;
        grouped.reserve(key_positions.size() + argument_positions.size());
        for (const size_t position : key_positions)
            grouped.push_back(columns[position]);
        for (const size_t position : argument_positions)
            grouped.push_back(columns[position]);
        grouped_chunks.emplace_back(std::move(grouped), num_rows);
        return;
    }

    if (!group_by_accumulator)
    {
        for (size_t i = 0; i < accumulators.size(); ++i)
        {
            const ColumnPtr column = columns[argument_positions[i]]->convertToFullIfWrapped();
            accumulators[i].add(*column);
        }

        return;
    }

    Columns key_columns;
    key_columns.reserve(key_positions.size());
    for (const size_t position : key_positions)
        key_columns.push_back(columns[position]->convertToFullIfWrapped());

    Columns value_columns;
    value_columns.reserve(argument_positions.size());
    for (const size_t position : argument_positions)
        value_columns.push_back(columns[position]->convertToFullIfWrapped());

    group_by_accumulator->add(key_columns, value_columns);
}

Chunk GPUAggregatingTransform::generate()
{
    if (input_grouped)
    {
        if (grouped_chunks.empty())
            return {};
        Chunk chunk = std::move(grouped_chunks.front());
        grouped_chunks.pop_front();
        return chunk;
    }

    if (generated)
        return {};

    generated = true;

    const Block & header = getOutputPort().getHeader();

    if (group_by_accumulator)
        return generateGroups(header);

    if (total_rows == 0 && empty_result_for_empty_set)
        return {};

    Columns columns;
    columns.reserve(accumulators.size());

    for (size_t i = 0; i < accumulators.size(); ++i)
    {
        auto column = header.getByPosition(i).type->createColumn();
        column->insert(accumulators[i].finalize());
        columns.push_back(std::move(column));
    }

    return Chunk(std::move(columns), 1);
}

Chunk GPUAggregatingTransform::generateGroups(const Block & header)
{
    const size_t num_groups = group_by_accumulator->finalize();

    if (num_groups == 0)
        return {};

    MutableColumns key_columns;
    key_columns.reserve(key_positions.size());
    for (size_t i = 0; i < key_positions.size(); ++i)
        key_columns.push_back(header.getByPosition(i).type->createColumn());

    MutableColumns value_columns;
    value_columns.reserve(argument_positions.size());
    for (size_t i = 0; i < argument_positions.size(); ++i)
        value_columns.push_back(header.getByPosition(key_positions.size() + i).type->createColumn());

    group_by_accumulator->copyGroupsTo(key_columns, value_columns);

    Columns columns;
    columns.reserve(key_columns.size() + value_columns.size());
    for (auto & column : key_columns)
        columns.push_back(std::move(column));
    for (auto & column : value_columns)
        columns.push_back(std::move(column));

    return Chunk(std::move(columns), num_groups);
}

}

#endif
