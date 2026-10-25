#include <Processors/Transforms/GPUAggregatingTransform.h>

#if USE_GPU

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <GPU/GPUDeviceColumns.h>
#include <GPU/GPUTypeMapping.h>
#include <Processors/DeviceChunk.h>
#include <Processors/QueryPlan/GPUAggregatingStep.h>

#include <algorithm>
#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

GPUAggregatingTransform::GPUAggregatingTransform(
    const SharedHeader & input_header_, const SharedHeader & output_header_, const Aggregator::Params & params, size_t batch_bytes)
    : IAccumulatingTransform(input_header_, output_header_)
    , num_aggregates(params.aggregates.size())
    , empty_result_for_empty_set(params.empty_result_for_aggregation_by_empty_set)
{
    const auto aggregations = gpuAggregationsOf(params);
    if (!aggregations)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "A GPU aggregation of aggregate functions the device does not have reached the pipeline");

    if (params.keys.empty())
    {
        accumulators.reserve(params.aggregates.size());
        for (size_t i = 0; i < params.aggregates.size(); ++i)
        {
            const auto & aggregate = params.aggregates[i];
            if (aggregate.argument_names.size() != 1)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU aggregation without keys of an aggregate of other than one argument");
            const String & argument_name = aggregate.argument_names.front();
            argument_positions.push_back(input_header_->getPositionByName(argument_name));
            accumulators.emplace_back(
                *input_header_->getByName(argument_name).type, *aggregate.function->getResultType(), (*aggregations)[i], batch_bytes);
        }

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

    DataTypes value_types;
    std::vector<GPU::GPUGroupByAggregate> aggregates;
    aggregates.reserve(params.aggregates.size());
    for (size_t i = 0; i < params.aggregates.size(); ++i)
    {
        const auto & aggregate = params.aggregates[i];
        const DataTypePtr result_type = aggregate.function->getResultType();

        if (aggregate.argument_names.empty())
        {
            aggregates.push_back({
                .aggregation = (*aggregations)[i],
                .column = GPU::GPUGroupByAggregate::no_column,
                .result_type = GPU::groupByResultTypeOrThrow(nullptr, *result_type, (*aggregations)[i]),
            });
            continue;
        }

        const size_t position = input_header_->getPositionByName(aggregate.argument_names.front());
        const DataTypePtr & argument_type = input_header_->getByPosition(position).type;
        auto column = std::find(argument_positions.begin(), argument_positions.end(), position);
        if (column == argument_positions.end())
        {
            argument_positions.push_back(position);
            value_types.push_back(argument_type);
            column = std::prev(argument_positions.end());
        }

        aggregates.push_back({
            .aggregation = (*aggregations)[i],
            .column = static_cast<uint32_t>(column - argument_positions.begin()),
            .result_type = GPU::groupByResultTypeOrThrow(argument_type.get(), *result_type, (*aggregations)[i]),
        });
    }

    group_by_accumulator.emplace(key_types, value_types, std::move(aggregates), batch_bytes);
}

void GPUAggregatingTransform::consume(Chunk chunk)
{
    const size_t num_rows = chunk.getNumRows();
    if (num_rows == 0)
        return;

    total_rows += num_rows;

    if (const auto device_columns = GPU::gpuColumnsOf(chunk))
    {
        if (group_by_accumulator)
        {
            if (group_by_accumulator->takesOnDevice(*device_columns, key_positions, argument_positions))
            {
                group_by_accumulator->addOnDevice(*device_columns, key_positions, argument_positions);
                return;
            }
        }
        else
        {
            const auto & views = device_columns->views();
            bool on_device = true;
            for (size_t i = 0; on_device && i < accumulators.size(); ++i)
                on_device = accumulators[i].takesOnDevice(views.at(argument_positions[i]));
            if (on_device)
            {
                for (size_t i = 0; i < accumulators.size(); ++i)
                    accumulators[i].addOnDevice(*device_columns, argument_positions[i]);
                return;
            }
        }
        materializeDeviceChunk(chunk, getInputPort().getHeader());
    }

    const Columns & columns = chunk.getColumns();

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
    if (generated)
        return group_by_accumulator ? nextGroupsChunk() : Chunk{};

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
    value_columns.reserve(num_aggregates);
    for (size_t i = 0; i < num_aggregates; ++i)
        value_columns.push_back(header.getByPosition(key_positions.size() + i).type->createColumn());

    group_by_accumulator->copyGroupsTo(key_columns, value_columns);

    groups.reserve(key_columns.size() + value_columns.size());
    for (auto & column : key_columns)
        groups.push_back(std::move(column));
    for (auto & column : value_columns)
        groups.push_back(std::move(column));
    groups_rows = num_groups;
    groups_offset = 0;

    return nextGroupsChunk();
}

Chunk GPUAggregatingTransform::nextGroupsChunk()
{
    if (groups_offset >= groups_rows)
    {
        groups.clear();
        return {};
    }
    const size_t rows = std::min(groups_per_chunk, groups_rows - groups_offset);
    Columns chunk_columns;
    chunk_columns.reserve(groups.size());
    for (const auto & column : groups)
        chunk_columns.push_back(groups_offset == 0 && rows == groups_rows ? column : column->cut(groups_offset, rows));
    groups_offset += rows;
    if (groups_offset >= groups_rows)
        groups.clear();
    return Chunk(std::move(chunk_columns), rows);
}

}

#endif
