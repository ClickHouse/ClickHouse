#include <Processors/Transforms/PartitionAggregateTransform.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnsNumber.h>
#include <Common/MemoryTrackerUtils.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/AggregationCommon.h>

namespace DB
{

PartitionAggregateTransform::PartitionAggregateTransform(
    SharedHeader input_header,
    SharedHeader output_header,
    ColumnNumbers key_positions_,
    std::vector<WindowFunctionDescription> functions_,
    SpillSettings spill_settings_)
    : IAccumulatingTransform(input_header, output_header)
    , key_positions(std::move(key_positions_))
    , functions(std::move(functions_))
    , spill_settings(std::move(spill_settings_))
{
    for (const auto & function : functions)
    {
        ColumnNumbers positions;
        for (const auto & name : function.argument_names)
            positions.push_back(input_header->getPositionByName(name));
        argument_positions.push_back(std::move(positions));

        const auto & aggregate_function = *function.aggregate_function;
        const size_t alignment = aggregate_function.alignOfData();
        total_state_size = (total_state_size + alignment - 1) / alignment * alignment;
        state_offsets.push_back(total_state_size);
        total_state_size += aggregate_function.sizeOfData();
        state_alignment = std::max(state_alignment, alignment);
    }

    for (const auto & column : input_header->getColumnsWithTypeAndName())
        is_const_column.push_back(column.column && isColumnConst(*column.column));
    /// The group of each row, appended to the buffered chunks.
    is_const_column.push_back(false);
}

PartitionAggregateTransform::~PartitionAggregateTransform()
{
    for (size_t i = 0; i < functions.size(); ++i)
    {
        const auto & aggregate_function = *functions[i].aggregate_function;
        if (!aggregate_function.hasTrivialDestructor())
            for (auto * place : places)
                aggregate_function.destroy(place + state_offsets[i]);
    }
}

void PartitionAggregateTransform::consume(Chunk chunk)
{
    const size_t num_rows = chunk.getNumRows();
    if (num_rows == 0)
        return;

    auto columns = chunk.detachColumns();

    Columns full_key_columns;
    ColumnRawPtrs key_columns;
    for (auto position : key_positions)
    {
        full_key_columns.push_back(columns[position]->convertToFullIfWrapped());
        key_columns.push_back(full_key_columns.back().get());
    }

    auto groups = ColumnUInt32::create(num_rows);
    auto & group_data = groups->getData();
    row_places.resize(num_rows);

    for (size_t row = 0; row < num_rows; ++row)
    {
        auto key = serializeKeysToPoolContiguous(row, key_columns.size(), key_columns, arena, nullptr);

        decltype(key_to_group)::LookupResult it;
        bool inserted;
        key_to_group.emplace(key, it, inserted);
        if (inserted)
        {
            /// Reserved before the states are created, so that they are always destroyed.
            places.reserve(places.size() + 1);
            auto * place = arena.alignedAlloc(total_state_size, state_alignment);
            size_t created = 0;
            try
            {
                for (; created < functions.size(); ++created)
                    functions[created].aggregate_function->create(place + state_offsets[created]);
            }
            catch (...)
            {
                for (size_t i = 0; i < created; ++i)
                    functions[i].aggregate_function->destroy(place + state_offsets[i]);
                key_to_group.erase(key);
                throw;
            }
            places.push_back(place);
            it->getMapped() = static_cast<UInt32>(places.size() - 1);
        }
        else
        {
            arena.rollback(key.size());
        }

        group_data[row] = it->getMapped();
        row_places[row] = places[group_data[row]];
    }

    std::unordered_map<size_t, ColumnPtr> materialized;
    for (size_t i = 0; i < functions.size(); ++i)
    {
        std::vector<const IColumn *> argument_columns;
        for (auto position : argument_positions[i])
        {
            auto & argument = materialized[position];
            if (!argument)
                argument = recursiveRemoveLowCardinality(columns[position]->convertToFullIfWrapped());
            argument_columns.push_back(argument.get());
        }
        functions[i].aggregate_function->addBatch(0, num_rows, row_places.data(), state_offsets[i], argument_columns.data(), &arena);
    }

    columns.push_back(std::move(groups));
    chunks.emplace_back(std::move(columns), num_rows);
    chunks_bytes += chunks.back().allocatedBytes();

    if (spill_settings.max_bytes_before_external && chunks_bytes > spill_settings.max_bytes_before_external
        && (!spill_settings.max_query_bytes_before_external
            || getCurrentQueryMemoryUsage() > static_cast<Int64>(spill_settings.max_query_bytes_before_external)))
        spill();
}

void PartitionAggregateTransform::spill()
{
    if (!spilled)
    {
        Block header;
        const auto & input_header = getInputPort().getHeader();
        for (size_t i = 0; i < input_header.columns(); ++i)
            if (!is_const_column[i])
                header.insert(input_header.getByPosition(i).cloneEmpty());
        header.insert({ColumnUInt32::create(), std::make_shared<DataTypeUInt32>(), "__partition_aggregate_group"});
        spilled.emplace(
            std::make_shared<const Block>(std::move(header)),
            spill_settings.tmp_data,
            chunks_bytes + spill_settings.min_free_disk_space);
    }

    const auto & header = spilled->getHeader();
    for (auto & chunk : chunks)
    {
        auto columns = chunk.detachColumns();
        Columns written;
        for (size_t i = 0; i < columns.size(); ++i)
            if (!is_const_column[i])
                written.push_back(columns[i]->convertToFullIfWrapped());
        (*spilled)->write(header.cloneWithColumns(std::move(written)));
    }

    chunks.clear();
    chunks_bytes = 0;
}

Chunk PartitionAggregateTransform::generate()
{
    if (!results_ready)
    {

        for (size_t i = 0; i < functions.size(); ++i)
        {
            const auto & aggregate_function = *functions[i].aggregate_function;
            auto column = aggregate_function.getResultType()->createColumn();
            column->reserve(places.size());
            for (auto * place : places)
            {
                /// Like `WindowTransform`: the result of a `-State` function is a state to merge into.
                if (aggregate_function.isState())
                    aggregate_function.insertMergeResultInto(place + state_offsets[i], *column, &arena);
                else
                    aggregate_function.insertResultInto(place + state_offsets[i], *column, &arena);
            }
            results.push_back(std::move(column));
        }
        results_ready = true;

        if (spilled)
        {
            spilled->finishWriting();
            spilled_reader.emplace(spilled->getReadStream());
        }
    }

    Columns columns;
    size_t num_rows = 0;
    if (next_chunk < chunks.size())
    {
        num_rows = chunks[next_chunk].getNumRows();
        columns = chunks[next_chunk].detachColumns();
        ++next_chunk;
    }
    else if (spilled_reader)
    {
        auto block = (*spilled_reader)->read();
        if (block.empty())
            return {};

        num_rows = block.rows();
        auto read_columns = block.getColumns();
        const auto & input_header = getInputPort().getHeader();
        size_t read_position = 0;
        for (size_t i = 0; i < is_const_column.size(); ++i)
        {
            if (is_const_column[i])
                columns.push_back(input_header.getByPosition(i).column->cloneResized(num_rows));
            else
                columns.push_back(std::move(read_columns[read_position++]));
        }
    }
    else
    {
        return {};
    }

    ColumnPtr groups = std::move(columns.back());
    columns.pop_back();
    for (const auto & result : results)
        columns.push_back(result->index(*groups, 0));
    return Chunk(std::move(columns), num_rows);
}

}
