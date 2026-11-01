#include <Processors/Transforms/GroupLimitSpillLayout.h>

#include <algorithm>
#include <numeric>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Merges/Algorithms/GroupLimitSortedAlgorithm.h>
#include <Common/ColumnsHashing.h>

namespace DB
{

namespace
{

constexpr auto FINGERPRINT_COLUMN_NAME = "__key_fingerprint";
constexpr auto SEEN_ROWS_COLUMN_NAME = "__seen_rows";
constexpr auto ARRIVAL_NUMBER_COLUMN_NAME = "__arrival_number";

/// Selects the non-constant columns in header order. Constants are restored from the header after
/// merging, so their values do not need to be written to the temporary runs.
ColumnNumbers calculateSpillColumnsPositions(const Block & header)
{
    ColumnNumbers positions;
    positions.reserve(header.columns());
    for (size_t pos = 0; pos < header.columns(); ++pos)
    {
        const auto & column = header.getByPosition(pos).column;
        if (!column || !isColumnConst(*column))
            positions.push_back(pos);
    }
    return positions;
}

/// Maps input key positions into the spill layout. Every key is non-constant and therefore spilled.
ColumnNumbers mapKeysToSpillPositions(const ColumnNumbers & key_columns_pos, const ColumnNumbers & spill_columns_pos)
{
    ColumnNumbers spill_positions;
    spill_positions.reserve(key_columns_pos.size());
    for (const auto key_pos : key_columns_pos)
    {
        const auto it = std::find(spill_columns_pos.begin(), spill_columns_pos.end(), key_pos);
        chassert(it != spill_columns_pos.end());
        spill_positions.push_back(it - spill_columns_pos.begin());
    }
    return spill_positions;
}

/// Prefixes service-column names until they are distinct from payload-column names. Run headers share
/// these names so the merger can map comparison and output columns independently.
String uniqueColumnName(const Block & header, String name)
{
    while (header.has(name))
        name = "_" + name;
    return name;
}

}

GroupLimitSpillLayout::GroupLimitSpillLayout(
    SharedHeader input_header_, const ColumnNumbers & input_key_columns_pos,
    SpillKeyRepresentation key_representation_, bool preserve_input_order, DataTypePtr seen_rows_type_)
    : input_header(std::move(input_header_))
    , key_representation(key_representation_)
    , seen_rows_type(std::move(seen_rows_type_))
    , spill_columns_pos(calculateSpillColumnsPositions(*input_header))
    , key_columns_pos(mapKeysToSpillPositions(input_key_columns_pos, spill_columns_pos))
    , arrival_number_column_pos(preserve_input_order ? std::optional<size_t>{spill_columns_pos.size()} : std::nullopt)
{
    Block input;
    for (const auto pos : spill_columns_pos)
        input.insert(input_header->getByPosition(pos));

    if (preserve_input_order)
    {
        auto type = std::make_shared<DataTypeUInt64>();
        const auto name = uniqueColumnName(input, ARRIVAL_NUMBER_COLUMN_NAME);
        input.insert({type->createColumn(), type, name});
        arrival_number_sort_description.emplace_back(name, 1, 1);
    }
    merged_header = std::make_shared<const Block>(input);

    Block seen;
    if (key_representation == SpillKeyRepresentation::Hash128)
    {
        auto type = std::make_shared<DataTypeUInt128>();
        input.insert({type->createColumn(), type, uniqueColumnName(input, FINGERPRINT_COLUMN_NAME)});
        seen.insert(input.getByPosition(input.columns() - 1));
    }
    else
    {
        for (const auto pos : key_columns_pos)
            seen.insert(input.getByPosition(pos));
    }

    key_sort_description.reserve(seen.columns());
    for (const auto & column : seen)
        key_sort_description.emplace_back(column.name, 1, 1);

    seen_rows_column_name = uniqueColumnName(input, SEEN_ROWS_COLUMN_NAME);
    input.insert({seen_rows_type->createColumn(), seen_rows_type, seen_rows_column_name});
    seen.insert(input.getByPosition(input.columns() - 1));

    /// Seen runs are inputs of the final merge, so they follow its order.
    run_sort_description = GroupLimitSortedAlgorithm::inputSortDescription(key_sort_description, seen_rows_column_name);
    input_run_header = std::make_shared<const Block>(std::move(input));
    seen_run_header = std::make_shared<const Block>(std::move(seen));
}

size_t GroupLimitSpillLayout::estimateServiceColumnsMemory(
    size_t num_rows, SpillKeyRepresentation key_representation, bool preserve_input_order)
{
    /// These columns are constructed at their final size, so allocation includes padding but no
    /// power-of-two capacity rounding.
    size_t bytes = 0;
    if (preserve_input_order)
    {
        using Array = ColumnUInt64::Container;
        bytes += PODArrayDetails::minimum_memory_for_elements(num_rows, sizeof(UInt64), Array::pad_left, Array::pad_right);
    }
    if (key_representation == SpillKeyRepresentation::Hash128)
    {
        using Array = ColumnUInt128::Container;
        bytes += PODArrayDetails::minimum_memory_for_elements(num_rows, sizeof(UInt128), Array::pad_left, Array::pad_right);
    }
    return bytes;
}

Chunk GroupLimitSpillLayout::prepareInputChunk(Chunk chunk, UInt64 first_arrival_number) const
{
    const size_t num_rows = chunk.getNumRows();
    auto input_columns = chunk.detachColumns();
    Columns columns;
    columns.reserve(input_run_header->columns());
    for (const auto pos : spill_columns_pos)
        columns.push_back(std::move(input_columns[pos]));

    chunk.setColumns(std::move(columns), num_rows);
    /// Match the in-memory filter's normalization before hashing. Fingerprints survive `Native` round trips,
    /// which can change an aggregate state's serialized bytes.
    materializeChunk(chunk);
    columns = chunk.detachColumns();

    if (arrival_number_column_pos)
    {
        auto arrival_numbers = ColumnUInt64::create(num_rows);
        std::iota(arrival_numbers->getData().begin(), arrival_numbers->getData().end(), first_arrival_number);
        columns.emplace_back(std::move(arrival_numbers));
    }

    if (key_representation == SpillKeyRepresentation::Hash128)
    {
        ColumnRawPtrs key_columns;
        key_columns.reserve(key_columns_pos.size());
        for (const auto pos : key_columns_pos)
            key_columns.push_back(columns[pos].get());
        auto hashes = ColumnUInt128::create(num_rows);
        for (size_t row = 0; row < num_rows; ++row)
            hashes->getData()[row] = ColumnsHashing::hash128(row, key_columns.size(), key_columns);
        columns.emplace_back(std::move(hashes));
    }

    /// The seen count stays constant while sorting and group limits can reduce the chunk's row count.
    columns.emplace_back(seen_rows_type->createColumnConst(num_rows, Field(UInt64{0})));
    chunk.setColumns(std::move(columns), num_rows);
    return chunk;
}

Chunk GroupLimitSpillLayout::prepareSeenChunk(SeenGroups seen) const
{
    chassert(seen.keys.size() + 1 == seen_run_header->columns());
    const size_t num_rows = seen.keys.front()->size();
    chassert(seen.seen_rows->size() == num_rows);
    Chunk chunk(std::move(seen.keys), num_rows);
    chunk.addColumn(std::move(seen.seen_rows));
    return chunk;
}

Chunk GroupLimitSpillLayout::restoreOutputChunk(Chunk chunk) const
{
    if (!arrival_number_column_pos && spill_columns_pos.size() == input_header->columns())
        return chunk;

    const size_t num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    if (arrival_number_column_pos)
        columns.erase(columns.begin() + *arrival_number_column_pos);

    if (spill_columns_pos.size() != input_header->columns())
    {
        Columns restored_columns(input_header->columns());
        for (size_t i = 0; i < spill_columns_pos.size(); ++i)
            restored_columns[spill_columns_pos[i]] = std::move(columns[i]);

        for (size_t pos = 0; pos < restored_columns.size(); ++pos)
        {
            if (!restored_columns[pos])
                restored_columns[pos] = input_header->getByPosition(pos).column->cloneResized(num_rows);
        }
        columns = std::move(restored_columns);
    }

    chunk.setColumns(std::move(columns), num_rows);
    return chunk;
}

}
