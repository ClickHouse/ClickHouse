#include <Processors/Transforms/DistinctSpillLayout.h>

#include <algorithm>
#include <numeric>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <Common/Arena.h>

namespace DB
{

namespace
{

constexpr auto FLAG_COLUMN_NAME = "__distinct_already_emitted";
constexpr auto ARRIVAL_NUMBER_COLUMN_NAME = "__distinct_arrival_number";

SortDescription buildSortDescription(const Block & header, const ColumnNumbers & key_columns_pos)
{
    SortDescription description;
    description.reserve(key_columns_pos.size());
    for (const auto pos : key_columns_pos)
        description.emplace_back(header.getByPosition(pos).name, 1, 1);
    return description;
}

/// Orders suppression rows before ordinary rows with equal keys, independently of run registration.
SortDescription buildRunSortDescription(const Block & header, SortDescription description, size_t flag_column_pos)
{
    description.emplace_back(header.getByPosition(flag_column_pos).name, -1, 1);
    return description;
}

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

/// Selects non-comparable keys for serialization, allowing their values to be sorted as bytes.
ColumnNumbers calculateSerializedKeyColumnsPositions(
    const Block & header, const ColumnNumbers & key_columns_pos, const ColumnNumbers & spill_key_columns_pos)
{
    ColumnNumbers positions;
    for (size_t i = 0; i < key_columns_pos.size(); ++i)
    {
        if (!header.getByPosition(key_columns_pos[i]).type->isComparable())
            positions.push_back(spill_key_columns_pos[i]);
    }
    return positions;
}

/// Serializes each value with `IColumn::serializeValueIntoArena` into a `String` column.
ColumnPtr serializeValues(const IColumn & column)
{
    const size_t num_rows = column.size();
    auto serialized = ColumnString::create();
    serialized->reserve(num_rows);

    Arena arena;
    for (size_t row = 0; row < num_rows; ++row)
    {
        const char * begin = nullptr;
        const auto value = column.serializeValueIntoArena(row, arena, begin, /*settings=*/ nullptr);
        serialized->insertData(value.data(), value.size());
        arena.rollback(value.size());
    }
    return serialized;
}

/// Reverses `serializeValues`, producing a column of the original type.
ColumnPtr deserializeValues(const IColumn & serialized, const IDataType & type)
{
    const size_t num_rows = serialized.size();
    auto column = type.createColumn();
    column->reserve(num_rows);

    for (size_t row = 0; row < num_rows; ++row)
    {
        ReadBufferFromString in(serialized.getDataAt(row));
        column->deserializeAndInsertFromArena(in, /*settings=*/ nullptr);
    }
    return column;
}

/// Prefixes service-column names until they are distinct from user-column names. Consumers address
/// these columns by position, so the generated names do not affect the row layout.
String uniqueColumnName(const Block & header, String name)
{
    while (header.has(name))
        name = "_" + name;
    return name;
}

/// Builds the run header from the spilled columns, optional arrival numbers, and the emitted flag.
/// Serialized keys retain their names and use the `String` type.
SharedHeader buildSpillHeader(
    const Block & header,
    const ColumnNumbers & spill_columns_pos,
    const ColumnNumbers & spill_serialized_key_columns_pos,
    bool with_arrival_numbers)
{
    Block spill_header;
    for (const auto pos : spill_columns_pos)
        spill_header.insert(header.getByPosition(pos));

    auto string_type = std::make_shared<DataTypeString>();
    for (const auto pos : spill_serialized_key_columns_pos)
    {
        auto & column = spill_header.getByPosition(pos);
        column.type = string_type;
        column.column = string_type->createColumn();
    }

    if (with_arrival_numbers)
    {
        auto arrival_number_type = std::make_shared<DataTypeUInt64>();
        spill_header.insert(
            {arrival_number_type->createColumn(), arrival_number_type, uniqueColumnName(header, ARRIVAL_NUMBER_COLUMN_NAME)});
    }

    auto flag_type = std::make_shared<DataTypeUInt8>();
    spill_header.insert({flag_type->createColumn(), flag_type, uniqueColumnName(header, FLAG_COLUMN_NAME)});
    return std::make_shared<const Block>(std::move(spill_header));
}

/// Removes the flag from the header of the merged and deduplicated stream.
SharedHeader buildMergedHeader(const Block & spill_header, size_t flag_column_pos)
{
    Block merged_header = spill_header;
    merged_header.erase(flag_column_pos);
    return std::make_shared<const Block>(std::move(merged_header));
}

/// Describes the arrival-number ordering when input order must be restored.
SortDescription buildArrivalNumberDescription(const Block & merged_header, std::optional<size_t> arrival_number_column_pos)
{
    SortDescription description;
    if (arrival_number_column_pos)
        description.emplace_back(merged_header.getByPosition(*arrival_number_column_pos).name, 1, 1);
    return description;
}

}

DistinctSpillLayout::DistinctSpillLayout(
    SharedHeader input_header_, const ColumnNumbers & input_key_columns_pos, bool preserve_input_order)
    : input_header(std::move(input_header_))
    , spill_columns_pos(calculateSpillColumnsPositions(*input_header))
    , key_columns_pos(mapKeysToSpillPositions(input_key_columns_pos, spill_columns_pos))
    , serialized_key_columns_pos(
          calculateSerializedKeyColumnsPositions(*input_header, input_key_columns_pos, key_columns_pos))
    , arrival_number_column_pos(preserve_input_order ? std::optional<size_t>{spill_columns_pos.size()} : std::nullopt)
    , flag_column_pos(spill_columns_pos.size() + preserve_input_order)
    , spill_header(buildSpillHeader(*input_header, spill_columns_pos, serialized_key_columns_pos, preserve_input_order))
    , merged_header(buildMergedHeader(*spill_header, flag_column_pos))
    , key_sort_description(buildSortDescription(*spill_header, key_columns_pos))
    , run_sort_description(buildRunSortDescription(*spill_header, key_sort_description, flag_column_pos))
    , arrival_number_sort_description(buildArrivalNumberDescription(*merged_header, arrival_number_column_pos))
{
}

Chunk DistinctSpillLayout::prepareInputChunk(Chunk chunk, UInt64 first_arrival_number) const
{
    if (spill_columns_pos.size() != input_header->columns())
    {
        const size_t num_rows = chunk.getNumRows();
        auto input_columns = chunk.detachColumns();

        Columns columns;
        columns.reserve(spill_columns_pos.size());
        for (const auto pos : spill_columns_pos)
            columns.push_back(std::move(input_columns[pos]));

        chunk.setColumns(std::move(columns), num_rows);
    }

    return serializeKeysAndAddServiceColumns(std::move(chunk), /*already_emitted=*/ false, first_arrival_number);
}

Chunk DistinctSpillLayout::prepareSuppressionChunk(MutableColumns key_columns) const
{
    const size_t num_rows = key_columns[0]->size();
    Columns columns(spill_columns_pos.size());
    for (size_t i = 0; i < key_columns.size(); ++i)
        columns[key_columns_pos[i]] = std::move(key_columns[i]);

    /// Suppression rows are never emitted, so default values suffice for their non-key payload.
    for (size_t i = 0; i < columns.size(); ++i)
    {
        if (!columns[i])
            columns[i] = input_header->getByPosition(spill_columns_pos[i]).type->createColumn()->cloneResized(num_rows);
    }

    /// Arrival numbers do not affect rows that are never emitted.
    return serializeKeysAndAddServiceColumns(
        Chunk(std::move(columns), num_rows), /*already_emitted=*/ true, /*first_arrival_number=*/ 0);
}

Chunk DistinctSpillLayout::serializeKeysAndAddServiceColumns(
    Chunk chunk, bool already_emitted, UInt64 first_arrival_number) const
{
    const size_t num_rows = chunk.getNumRows();

    /// The temporary files use `Native`, which cannot retain special column representations.
    removeSpecialColumnRepresentations(chunk);
    convertToFullIfConst(chunk);

    auto columns = chunk.detachColumns();
    for (const auto pos : serialized_key_columns_pos)
        columns[pos] = serializeValues(*columns[pos]);

    if (arrival_number_column_pos)
    {
        auto arrival_numbers = ColumnUInt64::create(num_rows);
        std::iota(arrival_numbers->getData().begin(), arrival_numbers->getData().end(), first_arrival_number);
        columns.emplace_back(std::move(arrival_numbers));
    }
    /// The flag stays constant while sorting and deduplication can reduce the chunk's row count.
    columns.emplace_back(ColumnConst::create(ColumnUInt8::create(1, static_cast<UInt8>(already_emitted)), num_rows));

    return Chunk(std::move(columns), num_rows);
}

Chunk DistinctSpillLayout::restoreOutputChunk(Chunk chunk) const
{
    if (!arrival_number_column_pos && serialized_key_columns_pos.empty()
        && spill_columns_pos.size() == input_header->columns())
        return chunk;

    const size_t num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    if (arrival_number_column_pos)
        columns.erase(columns.begin() + *arrival_number_column_pos);

    for (const auto pos : serialized_key_columns_pos)
        columns[pos] = deserializeValues(*columns[pos], *input_header->getByPosition(spill_columns_pos[pos]).type);

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

    return Chunk(std::move(columns), num_rows);
}

}
