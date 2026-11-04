#include <Processors/Transforms/GroupLimitSpillLayout.h>

#include <algorithm>
#include <numeric>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/FieldToDataType.h>
#include <Processors/Merges/Algorithms/GroupLimitSortedAlgorithm.h>
#include <Common/ColumnsHashing.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace
{

constexpr auto FINGERPRINT_COLUMN_NAME = "__key_fingerprint";
constexpr auto SEEN_ROWS_COLUMN_NAME = "__seen_rows";
constexpr auto ARRIVAL_NUMBER_COLUMN_NAME = "__arrival_number";

/// Spill runs carry the non-constant input columns. Constants are restored from the header after merging, so
/// their values do not need to be written to the temporary runs.
bool isSpilledColumn(const ColumnWithTypeAndName & column)
{
    return !column.column || !isColumnConst(*column.column);
}

/// Selects the spilled columns in header order.
ColumnNumbers calculateSpillColumnsPositions(const Block & header)
{
    ColumnNumbers positions;
    positions.reserve(header.columns());
    for (size_t pos = 0; pos < header.columns(); ++pos)
    {
        if (isSpilledColumn(header.getByPosition(pos)))
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

template <typename T>
ColumnPtr clampSeenRows(const ColumnUInt64::Container & counts, UInt64 group_limit_end)
{
    auto column = ColumnVector<T>::create(counts.size());
    auto & data = column->getData();
    for (size_t row = 0; row < counts.size(); ++row)
        data[row] = static_cast<T>(std::min(counts[row], group_limit_end));
    return column;
}

}

GroupLimitSpillLayout::GroupLimitSpillLayout(
    SharedHeader input_header_, const ColumnNumbers & input_key_columns_pos,
    SpillKeyRepresentation key_representation_, bool attach_arrival_numbers, UInt64 group_limit_end_)
    : input_header(std::move(input_header_))
    , key_representation(key_representation_)
    , group_limit_end(group_limit_end_)
    , seen_rows_type(applyVisitor(FieldToDataType(), Field(group_limit_end)))
    , spill_columns_pos(calculateSpillColumnsPositions(*input_header))
    , key_columns_pos(mapKeysToSpillPositions(input_key_columns_pos, spill_columns_pos))
    , arrival_number_column_pos(attach_arrival_numbers ? std::optional<size_t>{spill_columns_pos.size()} : std::nullopt)
{
    Block input;
    for (const auto pos : spill_columns_pos)
        input.insert(input_header->getByPosition(pos));

    if (attach_arrival_numbers)
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
    seen.insert({seen_rows_type->createColumn(), seen_rows_type, seen_rows_column_name});

    /// Seen runs are inputs of the final merge, so they follow its order.
    run_sort_description = GroupLimitSortedAlgorithm::inputSortDescription(key_sort_description, seen_rows_column_name);
    input_run_header = std::make_shared<const Block>(std::move(input));
    seen_run_header = std::make_shared<const Block>(std::move(seen));
}

bool GroupLimitSpillLayout::spillsSortColumns(const Block & input_header, const SortDescription & description)
{
    return std::ranges::all_of(description, [&](const SortColumnDescription & sort_column)
    {
        const auto * column = input_header.findByName(sort_column.column_name);
        return column && isSpilledColumn(*column);
    });
}

size_t GroupLimitSpillLayout::estimateServiceColumnsMemory(
    size_t num_rows, SpillKeyRepresentation key_representation, bool attach_arrival_numbers)
{
    /// These columns are constructed at their final size, so allocation includes padding but no
    /// power-of-two capacity rounding.
    size_t bytes = 0;
    if (attach_arrival_numbers)
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

    chunk.setColumns(std::move(columns), num_rows);
    return chunk;
}

Chunk GroupLimitSpillLayout::prepareSeenChunk(SeenGroups seen) const
{
    chassert(seen.keys.size() + 1 == seen_run_header->columns());
    const size_t num_rows = seen.keys.front()->size();
    chassert(seen.seen_rows->size() == num_rows);

    /// Counts that already have the seen-count type stay as they are, such as the constant 1 of `DISTINCT`.
    /// Other counts arrive as `UInt64` and are clamped and narrowed.
    ColumnPtr seen_rows = std::move(seen.seen_rows);
    if (seen_rows->getDataType() != seen_rows_type->getColumnType())
    {
        const auto & counts = assert_cast<const ColumnUInt64 &>(*seen_rows).getData();
        switch (seen_rows_type->getTypeId())
        {
            case TypeIndex::UInt8: seen_rows = clampSeenRows<UInt8>(counts, group_limit_end); break;
            case TypeIndex::UInt16: seen_rows = clampSeenRows<UInt16>(counts, group_limit_end); break;
            case TypeIndex::UInt32: seen_rows = clampSeenRows<UInt32>(counts, group_limit_end); break;
            default: UNREACHABLE();
        }
    }

    Chunk chunk(std::move(seen.keys), num_rows);
    chunk.addColumn(std::move(seen_rows));
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
