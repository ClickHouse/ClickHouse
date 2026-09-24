#include <Processors/Transforms/DistinctSpillLayout.h>

#include <algorithm>
#include <numeric>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Transforms/DistinctSetFilter.h>
#include <Common/ColumnsHashing.h>

namespace DB
{

namespace
{

constexpr auto FINGERPRINT_COLUMN_NAME = "__distinct_fingerprint";
constexpr auto FLAG_COLUMN_NAME = "__distinct_already_emitted";
constexpr auto ARRIVAL_NUMBER_COLUMN_NAME = "__distinct_arrival_number";

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

DistinctSpillLayout::DistinctSpillLayout(
    SharedHeader input_header_, const ColumnNumbers & input_key_columns_pos,
    DistinctKeyRepresentation key_representation_, bool preserve_input_order)
    : input_header(std::move(input_header_))
    , key_representation(key_representation_)
    , spill_columns_pos(calculateSpillColumnsPositions(*input_header))
    , key_columns_pos(mapKeysToSpillPositions(input_key_columns_pos, spill_columns_pos))
    , arrival_number_column_pos(preserve_input_order ? std::optional<size_t>{spill_columns_pos.size()} : std::nullopt)
{
    Block ordinary;
    for (const auto pos : spill_columns_pos)
        ordinary.insert(input_header->getByPosition(pos));

    if (preserve_input_order)
    {
        auto arrival_type = std::make_shared<DataTypeUInt64>();
        const auto arrival_name = uniqueColumnName(ordinary, ARRIVAL_NUMBER_COLUMN_NAME);
        ordinary.insert({arrival_type->createColumn(), arrival_type, arrival_name});
        arrival_number_sort_description.emplace_back(arrival_name, 1, 1);
    }
    merged_header = std::make_shared<const Block>(ordinary);

    Block suppression;
    if (key_representation == DistinctKeyRepresentation::Hash128)
    {
        auto type = std::make_shared<DataTypeUInt128>();
        ordinary.insert({type->createColumn(), type, uniqueColumnName(ordinary, FINGERPRINT_COLUMN_NAME)});
        suppression.insert(ordinary.getByPosition(ordinary.columns() - 1));
    }
    else
    {
        for (const auto pos : key_columns_pos)
            suppression.insert(ordinary.getByPosition(pos));
    }

    key_sort_description.reserve(suppression.columns());
    for (const auto & column : suppression)
        key_sort_description.emplace_back(column.name, 1, 1);

    /// Ordered merges compare arrival numbers after the already-emitted flag, so every input must
    /// provide an arrival column. Suppression rows represent keys already emitted and never become
    /// output rows; a constant zero satisfies the shared sort description without affecting precedence.
    if (arrival_number_column_pos)
        suppression.insert(ordinary.getByPosition(*arrival_number_column_pos));

    auto flag_type = std::make_shared<DataTypeUInt8>();
    const auto flag_name = uniqueColumnName(ordinary, FLAG_COLUMN_NAME);
    ordinary.insert({flag_type->createColumn(), flag_type, flag_name});
    suppression.insert(ordinary.getByPosition(ordinary.columns() - 1));

    /// Compare the already-emitted flag first so suppression rows precede ordinary rows with equal
    /// keys. When preserving input order, compare arrival numbers next; files are merged by size,
    /// so source order cannot identify the earliest ordinary row.
    run_sort_description = key_sort_description;
    run_sort_description.emplace_back(flag_name, -1, 1);
    if (preserve_input_order)
        run_sort_description.push_back(arrival_number_sort_description.front());
    input_run_header = std::make_shared<const Block>(std::move(ordinary));
    suppression_run_header = std::make_shared<const Block>(std::move(suppression));
}

Chunk DistinctSpillLayout::prepareInputChunk(Chunk chunk, UInt64 first_arrival_number) const
{
    const size_t num_rows = chunk.getNumRows();
    auto input_columns = chunk.detachColumns();
    Columns columns;
    columns.reserve(input_run_header->columns());
    for (const auto pos : spill_columns_pos)
        columns.push_back(std::move(input_columns[pos]));

    chunk.setColumns(std::move(columns), num_rows);

    /// Match the set's normalization before hashing. Fingerprints survive `Native` round trips,
    /// which can change an aggregate state's serialized bytes.
    materializeChunk(chunk);
    columns = chunk.detachColumns();

    if (arrival_number_column_pos)
    {
        auto arrival_numbers = ColumnUInt64::create(num_rows);
        std::iota(arrival_numbers->getData().begin(), arrival_numbers->getData().end(), first_arrival_number);
        columns.emplace_back(std::move(arrival_numbers));
    }

    if (key_representation == DistinctKeyRepresentation::Hash128)
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

    /// The flag stays constant while sorting and deduplication can reduce the chunk's row count.
    columns.emplace_back(ColumnConst::create(ColumnUInt8::create(1, UInt8{0}), num_rows));
    chunk.setColumns(std::move(columns), num_rows);
    return chunk;
}

Chunk DistinctSpillLayout::prepareSuppressionChunk(MutableColumns key_columns) const
{
    chassert(key_columns.size() + 1 + arrival_number_column_pos.has_value() == suppression_run_header->columns());
    const size_t num_rows = key_columns.front()->size();
    Chunk chunk(std::move(key_columns), num_rows);
    if (arrival_number_column_pos)
        chunk.addColumn(ColumnConst::create(ColumnUInt64::create(1, UInt64{0}), num_rows));
    chunk.addColumn(ColumnConst::create(ColumnUInt8::create(1, UInt8{1}), num_rows));
    return chunk;
}

Chunk DistinctSpillLayout::restoreOutputChunk(Chunk chunk) const
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
