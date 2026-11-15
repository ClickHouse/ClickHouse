#include <Processors/Transforms/DistinctSpillLayout.h>

#include <algorithm>
#include <numeric>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/SetVariants.h>
#include <Processors/Transforms/DistinctSetFilter.h>
#include <Common/ColumnsHashing.h>
#include <Common/Exception.h>

#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

constexpr auto COMPARISON_KEY_COLUMN_NAME = "__distinct_comparison_key";
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

template <typename Method>
ColumnPtr packExactKeys(const ColumnRawPtrs & key_columns, const Sizes & key_sizes, size_t num_rows)
{
    typename Method::State state(key_columns, key_sizes, /*context=*/ nullptr);
    using Key = typename Method::Key;
    auto packed = ColumnVector<Key>::create(num_rows);
    Arena pool;
    for (size_t row = 0; row < num_rows; ++row)
        packed->getData()[row] = state.getKeyHolder(row, pool);
    return packed;
}

ColumnPtr packExactKeys(
    DistinctKeyRepresentation representation, const ColumnRawPtrs & key_columns, size_t num_rows)
{
    Sizes key_sizes;
    const auto method = SetVariants::chooseMethod(key_columns, key_sizes);

#define PACK_EXACT_KEYS(NAME, REPRESENTATION) \
    case SetVariants::Type::NAME: \
    { \
        if (representation != DistinctKeyRepresentation::REPRESENTATION) \
            throw Exception(ErrorCodes::LOGICAL_ERROR, "DISTINCT spill key representation does not match the set method"); \
        using Method = typename decltype(std::declval<SetVariants>().NAME)::element_type; \
        return packExactKeys<Method>(key_columns, key_sizes, num_rows); \
    }

    switch (method)
    {
        PACK_EXACT_KEYS(key16, Key16)
        PACK_EXACT_KEYS(key32, Key32)
        PACK_EXACT_KEYS(key64, Key64)
        PACK_EXACT_KEYS(keys32, Keys32)
        PACK_EXACT_KEYS(keys64, Keys64)
        PACK_EXACT_KEYS(keys128, Keys128)
        PACK_EXACT_KEYS(keys256, Keys256)
        PACK_EXACT_KEYS(nullable_keys128, NullableKeys128)
        PACK_EXACT_KEYS(nullable_keys256, NullableKeys256)
        case SetVariants::Type::EMPTY:
        case SetVariants::Type::key8:
        case SetVariants::Type::key_string:
        case SetVariants::Type::key_fixed_string:
        case SetVariants::Type::hashed:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected DISTINCT set method for exact packed spill keys");
    }

#undef PACK_EXACT_KEYS

    UNREACHABLE();
}

template <typename Column>
size_t estimateColumnMemory(size_t num_rows)
{
    using Array = typename Column::Container;
    return PODArrayDetails::minimum_memory_for_elements(num_rows, sizeof(typename Column::ValueType), Array::pad_left, Array::pad_right);
}

size_t estimateComparisonKeyMemory(size_t num_rows, DistinctKeyRepresentation representation)
{
    switch (representation)
    {
        case DistinctKeyRepresentation::Columns: return 0;
        case DistinctKeyRepresentation::Key16: return estimateColumnMemory<ColumnUInt16>(num_rows);
        case DistinctKeyRepresentation::Key32:
        case DistinctKeyRepresentation::Keys32: return estimateColumnMemory<ColumnUInt32>(num_rows);
        case DistinctKeyRepresentation::Key64:
        case DistinctKeyRepresentation::Keys64: return estimateColumnMemory<ColumnUInt64>(num_rows);
        case DistinctKeyRepresentation::Hash128:
        case DistinctKeyRepresentation::Keys128:
        case DistinctKeyRepresentation::NullableKeys128: return estimateColumnMemory<ColumnUInt128>(num_rows);
        case DistinctKeyRepresentation::Keys256:
        case DistinctKeyRepresentation::NullableKeys256: return estimateColumnMemory<ColumnUInt256>(num_rows);
    }
    UNREACHABLE();
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
        auto type = std::make_shared<DataTypeUInt64>();
        const auto name = uniqueColumnName(ordinary, ARRIVAL_NUMBER_COLUMN_NAME);
        ordinary.insert({type->createColumn(), type, name});
        arrival_number_sort_description.emplace_back(name, 1, 1);
    }
    merged_header = std::make_shared<const Block>(ordinary);

    Block suppression;
    if (key_representation != DistinctKeyRepresentation::Columns)
    {
        auto type = getDistinctKeyRepresentationType(key_representation);
        ordinary.insert({type->createColumn(), type, uniqueColumnName(ordinary, COMPARISON_KEY_COLUMN_NAME)});
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

    auto flag_type = std::make_shared<DataTypeUInt8>();
    flag_column_name = uniqueColumnName(ordinary, FLAG_COLUMN_NAME);
    ordinary.insert({flag_type->createColumn(), flag_type, flag_column_name});
    suppression.insert(ordinary.getByPosition(ordinary.columns() - 1));

    input_run_header = std::make_shared<const Block>(std::move(ordinary));
    suppression_run_header = std::make_shared<const Block>(std::move(suppression));
}

size_t DistinctSpillLayout::estimateServiceColumnsMemory(
    size_t num_rows, DistinctKeyRepresentation key_representation, bool preserve_input_order)
{
    /// These columns are constructed at their final size, so allocation includes padding but no
    /// power-of-two capacity rounding.
    size_t bytes = 0;
    if (preserve_input_order)
    {
        using Array = ColumnUInt64::Container;
        bytes += PODArrayDetails::minimum_memory_for_elements(num_rows, sizeof(UInt64), Array::pad_left, Array::pad_right);
    }
    bytes += estimateComparisonKeyMemory(num_rows, key_representation);
    return bytes;
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
    /// Match the set's normalization before building its comparison key. Fingerprints survive `Native`
    /// round trips, which can change an aggregate state's serialized bytes.
    materializeChunk(chunk);
    columns = chunk.detachColumns();

    if (arrival_number_column_pos)
    {
        auto arrival_numbers = ColumnUInt64::create(num_rows);
        std::iota(arrival_numbers->getData().begin(), arrival_numbers->getData().end(), first_arrival_number);
        columns.emplace_back(std::move(arrival_numbers));
    }

    if (key_representation != DistinctKeyRepresentation::Columns)
    {
        ColumnRawPtrs key_columns;
        key_columns.reserve(key_columns_pos.size());
        for (const auto pos : key_columns_pos)
            key_columns.push_back(columns[pos].get());

        if (key_representation == DistinctKeyRepresentation::Hash128)
        {
            auto hashes = ColumnUInt128::create(num_rows);
            for (size_t row = 0; row < num_rows; ++row)
                hashes->getData()[row] = ColumnsHashing::hash128(row, key_columns.size(), key_columns);
            columns.emplace_back(std::move(hashes));
        }
        else
            columns.emplace_back(packExactKeys(key_representation, key_columns, num_rows));
    }

    /// The flag stays constant while sorting and deduplication can reduce the chunk's row count.
    columns.emplace_back(ColumnConst::create(ColumnUInt8::create(1, UInt8{0}), num_rows));
    chunk.setColumns(std::move(columns), num_rows);
    return chunk;
}

Chunk DistinctSpillLayout::prepareSuppressionChunk(MutableColumns key_columns) const
{
    chassert(key_columns.size() + 1 == suppression_run_header->columns());
    const size_t num_rows = key_columns.front()->size();
    Chunk chunk(std::move(key_columns), num_rows);
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
