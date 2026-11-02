#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Interpreters/RowDataStore.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/TableJoin.h>
#include <base/types.h>
#include <base/PackedStringRef.h>
#include <base/unaligned.h>
#include <base/getL2CacheSize.h>
#include <Common/CacheLine.h>
#include <Common/Exception.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>

#include <algorithm>
#include <cstring>
#include <optional>


namespace ProfileEvents
{
    extern const Event JoinBuildRowStoreMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}

namespace
{

#define APPLY_FOR_FIELD_SIZES(M) \
    M(1) M(2) M(3) M(4) M(5) M(6) M(7) M(8) \
    M(9) M(10) M(11) M(12) M(13) M(14) M(15) M(16) \
    M(17) M(18) M(19) M(20) M(21) M(22) M(23) M(24) \
    M(25) M(26) M(27) M(28) M(29) M(30) M(31) M(32)

template <size_t value_size, bool nullable, typename SourceRow, typename DestinationRow>
void gatherField(
    const char * __restrict values,
    const UInt8 * __restrict null_map,
    size_t runtime_value_size,
    size_t count,
    SourceRow source_row,
    DestinationRow destination_row)
{
    const size_t bytes = value_size ? value_size : runtime_value_size;
    for (size_t i = 0; i < count; ++i)
    {
        const size_t row = source_row(i);
        char * dst = destination_row(i);
        if constexpr (nullable)
            *dst++ = static_cast<char>(null_map[row]);
        memcpy(dst, values + row * bytes, bytes);
    }
}

template <typename SourceRow, typename DestinationRow>
void gatherColumn(const IColumn & source, size_t count, SourceRow source_row, DestinationRow destination_row)
{
    const auto copy = [&]<bool nullable>(const IColumn & values, const UInt8 * null_map)
    {
        chassert(values.isFixedAndContiguous());
        const size_t value_size = values.sizeOfValueIfFixed();
        const char * data = values.getRawData().data();
        switch (value_size)
        {
#define M(N) \
            case N: \
                gatherField<N, nullable>(data, null_map, value_size, count, source_row, destination_row); \
                break;
            APPLY_FOR_FIELD_SIZES(M)
#undef M
            default:
                gatherField<0, nullable>(data, null_map, value_size, count, source_row, destination_row);
        }
    };
    if (const auto * nullable = typeid_cast<const ColumnNullable *>(&source))
        copy.template operator()<true>(nullable->getNestedColumn(), nullable->getNullMapData().data());
    else
        copy.template operator()<false>(source, nullptr);
}

/// Only values longer than 15 bytes take room in the overflow buffer, shorter ones live inside the ref.
size_t computeOverflowBytes(const ColumnPtr & column, size_t start, size_t length)
{
    size_t bytes = 0;
    const ColumnString & string_column = assert_cast<const ColumnString &>(*removeNullable(column));
    const ColumnString::Offsets & offsets = string_column.getOffsets();
    for (size_t row = start; row < start + length; ++row)
    {
        const size_t value_size = offsets[row] - offsets[static_cast<ssize_t>(row) - 1];
        if (value_size > UnhashedPackedStringRef::MAX_SMALL_LEN)
            bytes += value_size;
    }

    return bytes;
}

template <bool is_nullable>
void gatherStringField(char * __restrict dst, const char * __restrict null_src, const ColumnString & column, size_t row_length, size_t offset, size_t start, size_t length, char *& overflow_cursor)
{
    const ColumnString::Offsets & offsets = column.getOffsets();
    const char * chars = reinterpret_cast<const char *>(column.getChars().data());
    for (size_t row = 0; row < length; ++row)
    {
        char * row_dst = dst + row * row_length + offset;
        if constexpr (is_nullable)
        {
            row_dst[0] = null_src[row];
            ++row_dst;
        }

        const size_t source_row = start + row;
        const UInt64 begin = offsets[static_cast<ssize_t>(source_row) - 1];
        const size_t value_size = offsets[source_row] - begin;
        const char * value = chars + begin;
        /// The store owns its bytes: a long value is copied to the overflow buffer and the ref points there.
        if (value_size > UnhashedPackedStringRef::MAX_SMALL_LEN)
        {
            memcpy(overflow_cursor, value, value_size);
            value = overflow_cursor;
            overflow_cursor += value_size;
        }

        unalignedStore<UnhashedPackedStringRef>(row_dst, UnhashedPackedStringRef::build(value, value_size));
    }
}

void doGatherRows(const RowDataStore::RowLayout & layout, size_t row_length, const Columns & columns, size_t start, size_t length, char * dst, char *& overflow_cursor)
{
    for (size_t i = 0; i < layout.size(); ++i)
    {
        const auto & field_layout = layout[i];
        if (field_layout.is_string)
        {
            if (field_layout.is_nullable)
            {
                const auto & nullable_column = assert_cast<const ColumnNullable &>(*columns[i]);
                const char * null_src = nullable_column.getNullMapColumn().getDataAt(start).data();
                const auto & strings = assert_cast<const ColumnString &>(nullable_column.getNestedColumn());
                gatherStringField<true>(dst, null_src, strings, row_length, field_layout.offset, start, length, overflow_cursor);
            }
            else
            {
                const auto & strings = assert_cast<const ColumnString &>(*columns[i]);
                gatherStringField<false>(dst, nullptr, strings, row_length, field_layout.offset, start, length, overflow_cursor);
            }
        }
        else
        {
            gatherColumn(
                *columns[i], length,
                [start](size_t row) { return start + row; },
                [dst, row_length, offset = field_layout.offset](size_t row) { return dst + row * row_length + offset; });
        }
    }
}

#undef APPLY_FOR_FIELD_SIZES

MutableColumns doScatterRows(const RowDataStore::RowLayout & layout, std::optional<size_t> batch_size_opt, const RowStorePointers & row_store_ptrs, size_t count)
{
    MutableColumns columns(layout.size());
    for (size_t i = 0; i < layout.size(); ++i)
        columns[i] = layout[i].type->createColumn();

    if (count == 0)
        return columns;

    for (size_t i = 0; i < layout.size(); ++i)
        columns[i]->reserve(count);

    const size_t batch_size = batch_size_opt.value_or(count);
    for (size_t batch_start = 0; batch_start < count; batch_start += batch_size)
    {
        const size_t remaining_batch_size = std::min(batch_size, count - batch_start);
        for (size_t i = 0; i < layout.size(); ++i)
            columns[i]->fillFromRowStorePtrs(layout[i].type, row_store_ptrs, layout[i].offset, layout[i].size, batch_start, remaining_batch_size);
    }
    return columns;
}

}

RowDataStore::RowLayoutPtr RowDataStore::computeLayout(const Columns & columns, const DataTypes & types)
{
    if (columns.size() != types.size())
        throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "RowDataStore got {} columns but {} types.", columns.size(), types.size());

    RowLayout layout;
    layout.reserve(columns.size());

    size_t offset = 0;
    for (size_t i = 0; i < columns.size(); ++i)
    {
        const auto & column = columns[i];

        bool is_nullable = false;
        size_t field_size = 0;
        const IColumn * check_col = column.get();
        if (const auto * nullable = typeid_cast<const ColumnNullable *>(check_col))
        {
            check_col = &nullable->getNestedColumn();
            is_nullable = true;
            field_size = 1;
        }

        const bool is_string = typeid_cast<const ColumnString *>(check_col) != nullptr;
        if (!check_col->isFixedAndContiguous() && !is_string)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "RowDataStore can only store fixed-size and contiguous or String columns, but got {}.", column->getFamilyName());

        field_size += is_string ? sizeof(UnhashedPackedStringRef) : check_col->sizeOfValueIfFixed();
        layout.push_back(FieldLayout{types[i], offset, field_size, is_nullable, is_string});
        offset += field_size;
    }
    return std::make_shared<const RowLayout>(std::move(layout));
}

size_t RowDataStore::cacheMissReduction(const RowLayout & layout)
{
    size_t columnar_cache_lines = 0;
    for (const auto & field : layout)
        /// A columnar string touches both its offsets and its chars.
        columnar_cache_lines += (field.is_string ? 2 : 1) + (field.is_nullable ? 1 : 0);
    const size_t row_store_cache_lines = (rowLengthOf(layout) + CH_CACHE_LINE_SIZE - 1) / CH_CACHE_LINE_SIZE;
    return columnar_cache_lines - row_store_cache_lines;
}

RowDataStore::RowDataStore(RowLayoutPtr layout_)
    : layout(std::move(layout_))
    , row_length(rowLengthOf(*layout))
{
}

size_t RowDataStore::allocatedBytes() const
{
    size_t bytes = chars.empty() ? 0 : chars.allocated_bytes();
    if (!overflow.empty())
        bytes += overflow.allocated_bytes();
    return bytes;
}

size_t RowDataStore::byteSizeAt(size_t index) const
{
    if (overflow.empty())
        return row_length;

    size_t bytes = row_length;
    const char * row = getRowAt(index);
    for (const auto & field : *layout)
    {
        if (!field.is_string)
            continue;
        const auto value = unalignedLoad<UnhashedPackedStringRef>(row + field.offset + field.is_nullable);
        if (value.isLarge())
            bytes += value.getLargeSize();
    }
    return bytes;
}

std::shared_ptr<RowDataStore> RowDataStore::create(const RowLayoutPtr & layout, const Columns & columns)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::JoinBuildRowStoreMicroseconds);

    /// Columns are materialized to make sure all blocks have
    /// the same split of columnar and row store columns.
    Columns materialized_columns;
    materialized_columns.reserve(columns.size());
    for (const auto & col : columns)
        materialized_columns.push_back(col->convertToFullIfWrapped()->convertToFullColumnIfLowCardinality());

    auto row_store = std::make_shared<RowDataStore>(layout);
    if (!materialized_columns.empty() && !materialized_columns[0]->empty())
        row_store->gatherRows(materialized_columns, 0, materialized_columns[0]->size());
    return row_store;
}

void RowDataStore::gatherRows(const Columns & columns, size_t start, size_t length)
{
    if (columns.size() != layout->size())
        throw Exception(
            ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH,
            "Number of appended columns ({}) does not match the number of columns in the layout ({}).",
            columns.size(),
            layout->size());

    if (length == 0)
        return;

    /// A store is gathered only once, so the overflow buffer is sized exactly here and never moves.
    size_t overflow_bytes = 0;
    for (size_t i = 0; i < layout->size(); ++i)
        if ((*layout)[i].is_string)
            overflow_bytes += computeOverflowBytes(columns[i], start, length);
    overflow.resize_exact(overflow_bytes);
    char * overflow_cursor = overflow.data();

    size_t data_size = chars.size();
    chars.resize_exact(data_size + length * row_length);
    char * dst = chars.data() + data_size;

    const size_t batch_size = rowStoreBatchSize(row_length);
    for (size_t batch_start = 0; batch_start < length; batch_start += batch_size)
    {
        const size_t remaining_batch_size = std::min(batch_size, length - batch_start);
        doGatherRows(*layout, row_length, columns, start + batch_start, remaining_batch_size, dst + batch_start * row_length, overflow_cursor);
    }
}

MutableColumns RowDataStore::scatterRows(size_t start, size_t length) const
{
    RowStorePointers row_store_ptrs;
    row_store_ptrs.base_ptr = getRowAt(start);
    row_store_ptrs.row_length = row_length;
    return doScatterRows(*layout, rowStoreBatchSize(row_length), row_store_ptrs, length);
}

MutableColumns RowDataStore::scatterRows(const PaddedPODArray<UInt64> & row_nums) const
{
    RowStorePointers row_store_ptrs;
    row_store_ptrs.ptrs.reserve(row_nums.size());
    for (auto row : row_nums)
        row_store_ptrs.ptrs.push_back(getRowAt(row));
    return doScatterRows(*layout, rowStoreBatchSize(row_length), row_store_ptrs, row_nums.size());
}

void RowDataStore::gatherFieldToRows(
    const IColumn & source,
    std::span<const UInt32> source_rows,
    std::span<char * const> destination_rows,
    size_t field_offset)
{
    chassert(source_rows.size() == destination_rows.size());
    gatherColumn(
        source, source_rows.size(),
        [source_rows](size_t row) { return source_rows[row]; },
        [destination_rows, field_offset](size_t row) { return destination_rows[row] + field_offset; });
}

const RowDataStore::FieldLayout & RowDataStore::getFieldLayout(size_t input_col_index) const
{
    return (*layout)[input_col_index];
}

static constexpr UInt64 MIN_BYTES_IN_BATCH = 32 * 1024;
static constexpr UInt64 MAX_BYTES_IN_BATCH = 512 * 1024;

size_t rowStoreBatchSize(size_t row_length)
{
    chassert(row_length != 0);
    const size_t batch_bytes = std::clamp<size_t>(getL2CacheSize() / 4, MIN_BYTES_IN_BATCH, MAX_BYTES_IN_BATCH);
    return std::max<size_t>(1, batch_bytes / std::max<size_t>(row_length, CH_CACHE_LINE_SIZE));
}

NameSet computeRowStoreCandidateColumns(
    const TableJoin & table_join, const Block & right_sample_block, UInt64 right_rows, UInt64 min_string_column_bytes)
{
    /// ANY joins materialize eagerly and don't run the batched fill the row store accelerates.
    if (table_join.kind() == JoinKind::Cross || table_join.strictness() == JoinStrictness::Any
        || table_join.getClauses().empty() || table_join.getMixedJoinExpression())
        return {};

    NameSet columns;
    for (const auto & column : right_sample_block)
    {
        const auto sample = column.type->createColumn();
        const auto nested = removeNullable(column.type)->createColumn();
        if (typeid_cast<const ColumnString *>(nested.get()))
        {
            /// The average comes from the statistics and includes the 8-byte offset. 0 means the average is unknown.
            const Float64 avg_value_bytes = table_join.getRightColumnAvgBytes(column.name);
            if (min_string_column_bytes != 0 && avg_value_bytes > 0
                && avg_value_bytes <= UnhashedPackedStringRef::MAX_SMALL_LEN + sizeof(ColumnString::Offset)
                && static_cast<Float64>(right_rows) * avg_value_bytes >= static_cast<Float64>(min_string_column_bytes))
                columns.insert(column.name);
        }
        else if (nested->isFixedAndContiguous() && sample->sizeOfValueIfFixed() <= 32)
            columns.insert(column.name);
    }
    return columns;
}
}
