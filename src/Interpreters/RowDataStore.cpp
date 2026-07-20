#include <Columns/ColumnNullable.h>
#include <Interpreters/RowDataStore.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/IColumn.h>
#include <base/types.h>
#include <base/getL2CacheSize.h>
#include <Common/Exception.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>

#include <algorithm>
#include <cstring>


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
    M(1) \
    M(2) \
    M(4) \
    M(8) \
    M(16) \
    M(32)

template <size_t field_size>
void gatherField(char * dst, const char * src, size_t row_length, size_t offset, size_t length)
{
    for (size_t row = 0; row < length; ++row)
        memcpy(dst + row * row_length + offset, src + row * field_size, field_size);
}

void gatherField(char * dst, const char * src, size_t row_length, size_t offset, size_t field_size, size_t length)
{
    for (size_t row = 0; row < length; ++row)
        memcpy(dst + row * row_length + offset, src + row * field_size, field_size);
}

template <size_t value_size>
void gatherNullableField(char * dst, const char * null_src, const char * data_src, size_t row_length, size_t offset, size_t length)
{
    for (size_t row = 0; row < length; ++row)
    {
        char * row_dst = dst + row * row_length + offset;
        row_dst[0] = null_src[row];
        memcpy(row_dst + 1, data_src + row * value_size, value_size);
    }
}

void gatherNullableField(char * dst, const char * null_src, const char * data_src, size_t row_length, size_t offset, size_t value_size, size_t length)
{
    for (size_t row = 0; row < length; ++row)
    {
        char * row_dst = dst + row * row_length + offset;
        row_dst[0] = null_src[row];
        memcpy(row_dst + 1, data_src + row * value_size, value_size);
    }
}

void doGatherRows(const RowDataStore::RowLayout & layout, size_t row_length, const Columns & columns, size_t start, size_t length, char * dst)
{
    for (size_t i = 0; i < layout.size(); ++i)
    {
        const auto & field_layout = layout[i];
        if (field_layout.is_nullable)
        {
            const auto * nullable_column = assert_cast<const ColumnNullable *>(columns[i].get());
            const char * null_src = nullable_column->getNullMapColumn().getDataAt(start).data();
            const char * data_src = nullable_column->getNestedColumn().getDataAt(start).data();
            const size_t value_size = field_layout.size - 1;

            switch (value_size)
            {
#define M(N) \
                case N: \
                    gatherNullableField<N>(dst, null_src, data_src, row_length, field_layout.offset, length); \
                    break;
                APPLY_FOR_FIELD_SIZES(M)
#undef M
                default:
                    gatherNullableField(dst, null_src, data_src, row_length, field_layout.offset, value_size, length);
            }
        }
        else
        {
            const char * src = columns[i]->getDataAt(start).data();
            const size_t field_size = field_layout.size;

            switch (field_size)
            {
#define M(N) \
                case N: \
                    gatherField<N>(dst, src, row_length, field_layout.offset, length); \
                    break;
                APPLY_FOR_FIELD_SIZES(M)
#undef M
                default:
                    gatherField(dst, src, row_length, field_layout.offset, field_size, length);
            }
        }
    }
}

#undef APPLY_FOR_FIELD_SIZES

MutableColumns doScatterRows(const RowDataStore::RowLayout & layout, std::optional<size_t> batch_size_opt, const PaddedPODArray<const char *> & row_store_ptrs)
{
    MutableColumns columns(layout.size());
    for (size_t i = 0; i < layout.size(); ++i)
        columns[i] = layout[i].sample_column->cloneEmpty();

    const size_t count = row_store_ptrs.size();
    if (count == 0)
        return columns;

    for (size_t i = 0; i < layout.size(); ++i)
        columns[i]->reserve(count);

    const size_t batch_size = batch_size_opt.value_or(count);
    for (size_t batch_start = 0; batch_start < count; batch_start += batch_size)
    {
        const size_t remaining_batch_size = std::min(batch_size, count - batch_start);
        for (size_t i = 0; i < layout.size(); ++i)
            columns[i]->fillFromRowStorePtrs(row_store_ptrs, layout[i].offset, layout[i].size, batch_start, remaining_batch_size);
    }
    return columns;
}

}

RowDataStore::RowLayout RowDataStore::computeLayout(const Columns & columns)
{
    RowLayout layout;
    layout.reserve(columns.size());

    size_t offset = 0;
    for (const auto & column : columns)
    {
        ColumnPtr sample_col = column->cloneEmpty();

        bool is_nullable = false;
        const IColumn * check_col = sample_col.get();
        if (const auto * nullable = typeid_cast<const ColumnNullable *>(check_col))
        {
            check_col = nullable->getNestedColumnPtr().get();
            is_nullable = true;
        }

        if (!check_col->isFixedAndContiguous())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "RowDataStore can only store fixed-size and contiguous columns, but got {}.", sample_col->getFamilyName());

        size_t field_size = sample_col->sizeOfValueIfFixed();
        layout.push_back(FieldLayout{sample_col, offset, field_size, is_nullable});
        offset += field_size;
    }
    return layout;
}

RowDataStore::RowDataStore(RowLayout && layout_)
    : layout(std::move(layout_))
    , row_length(layout.empty() ? 0 : layout.back().offset + layout.back().size)
{
}

std::shared_ptr<RowDataStore> RowDataStore::create(const Columns & columns)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::JoinBuildRowStoreMicroseconds);

    /// Columns are materialized to make sure all blocks have
    /// the same split of columnar and row store columns.
    Columns materialized_columns;
    materialized_columns.reserve(columns.size());
    for (const auto & col : columns)
        materialized_columns.push_back(col->convertToFullIfWrapped()->convertToFullColumnIfLowCardinality());

    RowLayout layout = computeLayout(materialized_columns);
    auto row_store = std::shared_ptr<RowDataStore>(new RowDataStore(std::move(layout)));
    if (!materialized_columns.empty() && !materialized_columns[0]->empty())
        row_store->gatherRows(materialized_columns, 0, materialized_columns[0]->size());
    return row_store;
}

void RowDataStore::gatherRows(const Columns & columns, size_t start, size_t length)
{
    if (columns.size() != layout.size())
        throw Exception(
            ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH,
            "Number of appended columns ({}) does not match the number of columns in the layout ({}).",
            columns.size(),
            layout.size());

    if (length == 0)
        return;

    size_t data_size = chars.size();
    chars.resize(data_size + length * row_length);
    char * dst = chars.data() + data_size;

    const size_t batch_size = getBatchSize().value_or(length);
    for (size_t batch_start = 0; batch_start < length; batch_start += batch_size)
    {
        const size_t remaining_batch_size = std::min(batch_size, length - batch_start);
        doGatherRows(layout, row_length, columns, start + batch_start, remaining_batch_size, dst + batch_start * row_length);
    }
}

MutableColumns RowDataStore::scatterRows(size_t start, size_t length) const
{
    PaddedPODArray<const char *> row_store_ptrs;
    row_store_ptrs.reserve(length);
    for (size_t row = 0; row < length; ++row)
        row_store_ptrs.push_back(getRowAt(start + row));
    return doScatterRows(layout, getBatchSize(), row_store_ptrs);
}

MutableColumns RowDataStore::scatterRows(const PaddedPODArray<UInt64> & row_nums) const
{
    PaddedPODArray<const char *> row_store_ptrs;
    row_store_ptrs.reserve(row_nums.size());
    for (auto row : row_nums)
        row_store_ptrs.push_back(getRowAt(row));
    return doScatterRows(layout, getBatchSize(), row_store_ptrs);
}

RowDataStore::FieldLayout RowDataStore::getFieldLayout(size_t input_col_index) const
{
    return layout[input_col_index];
}

static constexpr UInt64 MIN_BYTES_IN_BATCH = 32 * 1024;
static constexpr UInt64 MAX_BYTES_IN_BATCH = 512 * 1024;

std::optional<size_t> RowDataStore::getBatchSize() const
{
    if (row_length == 0)
        return std::nullopt;

    const size_t batch_bytes = std::clamp<size_t>(getL2CacheSize() / 4, MIN_BYTES_IN_BATCH, MAX_BYTES_IN_BATCH);
    return std::max<size_t>(1, batch_bytes / row_length);
}

bool isRowStorageUseful(const ColumnPtr & column)
{
    const IColumn * check_col = column.get();
    if (const auto * column_nullable = typeid_cast<const ColumnNullable *>(column.get()))
        check_col = column_nullable->getNestedColumnPtr().get();

    return check_col->isFixedAndContiguous() && column->sizeOfValueIfFixed() <= 64;
}
}
