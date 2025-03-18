#include <Processors/Transforms/DistinctTransform.h>
#include "Interpreters/BloomFilter.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}

DistinctTransform::DistinctTransform(
    const Block & header_,
    const SizeLimits & set_size_limits_,
    const UInt64 limit_hint_,
    const Names & columns_,
    const bool is_pre_distinct_)
    : ISimpleTransform(header_, header_, true)
    , is_pre_distinct(is_pre_distinct_)
    , limit_hint(limit_hint_)
    , set_size_limits(set_size_limits_)
{
    const size_t num_columns = columns_.empty() ? header_.columns() : columns_.size();
    key_columns_pos.reserve(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto pos = columns_.empty() ? i : header_.getPositionByName(columns_[i]);
        const auto & col = header_.getByPosition(pos).column;
        if (col && !isColumnConst(*col))
            key_columns_pos.emplace_back(pos);
    }
}

template <typename Method>
void DistinctTransform::buildFilter(
    Method & method,
    const ColumnRawPtrs & columns,
    IColumn::Filter & filter,
    const size_t rows,
    SetVariants & variants,
    size_t & passed) const
{
    typename Method::State state(columns, key_sizes, nullptr);
    auto bf_lookup = !leaky;

    for (size_t i = 0; i < rows; ++i)
    {

        auto row_hash = state.getHash(method.data, i, variants.string_pool);
        //auto row_hash  = ColumnsHashing::hash128(i, columns.size(), columns);

        if (leaky)
            bf_lookup = bloom_filter->findHashWithSeed(row_hash, 3213232);

        if (bf_lookup)
        {
            auto emplace_result = state.emplaceKey(method.data, i, variants.string_pool);
            /// Emit the record if there is no such key in the current set yet.
            /// Skip it otherwise.
            filter[i] = emplace_result.isInserted();
        } else
        {
            filter[i] = true;
            bloom_filter->addHashWithSeed(row_hash, 3213232);
            passed = passed + 1;
        }
    }
}

void DistinctTransform::transform(Chunk & chunk)
{
    if (unlikely(!chunk.hasRows()))
        return;

    /// Convert to full column, because SetVariant for sparse column is not implemented.
    convertToFullIfSparse(chunk);

    const auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    /// Special case, - only const columns, return single row
    if (unlikely(key_columns_pos.empty()))
    {
        for (auto & column : columns)
            column = column->cut(0, 1);

        chunk.setColumns(std::move(columns), 1);
        stopReading();
        return;
    }

    ColumnRawPtrs column_ptrs;
    column_ptrs.reserve(key_columns_pos.size());
    for (auto pos : key_columns_pos)
        column_ptrs.emplace_back(columns[pos].get());

    if (data.empty())
        data.init(SetVariants::chooseMethod(column_ptrs, key_sizes));

    const auto old_set_size = data.getTotalRowCount();
    const auto old_bf_size =  bf_passed;

    if ((!leaky) && is_pre_distinct && (!(limit_hint && limit_hint < 100000)) && old_set_size > 100000)
    {
        bloom_filter = std::make_unique<BloomFilter>(BloomFilterParameters(2500000, 3, 3213232));
        leaky = true;
    }

    IColumn::Filter filter(num_rows);

    switch (data.type)
    {
        case SetVariants::Type::EMPTY:
            break;
#define M(NAME) \
            case SetVariants::Type::NAME: \
                buildFilter(*data.NAME, column_ptrs, filter, num_rows, data, bf_passed); \
                break;
        APPLY_FOR_SET_VARIANTS(M)
#undef M
    }

    /// Just go to the next chunk if there isn't any new record in the current one.
    size_t new_set_size = data.getTotalRowCount();
    size_t new_bf_size = bf_passed;

    if (new_set_size + new_bf_size == old_set_size + old_bf_size)
        return;

    if (!set_size_limits.check(new_set_size, data.getTotalByteCount(), "DISTINCT", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
        return;

    for (auto & column : columns)
        column = column->filter(filter, -1);

    new_passes = ((new_set_size - old_set_size) + (new_bf_size - old_bf_size));

    leaky = leaky && new_passes > 1000 ? true: false;

    chunk.setColumns(std::move(columns), new_passes);

    /// Stop reading if we already reach the limit
    if (limit_hint && (new_set_size >= limit_hint || new_bf_size >= limit_hint))
    {
        stopReading();
        return;
    }
}

}
