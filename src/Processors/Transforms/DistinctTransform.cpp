#include <Processors/Transforms/DistinctTransform.h>
#include "Interpreters/BloomFilter.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}

static constexpr UInt64 SEED_GEN_A = 845897321;

DistinctTransform::DistinctTransform(
    const Block & header_,
    const SizeLimits & set_size_limits_,
    const UInt64 limit_hint_,
    const Names & columns_,
    bool is_pre_distinct_)
    : ISimpleTransform(header_, header_, true)
    , limit_hint(limit_hint_)
    , is_pre_distinct(is_pre_distinct_)
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
    size_t & passed_bf) const
{
    typename Method::State state(columns, key_sizes, nullptr);

    for (size_t i = 0; i < rows; ++i)
    {

        auto row_hash = state.getHash(method.data, i, variants.string_pool);
        //auto row_hash  = ColumnsHashing::hash128(i, columns.size(), columns);

        auto has_element = use_bf ? bloom_filter->findHashWithSeed(row_hash, SEED_GEN_A) : true;

        if (has_element)
        {
            auto emplace_result = state.emplaceKey(method.data, i, variants.string_pool);
            /// Emit the record if there is no such key in the current set yet.
            /// Skip it otherwise.
            filter[i] = emplace_result.isInserted();
        } else
        {
            bloom_filter->addHashWithSeed(row_hash, SEED_GEN_A);
            passed_bf++;
            filter[i] = true;
        }
    }
}

void DistinctTransform::transform(Chunk & chunk)
{
    if (unlikely(!chunk.hasRows()))
        return;

    /// Convert to full column, because SetVariant for sparse column is not implemented.
    convertToFullIfSparse(chunk);
    convertToFullIfConst(chunk);

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

    const auto old_set_size = data.getTotalRowCount();
    const auto old_bf_size =  total_passed_bf;
    const bool has_reasonable_limit = (limit_hint && limit_hint < max_rows_in_distinct_before_bloom_filter_passthrough);

    if ((!use_bf) && is_pre_distinct && (!has_reasonable_limit) && old_set_size > max_rows_in_distinct_before_bloom_filter_passthrough)
    {
        bloom_filter = std::make_unique<BloomFilter>(BloomFilterParameters(10000000, 1, 0));
        use_bf = true;
    }

    if (data.empty())
        data.init(SetVariants::chooseMethod(column_ptrs, key_sizes));

    IColumn::Filter filter(num_rows);

    switch (data.type)
    {
        case SetVariants::Type::EMPTY:
            break;
#define M(NAME) \
            case SetVariants::Type::NAME: \
                buildFilter(*data.NAME, column_ptrs, filter, num_rows, data, total_passed_bf); \
                break;
        APPLY_FOR_SET_VARIANTS(M)
#undef M
    }

    /// Just go to the next chunk if there isn't any new record in the current one.
    size_t new_set_size = data.getTotalRowCount();
    size_t new_bf_size = total_passed_bf;

    if (new_set_size + new_bf_size == old_set_size + old_bf_size)
        return;

    if (!set_size_limits.check(new_set_size, data.getTotalByteCount(), "DISTINCT", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
        return;

    for (auto & column : columns)
        column = column->filter(filter, -1);

    new_passes = ((new_set_size - old_set_size) + (new_bf_size - old_bf_size));

    use_bf = use_bf && new_passes > (num_rows * 0.05) ? true: false;

    chunk.setColumns(std::move(columns), new_passes);

    /// Stop reading if we already reach the limit
    if (limit_hint && (new_set_size >= limit_hint || new_bf_size >= limit_hint))
    {
        stopReading();
        return;
    }
}

}
