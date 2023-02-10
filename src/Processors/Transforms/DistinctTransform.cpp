#include <Processors/Transforms/DistinctTransform.h>

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
    const Names & columns_)
    : ISimpleTransform(header_, header_, true)
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
    SetVariants & variants) const
{
    typename Method::State state(columns, key_sizes, nullptr);

    for (size_t i = 0; i < rows; ++i)
    {
        auto emplace_result = state.emplaceKey(method.data, i, variants.string_pool);

        /// Emit the record if there is no such key in the current set yet.
        /// Skip it otherwise.
        filter[i] = emplace_result.isInserted();
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
    IColumn::Filter filter(num_rows);

    switch (data.type)
    {
        case SetVariants::Type::EMPTY:
            break;
#define M(NAME) \
            case SetVariants::Type::NAME: \
                buildFilter(*data.NAME, column_ptrs, filter, num_rows, data); \
                break;
        APPLY_FOR_SET_VARIANTS(M)
#undef M
    }

    /// Just go to the next chunk if there isn't any new record in the current one.
    size_t new_set_size = data.getTotalRowCount();
    if (new_set_size == old_set_size)
        return;

    if (!set_size_limits.check(new_set_size, data.getTotalByteCount(), "DISTINCT", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
        return;

    for (auto & column : columns)
        column = column->filter(filter, -1);

    chunk.setColumns(std::move(columns), new_set_size - old_set_size);

    /// Stop reading if we already reach the limit
    if (limit_hint && new_set_size >= limit_hint)
    {
        stopReading();
        return;
    }
}

}
