#include <Processors/Transforms/DistinctTransform.h>

#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SET_SIZE_LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
}

DistinctTransform::DistinctTransform(
    SharedHeader header_,
    const SizeLimits & set_size_limits_,
    const UInt64 limit_hint_,
    const Names & columns_)
    : ISimpleTransform(header_, header_, true)
    , limit_hint(limit_hint_)
    , set_size_limits(set_size_limits_)
{
    const size_t num_columns = columns_.empty() ? header_->columns() : columns_.size();
    key_columns_pos.reserve(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto pos = columns_.empty() ? i : header_->getPositionByName(columns_[i]);
        const auto & col = header_->getByPosition(pos).column;
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
    const IColumn::Filter * mask) const
{
    typename Method::State state(columns, key_sizes, nullptr);

    if (mask)
    {
        for (size_t i = 0; i < rows; ++i)
        {
            if (!(*mask)[i])
            {
                /// Already known duplicate row (by LC index), skip insertion
                filter[i] = 0;
                continue;
            }

            auto emplace_result = state.emplaceKey(method.data, i, variants.string_pool);
            filter[i] = emplace_result.isInserted();
        }
    }
    else
    {
        for (size_t i = 0; i < rows; ++i)
        {
            auto emplace_result = state.emplaceKey(method.data, i, variants.string_pool);

            /// Emit the record if there is no such key in the current set yet.
            /// Skip it otherwise.
            filter[i] = emplace_result.isInserted();
        }
    }
}

IColumn::Filter DistinctTransform::buildLowCardinalityMask(const ColumnLowCardinality & column, size_t num_rows)
{
    const auto & dictionary = column.getDictionary();
    const auto dict_size = dictionary.size();

    LCDictionaryKey dict_key;
    dict_key.hash = dictionary.getHash();
    dict_key.size = dict_size;

    auto & state = lc_dict_states[dict_key];

    /// The first time we see this dictionary, initialize the seen_indices array to keep track which entries
    /// in the dictionary have been seen.
    chassert(state.seen_count <= dict_size);
    if (state.seen_indices.size() != dict_size)
    {
        chassert(state.seen_indices.empty());
        chassert(state.seen_count == 0);
        state.seen_indices.resize_fill(dict_size);
    }

    /// If we've already seen all dictionary indices for this dictionary,
    /// then no row in this chunk (and also other chunks with the same dictionary) can produce a new distinct value.
    if (state.seen_count == dict_size)
        return {}; /// empty mask == no candidates

    auto & seen = state.seen_indices;

    const auto index_type_size = column.getSizeOfIndexType();
    const IColumn & indexes_column = *column.getIndexesPtr();

    IColumn::Filter mask;

    auto handle_index = [&](size_t idx, size_t row)
    {
        chassert(idx < dict_size);
        if (!seen[idx])
        {
            seen[idx] = 1;
            ++state.seen_count;

            if (mask.empty())
                mask.resize_fill(num_rows);

            mask[row] = 1; /// first time we see this dictionary index for this dictionary
        }
    };

    switch (index_type_size)
    {
        case sizeof(UInt8):
        {
            const auto & col = assert_cast<const ColumnUInt8 &>(indexes_column).getData();
            for (size_t row = 0; row < num_rows; ++row)
                handle_index(static_cast<size_t>(col[row]), row);
            break;
        }
        case sizeof(UInt16):
        {
            const auto & col = assert_cast<const ColumnUInt16 &>(indexes_column).getData();
            for (size_t row = 0; row < num_rows; ++row)
                handle_index(static_cast<size_t>(col[row]), row);
            break;
        }
        case sizeof(UInt32):
        {
            const auto & col = assert_cast<const ColumnUInt32 &>(indexes_column).getData();
            for (size_t row = 0; row < num_rows; ++row)
                handle_index(static_cast<size_t>(col[row]), row);
            break;
        }
        case sizeof(UInt64):
        {
            const auto & col = assert_cast<const ColumnUInt64 &>(indexes_column).getData();
            for (size_t row = 0; row < num_rows; ++row)
                handle_index(static_cast<size_t>(col[row]), row);
            break;
        }
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected size of index type for LowCardinality column in DistinctTransform");
    }

    return mask; /// if empty, then means no candidates in this chunk
}

void DistinctTransform::transform(Chunk & chunk)
{
    if (unlikely(!chunk.hasRows()))
        return;

    /// Convert to full column, because SetVariant for sparse column is not implemented.
    removeSpecialColumnRepresentations(chunk);
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

    std::optional<IColumn::Filter> lc_mask;

    if (key_columns_pos.size() == 1)
    {
        if (const auto * lc = typeid_cast<const ColumnLowCardinality *>(column_ptrs[0]))
        {
            lc_mask.emplace(buildLowCardinalityMask(*lc, num_rows));

            /// Empty mask -> no candidate rows in this chunk, emit nothing.
            if (lc_mask->empty())
                return;
        }
    }

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
            buildFilter(*data.NAME, column_ptrs, filter, num_rows, data, lc_mask ? &*lc_mask : nullptr); \
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
