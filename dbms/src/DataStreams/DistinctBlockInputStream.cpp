#include <DataStreams/DistinctBlockInputStream.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}

DistinctBlockInputStream::DistinctBlockInputStream(BlockInputStreamPtr input_, const Limits & limits, size_t limit_hint_, Names columns_)
    : columns_names(columns_)
    , limit_hint(limit_hint_)
    , max_rows(limits.max_rows_in_distinct)
    , max_bytes(limits.max_bytes_in_distinct)
    , overflow_mode(limits.distinct_overflow_mode)
{
    children.push_back(input_);
}

String DistinctBlockInputStream::getID() const
{
    std::stringstream res;
    res << "Distinct(" << children.back()->getID() << ")";
    return res.str();
}

Block DistinctBlockInputStream::readImpl()
{
    /// Execute until end of stream or until
    /// a block with some new records will be gotten.
    while (1)
    {
        /// Stop reading if we already reach the limit.
        if (limit_hint && data.getTotalRowCount() >= limit_hint)
            return Block();

        Block block = children[0]->read();
        if (!block)
            return Block();

        const ConstColumnPlainPtrs column_ptrs(getKeyColumns(block));
        if (column_ptrs.empty())
            return block;

        if (data.empty())
            data.init(SetVariants::chooseMethod(column_ptrs, key_sizes));

        const size_t old_set_size = data.getTotalRowCount();
        const size_t rows = block.rows();
        IColumn::Filter filter(rows);

        switch (data.type)
        {
            case SetVariants::Type::EMPTY:
                break;
    #define M(NAME) \
            case SetVariants::Type::NAME: \
                buildFilter(*data.NAME, column_ptrs, filter, rows, data); \
                break;
        APPLY_FOR_SET_VARIANTS(M)
    #undef M
        }

        /// Just go to the next block if there isn't any new record in the current one.
        if (data.getTotalRowCount() == old_set_size)
            continue;

        if (!checkLimits())
        {
            if (overflow_mode == OverflowMode::THROW)
                throw Exception("DISTINCT-Set size limit exceeded."
                    " Rows: " + toString(data.getTotalRowCount()) +
                    ", limit: " + toString(max_rows) +
                    ". Bytes: " + toString(data.getTotalByteCount()) +
                    ", limit: " + toString(max_bytes) + ".",
                    ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);

            if (overflow_mode == OverflowMode::BREAK)
                return Block();

            throw Exception("Logical error: unknown overflow mode", ErrorCodes::LOGICAL_ERROR);
        }

        size_t all_columns = block.columns();
        for (size_t i = 0; i < all_columns; ++i)
            block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->filter(filter, -1);

        return block;
    }
}

bool DistinctBlockInputStream::checkLimits() const
{
    if (max_rows && data.getTotalRowCount() > max_rows)
        return false;
    if (max_bytes && data.getTotalByteCount() > max_bytes)
        return false;
    return true;
}

template <typename Method>
void DistinctBlockInputStream::buildFilter(
    Method & method,
    const ConstColumnPlainPtrs & columns,
    IColumn::Filter & filter,
    size_t rows,
    SetVariants & variants) const
{
    typename Method::State state;
    state.init(columns);

    for (size_t i = 0; i < rows; ++i)
    {
        /// Make a key.
        typename Method::Key key = state.getKey(columns, columns.size(), i, key_sizes);

        typename Method::Data::iterator it = method.data.find(key);
        bool inserted;
        method.data.emplace(key, it, inserted);

        if (inserted)
            method.onNewKey(*it, columns.size(), i, variants.string_pool);

        /// Emit the record if there is no such key in the current set yet.
        /// Skip it otherwise.
        filter[i] = inserted;
    }
}

ConstColumnPlainPtrs DistinctBlockInputStream::getKeyColumns(const Block & block) const
{
    size_t columns = columns_names.empty() ? block.columns() : columns_names.size();

    ConstColumnPlainPtrs column_ptrs;
    column_ptrs.reserve(columns);

    for (size_t i = 0; i < columns; ++i)
    {
        auto & column = columns_names.empty()
            ? block.safeGetByPosition(i).column
            : block.getByName(columns_names[i]).column;

        /// Ignore all constant columns.
        if (!column->isConst())
            column_ptrs.emplace_back(column.get());
    }

    return column_ptrs;
}

}
