#include <DataStreams/DistinctBlockInputStream.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}

DistinctBlockInputStream::DistinctBlockInputStream(const BlockInputStreamPtr & input, const SizeLimits & set_size_limits, size_t limit_hint_, const Names & columns)
    : columns_names(columns)
    , limit_hint(limit_hint_)
    , set_size_limits(set_size_limits)
{
    children.push_back(input);
}

Block DistinctBlockInputStream::readImpl()
{
    /// Execute until end of stream or until
    /// a block with some new records will be gotten.
    while (1)
    {
        if (no_more_rows)
            return Block();

        /// Stop reading if we already reach the limit.
        if (limit_hint && data.getTotalRowCount() >= limit_hint)
            return Block();

        Block block = children[0]->read();
        if (!block)
            return Block();

        const ColumnRawPtrs column_ptrs(getKeyColumns(block));
        if (column_ptrs.empty())
        {
            /// Only constants. We need to return single row.
            no_more_rows = true;
            for (auto & elem : block)
                elem.column = elem.column->cut(0, 1);
            return block;
        }

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

        if (!set_size_limits.check(data.getTotalRowCount(), data.getTotalByteCount(), "DISTINCT", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
            return {};

        for (auto & elem : block)
            elem.column = elem.column->filter(filter, -1);

        return block;
    }
}


template <typename Method>
void DistinctBlockInputStream::buildFilter(
    Method & method,
    const ColumnRawPtrs & columns,
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

        typename Method::Data::iterator it;
        bool inserted;
        method.data.emplace(key, it, inserted);

        if (inserted)
            method.onNewKey(*it, columns.size(), variants.string_pool);

        /// Emit the record if there is no such key in the current set yet.
        /// Skip it otherwise.
        filter[i] = inserted;
    }
}


ColumnRawPtrs DistinctBlockInputStream::getKeyColumns(const Block & block) const
{
    size_t columns = columns_names.empty() ? block.columns() : columns_names.size();

    ColumnRawPtrs column_ptrs;
    column_ptrs.reserve(columns);

    for (size_t i = 0; i < columns; ++i)
    {
        auto & column = columns_names.empty()
            ? block.safeGetByPosition(i).column
            : block.getByName(columns_names[i]).column;

        /// Ignore all constant columns.
        if (!column->isColumnConst())
            column_ptrs.emplace_back(column.get());
    }

    return column_ptrs;
}

}
