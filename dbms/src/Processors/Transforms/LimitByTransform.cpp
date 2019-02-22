#include <Processors/Transforms/LimitByTransform.h>
#include <Common/SipHash.h>

namespace DB
{

LimitByTransform::LimitByTransform(const Block & header, size_t group_size_, const Names & columns)
    : ISimpleTransform(header, header, true), group_size(group_size_)
{
    key_positions.reserve(columns.size());

    for (const auto & name : columns)
    {
        auto position = header.getPositionByName(name);
        auto & column = header.getByPosition(position).column;

        /// Ignore all constant columns.
        if (!(column && column->isColumnConst()))
            key_positions.emplace_back(position);
    }
}

void LimitByTransform::transform(Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    IColumn::Filter filter(num_rows);
    size_t inserted_count = 0;

    for (size_t row = 0; row < num_rows; ++row)
    {
        UInt128 key(0, 0);
        SipHash hash;

        for (auto position : key_positions)
            columns[position]->updateHashWithValue(row, hash);

        hash.get128(key.low, key.high);

        if (keys_counts[key]++ < group_size)
        {
            inserted_count++;
            filter[row] = 1;
        }
        else
            filter[row] = 0;
    }

    /// Just go to the next block if there isn't any new records in the current one.
    if (!inserted_count)
        /// SimpleTransform will skip it.
        return;

    if (inserted_count < num_rows)
    {
        for (auto & column : columns)
            if (column->isColumnConst())
                column = column->cut(0, inserted_count);
            else
                column = column->filter(filter, inserted_count);
    }

    chunk.setColumns(std::move(columns), inserted_count);
}

}
