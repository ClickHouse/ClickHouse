#include <Processors/Transforms/LimitByTransform.h>
#include <Common/PODArray.h>
#include <Common/SipHash.h>

namespace DB
{

LimitByTransform::LimitByTransform(const Block & header, size_t group_length_, size_t group_offset_, const Names & columns)
    : ISimpleTransform(header, header, true)
    , group_length(group_length_)
    , group_offset(group_offset_)
{
    key_positions.reserve(columns.size());

    for (const auto & name : columns)
    {
        auto position = header.getPositionByName(name);
        const auto & column = header.getByPosition(position).column;

        /// Ignore all constant columns.
        if (!(column && isColumnConst(*column)))
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

        auto count = keys_counts[key]++;
        if (count >= group_offset && count < group_length + group_offset)
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
            if (isColumnConst(*column))
                column = column->cut(0, inserted_count);
            else
                column = column->filter(filter, inserted_count);
    }

    chunk.setColumns(std::move(columns), inserted_count);
}

}
