#include <Processors/Transforms/LimitByTransform.h>
#include <Columns/IColumn.h>
#include <Common/PODArray.h>
#include <Common/SipHash.h>

namespace DB
{

LimitByTransform::LimitByTransform(SharedHeader header, UInt64 group_length_, UInt64 group_offset_, bool in_order_, const Names & columns)
    : ISimpleTransform(header, header, true)
    , group_length(group_length_)
    , group_offset(group_offset_)
    , in_order(in_order_)
{
    key_positions.reserve(columns.size());

    for (const auto & name : columns)
    {
        auto position = header->getPositionByName(name);
        const auto & column = header->getByPosition(position).column;

        /// Ignore all constant columns.
        if (!(column && isColumnConst(*column)))
            key_positions.emplace_back(position);
    }
}

String LimitByTransform::getName() const
{
    return fmt::format("LimitByTransform{}", in_order ? " (InOrder)" : "");
}

void LimitByTransform::transform(Chunk & chunk)
{
    if (in_order)
        transformInOrder(chunk);
    else
        transformCommon(chunk);
}

void LimitByTransform::transformCommon(Chunk & chunk)
{
    UInt64 num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    IColumn::Filter filter(num_rows);
    UInt64 inserted_count = 0;

    for (UInt64 row = 0; row < num_rows; ++row)
    {
        SipHash hash;
        for (auto position : key_positions)
            columns[position]->updateHashWithValue(row, hash);

        const auto key = hash.get128();
        auto count = keys_counts[key]++;
        if (count >= group_offset
            && (group_length > std::numeric_limits<UInt64>::max() - group_offset || count < group_length + group_offset))
        {
            ++inserted_count;
            filter[row] = 1;
        }
        else
            filter[row] = 0;
    }

    finalizeChunk(chunk, std::move(columns), filter, num_rows, inserted_count);
}

void LimitByTransform::transformInOrder(Chunk & chunk)
{
    UInt64 num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    IColumn::Filter filter(num_rows);
    UInt64 inserted_count = 0;

    for (UInt64 row = 0; row < num_rows; ++row)
    {
        SipHash hash;
        for (auto position : key_positions)
            columns[position]->updateHashWithValue(row, hash);

        const auto key = hash.get128();

        if (first_row)
        {
            current_key = key;
            current_key_count = 0;
            first_row = false;
        }
        else if (current_key != key)
        {
            current_key = key;
            current_key_count = 0;
        }
        else
            ++current_key_count;

        if (current_key_count >= group_offset
            && (group_length > std::numeric_limits<UInt64>::max() - group_offset || current_key_count < group_length + group_offset))
        {
            ++inserted_count;
            filter[row] = 1;
        }
        else
            filter[row] = 0;
    }

    finalizeChunk(chunk, std::move(columns), filter, num_rows, inserted_count);
}

void LimitByTransform::finalizeChunk(
    Chunk & chunk, Columns && columns, const IColumn::Filter & filter, UInt64 num_rows, UInt64 inserted_count)
{
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

    if (rows_before_limit_at_least)
        rows_before_limit_at_least->add(inserted_count);

    chunk.setColumns(std::move(columns), inserted_count);
}

}
