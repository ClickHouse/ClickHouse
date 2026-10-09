#pragma once
#include <Columns/IColumn.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/RowsBeforeStepCounter.h>
#include <Common/HashTable/HashMap.h>


namespace DB
{

/// Executes LIMIT BY for specified columns.
class LimitByTransform : public ISimpleTransform
{
public:
    LimitByTransform(SharedHeader header, UInt64 group_length_, UInt64 group_offset_, bool in_order_, const Names & columns);

    String getName() const override;

    void setRowsBeforeLimitCounter(RowsBeforeStepCounterPtr counter) override { rows_before_limit_at_least.swap(counter); }

protected:
    void transform(Chunk & chunk) override;

private:
    void transformCommon(Chunk & chunk);
    void transformInOrder(Chunk & chunk);
    void finalizeChunk(Chunk & chunk, Columns && columns, const IColumn::Filter & filter, UInt64 num_rows, UInt64 inserted_count);

    using MapHashed = HashMap<UInt128, UInt64, UInt128TrivialHash>;

    MapHashed keys_counts;
    std::vector<size_t> key_positions;
    const UInt64 group_length;
    const UInt64 group_offset;
    const bool in_order;

    UInt128 current_key;
    UInt64 current_key_count = 0;
    bool first_row = true;

    RowsBeforeStepCounterPtr rows_before_limit_at_least;
};

}
