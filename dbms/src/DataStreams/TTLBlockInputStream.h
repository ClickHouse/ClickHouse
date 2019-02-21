#pragma once
#include <DataStreams/IBlockInputStream.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

class TTLBlockInputStream : public IBlockInputStream
{
public:
    TTLBlockInputStream(
        const BlockInputStreamPtr & input_,
        const MergeTreeData & storage_,
        const MergeTreeData::MutableDataPartPtr & data_part_,
        time_t current_time
    );

    String getName() const override { return "TTLBlockInputStream"; }

    Block getHeader() const override;

protected:
    Block readImpl() override;

private:
    const MergeTreeData & storage;

    /// min_ttl and empty_columns are updating while reading
    MergeTreeData::MutableDataPartPtr data_part;

    time_t current_time;

    Block header;

private:
    /// Removes values with expired ttl and computes new min_ttl and empty_columns for part
    void removeValuesWithExpiredColumnTTL(Block & block);

    /// Remove rows with expired table ttl and computes new min_ttl for part
    void removeRowsWithExpiredTableTTL(Block & block);
};

}
