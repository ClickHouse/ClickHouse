#pragma once
#include <DataStreams/IBlockInputStream.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPart.h>
#include <Core/Block.h>

#include <common/DateLUT.h>

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

    /// Finalizes ttl infos and updates data part
    void readSuffixImpl() override;

private:
    const MergeTreeData & storage;

    /// ttl_infos and empty_columns are updating while reading
    const MergeTreeData::MutableDataPartPtr & data_part;

    time_t current_time;

    MergeTreeDataPart::TTLInfos old_ttl_infos;
    MergeTreeDataPart::TTLInfos new_ttl_infos;
    NameSet empty_columns;

    size_t rows_removed = 0;
    Logger * log;
    DateLUTImpl date_lut;

    std::unordered_map<String, String> defaults_result_column;
    ExpressionActionsPtr defaults_expression;
private:
    /// Removes values with expired ttl and computes new min_ttl and empty_columns for part
    void removeValuesWithExpiredColumnTTL(Block & block);

    /// Remove rows with expired table ttl and computes new min_ttl for part
    void removeRowsWithExpiredTableTTL(Block & block);

    UInt32 getTimestampByIndex(const IColumn * column, size_t ind);
};

}
