#pragma once
#include <DataStreams/IBlockInputStream.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Core/Block.h>
#include <Interpreters/Aggregator.h>

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
        time_t current_time,
        bool force_
    );

    String getName() const override { return "TTL"; }

    Block getHeader() const override { return header; }

protected:
    Block readImpl() override;

    /// Finalizes ttl infos and updates data part
    void readSuffixImpl() override;

private:
    const MergeTreeData & storage;

    /// ttl_infos and empty_columns are updating while reading
    const MergeTreeData::MutableDataPartPtr & data_part;

    time_t current_time;
    bool force;

    std::unique_ptr<Aggregator> aggregator;
    std::vector<Field> current_key_value;
    AggregatedDataVariants agg_result;
    ColumnRawPtrs agg_key_columns;
    Aggregator::AggregateColumns agg_aggregate_columns;
    bool agg_no_more_keys = false;

    IMergeTreeDataPart::TTLInfos old_ttl_infos;
    IMergeTreeDataPart::TTLInfos new_ttl_infos;
    NameSet empty_columns;

    size_t rows_removed = 0;
    Logger * log;
    const DateLUTImpl & date_lut;

    /// TODO rewrite defaults logic to evaluteMissingDefaults
    std::unordered_map<String, String> defaults_result_column;
    ExpressionActionsPtr defaults_expression;

    Block header;
private:
    /// Removes values with expired ttl and computes new_ttl_infos and empty_columns for part
    void removeValuesWithExpiredColumnTTL(Block & block);

    /// Removes rows with expired table ttl and computes new ttl_infos for part
    void removeRowsWithExpiredTableTTL(Block & block);

    // Calculate aggregates of aggregate_columns into agg_result
    void calculateAggregates(const MutableColumns & aggregate_columns, size_t start_pos, size_t length);

    /// Finalize agg_result into result_columns
    void finalizeAggregates(MutableColumns & result_columns);

    /// Updates TTL for moves
    void updateMovesTTL(Block & block);

    UInt32 getTimestampByIndex(const IColumn * column, size_t ind);
    bool isTTLExpired(time_t ttl) const;
};

}
