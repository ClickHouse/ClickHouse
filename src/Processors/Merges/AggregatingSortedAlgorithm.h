#pragma once

#include <Processors/Merges/IMergingAlgorithmWithDelayedChunk.h>
#include <Processors/Merges/MergedData.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/AlignedBuffer.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Common/Arena.h>

namespace DB
{

class AggregatingSortedAlgorithm final : public IMergingAlgorithmWithDelayedChunk
{
public:
    AggregatingSortedAlgorithm(
        const Block & header, size_t num_inputs,
        SortDescription description_, size_t max_block_size);

    void initialize(Chunks chunks) override;
    void consume(Chunk chunk, size_t source_num) override;
    Status merge() override;

    struct SimpleAggregateDescription;
    struct AggregateDescription;

    /// This structure define columns into one of three types:
    /// * columns which are not aggregate functions and not needed to be aggregated
    /// * usual aggregate functions, which stores states into ColumnAggregateFunction
    /// * simple aggregate functions, which store states into ordinary columns
    struct ColumnsDefinition
    {
        /// Columns with which numbers should not be aggregated.
        ColumnNumbers column_numbers_not_to_aggregate;
        std::vector<AggregateDescription> columns_to_aggregate;
        std::vector<SimpleAggregateDescription> columns_to_simple_aggregate;

        /// Does SimpleAggregateFunction allocates memory in arena?
        bool allocates_memory_in_arena = false;
    };

private:
    /// Specialization for AggregatingSortedAlgorithm.
    struct AggregatingMergedData : public MergedData
    {
    private:
        using MergedData::pull;
        using MergedData::insertRow;

    public:
        AggregatingMergedData(MutableColumns columns_, UInt64 max_block_size_, ColumnsDefinition & def_);

        void startGroup(const ColumnRawPtrs & raw_columns, size_t row);
        void finishGroup();

        bool isGroupStarted() const { return is_group_started; }
        void addRow(SortCursor & cursor);

        Chunk pull();

    private:
        ColumnsDefinition & def;

        /// Memory pool for SimpleAggregateFunction
        /// (only when allocates_memory_in_arena == true).
        std::unique_ptr<Arena> arena;

        bool is_group_started = false;

        /// Initialize aggregate descriptions with columns.
        void initAggregateDescription();
    };

    /// Order between members is important because merged_data has reference to columns_definition.
    ColumnsDefinition columns_definition;
    AggregatingMergedData merged_data;
};

}
