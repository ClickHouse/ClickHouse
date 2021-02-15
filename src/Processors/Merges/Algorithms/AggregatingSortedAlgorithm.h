#pragma once

#include <Processors/Merges/Algorithms/IMergingAlgorithmWithDelayedChunk.h>
#include <Processors/Merges/Algorithms/MergedData.h>

namespace DB
{

/** Merges several sorted inputs to one.
  * During this for each group of consecutive identical values of the primary key (the columns by which the data is sorted),
  * merges them into one row. When merging, the data is pre-aggregated - merge of states of aggregate functions,
  * corresponding to a one value of the primary key. For columns that are not part of the primary key and which do not have the AggregateFunction type,
  * when merged, the first value is selected.
  */
class AggregatingSortedAlgorithm final : public IMergingAlgorithmWithDelayedChunk
{
public:
    AggregatingSortedAlgorithm(
        const Block & header, size_t num_inputs,
        SortDescription description_, size_t max_block_size);

    void initialize(Inputs inputs) override;
    void consume(Input & input, size_t source_num) override;
    Status merge() override;

    struct SimpleAggregateDescription;
    struct AggregateDescription;

    /// This structure define columns into one of three types:
    /// * columns which are not aggregate functions and not needed to be aggregated
    /// * usual aggregate functions, which stores states into ColumnAggregateFunction
    /// * simple aggregate functions, which store states into ordinary columns
    struct ColumnsDefinition
    {
        ColumnsDefinition(); /// Is needed because destructor is defined.
        ColumnsDefinition(ColumnsDefinition &&) noexcept; /// Is needed because destructor is defined.
        ~ColumnsDefinition(); /// Is needed because otherwise std::vector's destructor uses incomplete types.

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

        /// Group is a group of rows with the same sorting key. It represents single row in result.
        /// Algorithm is: start group, add several rows, finish group.
        /// Then pull chunk when enough groups were added.
        void startGroup(const ColumnRawPtrs & raw_columns, size_t row);
        void finishGroup();

        bool isGroupStarted() const { return is_group_started; }
        void addRow(SortCursor & cursor); /// Possible only when group was started.

        Chunk pull(); /// Possible only if group was finished.

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
