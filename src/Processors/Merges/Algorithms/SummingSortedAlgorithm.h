#pragma once

#include <Processors/Merges/Algorithms/IMergingAlgorithmWithDelayedChunk.h>
#include <Processors/Merges/Algorithms/MergedData.h>


namespace DB
{

/** Merges several sorted inputs into one.
  * For each group of consecutive identical values of the primary key (the columns by which the data is sorted),
  *  collapses them into one row, summing all the numeric columns except the primary key.
  * If in all numeric columns, except for the primary key, the result is zero, it deletes the row.
  */
class SummingSortedAlgorithm final : public IMergingAlgorithmWithDelayedChunk
{
public:
    SummingSortedAlgorithm(
        const Block & header, size_t num_inputs,
        SortDescription description_,
        /// List of columns to be summed. If empty, all numeric columns that are not in the description are taken.
        const Names & column_names_to_sum,
        /// List of partition key columns. They have to be excluded.
        const Names & partition_key_columns,
        size_t max_block_size_rows,
        size_t max_block_size_bytes);

    const char * getName() const override { return "SummingSortedAlgorithm"; }
    void initialize(Inputs inputs) override;
    void consume(Input & input, size_t source_num) override;
    Status merge() override;

    MergedStats getMergedStats() const override { return merged_data.getMergedStats(); }

    struct AggregateDescription;
    struct MapDescription;

    /// This structure define columns into one of three types:
    /// * columns which values not needed to be aggregated
    /// * aggregate functions and columns which needed to be summed
    /// * mapping for nested columns
    struct ColumnsDefinition
    {
        ColumnsDefinition(); /// Is needed because destructor is defined.
        ColumnsDefinition(ColumnsDefinition &&) noexcept; /// Is needed because destructor is defined.
        ~ColumnsDefinition(); /// Is needed because otherwise std::vector's destructor uses incomplete types.

        /// Columns with which values should not be aggregated.
        ColumnNumbers column_numbers_not_to_aggregate;
        /// Columns which should be aggregated.
        std::vector<AggregateDescription> columns_to_aggregate;
        /// Mapping for nested columns.
        std::vector<MapDescription> maps_to_sum;

        /// Names of columns from header.
        Names column_names;

        /// Does SimpleAggregateFunction allocates memory in arena?
        bool allocates_memory_in_arena = false;
    };

    /// Specialization for SummingSortedTransform. Inserts only data for non-aggregated columns.
    class SummingMergedData : public MergedData
    {
    private:
        using MergedData::pull;
        using MergedData::insertRow;

    public:
        SummingMergedData(UInt64 max_block_size_rows, UInt64 max_block_size_bytes_, ColumnsDefinition & def_);

        void initialize(const Block & header, const IMergingAlgorithm::Inputs & inputs) override;

        void startGroup(ColumnRawPtrs & raw_columns, size_t row);
        void finishGroup();

        bool isGroupStarted() const { return is_group_started; }
        void addRow(ColumnRawPtrs & raw_columns, size_t row); /// Possible only when group was started.

        Chunk pull();

    private:
        ColumnsDefinition & def;

        /// Memory pool for SimpleAggregateFunction
        /// (only when allocates_memory_in_arena == true).
        std::unique_ptr<Arena> arena;
        size_t arena_size = 0;

        bool is_group_started = false;

        Row current_row;
        bool current_row_is_zero = true;    /// Are all summed columns zero (or empty)? It is updated incrementally.

        void addRowImpl(ColumnRawPtrs & raw_columns, size_t row);

        /// Initialize aggregate descriptions with columns.
        void initAggregateDescription();
    };

private:
    /// Order between members is important because merged_data has reference to columns_definition.
    ColumnsDefinition columns_definition;
    SummingMergedData merged_data;
};

}
