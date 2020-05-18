#pragma once

#include <Processors/Merges/Algorithms/IMergingAlgorithmWithDelayedChunk.h>
#include <Processors/Merges/Algorithms/MergedData.h>
#include <Core/Row.h>

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
        size_t max_block_size);

    void initialize(Chunks chunks) override;
    void consume(Chunk & chunk, size_t source_num) override;
    Status merge() override;

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
    };

    /// Specialization for SummingSortedTransform. Inserts only data for non-aggregated columns.
    class SummingMergedData : public MergedData
    {
    private:
        using MergedData::pull;
        using MergedData::insertRow;

    public:
        SummingMergedData(MutableColumns columns_, UInt64 max_block_size_, ColumnsDefinition & def_);

        void startGroup(ColumnRawPtrs & raw_columns, size_t row);
        void finishGroup();

        bool isGroupStarted() const { return is_group_started; }
        void addRow(ColumnRawPtrs & raw_columns, size_t row); /// Possible only when group was started.

        Chunk pull();

    private:
        ColumnsDefinition & def;

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
