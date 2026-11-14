#pragma once

#include <Common/VectorWithMemoryTracking.h>
#include <Core/Block_fwd.h>
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
        SharedHeader header, size_t num_inputs,
        SortDescription description_,
        /// List of columns to be summed. If empty, all numeric columns that are not in the description are taken.
        const Names & column_names_to_sum,
        /// List of partition key columns. They have to be excluded.
        const Names & partition_and_sorting_required_columns,
        size_t max_block_size_rows,
        size_t max_block_size_bytes,
        std::optional<size_t> max_dynamic_subcolumns_,
        const String & sum_function_name,
        const String & sum_function_map_name,
        bool remove_default_values,
        bool aggregate_all_columns,
        bool allow_tuple_element_aggregation);

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

        /// Memory pool for SimpleAggregateFunction
        /// (only when allocates_memory_in_arena == true).
        std::unique_ptr<Arena> arena;
        size_t arena_size = 0;

        /// Columns with which values should not be aggregated.
        ColumnNumbers column_numbers_not_to_aggregate;
        /// Columns which should be aggregated.
        VectorWithMemoryTracking<AggregateDescription> columns_to_aggregate;
        /// Mapping for nested columns.
        VectorWithMemoryTracking<MapDescription> maps_to_sum;

        /// Names of columns from header.
        Names column_names;

        /// Does SimpleAggregateFunction allocates memory in arena?
        bool allocates_memory_in_arena = false;

        /// Per-column flag: true for Float32/Float64 columns that need bit-exact
        /// copy via insertFrom() to avoid SNaN→QNaN conversion in Field roundtrip.
        VectorWithMemoryTracking<bool> columns_need_exact_copy;

        /// Record the origin header before tuple flattening.
        SharedHeader origin_header;

        bool allow_tuple_element_aggregation = false;

        /// Whether the result of a group of one row is that row: all aggregated columns are summed numbers
        /// (`0 + x` is `x`, except for -0.0), so such rows can be copied (see `insertRowsOfOwnGroups`).
        bool can_copy_rows_of_own_groups = false;
        /// Whether a row whose sums are all zero is removed (see `finishGroup`).
        bool removes_zero_rows = false;
    };

    /// Specialization for SummingSortedTransform. Inserts only data for non-aggregated columns.
    class SummingMergedData : public MergedData
    {
    private:
        using MergedData::pull;
        using MergedData::insertRow;

    public:
        SummingMergedData(UInt64 max_block_size_rows, UInt64 max_block_size_bytes_, std::optional<size_t> max_dynamic_subcolumns_, ColumnsDefinition & def_);

        void initialize(const Block & header, const IMergingAlgorithm::Inputs & inputs) override;

        void startGroup(ColumnRawPtrs & raw_columns, size_t row);
        void finishGroup();

        bool isGroupStarted() const { return is_group_started; }
        void addRow(ColumnRawPtrs & raw_columns, size_t row); /// Possible only when group was started.

        /// Writes the rows at the positions [begin, end) (through `permutation` if it is set), each a group of its own,
        /// like `startGroup` and `finishGroup` for each of them would (possible only when no group is started and
        /// `can_copy_rows_of_own_groups`): skips the rows with only zero sums if such rows are removed.
        void insertRowsOfOwnGroups(const ColumnRawPtrs & raw_columns, size_t begin, size_t end, const IColumn::Permutation * permutation);

        /// Whether the block size is limited by bytes too (then the rows are counted by `rowsToInsertBeforeFlush`).
        bool limitsBlockBytes() const { return max_block_size_bytes != 0 || use_average_block_size; }

        Chunk pull();

    private:
        ColumnsDefinition & def;

        bool is_group_started = false;

        Row current_row;
        /// Some types like Dynamic/JSON doesn't work well with Fields, for them we save values in IColumn.
        Columns current_row_columns;
        bool current_row_is_zero = true;    /// Are all summed columns zero (or empty)? It is updated incrementally.

        void addRowImpl(ColumnRawPtrs & raw_columns, size_t row);

        /// The data of the summed columns of the current batch and the rows to insert (see `insertRowsOfOwnGroups`).
        VectorWithMemoryTracking<const char *> sum_data;
        VectorWithMemoryTracking<size_t> rows_to_insert;

        /// Initialize aggregate descriptions with columns.
        void initAggregateDescription();
    };

private:
    /// Order between members is important because merged_data has reference to columns_definition.
    ColumnsDefinition columns_definition;
    SummingMergedData merged_data;

    /// The merge loop without `take_batches` has no check for batches, to keep the per-row path as it is.
    template <bool take_batches>
    Status mergeImpl();

    /// Whether `merge` takes the rows of a single source (the block of an INSERT) that differ from both neighbours in
    /// one go (decided in `initialize`, see `mergeImpl`).
    bool take_runs_of_distinct_keys = false;
    /// The position of the first row of the current group and whether the previous group had one row (for the runs above).
    size_t group_start_pos = std::numeric_limits<size_t>::max();
    bool previous_group_has_one_row = true;

    /// Finishes the group of the current row and writes the following rows of the batch, each a group
    /// of its own, in one go (see `mergeImpl`). With `compare_keys` the source may have duplicate keys:
    /// only the rows that differ from the next row are taken. Returns false if it did nothing.
    bool takeRowsOfOwnGroups(SortCursor current, size_t batch_size, bool compare_keys);
};

}
