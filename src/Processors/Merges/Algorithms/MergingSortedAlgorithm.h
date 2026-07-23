#pragma once

#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Merges/Algorithms/MergedData.h>
#include <Core/Block_fwd.h>
#include <Core/SortDescription.h>
#include <Core/SortCursor.h>

#include <optional>


namespace DB
{

/// Merges several sorted inputs into one sorted output.
class MergingSortedAlgorithm final : public IMergingAlgorithm
{
public:
    MergingSortedAlgorithm(
        SharedHeader header_,
        size_t num_inputs,
        const SortDescription & description_,
        size_t max_block_size_,
        size_t max_block_size_bytes_,
        std::optional<size_t> max_dynamic_subcolumns_,
        SortingQueueStrategy sorting_queue_strategy_,
        UInt64 limit_ = 0,
        WriteBuffer * out_row_sources_buf_ = nullptr,
        const std::optional<String> & filter_column_name_ = std::nullopt,
        bool use_average_block_sizes = false,
        bool apply_virtual_row_conversions_ = true);

    void addInput();

    const char * getName() const override { return "MergingSortedAlgorithm"; }
    void initialize(Inputs inputs) override;
    void consume(Input & input, size_t source_num) override;
    Status merge() override;

    MergedStats getMergedStats() const override { return merged_data.getMergedStats(); }

private:
    SharedHeader header;

    MergedData merged_data;

    /// Settings
    SortDescription description;
    const UInt64 limit;
    bool has_collation = false;

    /// Used in Vertical merge algorithm to gather non-PK/non-index columns (on next step)
    /// If it is not nullptr then it should be populated during execution
    WriteBuffer * out_row_sources_buf = nullptr;

    /// The position of filter column if filter is set.
    ssize_t filter_column_position = -1;

    bool apply_virtual_row_conversions;

    /// Chunks currently being merged.
    Inputs current_inputs;

    SortingQueueStrategy sorting_queue_strategy;

    SortCursorImpls cursors;

    SortQueueVariants queue_variants;

    template <typename TSortingQueue>
    Status mergeImpl(TSortingQueue & queue);

    template <typename TSortingQueue>
    Status mergeBatchImpl(TSortingQueue & queue);

    bool hasFilter() const { return filter_column_position != -1; }
    void insertRow(const SortCursorImpl & current);
    void insertRows(const SortCursorImpl & current, size_t num_rows);
    void insertChunk(size_t source_num);

    size_t coveredSortPrefixSize(const Block & pk_block) const;
    void setPendingVirtualRow(size_t source_num, const Chunk & chunk, size_t num_covered);
    std::optional<size_t> sourceBlockedByPendingVirtualRow(const SortCursorImpl & cursor, size_t row) const;
    Status fetchPendingVirtualRowSource(size_t source_num);
    Status fetchAnyPendingVirtualRowSource();

    /// Boundaries announced by virtual rows that cover only a prefix of the sort description
    /// (the covered leading sort columns of the row, one entry per input, empty when none).
    /// Such a virtual row is not comparable on the full sort description, so it does not enter
    /// the queue; instead the merge does not emit rows that are not strictly below the boundary
    /// until the source's real data arrives.
    std::vector<std::optional<Columns>> pending_virtual_row_boundaries;
    size_t num_pending_virtual_rows = 0;

    /// Per-input pending virtual-row boundary (empty when none), used for debug checks to ensure virtual rows are placed correctly.
    std::vector<Columns> virtual_row_boundary;
};

}
