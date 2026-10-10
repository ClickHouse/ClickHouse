#pragma once

#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Merges/Algorithms/MergedData.h>
#include <Processors/Merges/Algorithms/RowFilterInfo.h>
#include <Core/Block_fwd.h>
#include <Core/SortDescription.h>
#include <Core/SortCursor.h>


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

    /// The position of the filter column of lightweight delete, which is a stored column no
    /// transform computes. A TTL merge's mask arrives on the chunks instead.
    ssize_t filter_column_position = -1;

    bool apply_virtual_row_conversions;

    /// Chunks currently being merged.
    Inputs current_inputs;

    /// Per source, the rows to keep in the chunk above, or nullptr when that chunk is not
    /// filtered. Valid for exactly as long as the chunk, so it is refreshed whenever one arrives
    /// and dropped when one is passed through whole.
    std::vector<const IColumnFilter *> source_row_filter_masks;

    SortingQueueStrategy sorting_queue_strategy;

    SortCursorImpls cursors;

    SortQueueVariants queue_variants;

    template <typename TSortingQueue>
    Status mergeImpl(TSortingQueue & queue);

    template <typename TSortingQueue>
    Status mergeBatchImpl(TSortingQueue & queue);

    bool hasFilter() const { return filter_column_position != -1; }

    /// Where the rows to keep come from: the filter column of the header, or the chunk's
    /// `RowFilterInfo`. Resolved once per chunk, so the per-row path is one indexed read.
    const IColumnFilter * resolveRowFilterMask(const Chunk & chunk) const;

    void insertRow(const SortCursorImpl & current);
    void insertRows(const SortCursorImpl & current, size_t num_rows);
    void insertChunk(size_t source_num);

    /// Per-input pending virtual-row boundary (empty when none), used for debug checks to ensure virtual rows are placed correctly.
    std::vector<Columns> virtual_row_boundary;
};

}
