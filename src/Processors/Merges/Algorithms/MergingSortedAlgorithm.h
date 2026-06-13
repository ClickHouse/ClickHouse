#pragma once

#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Merges/Algorithms/MergedData.h>
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
        bool apply_virtual_row_conversions_ = true,
        size_t virtual_row_prefetch_window_ = 0);

    void addInput();

    const char * getName() const override { return "MergingSortedAlgorithm"; }
    void initialize(Inputs inputs) override;
    void consume(Input & input, size_t source_num) override;
    Status merge() override;
    void onSourceExhausted(size_t source_num) override;

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

    /// When sources are deferred behind virtual rows, let the ones the merge will need
    /// soon read ahead in parallel, at most this many at once, so that the number of
    /// simultaneously resident readers stays bounded by the window rather than by the
    /// number of parts. Zero disables the read-ahead.
    const size_t virtual_row_prefetch_window;

    enum class SourceDeferralState : char
    {
        NotDeferred,        /// Real data was requested or has arrived.
        Deferred,           /// Still behind its virtual row, no read started.
        PrefetchIssued,     /// Read-ahead was issued, real data has not arrived yet.
    };

    /// Sources whose initial chunk is a virtual row (so their real data has not been
    /// requested yet), ordered by the virtual row sort key, i.e. in the order the merge
    /// will need them.
    std::vector<size_t> deferred_sources_in_merge_order;
    /// Position in `deferred_sources_in_merge_order` of the next source to prefetch.
    size_t next_deferred_source_pos = 0;
    /// Number of issued prefetches whose real data has not arrived yet.
    size_t prefetches_in_flight = 0;
    /// Per-source deferral state.
    std::vector<SourceDeferralState> source_deferral_state;
    /// Sources to report for read-ahead with the next `merge` status.
    std::vector<size_t> sources_to_prefetch;

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
    void topUpPrefetch();
    /// Mark a deferred source as no longer reading ahead (its real data arrived or it
    /// finished without any), freeing a read-ahead slot and refilling the window.
    void releaseDeferredSource(size_t source_num);
    void insertRow(const SortCursorImpl & current);
    void insertRows(const SortCursorImpl & current, size_t num_rows);
    void insertChunk(size_t source_num);

    /// Per-input pending virtual-row boundary (empty when none), used for debug checks to ensure virtual rows are placed correctly.
    std::vector<Columns> virtual_row_boundary;
};

}
