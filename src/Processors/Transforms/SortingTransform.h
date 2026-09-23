#pragma once

#include <Processors/IProcessor.h>
#include <Core/SortDescription.h>
#include <Core/SortCursor.h>
#include <Processors/ISource.h>


namespace DB
{

/// Merges an array of sorted chunks into a stream bounded by `max_merged_block_size` and `limit`.
class MergeSorter
{
public:
    enum class Mode
    {
        PreserveRows,
        /// Each input chunk is unique on the sort description. Equal keys retain the first input row.
        MergeUniqueChunks,
    };

    /// A nonzero `preferred_block_bytes` reduces the row limit using the average allocated row size.
    /// The byte target is approximate and keeps at least 128 rows unless the row limit is smaller.
    /// Zero disables byte-based sizing.
    MergeSorter(
        SharedHeader header, Chunks chunks_, const SortDescription & description,
        size_t max_merged_block_size_, UInt64 limit_, Mode mode_ = Mode::PreserveRows, size_t preferred_block_bytes = 0);

    /// Consumes at most `max_merged_block_size` rows. Duplicate-only progress returns columns with zero
    /// rows; an empty chunk marks completion. A nonzero `limit` counts emitted rows.
    Chunk read();

    /// Returns the row limit, reduced by the optional byte target using the input's average row size.
    size_t getMaxMergedBlockSize() const { return max_merged_block_size; }

private:
    Chunks chunks;
    size_t max_merged_block_size;
    UInt64 limit;
    const Mode mode;
    SortQueueVariants queue_variants;
    size_t total_merged_rows = 0;

    SortCursorImpls cursors;

    /// Input columns remain owned by `chunks` until merging finishes. The row position is saved
    /// separately because the source cursor advances after its rows are consumed.
    SortCursorImpl * last_emitted_cursor = nullptr;
    size_t last_emitted_row = 0;

    template <Mode merge_mode, typename TSortingQueue>
    Chunk mergeBatchImpl(TSortingQueue & queue);

    MutableColumns createMergedColumns() const;

};


class MergeSorterSource final : public ISource
{
public:
    MergeSorterSource(
        SharedHeader header, Chunks chunks, const SortDescription & description, size_t max_merged_block_size,
        UInt64 limit, MergeSorter::Mode mode = MergeSorter::Mode::PreserveRows, size_t preferred_block_bytes = 0)
        : ISource(header), merge_sorter(header, std::move(chunks), description, max_merged_block_size, limit, mode, preferred_block_bytes)
    {
    }

    String getName() const override { return "MergeSorterSource"; }

    void cancel(CancelReason reason) noexcept override;
    using ISource::cancel;

    /// These rows were already counted when they were read from the original source.
    std::optional<ReadProgress> getReadProgress() override { return std::nullopt; }

protected:
    Chunk generate() override { return merge_sorter.read(); }

private:
    MergeSorter merge_sorter;
};

/** Base class for sorting.
 *  Currently there are two implementations: MergeSortingTransform and FinishSortingTransform.
 */
class SortingTransform : public IProcessor
{
public:
    /// limit - if not 0, allowed to return just first 'limit' rows in sorted order.
    SortingTransform(SharedHeader header,
        const SortDescription & description_,
        size_t max_merged_block_size_,
        UInt64 limit_,
        bool increase_sort_description_compile_attempts);

    ~SortingTransform() override;

protected:
    Status prepare() final;
    void work() final;

    virtual void consume(Chunk chunk) = 0;
    virtual void generate() = 0;
    virtual void serialize();

    SortDescription description;
    size_t max_merged_block_size;
    const UInt64 limit;

    /// Before operation, will remove constant columns from blocks. And after, place constant columns back.
    /// (to avoid excessive virtual function calls and because constants cannot be serialized in Native format for temporary files)
    /// Save original block structure here.
    Block header_without_constants;
    /// Columns which were constant in header and we need to remove from chunks.
    std::vector<bool> const_columns_to_remove;

    void removeConstColumns(Chunk & chunk);
    void enrichChunkWithConstants(Chunk & chunk);

    enum class Stage : uint8_t
    {
        Consume = 0,
        Generate,
        Serialize,
    };

    Stage stage = Stage::Consume;

    bool generated_prefix = false;
    Chunk current_chunk;
    Chunk generated_chunk;
    Chunks chunks;

    std::unique_ptr<MergeSorter> merge_sorter;
    Processors processors;

private:
    Status prepareConsume();
    Status prepareSerialize();
    Status prepareGenerate();
};

}
