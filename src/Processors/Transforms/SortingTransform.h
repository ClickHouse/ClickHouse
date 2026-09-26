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

    MergeSorter(
        SharedHeader header, Chunks chunks_, const SortDescription & description,
        size_t max_merged_block_size_, UInt64 limit_, Mode mode_ = Mode::PreserveRows);

    /// Consumes at most `max_merged_block_size` rows. Duplicate-only progress returns columns with zero
    /// rows; an empty chunk marks completion. A nonzero `limit` counts emitted rows.
    Chunk read();

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
        UInt64 limit, MergeSorter::Mode mode = MergeSorter::Mode::PreserveRows)
        : ISource(header), merge_sorter(header, std::move(chunks), description, max_merged_block_size, limit, mode)
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

/// Provides the sorting processor lifecycle shared by `MergeSortingTransform` and `FinishSortingTransform`.
class SortingTransform : public IProcessor
{
public:

    /// A nonzero `limit_` allows returning only that many rows from the beginning of the sorted result.
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
    virtual Status prepareSerialize();

    SortDescription description;
    size_t max_merged_block_size;
    const UInt64 limit;

    /// Holds the header without constant columns. Removing constants avoids excessive virtual calls
    /// during sorting and lets temporary files use the `Native` format, which cannot serialize
    /// `ColumnConst`. The original input header supplies their values when output chunks are restored.
    Block header_without_constants;

    /// Marks columns that are constant in the input header and must be removed from incoming chunks.
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
    Status prepareGenerate();
};

}
