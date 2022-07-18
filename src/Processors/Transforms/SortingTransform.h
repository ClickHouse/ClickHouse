#pragma once

#include <Processors/IProcessor.h>
#include <Core/SortDescription.h>
#include <Core/SortCursor.h>
#include <Processors/ISource.h>


namespace DB
{

/** Part of implementation. Merging array of ready (already read from somewhere) chunks.
  * Returns result of merge as stream of chunks, not more than 'max_merged_block_size' rows in each.
  */
class MergeSorter
{
public:
    MergeSorter(const Block & header, Chunks chunks_, SortDescription & description_, size_t max_merged_block_size_, UInt64 limit_);

    Chunk read();

private:
    Chunks chunks;
    SortDescription description;
    size_t max_merged_block_size;
    UInt64 limit;
    size_t total_merged_rows = 0;

    SortCursorImpls cursors;

    bool has_collation = false;

    SortingHeap<SortCursor> queue_without_collation;
    SortingHeap<SimpleSortCursor> queue_simple;
    SortingHeap<SortCursorWithCollation> queue_with_collation;

    /** Two different cursors are supported - with and without Collation.
      *  Templates are used (instead of virtual functions in SortCursor) for zero-overhead.
      */
    template <typename TSortingHeap>
    Chunk mergeImpl(TSortingHeap & queue);
};


class MergeSorterSource : public ISource
{
public:
    MergeSorterSource(const Block & header, Chunks chunks, SortDescription & description, size_t max_merged_block_size, UInt64 limit)
        : ISource(header), merge_sorter(header, std::move(chunks), description, max_merged_block_size, limit)
    {
    }

    String getName() const override { return "MergeSorterSource"; }

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
    SortingTransform(const Block & header,
        const SortDescription & description_,
        size_t max_merged_block_size_,
        UInt64 limit_,
        bool increase_sort_description_compile_attempts);

    ~SortingTransform() override;

protected:
    Status prepare() override final;
    void work() override final;

    virtual void consume(Chunk chunk) = 0;
    virtual void generate() = 0;
    virtual void serialize();

    SortDescription description;
    size_t max_merged_block_size;
    UInt64 limit;

    /// Before operation, will remove constant columns from blocks. And after, place constant columns back.
    /// (to avoid excessive virtual function calls and because constants cannot be serialized in Native format for temporary files)
    /// Save original block structure here.
    Block header_without_constants;
    /// Columns which were constant in header and we need to remove from chunks.
    std::vector<bool> const_columns_to_remove;

    void removeConstColumns(Chunk & chunk);
    void enrichChunkWithConstants(Chunk & chunk);

    enum class Stage
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
