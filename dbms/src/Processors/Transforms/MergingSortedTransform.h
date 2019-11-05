#pragma once
#include <Processors/IProcessor.h>
#include <Core/SortDescription.h>
#include <Core/SortCursor.h>
#include <Processors/SharedChunk.h>

#include <queue>

namespace DB
{

class MergingSortedTransform : public IProcessor
{
public:
    MergingSortedTransform(
        const Block & header,
        size_t num_inputs,
        const SortDescription & description_,
        size_t max_block_size,
        UInt64 limit = 0,
        bool quiet = false,
        bool have_all_inputs = true);

    String getName() const override { return "MergingSortedTransform"; }
    Status prepare() override;
    void work() override;

    void addInput();
    void setHaveAllInputs();

protected:

    class MergedData
    {
    public:
        explicit MergedData(const Block & header)
        {
            columns.reserve(header.columns());
            for (const auto & column : header)
                columns.emplace_back(column.type->createColumn());
        }

        void insertRow(const ColumnRawPtrs & raw_columns, size_t row)
        {
            size_t num_columns = raw_columns.size();
            for (size_t i = 0; i < num_columns; ++i)
                columns[i]->insertFrom(*raw_columns[i], row);

            ++total_merged_rows;
            ++merged_rows;
        }

        void insertFromChunk(Chunk && chunk, size_t limit_rows)
        {
            if (merged_rows)
                throw Exception("Cannot insert to MergedData from Chunk because MergedData is not empty.",
                                ErrorCodes::LOGICAL_ERROR);

            auto num_rows = chunk.getNumRows();
            columns = chunk.mutateColumns();
            if (limit_rows && num_rows > limit_rows)
                for (auto & column : columns)
                    column = (*column->cut(0, limit_rows)->convertToFullColumnIfConst()).mutate();

            total_merged_rows += num_rows;
            merged_rows = num_rows;
        }

        Chunk pull()
        {
            MutableColumns empty_columns;
            empty_columns.reserve(columns.size());

            for (const auto & column : columns)
                empty_columns.emplace_back(column->cloneEmpty());

            empty_columns.swap(columns);
            Chunk chunk(std::move(empty_columns), merged_rows);
            merged_rows = 0;

            return chunk;
        }

        UInt64 totalMergedRows() const { return total_merged_rows; }
        UInt64 mergedRows() const { return merged_rows; }

    private:
        UInt64 total_merged_rows = 0;
        UInt64 merged_rows = 0;
        MutableColumns columns;
    };

    /// Settings
    SortDescription description;
    const size_t max_block_size;
    UInt64 limit;
    bool has_collation = false;
    bool quiet = false;

    std::atomic<bool> have_all_inputs;

    MergedData merged_data;

    /// Used in Vertical merge algorithm to gather non-PK/non-index columns (on next step)
    /// If it is not nullptr then it should be populated during execution
    WriteBuffer * out_row_sources_buf = nullptr;

    /// Chunks currently being merged.
    std::vector<SharedChunkPtr> source_chunks;

    using CursorImpls = std::vector<SortCursorImpl>;
    CursorImpls cursors;

    using Queue = std::priority_queue<SortCursor>;
    Queue queue_without_collation;

    using QueueWithCollation = std::priority_queue<SortCursorWithCollation>;
    QueueWithCollation queue_with_collation;

private:

    /// Processor state.
    bool is_initialized = false;
    bool is_finished = false;
    bool need_data = false;
    size_t next_input_to_read = 0;

    template <typename TSortCursor>
    void merge(std::priority_queue<TSortCursor> & queue);

    void insertFromChunk(size_t source_num);

    void updateCursor(Chunk chunk, size_t source_num)
    {
        auto num_rows = chunk.getNumRows();
        auto columns = chunk.detachColumns();
        for (auto & column : columns)
            column = column->convertToFullColumnIfConst();

        chunk.setColumns(std::move(columns), num_rows);

        auto & shared_chunk_ptr = source_chunks[source_num];

        if (!shared_chunk_ptr)
        {
            shared_chunk_ptr = new detail::SharedChunk(std::move(chunk));
            cursors[source_num] = SortCursorImpl(shared_chunk_ptr->getColumns(), description, source_num);
            has_collation |= cursors[source_num].has_collation;
        }
        else
        {
            *shared_chunk_ptr = std::move(chunk);
            cursors[source_num].reset(shared_chunk_ptr->getColumns(), {});
        }

        shared_chunk_ptr->all_columns = cursors[source_num].all_columns;
        shared_chunk_ptr->sort_columns = cursors[source_num].sort_columns;
    }

    void pushToQueue(size_t source_num)
    {
        if (has_collation)
            queue_with_collation.push(SortCursorWithCollation(&cursors[source_num]));
        else
            queue_without_collation.push(SortCursor(&cursors[source_num]));
    }

    template <typename TSortCursor>
    void initQueue(std::priority_queue<TSortCursor> & queue)
    {
        for (auto & cursor : cursors)
            if (!cursor.empty())
                queue.push(TSortCursor(&cursor));
    }
};

}
