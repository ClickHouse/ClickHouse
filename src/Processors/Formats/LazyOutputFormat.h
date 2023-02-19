#pragma once
#include <Processors/Formats/IOutputFormat.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <QueryPipeline/ProfileInfo.h>
#include <IO/WriteBuffer.h>

namespace DB
{

/// LazyOutputFormat is used to retrieve ready data from executing pipeline.
/// You can periodically call `getChunk` from separate thread.
/// Used in PullingAsyncPipelineExecutor.
class LazyOutputFormat : public IOutputFormat
{

public:
    explicit LazyOutputFormat(const Block & header)
        : IOutputFormat(header, out), queue(2) {}

    String getName() const override { return "LazyOutputFormat"; }

    Chunk getChunk(UInt64 milliseconds = 0)
    {
        if (isFinished())
            return {};

        if (chunks_to_output.empty())
        {
            if (milliseconds)
            {
                if (!queue.tryPop(chunks_to_output, milliseconds))
                    return {};
            }
            else
            {
                if (!queue.pop(chunks_to_output))
                    return {};
            }
        }

        Chunk chunk;
        if (!chunks_to_output.empty())
        {
            chunk = std::move(chunks_to_output.front());
            chunks_to_output.pop_front();
            info.update(chunk.getNumRows(), chunk.allocatedBytes());
        }
        return chunk;
    }

    Chunk getTotals();
    Chunk getExtremes();

    bool isFinished() { return chunks_to_output.empty() && queue.isFinishedAndEmpty(); }

    ProfileInfo & getProfileInfo() { return info; }

    void setRowsBeforeLimit(size_t rows_before_limit) override;

    void onCancel() override
    {
        /* buffered_chunks.clear(); */
        /* chunks_to_output.clear(); */
        /* currently_buffered_rows = 0; */
        /* currently_buffered_bytes = 0; */
        queue.clearAndFinish();
    }

    void finalizeImpl() override
    {
        flushBufferedChunks();
        queue.finish();
    }

    bool expectMaterializedColumns() const override { return false; }

protected:
    void consume(Chunk chunk) override
    {
        currently_buffered_rows += chunk.getNumRows();
        currently_buffered_bytes += chunk.bytes();
        buffered_chunks.push_back(std::move(chunk));
        if (currently_buffered_rows >= rows_to_buffer || currently_buffered_bytes >= bytes_to_buffer)
            flushBufferedChunks();
    }

    void flushBufferedChunks()
    {
        std::ignore = queue.emplace(std::move(buffered_chunks));
        buffered_chunks.clear();
        currently_buffered_rows = 0;
        currently_buffered_bytes = 0;
    }

    void consumeTotals(Chunk chunk) override { totals = std::move(chunk); }
    void consumeExtremes(Chunk chunk) override { extremes = std::move(chunk); }

private:
    using Chunks = std::deque<Chunk>;

    const size_t rows_to_buffer = DEFAULT_BLOCK_SIZE;
    const size_t bytes_to_buffer = 1024 * 1024;

    Chunks buffered_chunks;
    Chunks chunks_to_output;

    size_t currently_buffered_rows = 0;
    size_t currently_buffered_bytes = 0;

    ConcurrentBoundedQueue<Chunks> queue;
    Chunk totals;
    Chunk extremes;

    /// Is not used.
    static WriteBuffer out;

    ProfileInfo info;
};

}
