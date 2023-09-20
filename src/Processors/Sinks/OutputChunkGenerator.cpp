#include <Processors/Sinks/IOutputChunkGenerator.h>

namespace DB
{

/// Default implementation. The new chunk received is forwarded as-is to the next stages of the query
class ForwardEverythingGenerator : public IOutputChunkGenerator
{
public:

    explicit ForwardEverythingGenerator() = default;

    void onNewChunkArrived(Chunk chunk) override
    {
        in_chunk = chunk.clone();
    }

    void onRowsProcessed(size_t /*row_count*/, bool /*append*/) override
    {}

    Chunk generateChunk() override
    {
        return std::move(in_chunk);
    }

private:
    Chunk in_chunk;
};

/// Specific implementation which generates a chunk with just a subset of the rows received originally
/// Rows are assumed to be processed in the same order than they appear in the original chunk
/// Is up to the client to decide how many rows process at once, but after each range processed,
/// onRowsProcessed() has to be called, indicating whether append that range to the output chunk or not
class CopyRangesGenerator : public IOutputChunkGenerator
{
public:
    explicit CopyRangesGenerator() = default;

    void onNewChunkArrived(Chunk chunk) override
    {
        out_cols = chunk.cloneEmptyColumns();
        in_chunk = std::move(chunk);
        row_offset = 0;
        final_chunk_rows = 0;
    }

    void onRowsProcessed(size_t row_count, bool append) override
    {
        if (append)
        {
            const Columns& in_cols = in_chunk.getColumns();
            for (size_t i = 0; i < out_cols.size(); i++)
            {
                out_cols[i]->insertRangeFrom(*(in_cols[i]), row_offset, row_count);
            }
            final_chunk_rows += row_count;
        }

        row_offset += row_count;
    }

    Chunk generateChunk() override
    {
        return Chunk(std::move(out_cols), final_chunk_rows);
    }

private:
    Chunk in_chunk;
    MutableColumns out_cols;
    size_t row_offset = 0;
    size_t final_chunk_rows = 0;
};

std::unique_ptr<IOutputChunkGenerator> IOutputChunkGenerator::createCopyRanges(bool deduplicate_later)
{
    // If MV is responsible for deduplication, block won't be considered duplicated.
    // So default implementation, forwarding all the data, is used
    if (deduplicate_later)
    {
        return createDefault();
    }

    return std::make_unique<CopyRangesGenerator>();
}

std::unique_ptr<IOutputChunkGenerator> IOutputChunkGenerator::createDefault()
{
    return std::make_unique<ForwardEverythingGenerator>();
}

}
