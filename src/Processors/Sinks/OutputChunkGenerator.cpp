#include <Processors/Sinks/IOutputChunkGenerator.h>

namespace DB
{

class ForwardEverythingGenerator : public IOutputChunkGenerator
{
public:

    explicit ForwardEverythingGenerator() = default;

    void onNewChunkArrived(Chunk chunk) override
    {
        in_chunk = chunk.clone();
    }

    void onRowsProcessed(size_t /*row_count*/, bool /*append*/)  override
    {}

    Chunk generateChunk()  override
    {
        return std::move(in_chunk);
    }

private:
    Chunk in_chunk;
};

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

    void onRowsProcessed(size_t row_count, bool append)  override
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

    Chunk generateChunk()  override
    {
        return Chunk(std::move(out_cols), final_chunk_rows);
    }

private:
    Chunk in_chunk;
    MutableColumns out_cols;
    size_t row_offset = 0;
    size_t final_chunk_rows = 0;
};

std::unique_ptr<IOutputChunkGenerator> IOutputChunkGenerator::createCopyRanges(ContextPtr context)
{
    // If MV is responsible for deduplication, block is not considered duplicated.
    if (context->getSettingsRef().deduplicate_blocks_in_dependent_materialized_views)
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
