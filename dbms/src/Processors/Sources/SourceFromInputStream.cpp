#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <DataTypes/DataTypeAggregateFunction.h>

namespace DB
{

SourceFromInputStream::SourceFromInputStream(Block header, BlockInputStreamPtr stream)
    : ISource(std::move(header)), stream(std::move(stream))
{
    auto & sample = getPort().getHeader();
    for (auto & type : sample.getDataTypes())
        if (typeid_cast<const DataTypeAggregateFunction *>(type.get()))
            has_aggregate_functions = true;
}

Chunk SourceFromInputStream::generate()
{
    if (stream_finished)
        return {};

    if (!initialized)
    {
        stream->readPrefix();
        initialized = true;
    }

    auto block = stream->read();
    if (!block)
    {
        stream->readSuffix();
        stream_finished = true;
        return {};
    }

    assertBlocksHaveEqualStructure(getPort().getHeader(), block, "SourceFromInputStream");

    UInt64 num_rows = block.rows();
    Chunk chunk(block.getColumns(), num_rows);

    if (has_aggregate_functions)
    {
        auto info = std::make_shared<AggregatedChunkInfo>();
        info->bucket_num = block.info.bucket_num;
        info->is_overflows = block.info.is_overflows;
        chunk.setChunkInfo(std::move(info));
    }

    return chunk;
}

}
