#include <Processors/Sources/SourceFromSingleChunk.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Processors/Transforms/AggregatingTransform.h>

namespace DB
{

SourceFromSingleChunk::SourceFromSingleChunk(Block header, Chunk chunk_) : ISource(std::move(header)), chunk(std::move(chunk_))
{
}

SourceFromSingleChunk::SourceFromSingleChunk(Block data) : ISource(data.cloneEmpty()), chunk(data.getColumns(), data.rows())
{
    const auto & sample = getPort().getHeader();
    bool has_aggregate_functions = false;
    for (auto & type : sample.getDataTypes())
        if (typeid_cast<const DataTypeAggregateFunction *>(type.get()))
            has_aggregate_functions = true;

    if (has_aggregate_functions)
    {
        auto info = std::make_shared<AggregatedChunkInfo>();
        info->bucket_num = data.info.bucket_num;
        info->is_overflows = data.info.is_overflows;
        chunk.getChunkInfos().add(std::move(info));
    }
}

String SourceFromSingleChunk::getName() const
{
    return "SourceFromSingleChunk";
}

Chunk SourceFromSingleChunk::generate()
{
    return std::move(chunk);
}

}
