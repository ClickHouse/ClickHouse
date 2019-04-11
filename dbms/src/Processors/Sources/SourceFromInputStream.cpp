#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <DataTypes/DataTypeAggregateFunction.h>

namespace DB
{

SourceFromInputStream::SourceFromInputStream(InputStreamHolderPtr holder_)
    : ISource(holder_->getStream().getHeader()), holder(std::move(holder_))
{
    auto & sample = getPort().getHeader();
    for (auto & type : sample.getDataTypes())
        if (typeid_cast<const DataTypeAggregateFunction *>(type.get()))
            has_aggregate_functions = true;
}

Chunk SourceFromInputStream::generate()
{
    if (holder->isFinished())
        return {};

    auto block = holder->read();
    if (!block)
    {
        holder->readSuffix();
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
