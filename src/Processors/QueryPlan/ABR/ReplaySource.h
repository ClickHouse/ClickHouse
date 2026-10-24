#pragma once

#include <Core/ProtocolDefines.h>
#include <Formats/NativeReader.h>
#include <IO/ReadBuffer.h>
#include <Processors/ISource.h>
#include <Processors/Transforms/AggregatingTransform.h>

namespace DB
{

/// Source that deserializes blocks from a ReadBuffer written by NativeBufferingFormat.
class ReplaySource : public ISource
{
public:
    ReplaySource(SharedHeader header, std::unique_ptr<ReadBuffer> buf_, bool add_aggregation_info_)
        : ISource(header)
        , buf(std::move(buf_))
        , reader(*buf, *header, DBMS_TCP_PROTOCOL_VERSION)
        , add_aggregation_info(add_aggregation_info_)
    {
    }

    String getName() const override { return "ReplaySource"; }

protected:
    Chunk generate() override
    {
        Block block = reader.read();
        if (block.empty())
            return {};
        Chunk chunk(block.getColumns(), block.rows());
        if (add_aggregation_info)
        {
            auto info = std::make_shared<AggregatedChunkInfo>();
            info->bucket_num = block.info.bucket_num;
            info->is_overflows = block.info.is_overflows;
            info->out_of_order_buckets = block.info.out_of_order_buckets;
            chunk.getChunkInfos().add(std::move(info));
        }
        return chunk;
    }

private:
    std::unique_ptr<ReadBuffer> buf;
    NativeReader reader;
    bool add_aggregation_info;
};

}
