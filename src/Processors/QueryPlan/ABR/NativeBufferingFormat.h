#pragma once

#include <Core/ProtocolDefines.h>
#include <Formats/NativeWriter.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Transforms/AggregatingTransform.h>

namespace DB
{

/// Output format that serializes blocks in Native format to a WriteBuffer,
/// capturing totals and extremes separately for later replay.
class NativeBufferingFormat : public IOutputFormat
{
public:
    NativeBufferingFormat(SharedHeader header, WriteBuffer & buf)
        : IOutputFormat(header, buf)
        , writer(buf, DBMS_TCP_PROTOCOL_VERSION, header)
    {
    }

    String getName() const override { return "NativeBufferingFormat"; }

    Block totals;
    Block extremes;

protected:
    void consume(Chunk chunk) override
    {
        auto block = getPort(PortKind::Main).getHeader().cloneWithColumns(chunk.detachColumns());
        if (auto agg_info = chunk.getChunkInfos().get<AggregatedChunkInfo>())
        {
            block.info.is_overflows = agg_info->is_overflows;
            block.info.bucket_num = agg_info->bucket_num;
            block.info.out_of_order_buckets = agg_info->out_of_order_buckets;
        }
        if (block.rows())
            writer.write(block);
    }

    void consumeTotals(Chunk chunk) override
    {
        totals = getPort(PortKind::Totals).getHeader().cloneWithColumns(chunk.detachColumns());
    }

    void consumeExtremes(Chunk chunk) override
    {
        extremes = getPort(PortKind::Extremes).getHeader().cloneWithColumns(chunk.detachColumns());
    }

    void finalizeImpl() override { writer.flush(); }

private:
    NativeWriter writer;
};

}
