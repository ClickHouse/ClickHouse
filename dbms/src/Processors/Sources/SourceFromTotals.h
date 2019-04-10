#pragma once
#include <Processors/ISource.h>
#include <DataStreams/IBlockInputStream.h>

namespace DB
{

class IBlockInputStream;
using BlockInputStreamPtr = std::shared_ptr<IBlockInputStream>;
using BlockInputStreams = std::vector<BlockInputStreamPtr>;

class SourceFromTotals : public ISource
{
public:
    explicit SourceFromTotals(BlockInputStreams streams_)
        : ISource(streams_.at(0)->getHeader()), streams(std::move(streams_)) {}

    String getName() const override { return "SourceFromTotals"; }

    Chunk generate() override
    {
        if (generated)
            return {};

        generated = true;

        for (auto & stream : streams)
            if (auto block = stream->getTotals())
                return Chunk(block.getColumns(), 1);

        return {};
    }

private:
    bool generated = false;
    BlockInputStreams streams;
};

}
