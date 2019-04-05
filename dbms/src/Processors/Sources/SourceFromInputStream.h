#pragma once
#include <Processors/ISource.h>
#include <DataStreams/IBlockInputStream.h>

namespace DB
{

class IBlockInputStream;
using BlockInputStreamPtr = std::shared_ptr<IBlockInputStream>;

class SourceFromInputStream : public ISource
{
public:
    SourceFromInputStream(Block header, BlockInputStreamPtr stream);
    String getName() const override { return "SourceFromInputStream"; }

    Chunk generate() override;

    BlockInputStreamPtr & getStream() { return stream; }

private:
    bool initialized = false;
    bool stream_finished = false;
    BlockInputStreamPtr stream;
};

}
