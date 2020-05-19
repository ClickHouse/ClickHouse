#include <Processors/Sources/SinkToOutputStream.h>
#include <DataStreams/IBlockOutputStream.h>

namespace DB
{

SinkToOutputStream::SinkToOutputStream(BlockOutputStreamPtr stream_)
    : ISink(stream_->getHeader())
    , stream(std::move(stream_))
{
}

void SinkToOutputStream::consume(Chunk chunk)
{
    if (!initialized)
        stream->writePrefix();

    initialized = true;

    stream->write(getPort().getHeader().cloneWithColumns(chunk.detachColumns()));
}

void SinkToOutputStream::onFinish()
{
    stream->writeSuffix();
}

}
