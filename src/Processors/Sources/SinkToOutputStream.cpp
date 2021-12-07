#include <Processors/Sources/SinkToOutputStream.h>
#include <DataStreams/IBlockOutputStream.h>


namespace DB
{

SinkToOutputStream::SinkToOutputStream(BlockOutputStreamPtr stream_)
    : ISink(stream_->getHeader())
    , stream(std::move(stream_))
{
    stream->writePrefix();
}

void SinkToOutputStream::consume(Chunk chunk)
{
    stream->write(getPort().getHeader().cloneWithColumns(chunk.detachColumns()));
}

void SinkToOutputStream::onFinish()
{
    stream->writeSuffix();
}

}
