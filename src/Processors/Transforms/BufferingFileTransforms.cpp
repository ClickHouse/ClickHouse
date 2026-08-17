#include <Processors/Transforms/BufferingFileTransforms.h>

#include <Common/formatReadable.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

BufferingToFileSink::BufferingToFileSink(SharedHeader header, TemporaryBlockStreamHolder tmp_stream_, LoggerPtr log_)
    : ISink(std::move(header))
    , tmp_stream(std::move(tmp_stream_))
    , log(log_)
{
    outputs.emplace_back(Block(), this);
    LOG_INFO(log, "Writing part of data into temporary file {}", tmp_stream.getHolder()->describeFilePath());
}

IProcessor::Status BufferingToFileSink::prepare()
{
    auto status = ISink::prepare();
    if (status == Status::Finished)
        outputs.front().finish();
    return status;
}

void BufferingToFileSink::consume(Chunk chunk)
{
    Block block = getPort().getHeader().cloneWithColumns(chunk.detachColumns());
    tmp_stream->write(block);
}

void BufferingToFileSink::onFinish()
{
    auto stat = tmp_stream.finishWriting();
    LOG_INFO(log, "Done writing part of data into temporary file {}, compressed {}, uncompressed {}",
        tmp_stream.getHolder()->describeFilePath(),
        ReadableSize(static_cast<double>(stat.compressed_size)), ReadableSize(static_cast<double>(stat.uncompressed_size)));
}

BufferingFromFileSource::BufferingFromFileSource(SharedHeader header, TemporaryBlockStreamHolder & tmp_stream_, LoggerPtr log_)
    : ISource(std::move(header))
    , tmp_stream(tmp_stream_)
    , log(log_)
{
    inputs.emplace_back(Block(), this);
}

IProcessor::Status BufferingFromFileSource::prepare()
{
    if (!inputs.front().isFinished())
    {
        if (inputs.front().hasData())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The dummy input of BufferingFromFileSource must not carry data");

        inputs.front().setNeeded();
        return Status::NeedData;
    }

    return ISource::prepare();
}

Chunk BufferingFromFileSource::generate()
{
    if (!tmp_read_stream)
    {
        LOG_INFO(log, "Start reading part of data from temporary file");
        tmp_read_stream = tmp_stream.getReadStream();
    }

    Block block = tmp_read_stream.value()->read();
    if (block.empty())
        return {};

    UInt64 num_rows = block.rows();
    return Chunk(block.getColumns(), num_rows);
}

}
