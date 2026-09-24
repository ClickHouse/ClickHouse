#include <Processors/Transforms/BufferingFileTransforms.h>

#include <Common/formatReadable.h>
#include <Common/logger_useful.h>

namespace DB
{

BufferingToFileSink::BufferingToFileSink(SharedHeader header, TemporaryBlockStreamHolder tmp_stream_, LoggerPtr log_)
    : ISink(std::move(header))
    , tmp_stream(std::move(tmp_stream_))
    , log(log_)
{
    outputs.emplace_back(Block(), this);
    LOG_TRACE(log, "Writing part of data into temporary file {}", tmp_stream.getHolder()->describeFilePath());
}

IProcessor::Status BufferingToFileSink::prepare()
{
    if (getCompletionPort().isFinished())
    {
        getPort().close();
        return Status::Finished;
    }
    auto status = ISink::prepare();
    if (status == Status::Finished)
        getCompletionPort().finish();
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
    LOG_TRACE(log, "Done writing part of data into temporary file {}, compressed {}, uncompressed {}",
        tmp_stream.getHolder()->describeFilePath(),
        ReadableSize(static_cast<double>(stat.compressed_size)), ReadableSize(static_cast<double>(stat.uncompressed_size)));
}

BufferingFromFileSource::BufferingFromFileSource(TemporaryBlockStreamHolder tmp_stream_)
    : ISource(std::make_shared<const Block>(tmp_stream_.getHeader()))
    , tmp_stream(std::move(tmp_stream_))
{
    outputs.emplace_back(Block(), this);
}

IProcessor::Status BufferingFromFileSource::prepare()
{
    if (getCompletionPort().isFinished())
        getPort().finish();
    auto status = ISource::prepare();

    /// A downstream row limit can close the output before the reader reaches the end of the file.
    /// Release the reader and any prefetched chunk in `work`, outside the executor's preparation lock,
    /// before reporting completion so the next merge does not overlap these allocations.
    if (status == Status::Finished)
    {
        finished = true;
        if (tmp_read_stream || has_input)
            return Status::Ready;
        getCompletionPort().finish();
    }
    return status;
}

void BufferingFromFileSource::work()
{
    if (finished)
    {
        tmp_read_stream.reset();
        current_chunk = {};
        has_input = false;
    }
    else
        ISource::work();
}

void BufferingFromFileSource::cancel(CancelReason reason) noexcept
{

    /// A partial result must finish processing data already read into temporary files.
    if (reason == CancelReason::PartialResult)
        return;

    ISource::cancel(reason);
}

Chunk BufferingFromFileSource::generate()
{
    if (!tmp_read_stream)
        tmp_read_stream.emplace(tmp_stream.getReadStream());

    Block block = (*tmp_read_stream)->read();
    if (block.empty())
    {
        tmp_read_stream.reset();
        return {};
    }

    const auto num_rows = block.rows();
    return Chunk(block.detachColumns(), num_rows);
}

}
