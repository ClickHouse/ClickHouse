#include <IO/PipelineReadBuffer.h>
#include <IO/ReaderExecutor.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}

PipelineReadBuffer::PipelineReadBuffer(std::unique_ptr<ReaderExecutor> executor_)
    : ReadBufferFromFileBase(0, nullptr, 0)
    , executor(std::move(executor_))
    , read_position(executor->getPosition())
{
    LOG_DEBUG(log, "Created, total_size={}, read_position={}", executor->totalSize(), read_position);
}

PipelineReadBuffer::~PipelineReadBuffer() = default;

bool PipelineReadBuffer::nextImpl()
{
    std::optional<Stopwatch> watch;
    if (profile_callback)
        watch.emplace(clock_type);

    /// Detach first: `advance` can free the buffer `working_buffer` / `pos` point into.
    const size_t consumed = working_buffer.size();
    detachBuffer();
    chain.advance(consumed);
    if (chain.atEnd())
    {
        chain = executor->readNextWindow();
        if (chain.atEnd())
        {
            LOG_TEST(log, "nextImpl: EOF at {}", read_position);
            return false;
        }
    }

    auto span = chain.peek();

    /// Report the read so `MergeTreeReadPool`'s slow-read backoff still sees it.
    if (profile_callback)
    {
        ProfileInfo info{};
        info.bytes_requested = span.size;
        info.bytes_read = span.size;
        info.nanoseconds = watch->elapsed();
        profile_callback(info);
    }

    internal_buffer = Buffer(span.data, span.data + span.size);
    working_buffer = internal_buffer;
    pos = working_buffer.begin();
    read_position = span.logical_offset + span.size;
    return true;
}

void PipelineReadBuffer::detachBuffer()
{
    internal_buffer = working_buffer = Buffer(nullptr, nullptr);
    pos = nullptr;
}

off_t PipelineReadBuffer::seek(off_t off, int whence)
{
    size_t new_pos = 0;
    if (whence == SEEK_SET)
    {
        if (off < 0)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                "PipelineReadBuffer::seek: SEEK_SET with negative offset {}", off);
        new_pos = static_cast<size_t>(off);
    }
    else if (whence == SEEK_CUR)
    {
        off_t cur = getPosition();
        if (off < 0 && static_cast<size_t>(-off) > static_cast<size_t>(cur))
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                "PipelineReadBuffer::seek: SEEK_CUR offset {} from position {} would underflow",
                off, cur);
        new_pos = static_cast<size_t>(cur + off);
    }
    else
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "PipelineReadBuffer::seek: unsupported whence");

    /// If the target lands inside the bytes already in `working_buffer`, just reposition `pos`:
    /// no executor seek, no dropped window. The compressed reader over-reads a full block and then
    /// seeks back to a mark inside it; propagating that as a backward seek to the executor would
    /// refetch and -- since a held source connection is forward-only -- break long-connection reuse.
    if (!working_buffer.empty()
        && read_position - working_buffer.size() <= new_pos
        && new_pos <= read_position)
    {
        pos = working_buffer.end() - (read_position - new_pos);
        return static_cast<off_t>(new_pos);
    }

    LOG_DEBUG(log, "seek to {}", new_pos);

    detachBuffer();
    chain = ChainedBuffers{};
    executor->seek(new_pos);
    read_position = new_pos;
    return static_cast<off_t>(new_pos);
}

off_t PipelineReadBuffer::getPosition()
{
    return read_position - available();
}

void PipelineReadBuffer::setReadUntilPosition(size_t position)
{
    executor->setReadUntil(position);

    /// If the new bound is below what's already buffered, drop the buffer and
    /// re-anchor the executor at the exposed position. Otherwise the executor
    /// stays at the end of the already-read chunk, and a later bound extension
    /// would resume there, skipping the bytes in between.
    if (position < read_position)
    {
        const size_t current = read_position - available();
        detachBuffer();
        chain = ChainedBuffers{};
        executor->seek(current);
        read_position = current;
    }
}

void PipelineReadBuffer::setReadUntilEnd()
{
    executor->setReadUntil(std::nullopt);
}

std::optional<size_t> PipelineReadBuffer::tryGetFileSize()
{
    /// Unknown-size sources (S3 HEAD without Content-Length) must surface as
    /// `nullopt`, not a meaningless `~uint64_t::max()` byte count.
    if (executor->hasUnknownSize())
        return std::nullopt;
    return executor->totalSize();
}

bool PipelineReadBuffer::checkIfActuallySeekable()
{
    /// Unknown-size sources are streamed through `nextImpl`, not seeked: a `true`
    /// answer leads formats to `getFileSizeFromReadBuffer`, which throws.
    return !executor->hasUnknownSize();
}

String PipelineReadBuffer::getFileName() const
{
    /// Surface the object path so format/decompression diagnostics name the
    /// failing object instead of this wrapper.
    String name = executor->getFileName();
    return name.empty() ? "PipelineReadBuffer" : name;
}

}
