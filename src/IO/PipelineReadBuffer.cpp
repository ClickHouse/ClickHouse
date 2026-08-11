#include <IO/PipelineReadBuffer.h>
#include <IO/ReaderExecutor.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <Common/scope_guard_safe.h>
#include <cstring>

namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int LOGICAL_ERROR;
}

PipelineReadBuffer::PipelineReadBuffer(std::unique_ptr<ReaderExecutor> executor_, size_t hold_consumed_)
    : ReadBufferFromFileBase(0, nullptr, 0)
    , executor(std::move(executor_))
    , hold_consumed(hold_consumed_)
    , read_position(executor->getPosition())
{
    LOG_TRACE(log, "Created, total_size={}, read_position={}", executor->totalSize(), read_position);
}

String PipelineReadBuffer::getFileName() const
{
    /// Surface the object path so format/decompression diagnostics
    /// (`getFileNameFromReadBuffer`) name the failing object instead of this
    /// wrapper. Falls back to the wrapper name only when no path is known.
    String name = executor->getFileName();
    return name.empty() ? "PipelineReadBuffer" : name;
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

    /// Detach BEFORE asking the chain to rewind or releasing it. This makes
    /// the next `nextImpl` advance by 0 (instead of by the size of the
    /// partially-consumed previous span), so the rewind position is
    /// preserved - and leaves no base-class pointer into storage the chain
    /// may free.
    detachBuffer();

    if (chain.tryRewind(new_pos))
    {
        LOG_TRACE(log, "seek: rewound inside chain");
        read_position = new_pos;
        return new_pos;
    }

    if (rewindIntoHeld(new_pos))
    {
        LOG_TRACE(log, "seek: rewound into held consumed bytes");
        read_position = new_pos;
        return new_pos;
    }

    LOG_TRACE(log, "seek: delegating to executor");
    executor->seek(new_pos);
    chain = ChainedBuffers{};
    held = ChainedBuffers{};
    read_position = new_pos;
    return static_cast<off_t>(new_pos);
}

void PipelineReadBuffer::detachBuffer()
{
    internal_buffer = working_buffer = Buffer(nullptr, nullptr);
    pos = nullptr;
}

off_t PipelineReadBuffer::getPosition()
{
    return read_position - available();
}

std::optional<size_t> PipelineReadBuffer::tryGetFileSize()
{
    /// Unknown-size sources (S3 HEAD without Content-Length) must surface as
    /// `nullopt`, not as `executor->totalSize()` (which returns
    /// `UnknownSize - data_start_offset ≈ uint64_t::max`). The downstream
    /// `FormatFactory::wrapReadBufferIfNeeded` compares this to
    /// `max_download_buffer_size` to decide whether to wrap with
    /// `ParallelReadBuffer`; a max-valued size enables parallel reads that
    /// can't be satisfied and trip `UNEXPECTED_END_OF_FILE`.
    if (executor->hasUnknownSize())
        return std::nullopt;
    return executor->totalSize();
}

void PipelineReadBuffer::setReadUntilPosition(size_t position)
{
    /// `position` is in this buffer's coordinates - the executor's logical file
    /// offset (the post-decryption .bin offset that marks address). The BOUNDARY
    /// is owned here (exposure clamp + EOF); the executor still receives it as
    /// the read extent to bound its long connection.
    chassert(!read_until || position >= *read_until);
    read_until = position;
    executor->setReadBound(position);
}

void PipelineReadBuffer::setReadUntilEnd()
{
    /// Read to the file end: clear the boundary. A read that runs to EOF drains
    /// its connection naturally, so no explicit bound is needed.
    read_until.reset();
    executor->setReadBound(std::nullopt);
}

void PipelineReadBuffer::setPlannedReadEnd(size_t position)
{
    /// Advisory: joins the executor's read bound but never moves this buffer's
    /// own `read_until` (per-range EOF keeps advancing beneath it).
    executor->setReadBound(position);
}

void PipelineReadBuffer::setRequestMap(std::vector<std::pair<size_t, size_t>> ranges)  // STYLE_CHECK_ALLOW_STD_CONTAINERS
{
    executor->setRequestMap(std::move(ranges));
}

void PipelineReadBuffer::prefetch(Priority)
{
    executor->prefetch();
}

bool PipelineReadBuffer::supportsReadAt()
{
    /// A `true` answer tells random-read formats (Parquet/ORC/Arrow) the source
    /// is randomly addressable; their first move is to locate the footer at the
    /// end via `getFileSizeFromReadBuffer`, which throws `UNKNOWN_FILE_SIZE` when
    /// the size is unknown. Don't advertise random reads for unknown-size sources
    /// - they stream through `nextImpl` instead.
    return !executor->hasUnknownSize() && executor->canReadAt();
}

size_t PipelineReadBuffer::readBigAt(
    char * to, size_t n, size_t offset,
    const std::function<bool(size_t)> & progress_callback) const
{
    if (n == 0)
        return 0;

    const size_t total = executor->totalSize();
    if (offset >= total)
        return 0;
    const size_t want = std::min(n, total - offset);

    /// Drive a fresh, isolated `ReaderExecutor` through the regular
    /// `readNextWindow` path. The transient owns its own position, plan/display
    /// and fill lane, so concurrent `readBigAt` calls don't interfere with
    /// each other or with the main reader. Reusing the existing pipeline avoids
    /// duplicating the cache-walk + source-read logic.
    auto sub = executor->makeTransientForReadAt(offset, want);
    /// Roll the transient's I/O stats into the parent on every exit path so the
    /// random-access read shows up in the parent's reader_executor_log row /
    /// ProfileEvents (the transient does not emit its own). Runs before `sub` is
    /// destroyed (reverse declaration order).
    SCOPE_EXIT_SAFE(executor->mergeTransientStats(*sub));

    size_t total_copied = 0;
    while (total_copied < want)
    {
        ChainedBuffers window = sub->readNextWindow();
        if (window.empty())
            break;
        /// Consume through the chain cursor, not by concatenating raw nodes: the chain
        /// can hold overlapping nodes (each byte reachable once - `advance` drops a node
        /// the cursor has passed), so only the cursor walk maps bytes to positions.
        while (total_copied < want)
        {
            const auto span = window.peek();
            if (span.size == 0)
                break;
            if (span.offset != offset + total_copied)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "PipelineReadBuffer::readBigAt: window not contiguous at {} (expected {})",
                    span.offset, offset + total_copied);
            const size_t copy = std::min(span.size, want - total_copied);
            std::memcpy(to + total_copied, span.data, copy);
            total_copied += copy;
            window.advance(copy);
        }

        /// `progress_callback(m)` publishes bytes-so-far and returns
        /// true to ask us to stop — typically from `ParallelReadBuffer`
        /// when another worker fulfilled the request or an emergency
        /// stop fired. Call once per window (8 MiB at the default
        /// `DEFAULT_WINDOW_SIZE`) so cancellation interrupts before
        /// committing to the next source/cache walk without paying for
        /// a callback per copied node.
        if (progress_callback && progress_callback(total_copied))
            return total_copied;
    }
    return total_copied;
}

bool PipelineReadBuffer::checkIfActuallySeekable()
{
    /// Same reason as `supportsReadAt`: a seekable probe also leads formats to
    /// `getFileSizeFromReadBuffer`. Unknown-size sources are not seekable here.
    return !executor->hasUnknownSize();
}

bool PipelineReadBuffer::rewindIntoHeld(size_t new_pos)
{
    if (held.empty())
        return false;
    const ByteRange held_range = held.range();
    if (new_pos < held_range.offset || new_pos >= held_range.end())
        return false;
    /// The held tail must still reach the live chain (or the cursor when it is
    /// drained): a hole in between means those bytes were re-fetched territory.
    const size_t resume = chain.empty() ? read_position : chain.range().offset;
    if (held_range.end() < resume)
        return false;

    /// Rebuild the live chain as [new_pos, held end) + the old live chain, and
    /// drop the re-opened tail from the store (those bytes are live again and
    /// will be re-parked as they are re-consumed).
    ChainedBuffers rebuilt = held.slice(ByteRange{new_pos, held_range.end() - new_pos});
    rebuilt.append(std::move(chain));
    chain = std::move(rebuilt);
    held = new_pos > held_range.offset
        ? held.slice(ByteRange{held_range.offset, new_pos - held_range.offset})
        : ChainedBuffers{};
    return true;
}

bool PipelineReadBuffer::nextImpl()
{
    std::optional<Stopwatch> watch;
    if (profile_callback)
        watch.emplace(clock_type);

    /// Tell the chain that the bytes we exposed last time are now fully
    /// consumed (the caller would not have called us otherwise). This is
    /// where the chain releases nodes whose data we no longer need.
    /// `working_buffer.size()` is 0 right after construction or right
    /// after `seek` — so the first call and post-seek calls don't
    /// over-advance. Detach first: `advance` can free the buffer
    /// `working_buffer` / `pos` point into.
    const size_t consumed = working_buffer.size();
    detachBuffer();
    /// Park the span the chain is about to release: `slice` copies node
    /// references (no data), so backward seeks within `hold_consumed` re-serve
    /// from memory. Trim the store from the front to the hold window (`advance`
    /// keeps a partial front node whole, so memory is bounded by
    /// `hold_consumed` plus one node).
    if (hold_consumed && consumed)
    {
        held.append(chain.slice(ByteRange{read_position - consumed, consumed}));
        const ByteRange held_range = held.range();
        if (held_range.size > hold_consumed)
            held.advance(held_range.size - hold_consumed);
    }
    chain.advance(consumed);

    /// The boundary EOF is OURS: never ask the executor at/past `read_until` -
    /// a later advance resumes, first from whatever the chain retained.
    if (read_until && read_position >= *read_until)
    {
        LOG_TRACE(log, "nextImpl: at read_until {}, reporting EOF", *read_until);
        return false;
    }

    if (chain.atEnd())
    {
        LOG_TEST(log, "nextImpl: chain exhausted, requesting next window at position {}", read_position);
        chain = executor->readNextWindow();
        if (chain.atEnd())
        {
            LOG_TRACE(log, "nextImpl: EOF");
            return false;
        }
        LOG_TEST(log, "nextImpl: got window [{}, {}), {} nodes",
            chain.range().offset, chain.range().end(), chain.getNodes().size());
    }

    /// The executor serves plaintext (it decrypts each window in `readNextWindow`),
    /// so the span is exposed directly - no copy, no decrypt on the read path.
    auto span = chain.peek();
    /// Expose only up to the boundary; the surplus stays in the chain
    /// (unconsumed) and serves the resume after the next advance.
    if (read_until && span.offset + span.size > *read_until)
    {
        chassert(*read_until > span.offset);
        span.size = *read_until - span.offset;
    }

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
    read_position = span.offset + span.size;
    LOG_TEST(log, "nextImpl: serving {} bytes at offset {}, read_position advanced to {}",
        span.size, span.offset, read_position);
    return true;
}

}
