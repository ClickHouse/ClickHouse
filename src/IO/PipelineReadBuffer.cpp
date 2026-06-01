#include <IO/PipelineReadBuffer.h>
#include <IO/ReaderExecutor.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/scope_guard_safe.h>
#include <cstring>

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

bool PipelineReadBuffer::nextImpl()
{
    /// Tell the rope that the bytes we exposed last time are now fully
    /// consumed (the caller would not have called us otherwise). This is
    /// where the rope releases nodes whose data we no longer need.
    /// `working_buffer.size()` is 0 right after construction or right
    /// after `seek` — so the first call and post-seek calls don't
    /// over-advance.
    rope.advance(working_buffer.size());

    if (rope.atEnd())
    {
        LOG_TRACE(log, "nextImpl: rope exhausted, requesting next window at position {}", read_position);
        rope = executor->readNextWindow();
        if (rope.atEnd())
        {
            LOG_TRACE(log, "nextImpl: EOF");
            return false;
        }
        LOG_TRACE(log, "nextImpl: got window [{}, {}), {} nodes",
            rope.range().offset, rope.range().end(), rope.getNodes().size());
    }

    auto span = rope.peek();
    if (executor->needsDecryption())
    {
        /// Decrypt only the span we are about to serve - read-ahead/prefetched
        /// bytes never peeked are never decrypted. Decrypt into a scratch buffer
        /// so the rope stays encrypted and a rewind that re-serves this span
        /// re-decrypts it cleanly (CTR is position-addressable).
        decrypt_buf.resize(span.size);
        std::memcpy(decrypt_buf.data(), span.data, span.size);
        executor->decryptInPlace(decrypt_buf.data(), span.size, span.logical_offset);
        internal_buffer = Buffer(decrypt_buf.data(), decrypt_buf.data() + span.size);
    }
    else
    {
        internal_buffer = Buffer(span.data, span.data + span.size);
    }
    working_buffer = internal_buffer;
    pos = working_buffer.begin();
    read_position = span.logical_offset + span.size;
    LOG_TRACE(log, "nextImpl: serving {} bytes at offset {}, read_position advanced to {}",
        span.size, span.logical_offset, read_position);
    return true;
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

    LOG_DEBUG(log, "seek to {}", new_pos);

    /// Reset `working_buffer` BEFORE asking the rope to rewind. This makes
    /// the next `nextImpl` advance by 0 (instead of by the size of the
    /// partially-consumed previous span), so the rewind position is
    /// preserved.
    resetWorkingBuffer();

    if (rope.tryRewind(new_pos))
    {
        LOG_TRACE(log, "seek: rewound inside rope");
        read_position = new_pos;
        return new_pos;
    }

    LOG_TRACE(log, "seek: delegating to executor");
    executor->seek(new_pos);
    rope = Rope{};
    read_position = new_pos;
    return new_pos;
}

off_t PipelineReadBuffer::getPosition()
{
    return read_position - available();
}

void PipelineReadBuffer::setReadUntilPosition(size_t position)
{
    /// `position` is in this buffer's coordinates - the executor's logical file
    /// offset (the post-decryption .bin offset that marks address). Advertise it
    /// as the read extent so the executor bounds its live connection there.
    executor->setReadExtent(position);
}

void PipelineReadBuffer::setReadUntilEnd()
{
    /// Read to the file end: clear the extent. A read that runs to EOF drains its
    /// connection naturally, so no explicit bound is needed.
    executor->setReadExtent(std::nullopt);
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

String PipelineReadBuffer::getFileName() const
{
    /// Surface the object path so format/decompression diagnostics
    /// (`getFileNameFromReadBuffer`) name the failing object instead of this
    /// wrapper. Falls back to the wrapper name only when no path is known.
    String name = executor->getFileName();
    return name.empty() ? "PipelineReadBuffer" : name;
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

bool PipelineReadBuffer::checkIfActuallySeekable()
{
    /// Same reason as `supportsReadAt`: a seekable probe also leads formats to
    /// `getFileSizeFromReadBuffer`. Unknown-size sources are not seekable here.
    return !executor->hasUnknownSize();
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
    /// `readNextWindow` path. The transient owns its own position / live_buffer
    /// / prefetch state so concurrent `readBigAt` calls don't interfere with
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
        Rope window = sub->readNextWindow();
        if (window.empty())
            break;
        for (const auto & node : window.getNodes())
        {
            if (total_copied >= want)
                break;
            const size_t copy = std::min(node.size, want - total_copied);
            std::memcpy(to + total_copied, node.data(), copy);
            /// The transient returns encrypted bytes (decryption is deferred to
            /// the consumer); decrypt the copied prefix at its logical offset.
            if (sub->needsDecryption())
                sub->decryptInPlace(to + total_copied, copy, node.logical_offset);
            total_copied += copy;
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

}
