#include <IO/LongConnection.h>
#include <IO/FetchMachine.h>
#include <Common/Exception.h>

namespace DB
{

size_t readIntoBlock(ReadBuffer & buf, char * dest, size_t chunk)
{
    if (buf.supportsExternalBufferMode())
    {
        size_t total = 0;
        while (total < chunk)
        {
            /// Re-arm at `dest + total`: the source's internal position has
            /// advanced by `total` already, so successive `next` calls land
            /// contiguously in `dest`.
            buf.set(dest + total, chunk - total);
            if (!buf.next())
                break;
            size_t got = buf.available();
            if (got == 0)
                break;  /// Defensive: source returned `true` with no data.
            buf.position() = buf.buffer().end();
            total += got;
        }
        return total;
    }

    return buf.read(dest, chunk);
}

ChainedBuffers LongConnection::readInto(
    VectorWithMemoryTracking<std::shared_ptr<OwnedChainedBuffer>> blocks, size_t file_pos,
    const MachineBase * stop)
{
    ChainedBuffers chain;
    size_t total_read = 0;
    for (auto & block : blocks)
    {
        /// Stop BETWEEN blocks: a long connection stops freely - it stays put with
        /// its frontier and continues later, nothing forfeited.
        if (stop && stop->interrupt_requested.load(std::memory_order_relaxed))
            break;
        const size_t got = readIntoBlock(*buffer, block->data(), block->size());
        if (got == 0)
        {
            /// `readIntoBlock` returns 0 only at EOF; short of the bound that means an
            /// unknown-size stream ended - the GET is complete, latch it as exhausted.
            if (current_position + total_read < read_until)
                saw_eof = true;
            break;
        }
        chain.append(ChainedBufferNode{block, 0, got, file_pos + total_read});
        total_read += got;
    }
    current_position += total_read;
    return chain;
}

size_t LongConnection::skipForward(size_t gap, size_t block_bytes)
{
    /// The source is in external-buffer mode, so discard through a scratch block
    /// (mirrors `readIntoBlock`): the bytes cross the wire (over-read) but the source
    /// request is saved. Short only at EOF.
    if (gap == 0)
        return 0;
    const size_t scratch_size = std::min(gap, block_bytes);
    auto scratch = std::make_shared<OwnedChainedBuffer>(scratch_size);
    size_t skipped = 0;
    while (skipped < gap)
    {
        const size_t got = readIntoBlock(*buffer, scratch->data(), std::min(gap - skipped, scratch_size));
        if (got == 0)
        {
            saw_eof = true;
            break;
        }
        skipped += got;
    }
    current_position += skipped;
    return skipped;
}

LongConnection::DrainResult
LongConnection::drainTail(size_t max_tail, size_t block_bytes, LoggerPtr logger) noexcept
{
    /// An exhausted stream (at the bound, or EOF on an unknown-size source) has
    /// nothing left to drain.
    if (exhausted())
        return {};
    const size_t tail = read_until - current_position;
    if (tail > max_tail)
        return {};
    /// The drained tail is discarded - it only lets the underlying HTTP connection return to
    /// the keep-alive pool - so a read error here must not abort an otherwise valid query.
    /// Swallow it and report the failure; the caller then releases the connection as incomplete.
    try
    {
        return {.bytes = skipForward(tail, block_bytes), .failed = false};
    }
    catch (...)
    {
        tryLogCurrentException(logger, "Failed to drain a held source connection; releasing it as incomplete");
        return {.bytes = 0, .failed = true};
    }
}

std::optional<LongConnection> takeLongConnection(std::optional<LongConnection> & src)
{
    /// A plain `std::optional` move leaves the source engaged with a moved-from value,
    /// so reset it: the connection must be a single owner.
    std::optional<LongConnection> taken = std::move(src);
    src.reset();
    return taken;
}

}
