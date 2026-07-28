#pragma once

#include <IO/ReadBufferFromFileBase.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ChainedBuffers.h>

#include <Common/Logger.h>
#include <memory>

namespace DB
{

class ReaderExecutor;

class PipelineReadBuffer : public ReadBufferFromFileBase
{
public:
    explicit PipelineReadBuffer(std::unique_ptr<ReaderExecutor> executor, size_t hold_consumed_ = 0);

    String getFileName() const override;
    off_t seek(off_t off, int whence) override;
    off_t getPosition() override;
    std::optional<size_t> tryGetFileSize() override;

    /// Advertise the read boundary to the executor. `MergeTreeReaderStream`
    /// drives this per mark range (`adjustRightMark`); the executor serves and
    /// EOFs at it, while its producer may fetch past it by the consumed run's
    /// earned reach (see `ReaderExecutor::setReadExtent`).
    void setReadUntilPosition(size_t position) override;
    void setReadUntilEnd() override;

    /// Parquet's prefetcher takes a fast `RandomRead` path when both are true,
    /// fan-out via `readBigAt` with no shared mutex. Without these overrides
    /// it falls back to serial seek+read under a single lock, which on big
    /// remote parquets with a small cache (`03988_cached_read_big_at`) times
    /// out.
    bool supportsReadAt() override;
    size_t readBigAt(char * to, size_t n, size_t offset,
                     const std::function<bool(size_t)> & progress_callback) const override;

    /// Random-read / size probes must be denied for unknown-size sources: a
    /// `true` answer leads formats (Parquet/ORC/Arrow) to call
    /// `getFileSizeFromReadBuffer`, which throws `UNKNOWN_FILE_SIZE`. Such
    /// sources are read by streaming through `nextImpl` instead.
    bool checkIfActuallySeekable() override;

private:
    bool nextImpl() override;

    /// Null out the base-class buffer fields (`internal_buffer` / `working_buffer` /
    /// `pos`). Must run BEFORE any chain operation that can free the node they point
    /// into (`advance`, rewind, replacement), so no dangling pointers survive in the
    /// base state even across an EOF or exception exit.
    void detachBuffer();

    /// Rewind into the trailing retention store: rebuild `chain` as the held
    /// tail from `new_pos` plus the live chain. The held stream is contiguous
    /// by construction (windows arrive sequentially; `held` is cleared on
    /// executor-delegated seeks), so the only checks are coverage and that the
    /// held tail still reaches the live front.
    bool rewindIntoHeld(size_t new_pos);

    std::unique_ptr<ReaderExecutor> executor;
    /// Trailing retention (`Options::hold_consumed`): consumed spans are parked
    /// in `held` (up to this many bytes) so a backward seek within them
    /// re-serves from memory instead of refetching.
    const size_t hold_consumed;
    /// The chain-with-cursor we're currently streaming from. Empty between
    /// windows. `nextImpl` advances it by `working_buffer.size()`,
    /// `seek` either rewinds it via `tryRewind` or replaces it on a
    /// long-distance jump.
    ChainedBuffers chain;
    /// The retention store: spans the cursor consumed, newest at the back,
    /// trimmed to the last `hold_consumed` bytes. Shares the chain's blocks
    /// (node references, no copies). Always empty when `hold_consumed == 0`.
    ChainedBuffers held;
    /// Logical offset just past the last byte exposed via `working_buffer`.
    /// `getPosition()` subtracts `available()` to get the caller's
    /// current read position.
    size_t read_position = 0;
    LoggerPtr log = getLogger("PipelineReadBuffer");
};

}
