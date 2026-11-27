#pragma once

#include <IO/ReadBufferFromFileBase.h>
#include <IO/ChainedBuffers.h>
#include <Common/Logger.h>

#include <memory>
#include <optional>

namespace DB
{

class ReaderExecutor;

/// Thin `ReadBufferFromFileBase` over a `ReaderExecutor` (experimental
/// `use_reader_executor` path). `nextImpl` streams the executor's `ChainedBuffers` windows,
/// pointing `working_buffer` at the current span; `seek` delegates to the executor.
/// Legacy callers see a normal seekable file buffer.
class PipelineReadBuffer : public ReadBufferFromFileBase
{
public:
    explicit PipelineReadBuffer(std::unique_ptr<ReaderExecutor> executor);
    ~PipelineReadBuffer() override;

    String getFileName() const override;
    off_t seek(off_t off, int whence) override;
    off_t getPosition() override;
    std::optional<size_t> tryGetFileSize() override;

    /// A hard read bound (e.g. `StorageLog`/`StorageStripeLog` cap reads to the
    /// size snapshotted under the read lock so concurrent appends aren't read).
    void setReadUntilPosition(size_t position) override;
    void setReadUntilEnd() override;

    /// Random-read / size probes must be denied for unknown-size sources: a
    /// `true` answer leads formats (Parquet/ORC/Arrow) to call
    /// `getFileSizeFromReadBuffer`, which throws `UNKNOWN_FILE_SIZE`. Such
    /// sources are read by streaming through `nextImpl` instead.
    bool checkIfActuallySeekable() override;

private:
    bool nextImpl() override;

    /// Empty the base `ReadBuffer` fields so they don't dangle when `chain` storage
    /// they point into is released.
    void detachBuffer();

    std::unique_ptr<ReaderExecutor> executor;
    /// The chain-with-cursor currently being streamed; empty between windows.
    /// `nextImpl` advances it by `working_buffer.size()`, then refills from the
    /// executor when exhausted. Reset on seek / read-bound re-anchor.
    ChainedBuffers chain;
    /// Logical offset just past the last byte exposed via `working_buffer`;
    /// `getPosition` subtracts `available` from it.
    size_t read_position = 0;
    LoggerPtr log = getLogger("PipelineReadBuffer");
};

}
