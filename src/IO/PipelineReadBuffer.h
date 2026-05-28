#pragma once

#include <IO/ReadBufferFromFileBase.h>
#include <IO/Rope.h>

#include <Common/Logger.h>
#include <memory>

namespace DB
{

class ReaderExecutor;

class PipelineReadBuffer : public ReadBufferFromFileBase
{
public:
    explicit PipelineReadBuffer(std::unique_ptr<ReaderExecutor> executor);

    String getFileName() const override;
    off_t seek(off_t off, int whence) override;
    off_t getPosition() override;
    std::optional<size_t> tryGetFileSize() override;

    /// Parquet's prefetcher takes a fast `RandomRead` path when both are true,
    /// fan-out via `readBigAt` with no shared mutex. Without these overrides
    /// it falls back to serial seek+read under a single lock, which on big
    /// remote parquets with a small cache (`03988_cached_read_big_at`) times
    /// out.
    bool supportsReadAt() override;
    size_t readBigAt(char * to, size_t n, size_t offset,
                     const std::function<bool(size_t)> & progress_callback) const override;

private:
    bool nextImpl() override;

    std::unique_ptr<ReaderExecutor> executor;
    /// The rope-with-cursor we're currently streaming from. Empty between
    /// windows. `nextImpl` advances it by `working_buffer.size()`,
    /// `seek` either rewinds it via `tryRewind` or replaces it on a
    /// long-distance jump.
    Rope rope;
    /// Logical offset just past the last byte exposed via `working_buffer`.
    /// `getPosition()` subtracts `available()` to get the caller's
    /// current read position.
    size_t read_position = 0;
    LoggerPtr log = getLogger("PipelineReadBuffer");
};

}
