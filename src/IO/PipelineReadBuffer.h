#pragma once

#include <IO/ReadBufferFromFileBase.h>
#include <IO/Rope.h>

#include <Common/Logger.h>
#include <memory>

namespace DB
{

class ReaderExecutor;

/// Thin ReadBufferFromFileBase wrapping a ReaderExecutor.
/// Walks rope nodes from the executor, exposing them as the working buffer.
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
    Rope current_rope;
    RopeNode current_node;
    bool has_current_node = false;
    size_t read_position = 0;  /// logical offset past the last served node
    LoggerPtr log = getLogger("PipelineReadBuffer");
};

}
