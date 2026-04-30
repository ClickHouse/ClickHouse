#pragma once

#include <IO/ReadBufferFromFileBase.h>
#include <IO/Rope.h>

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

private:
    bool nextImpl() override;

    std::unique_ptr<ReaderExecutor> executor;
    Rope current_rope;
    RopeNode current_node;
    bool has_current_node = false;
    size_t read_position = 0;  /// logical offset past the last served node
};

}
