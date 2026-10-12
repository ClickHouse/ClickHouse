#pragma once

#include <Interpreters/TemporaryDataOnDisk.h>
#include <Processors/ISink.h>
#include <Processors/ISource.h>
#include <Common/Logger.h>

namespace DB
{

/// Writes incoming blocks to an owned temporary stream. Its extra output carries no data and finishes
/// after the file is finalized. The coordinating processor waits for this signal before taking the
/// completed file and creating a reader for it.
class BufferingToFileSink : public ISink
{
public:
    BufferingToFileSink(SharedHeader header, TemporaryBlockStreamHolder tmp_stream_, LoggerPtr log_);

    String getName() const override { return "BufferingToFileSink"; }

    Status prepare() override;
    void consume(Chunk chunk) override;
    void onFinish() override;

    OutputPort & getCompletionPort() { return outputs.front(); }

    /// Transfers ownership of a finalized file to the coordinator of the subsequent merge.
    TemporaryBlockStreamHolder releaseFile()
    {
        chassert(tmp_stream.getHolder()->isFinalized());
        return std::move(tmp_stream);
    }

private:
    TemporaryBlockStreamHolder tmp_stream;
    LoggerPtr log;
};

/// Owns a completed temporary stream and opens its reader when a merge starts consuming it.
/// Reader buffers are released at the end of the file or when a downstream limit closes the output.
/// The extra output carries no rows and finishes after that release, allowing a coordinator to wait
/// for reader cleanup before starting the next intermediate merge.
class BufferingFromFileSource : public ISource
{
public:
    explicit BufferingFromFileSource(TemporaryBlockStreamHolder tmp_stream_);

    String getName() const override { return "BufferingFromFileSource"; }

    /// These rows were already counted when they were read from the original source.
    std::optional<ReadProgress> getReadProgress() override { return std::nullopt; }

    OutputPort & getCompletionPort() { return outputs.back(); }

    Status prepare() override;
    void work() override;
    Chunk generate() override;
    void cancel(CancelReason reason) noexcept override;
    using ISource::cancel;

private:
    TemporaryBlockStreamHolder tmp_stream;
    std::optional<TemporaryBlockStreamReaderHolder> tmp_read_stream;
};

}
