#pragma once

#include <Interpreters/TemporaryDataOnDisk.h>
#include <Processors/ISink.h>
#include <Processors/ISource.h>
#include <Common/logger_useful.h>

namespace DB
{

/// Writes incoming blocks to an owned temporary stream. Its extra output carries no data and finishes
/// after the file is finalized. The pipeline connects this completion signal to the reader or to a
/// processor that coordinates when reading may start.
class BufferingToFileSink : public ISink
{
public:
    BufferingToFileSink(SharedHeader header, TemporaryBlockStreamHolder tmp_stream_, LoggerPtr log_);

    String getName() const override { return "BufferingToFileSink"; }

    Status prepare() override;
    void consume(Chunk chunk) override;
    void onFinish() override;

    OutputPort & getCompletionPort() { return outputs.front(); }
    TemporaryBlockStreamHolder & getHolder() { return tmp_stream; }

private:
    TemporaryBlockStreamHolder tmp_stream;
    LoggerPtr log;
};

/// Reads a completed temporary stream after its extra input finishes. This input carries no data;
/// the pipeline connects it to the sink's completion signal or to a coordinating processor.
/// The source borrows the holder owned by `BufferingToFileSink`. The pipeline retains both processors
/// throughout execution, keeping the holder alive while this source reads.
class BufferingFromFileSource : public ISource
{
public:
    BufferingFromFileSource(SharedHeader header, TemporaryBlockStreamHolder & tmp_stream_, LoggerPtr log_);

    String getName() const override { return "BufferingFromFileSource"; }

    /// These rows were already counted when they were read from the original source.
    std::optional<ReadProgress> getReadProgress() override { return std::nullopt; }

    Status prepare() override;
    Chunk generate() override;
    void cancel(CancelReason reason) noexcept override;
    using ISource::cancel;

    InputPort & getCompletionPort() { return inputs.front(); }

private:
    TemporaryBlockStreamHolder & tmp_stream;
    std::optional<TemporaryBlockStreamReaderHolder> tmp_read_stream;
    LoggerPtr log;
};

}
