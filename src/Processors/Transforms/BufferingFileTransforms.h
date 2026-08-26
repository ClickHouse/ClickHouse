#pragma once

#include <Interpreters/TemporaryDataOnDisk.h>
#include <Processors/ISink.h>
#include <Processors/ISource.h>
#include <Common/logger_useful.h>

namespace DB
{

/// A sink that writes the incoming stream of blocks into a temporary file.
/// It has an extra dummy output port that is connected to the processor that spawned it,
/// so that the pipeline stays connected (see MergeSortingTransform::updatePipeline).
class BufferingToFileSink : public ISink
{
public:
    BufferingToFileSink(SharedHeader header, TemporaryBlockStreamHolder tmp_stream_, LoggerPtr log_);

    String getName() const override { return "BufferingToFileSink"; }

    Status prepare() override;
    void consume(Chunk chunk) override;
    void onFinish() override;

    TemporaryBlockStreamHolder & getHolder() { return tmp_stream; }

private:
    TemporaryBlockStreamHolder tmp_stream;
    LoggerPtr log;
};

/// A source that reads back the blocks written by the corresponding BufferingToFileSink.
/// It has an extra dummy input port connected to the sink's dummy output port, so it starts
/// producing data only after the sink finishes writing.
class BufferingFromFileSource : public ISource
{
public:
    BufferingFromFileSource(SharedHeader header, TemporaryBlockStreamHolder & tmp_stream_, LoggerPtr log_);

    String getName() const override { return "BufferingFromFileSource"; }

    /// These rows were already counted when they were read from the original source.
    std::optional<ReadProgress> getReadProgress() override { return std::nullopt; }

    Status prepare() override;
    Chunk generate() override;

private:
    TemporaryBlockStreamHolder & tmp_stream;
    std::optional<TemporaryBlockStreamReaderHolder> tmp_read_stream;
    LoggerPtr log;
};

}
