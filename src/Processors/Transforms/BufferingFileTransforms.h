#pragma once

#include <Processors/ISink.h>
#include <Processors/ISource.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Common/Logger.h>

#include <optional>

namespace DB
{

/// Writes the chunks it consumes into a temporary file. Used by the transforms that expand their
/// pipeline at runtime to spill a sorted run: the sink is connected to the run's producer, and the
/// paired `BufferingFromFileSource` reads the run back for the final merge.
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

/// Reads back the run written by the paired `BufferingToFileSink`. Its input port carries no data; it
/// only signals that the sink has finished writing, so reading cannot start before the file is complete.
class BufferingFromFileSource : public ISource
{
public:
    BufferingFromFileSource(SharedHeader header, TemporaryBlockStreamHolder & tmp_stream_, LoggerPtr log_);

    String getName() const override { return "BufferingFromFileSource"; }

    Status prepare() override;

    /// These rows were already counted when they were read from the original source.
    std::optional<ReadProgress> getReadProgress() override { return std::nullopt; }

    Chunk generate() override;

private:
    TemporaryBlockStreamHolder & tmp_stream;
    std::optional<TemporaryBlockStreamReaderHolder> tmp_read_stream;
    LoggerPtr log;
};

}
