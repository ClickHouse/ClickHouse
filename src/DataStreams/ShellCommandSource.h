#pragma once

#include <memory>

#include <common/logger_useful.h>
#include <Common/ShellCommand.h>
#include <Common/ThreadPool.h>
#include <IO/ReadHelpers.h>
#include <Formats/FormatFactory.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Executors/PullingPipelineExecutor.h>


namespace DB
{

/// Owns ShellCommand and calls wait for it.
class ShellCommandOwningTransform final : public ISimpleTransform
{
public:
    ShellCommandOwningTransform(
        const Block & header,
        Poco::Logger * log_,
        std::unique_ptr<ShellCommand> command_)
        : ISimpleTransform(header, header, true)
        , command(std::move(command_))
        , log(log_)
    {
    }

    String getName() const override { return "ShellCommandOwningTransform"; }
    void transform(Chunk &) override {}

    Status prepare() override
    {
        auto status = ISimpleTransform::prepare();
        if (status == Status::Finished)
        {
            std::string err;
            readStringUntilEOF(err, command->err);
            if (!err.empty())
                LOG_ERROR(log, "Having stderr: {}", err);

            command->wait();
        }

        return status;
    }

private:
    std::unique_ptr<ShellCommand> command;
    Poco::Logger * log;
};

/** A stream, that runs child process and sends data to its stdin in background thread,
  * and receives data from its stdout.
  */
class ShellCommandSourceWithBackgroundThread final : public SourceWithProgress
{
public:
    ShellCommandSourceWithBackgroundThread(
        ContextPtr context,
        const std::string & format,
        const Block & sample_block,
        std::unique_ptr<ShellCommand> command_,
        Poco::Logger * log_,
        std::function<void(ShellCommand &)> && send_data_)
        : SourceWithProgress(sample_block)
        , command(std::move(command_))
        , send_data(std::move(send_data_))
        , thread([this] { send_data(*command); })
        , log(log_)
    {
        pipeline.init(Pipe(FormatFactory::instance().getInput(format, command->out, sample_block, context, DEFAULT_BLOCK_SIZE)));
        executor = std::make_unique<PullingPipelineExecutor>(pipeline);
    }

    ~ShellCommandSourceWithBackgroundThread() override
    {
        if (thread.joinable())
            thread.join();
    }

protected:
    Chunk generate() override
    {
        Chunk chunk;
        executor->pull(chunk);
        return chunk;
    }

public:
    Status prepare() override
    {
        auto status = SourceWithProgress::prepare();

        if (status == Status::Finished)
        {
            std::string err;
            readStringUntilEOF(err, command->err);
            if (!err.empty())
                LOG_ERROR(log, "Having stderr: {}", err);

            if (thread.joinable())
                thread.join();

            command->wait();
        }

        return status;
    }

    String getName() const override { return "SourceWithBackgroundThread"; }

    QueryPipeline pipeline;
    std::unique_ptr<PullingPipelineExecutor> executor;
    std::unique_ptr<ShellCommand> command;
    std::function<void(ShellCommand &)> send_data;
    ThreadFromGlobalPool thread;
    Poco::Logger * log;
};

}
