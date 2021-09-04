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

/** A stream, that runs child process and sends data to its stdin in background thread,
  * and receives data from its stdout.
  */
class ShellCommandSource final : public SourceWithProgress
{
public:
    using SendDataTask = std::function<void (void)>;

    ShellCommandSource(
        ContextPtr context,
        const std::string & format,
        const Block & sample_block,
        std::unique_ptr<ShellCommand> command_,
        Poco::Logger * log_,
        std::vector<SendDataTask> && send_data_tasks,
        size_t max_block_size = DEFAULT_BLOCK_SIZE)
        : SourceWithProgress(sample_block)
        , command(std::move(command_))
        , log(log_)
    {
        for (auto && send_data_task : send_data_tasks)
            send_data_threads.emplace_back([task = std::move(send_data_task)]() { task(); });

        pipeline.init(Pipe(FormatFactory::instance().getInput(format, command->out, sample_block, context, max_block_size)));
        executor = std::make_unique<PullingPipelineExecutor>(pipeline);
    }

    ShellCommandSource(
        ContextPtr context,
        const std::string & format,
        const Block & sample_block,
        std::unique_ptr<ShellCommand> command_,
        Poco::Logger * log_,
        size_t max_block_size = DEFAULT_BLOCK_SIZE)
        : SourceWithProgress(sample_block)
        , command(std::move(command_))
        , log(log_)
    {
        pipeline.init(Pipe(FormatFactory::instance().getInput(format, command->out, sample_block, context, max_block_size)));
        executor = std::make_unique<PullingPipelineExecutor>(pipeline);
    }

    ~ShellCommandSource() override
    {
        for (auto & thread : send_data_threads)
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

            for (auto & thread : send_data_threads)
                if (thread.joinable())
                    thread.join();

            command->wait();
        }

        return status;
    }

    String getName() const override { return "ShellCommandSource"; }

private:

    QueryPipeline pipeline;
    std::unique_ptr<PullingPipelineExecutor> executor;
    std::unique_ptr<ShellCommand> command;
    std::vector<ThreadFromGlobalPool> send_data_threads;
    Poco::Logger * log;
};

}
