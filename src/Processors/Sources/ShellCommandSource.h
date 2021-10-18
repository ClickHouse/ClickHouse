#pragma once

#include <memory>

#include <base/logger_useful.h>
#include <base/BorrowedObjectPool.h>

#include <Common/ShellCommand.h>
#include <Common/ThreadPool.h>

#include <IO/ReadHelpers.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Formats/IInputFormat.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Executors/PullingPipelineExecutor.h>


namespace DB
{

/** A stream, that get child process and sends data using tasks in background threads.
  * For each send data task background thread is created. Send data task must send data to process input pipes.
  * ShellCommandPoolSource receives data from process stdout.
  *
  * If process_pool is passed in constructor then after source is destroyed process is returned to pool.
  */

using ProcessPool = BorrowedObjectPool<std::unique_ptr<ShellCommand>>;

struct ShellCommandSourceConfiguration
{
    /// Read fixed number of rows from command output
    bool read_fixed_number_of_rows = false;
    /// Valid only if read_fixed_number_of_rows = true
    bool read_number_of_rows_from_process_output = false;
    /// Valid only if read_fixed_number_of_rows = true
    size_t number_of_rows_to_read = 0;
    /// Max block size
    size_t max_block_size = DBMS_DEFAULT_BUFFER_SIZE;
};

class ShellCommandSource final : public SourceWithProgress
{
public:

    using SendDataTask = std::function<void(void)>;

    ShellCommandSource(
        ContextPtr context,
        const std::string & format,
        const Block & sample_block,
        std::unique_ptr<ShellCommand> && command_,
        std::vector<SendDataTask> && send_data_tasks = {},
        const ShellCommandSourceConfiguration & configuration_ = {},
        std::shared_ptr<ProcessPool> process_pool_ = nullptr)
        : SourceWithProgress(sample_block)
        , command(std::move(command_))
        , configuration(configuration_)
        , process_pool(process_pool_)
    {
        for (auto && send_data_task : send_data_tasks)
        {
            send_data_threads.emplace_back([task = std::move(send_data_task), this]()
            {
                try
                {
                    task();
                }
                catch (...)
                {
                    std::lock_guard<std::mutex> lock(send_data_lock);
                    exception_during_send_data = std::current_exception();
                }
            });
        }

        size_t max_block_size = configuration.max_block_size;

        if (configuration.read_fixed_number_of_rows)
        {
            /** Currently parallel parsing input format cannot read exactly max_block_size rows from input,
              * so it will be blocked on ReadBufferFromFileDescriptor because this file descriptor represent pipe that does not have eof.
              */
            auto context_for_reading = Context::createCopy(context);
            context_for_reading->setSetting("input_format_parallel_parsing", false);
            context = context_for_reading;

            if (configuration.read_number_of_rows_from_process_output)
            {
                readText(configuration.number_of_rows_to_read, command->out);
                char dummy;
                readChar(dummy, command->out);
            }

            max_block_size = configuration.number_of_rows_to_read;
        }

        pipeline = QueryPipeline(Pipe(context->getInputFormat(format, command->out, sample_block, max_block_size)));
        executor = std::make_unique<PullingPipelineExecutor>(pipeline);
    }

    ~ShellCommandSource() override
    {
        for (auto & thread : send_data_threads)
            if (thread.joinable())
                thread.join();

        if (command && process_pool)
            process_pool->returnObject(std::move(command));
    }

protected:

    Chunk generate() override
    {
        rethrowExceptionDuringSendDataIfNeeded();

        if (configuration.read_fixed_number_of_rows && configuration.number_of_rows_to_read == current_read_rows)
            return {};

        Chunk chunk;

        try
        {
            if (!executor->pull(chunk))
                return {};

            current_read_rows += chunk.getNumRows();
        }
        catch (...)
        {
            command = nullptr;
            throw;
        }

        return chunk;
    }

    Status prepare() override
    {
        auto status = SourceWithProgress::prepare();

        if (status == Status::Finished)
        {
            for (auto & thread : send_data_threads)
                if (thread.joinable())
                    thread.join();

            rethrowExceptionDuringSendDataIfNeeded();
        }

        return status;
    }

    String getName() const override { return "ShellCommandSource"; }

private:

    void rethrowExceptionDuringSendDataIfNeeded()
    {
        std::lock_guard<std::mutex> lock(send_data_lock);
        if (exception_during_send_data)
        {
            command = nullptr;
            std::rethrow_exception(exception_during_send_data);
        }
    }

    std::unique_ptr<ShellCommand> command;
    ShellCommandSourceConfiguration configuration;

    size_t current_read_rows = 0;

    std::shared_ptr<ProcessPool> process_pool;

    QueryPipeline pipeline;
    std::unique_ptr<PullingPipelineExecutor> executor;

    std::vector<ThreadFromGlobalPool> send_data_threads;
    std::mutex send_data_lock;
    std::exception_ptr exception_during_send_data;
};
}
