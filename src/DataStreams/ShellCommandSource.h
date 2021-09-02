#pragma once

#include <memory>

#include <common/logger_useful.h>
#include <common/BorrowedObjectPool.h>

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

/** A stream, that get child process and sends data tasks.
  * For each send data task background thread is created, send data tasks must send data to process input pipes.
  * ShellCommandSource receives data from process stdout.
  */
class ShellCommandSource final : public SourceWithProgress
{
public:
    using SendDataTask = std::function<void (void)>;

    ShellCommandSource(
        ContextPtr context,
        const std::string & format,
        const Block & sample_block,
        std::unique_ptr<ShellCommand> && command_,
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
        std::unique_ptr<ShellCommand> && command_,
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

/** A stream, that get child process and sends data tasks.
  * For each send data task background thread is created, send data tasks must send data to process input pipes.
  * ShellCommandPoolSource receives data from process stdout.
  *
  * Main difference with ShellCommandSource is that ShellCommandPoolSource initialized with process_pool and rows_to_read.
  * Rows to read are necessary because processes in pool are not destroyed and work in read write loop.
  * Source need to finish generating new chunks after rows_to_read rows are generated from process.
  *
  * If rows_to_read are not specified it is expected that script will output rows_to_read before other data.
  *
  * After source is destroyed process is returned to pool.
  */

using ProcessPool = BorrowedObjectPool<std::unique_ptr<ShellCommand>>;

class ShellCommandPoolSource final : public SourceWithProgress
{
public:
    using SendDataTask = std::function<void(void)>;

    ShellCommandPoolSource(
        ContextPtr context,
        const std::string & format,
        const Block & sample_block,
        std::shared_ptr<ProcessPool> process_pool_,
        std::unique_ptr<ShellCommand> && command_,
        size_t rows_to_read_,
        Poco::Logger * log_,
        std::vector<SendDataTask> && send_data_tasks)
        : SourceWithProgress(sample_block)
        , process_pool(process_pool_)
        , command(std::move(command_))
        , rows_to_read(rows_to_read_)
        , log(log_)
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

        pipeline.init(Pipe(FormatFactory::instance().getInput(format, command->out, sample_block, context, rows_to_read)));
        executor = std::make_unique<PullingPipelineExecutor>(pipeline);
    }

    ShellCommandPoolSource(
        ContextPtr context,
        const std::string & format,
        const Block & sample_block,
        std::shared_ptr<ProcessPool> process_pool_,
        std::unique_ptr<ShellCommand> && command_,
        Poco::Logger * log_,
        std::vector<SendDataTask> && send_data_tasks)
        : SourceWithProgress(sample_block)
        , process_pool(process_pool_)
        , command(std::move(command_))
        , log(log_)
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

        readText(rows_to_read, command->out);
        pipeline.init(Pipe(FormatFactory::instance().getInput(format, command->out, sample_block, context, rows_to_read)));
        executor = std::make_unique<PullingPipelineExecutor>(pipeline);
    }


    ~ShellCommandPoolSource() override
    {
        for (auto & thread : send_data_threads)
            if (thread.joinable())
                thread.join();

        if (command)
            process_pool->returnObject(std::move(command));
    }

protected:
    Chunk generate() override
    {
        rethrowExceptionDuringReadIfNeeded();

        if (current_read_rows == rows_to_read)
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
            tryLogCurrentException(log);
            command = nullptr;
            throw;
        }

        return chunk;
    }

public:
    Status prepare() override
    {
        auto status = SourceWithProgress::prepare();

        if (status == Status::Finished)
        {
            for (auto & thread : send_data_threads)
                if (thread.joinable())
                    thread.join();

            rethrowExceptionDuringReadIfNeeded();
        }

        return status;
    }

    void rethrowExceptionDuringReadIfNeeded()
    {
        std::lock_guard<std::mutex> lock(send_data_lock);
        if (exception_during_send_data)
        {
            command = nullptr;
            std::rethrow_exception(exception_during_send_data);
        }
    }

    String getName() const override { return "ShellCommandPoolSource"; }

    std::shared_ptr<ProcessPool> process_pool;
    std::unique_ptr<ShellCommand> command;
    QueryPipeline pipeline;
    std::unique_ptr<PullingPipelineExecutor> executor;
    size_t rows_to_read = 0;
    Poco::Logger * log;
    std::vector<ThreadFromGlobalPool> send_data_threads;

    size_t current_read_rows = 0;

    std::mutex send_data_lock;
    std::exception_ptr exception_during_send_data;
};
}
