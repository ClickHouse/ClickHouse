#include <Processors/Sources/ShellCommandSource.h>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <QueryPipeline/Pipe.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int TIMEOUT_EXCEEDED;
}

namespace
{
    /** A stream, that get child process and sends data using tasks in background threads.
    * For each send data task background thread is created. Send data task must send data to process input pipes.
    * ShellCommandPoolSource receives data from process stdout.
    *
    * If process_pool is passed in constructor then after source is destroyed process is returned to pool.
    */
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
                    /// TODO: Move to generate
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

            if (configuration.read_fixed_number_of_rows && configuration.number_of_rows_to_read >= current_read_rows)
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

    class SendingChunkHeaderTransform final : public ISimpleTransform
    {
    public:
        SendingChunkHeaderTransform(const Block & header, WriteBuffer & buffer_)
            : ISimpleTransform(header, header, false)
            , buffer(buffer_)
        {
        }

        String getName() const override { return "SendingChunkHeaderTransform"; }

    protected:

        void transform(Chunk & chunk) override
        {
            writeText(chunk.getNumRows(), buffer);
            writeChar('\n', buffer);
        }

    private:
        WriteBuffer & buffer;
    };

}

ShellCommandCoordinator::ShellCommandCoordinator(const Configuration & configuration_)
    : configuration(configuration_)
{
    if (configuration.is_executable_pool)
        process_pool = std::make_shared<ProcessPool>(configuration.pool_size ? configuration.pool_size : std::numeric_limits<size_t>::max());
}

Pipe ShellCommandCoordinator::createPipe(
    const std::string & command,
    const std::vector<std::string> & arguments,
    std::vector<Pipe> && input_pipes,
    Block sample_block,
    ContextPtr context,
    const ShellCommandSourceConfiguration & source_configuration)
{
    ShellCommand::Config command_config(command);
    command_config.arguments = arguments;
    for (size_t i = 1; i < input_pipes.size(); ++i)
        command_config.write_fds.emplace_back(i + 2);

    std::unique_ptr<ShellCommand> process;

    bool is_executable_pool = (process_pool != nullptr);
    if (is_executable_pool)
    {
        bool result = process_pool->tryBorrowObject(
            process,
            [&command_config, this]()
            {
                command_config.terminate_in_destructor_strategy
                    = ShellCommand::DestructorStrategy{true /*terminate_in_destructor*/, configuration.command_termination_timeout};

                if (configuration.execute_direct)
                    return ShellCommand::executeDirect(command_config);
                else
                    return ShellCommand::execute(command_config);
            },
            configuration.max_command_execution_time * 10000);

        if (!result)
            throw Exception(
                ErrorCodes::TIMEOUT_EXCEEDED,
                "Could not get process from pool, max command execution timeout exceeded {} seconds",
                configuration.max_command_execution_time);
    }
    else
    {
        if (configuration.execute_direct)
            process = ShellCommand::executeDirect(command_config);
        else
            process = ShellCommand::execute(command_config);
    }

    std::vector<ShellCommandSource::SendDataTask> tasks;
    tasks.reserve(input_pipes.size());

    for (size_t i = 0; i < input_pipes.size(); ++i)
    {
        WriteBufferFromFile * write_buffer = nullptr;

        if (i == 0)
        {
            write_buffer = &process->in;
        }
        else
        {
            auto descriptor = i + 2;
            auto it = process->write_fds.find(descriptor);
            if (it == process->write_fds.end())
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Process does not contain descriptor to write {}", descriptor);

            write_buffer = &it->second;
        }

        input_pipes[i].resize(1);

        if (configuration.send_chunk_header)
        {
            auto transform = std::make_shared<SendingChunkHeaderTransform>(input_pipes[i].getHeader(), *write_buffer);
            input_pipes[i].addTransform(std::move(transform));
        }

        auto pipeline = std::make_shared<QueryPipeline>(std::move(input_pipes[i]));
        auto out = context->getOutputFormat(configuration.format, *write_buffer, materializeBlock(pipeline->getHeader()));
        out->setAutoFlush();
        pipeline->complete(std::move(out));

        ShellCommandSource::SendDataTask task = [pipeline, write_buffer, is_executable_pool]()
        {
            CompletedPipelineExecutor executor(*pipeline);
            executor.execute();

            if (!is_executable_pool)
                write_buffer->close();
        };

        tasks.emplace_back(std::move(task));
    }

    Pipe pipe(std::make_unique<ShellCommandSource>(context, configuration.format, std::move(sample_block), std::move(process), std::move(tasks), source_configuration, process_pool));
    return pipe;
}

}
