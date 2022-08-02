#include <Processors/Sources/ShellCommandSource.h>

#include <poll.h>

#include <Common/Stopwatch.h>

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
    extern const int CANNOT_FCNTL;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int CANNOT_POLL;
    extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
}

static bool tryMakeFdNonBlocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (-1 == flags)
        return false;
    if (-1 == fcntl(fd, F_SETFL, flags | O_NONBLOCK))
        return false;

    return true;
}

static void makeFdNonBlocking(int fd)
{
    bool result = tryMakeFdNonBlocking(fd);
    if (!result)
        throwFromErrno("Cannot set non-blocking mode of pipe", ErrorCodes::CANNOT_FCNTL);
}

static bool tryMakeFdBlocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (-1 == flags)
        return false;

    if (-1 == fcntl(fd, F_SETFL, flags & (~O_NONBLOCK)))
        return false;

    return true;
}

static void makeFdBlocking(int fd)
{
    bool result = tryMakeFdBlocking(fd);
    if (!result)
        throwFromErrno("Cannot set blocking mode of pipe", ErrorCodes::CANNOT_FCNTL);
}

static bool pollFd(int fd, size_t timeout_milliseconds, int events)
{
    pollfd pfd;
    pfd.fd = fd;
    pfd.events = events;
    pfd.revents = 0;

    Stopwatch watch;

    int res;

    while (true)
    {
        res = poll(&pfd, 1, timeout_milliseconds);

        if (res < 0)
        {
            if (errno == EINTR)
            {
                watch.stop();
                timeout_milliseconds -= watch.elapsedMilliseconds();
                watch.start();

                continue;
            }
            else
            {
                throwFromErrno("Cannot poll", ErrorCodes::CANNOT_POLL);
            }
        }
        else
        {
            break;
        }
    }

    return res > 0;
}

class TimeoutReadBufferFromFileDescriptor : public BufferWithOwnMemory<ReadBuffer>
{
public:
    explicit TimeoutReadBufferFromFileDescriptor(int fd_, size_t timeout_milliseconds_)
        : fd(fd_)
        , timeout_milliseconds(timeout_milliseconds_)
    {
        makeFdNonBlocking(fd);
    }

    bool nextImpl() override
    {
        size_t bytes_read = 0;

        while (!bytes_read)
        {
            if (!pollFd(fd, timeout_milliseconds, POLLIN))
                throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Pipe read timeout exceeded {} milliseconds", timeout_milliseconds);

            ssize_t res = ::read(fd, internal_buffer.begin(), internal_buffer.size());

            if (-1 == res && errno != EINTR)
                throwFromErrno("Cannot read from pipe", ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);

            if (res == 0)
                break;

            if (res > 0)
                bytes_read += res;
        }

        if (bytes_read > 0)
        {
            working_buffer = internal_buffer;
            working_buffer.resize(bytes_read);
        }
        else
        {
            return false;
        }

        return true;
    }

    void reset() const
    {
        makeFdBlocking(fd);
    }

    ~TimeoutReadBufferFromFileDescriptor() override
    {
        tryMakeFdBlocking(fd);
    }

private:
    int fd;
    size_t timeout_milliseconds;
};

class TimeoutWriteBufferFromFileDescriptor : public BufferWithOwnMemory<WriteBuffer>
{
public:
    explicit TimeoutWriteBufferFromFileDescriptor(int fd_, size_t timeout_milliseconds_)
        : fd(fd_)
        , timeout_milliseconds(timeout_milliseconds_)
    {
        makeFdNonBlocking(fd);
    }

    void nextImpl() override
    {
        if (!offset())
            return;

        size_t bytes_written = 0;

        while (bytes_written != offset())
        {
            if (!pollFd(fd, timeout_milliseconds, POLLOUT))
                throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Pipe write timeout exceeded {} milliseconds", timeout_milliseconds);

            ssize_t res = ::write(fd, working_buffer.begin() + bytes_written, offset() - bytes_written);

            if ((-1 == res || 0 == res) && errno != EINTR)
                throwFromErrno("Cannot write into pipe", ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR);

            if (res > 0)
                bytes_written += res;
        }
    }

    void reset() const
    {
        makeFdBlocking(fd);
    }

    ~TimeoutWriteBufferFromFileDescriptor() override
    {
        tryMakeFdBlocking(fd);
    }

private:
    int fd;
    size_t timeout_milliseconds;
};

class ShellCommandHolder
{
public:
    using ShellCommandBuilderFunc = std::function<std::unique_ptr<ShellCommand>()>;

    explicit ShellCommandHolder(ShellCommandBuilderFunc && func_)
        : func(std::move(func_))
    {}

    std::unique_ptr<ShellCommand> buildCommand()
    {
        if (returned_command)
            return std::move(returned_command);

        return func();
    }

    void returnCommand(std::unique_ptr<ShellCommand> command)
    {
        returned_command = std::move(command);
    }

private:
    std::unique_ptr<ShellCommand> returned_command;
    ShellCommandBuilderFunc func;
};

namespace
{
    /** A stream, that get child process and sends data using tasks in background threads.
    * For each send data task background thread is created. Send data task must send data to process input pipes.
    * ShellCommandPoolSource receives data from process stdout.
    *
    * If process_pool is passed in constructor then after source is destroyed process is returned to pool.
    */
    class ShellCommandSource final : public ISource
    {
    public:

        using SendDataTask = std::function<void(void)>;

        ShellCommandSource(
            ContextPtr context_,
            const std::string & format_,
            size_t command_read_timeout_milliseconds,
            const Block & sample_block_,
            std::unique_ptr<ShellCommand> && command_,
            std::vector<SendDataTask> && send_data_tasks = {},
            const ShellCommandSourceConfiguration & configuration_ = {},
            std::unique_ptr<ShellCommandHolder> && command_holder_ = nullptr,
            std::shared_ptr<ProcessPool> process_pool_ = nullptr)
            : ISource(sample_block_)
            , context(context_)
            , format(format_)
            , sample_block(sample_block_)
            , command(std::move(command_))
            , configuration(configuration_)
            , timeout_command_out(command->out.getFD(), command_read_timeout_milliseconds)
            , command_holder(std::move(command_holder_))
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
                    /// Initialize executor in generate
                    return;
                }

                max_block_size = configuration.number_of_rows_to_read;
            }

            pipeline = QueryPipeline(Pipe(context->getInputFormat(format, timeout_command_out, sample_block, max_block_size)));
            executor = std::make_unique<PullingPipelineExecutor>(pipeline);
        }

        ~ShellCommandSource() override
        {
            for (auto & thread : send_data_threads)
                if (thread.joinable())
                    thread.join();

            if (command_is_invalid)
                command = nullptr;

            if (command_holder && process_pool)
            {
                bool valid_command = configuration.read_fixed_number_of_rows && current_read_rows >= configuration.number_of_rows_to_read;

                if (command && valid_command)
                    command_holder->returnCommand(std::move(command));

                process_pool->returnObject(std::move(command_holder));
            }
        }

    protected:

        Chunk generate() override
        {
            rethrowExceptionDuringSendDataIfNeeded();

            Chunk chunk;

            try
            {
                if (configuration.read_fixed_number_of_rows)
                {
                    if (!executor && configuration.read_number_of_rows_from_process_output)
                    {
                        readText(configuration.number_of_rows_to_read, timeout_command_out);
                        char dummy;
                        readChar(dummy, timeout_command_out);

                        size_t max_block_size = configuration.number_of_rows_to_read;
                        pipeline = QueryPipeline(Pipe(context->getInputFormat(format, timeout_command_out, sample_block, max_block_size)));
                        executor = std::make_unique<PullingPipelineExecutor>(pipeline);
                    }

                    if (current_read_rows >= configuration.number_of_rows_to_read)
                        return {};
                }

                if (!executor->pull(chunk))
                    return {};

                current_read_rows += chunk.getNumRows();
            }
            catch (...)
            {
                command_is_invalid = true;
                throw;
            }

            return chunk;
        }

        Status prepare() override
        {
            auto status = ISource::prepare();

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
                command_is_invalid = true;
                std::rethrow_exception(exception_during_send_data);
            }
        }

        ContextPtr context;
        std::string format;
        Block sample_block;

        std::unique_ptr<ShellCommand> command;
        ShellCommandSourceConfiguration configuration;

        TimeoutReadBufferFromFileDescriptor timeout_command_out;

        size_t current_read_rows = 0;

        ShellCommandHolderPtr command_holder;
        std::shared_ptr<ProcessPool> process_pool;

        QueryPipeline pipeline;
        std::unique_ptr<PullingPipelineExecutor> executor;

        std::vector<ThreadFromGlobalPool> send_data_threads;

        std::mutex send_data_lock;
        std::exception_ptr exception_during_send_data;

        std::atomic<bool> command_is_invalid {false};
    };

    class SendingChunkHeaderTransform final : public ISimpleTransform
    {
    public:
        SendingChunkHeaderTransform(const Block & header, std::shared_ptr<TimeoutWriteBufferFromFileDescriptor> buffer_)
            : ISimpleTransform(header, header, false)
            , buffer(buffer_)
        {
        }

        String getName() const override { return "SendingChunkHeaderTransform"; }

    protected:

        void transform(Chunk & chunk) override
        {
            writeText(chunk.getNumRows(), *buffer);
            writeChar('\n', *buffer);
        }

    private:
        std::shared_ptr<TimeoutWriteBufferFromFileDescriptor> buffer;
    };

}

ShellCommandSourceCoordinator::ShellCommandSourceCoordinator(const Configuration & configuration_)
    : configuration(configuration_)
{
    if (configuration.is_executable_pool)
        process_pool = std::make_shared<ProcessPool>(configuration.pool_size ? configuration.pool_size : std::numeric_limits<size_t>::max());
}

Pipe ShellCommandSourceCoordinator::createPipe(
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
    std::unique_ptr<ShellCommandHolder> process_holder;

    auto destructor_strategy = ShellCommand::DestructorStrategy{true /*terminate_in_destructor*/, configuration.command_termination_timeout_seconds};
    command_config.terminate_in_destructor_strategy = destructor_strategy;

    bool is_executable_pool = (process_pool != nullptr);
    if (is_executable_pool)
    {
        bool execute_direct = configuration.execute_direct;

        bool result = process_pool->tryBorrowObject(
            process_holder,
            [command_config, execute_direct]()
            {
                ShellCommandHolder::ShellCommandBuilderFunc func = [command_config, execute_direct]() mutable
                {
                    if (execute_direct)
                        return ShellCommand::executeDirect(command_config);
                    else
                        return ShellCommand::execute(command_config);
                };

                return std::make_unique<ShellCommandHolder>(std::move(func));
            },
            configuration.max_command_execution_time_seconds * 10000);

        if (!result)
            throw Exception(
                ErrorCodes::TIMEOUT_EXCEEDED,
                "Could not get process from pool, max command execution timeout exceeded {} seconds",
                configuration.max_command_execution_time_seconds);

        process = process_holder->buildCommand();
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

        int write_buffer_fd = write_buffer->getFD();
        auto timeout_write_buffer = std::make_shared<TimeoutWriteBufferFromFileDescriptor>(write_buffer_fd, configuration.command_write_timeout_milliseconds);

        input_pipes[i].resize(1);

        if (configuration.send_chunk_header)
        {
            auto transform = std::make_shared<SendingChunkHeaderTransform>(input_pipes[i].getHeader(), timeout_write_buffer);
            input_pipes[i].addTransform(std::move(transform));
        }

        auto pipeline = std::make_shared<QueryPipeline>(std::move(input_pipes[i]));
        auto out = context->getOutputFormat(configuration.format, *timeout_write_buffer, materializeBlock(pipeline->getHeader()));
        out->setAutoFlush();
        pipeline->complete(std::move(out));

        ShellCommandSource::SendDataTask task = [pipeline, timeout_write_buffer, write_buffer, is_executable_pool]()
        {
            CompletedPipelineExecutor executor(*pipeline);
            executor.execute();
            timeout_write_buffer->finalize();

            if (!is_executable_pool)
            {
                timeout_write_buffer->reset();

                write_buffer->close();
            }
        };

        tasks.emplace_back(std::move(task));
    }

    auto source = std::make_unique<ShellCommandSource>(
        context,
        configuration.format,
        configuration.command_read_timeout_milliseconds,
        std::move(sample_block),
        std::move(process),
        std::move(tasks),
        source_configuration,
        std::move(process_holder),
        process_pool);

    return Pipe(std::move(source));
}

}
