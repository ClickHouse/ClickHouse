#include "ExecutablePoolDictionarySource.h"

#include <functional>
#include <common/scope_guard.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/QueryPipeline.h>
#include <DataStreams/formatBlock.h>
#include <Interpreters/Context.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/IInputFormat.h>
#include <Common/ShellCommand.h>
#include <Common/ThreadPool.h>
#include <common/logger_useful.h>
#include <common/LocalDateTime.h>
#include "DictionarySourceFactory.h"
#include "DictionarySourceHelpers.h"
#include "DictionaryStructure.h"
#include "registerDictionaries.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int DICTIONARY_ACCESS_DENIED;
    extern const int UNSUPPORTED_METHOD;
    extern const int TIMEOUT_EXCEEDED;
}

ExecutablePoolDictionarySource::ExecutablePoolDictionarySource(
    const DictionaryStructure & dict_struct_,
    const Configuration & configuration_,
    Block & sample_block_,
    ContextPtr context_)
    : log(&Poco::Logger::get("ExecutablePoolDictionarySource"))
    , dict_struct{dict_struct_}
    , configuration{configuration_}
    , sample_block{sample_block_}
    , context{context_}
    /// If pool size == 0 then there is no size restrictions. Poco max size of semaphore is integer type.
    , process_pool{std::make_shared<ProcessPool>(configuration.pool_size == 0 ? std::numeric_limits<int>::max() : configuration.pool_size)}
{
    /// Remove keys from sample_block for implicit_key dictionary because
    /// these columns will not be returned from source
    /// Implicit key means that the source script will return only values,
    /// and the correspondence to the requested keys is determined implicitly - by the order of rows in the result.
    if (configuration.implicit_key)
    {
        auto keys_names = dict_struct.getKeysNames();

        for (auto & key_name : keys_names)
        {
            size_t key_column_position_in_block = sample_block.getPositionByName(key_name);
            sample_block.erase(key_column_position_in_block);
        }
    }
}

ExecutablePoolDictionarySource::ExecutablePoolDictionarySource(const ExecutablePoolDictionarySource & other)
    : log(&Poco::Logger::get("ExecutablePoolDictionarySource"))
    , update_time{other.update_time}
    , dict_struct{other.dict_struct}
    , configuration{other.configuration}
    , sample_block{other.sample_block}
    , context{Context::createCopy(other.context)}
    , process_pool{std::make_shared<ProcessPool>(configuration.pool_size)}
{
}

Pipe ExecutablePoolDictionarySource::loadAll()
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "ExecutablePoolDictionarySource does not support loadAll method");
}

Pipe ExecutablePoolDictionarySource::loadUpdatedAll()
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "ExecutablePoolDictionarySource does not support loadUpdatedAll method");
}

namespace
{
    /** A stream, that runs child process and sends data to its stdin in background thread,
      *  and receives data from its stdout.
      */
    class PoolSourceWithBackgroundThread final : public SourceWithProgress
    {
    public:
        PoolSourceWithBackgroundThread(
            std::shared_ptr<ProcessPool> process_pool_,
            std::unique_ptr<ShellCommand> && command_,
            Pipe pipe,
            size_t read_rows_,
            Poco::Logger * log_,
            std::function<void(WriteBufferFromFile &)> && send_data_)
            : SourceWithProgress(pipe.getHeader())
            , process_pool(process_pool_)
            , command(std::move(command_))
            , rows_to_read(read_rows_)
            , log(log_)
            , send_data(std::move(send_data_))
            , thread([this]
            {
                try
                {
                    send_data(command->in);
                }
                catch (...)
                {
                    std::lock_guard<std::mutex> lck(exception_during_read_lock);
                    exception_during_read = std::current_exception();
                }
            })
        {
            pipeline.init(std::move(pipe));
            executor = std::make_unique<PullingPipelineExecutor>(pipeline);
        }

        ~PoolSourceWithBackgroundThread() override
        {
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
                if (thread.joinable())
                    thread.join();

                rethrowExceptionDuringReadIfNeeded();
            }

            return status;
        }

        void rethrowExceptionDuringReadIfNeeded()
        {
            std::lock_guard<std::mutex> lck(exception_during_read_lock);
            if (exception_during_read)
            {
                command = nullptr;
                std::rethrow_exception(exception_during_read);
            }
        }

        String getName() const override { return "PoolWithBackgroundThread"; }

        std::shared_ptr<ProcessPool> process_pool;
        std::unique_ptr<ShellCommand> command;
        QueryPipeline pipeline;
        std::unique_ptr<PullingPipelineExecutor> executor;
        size_t rows_to_read;
        Poco::Logger * log;
        std::function<void(WriteBufferFromFile &)> send_data;
        ThreadFromGlobalPool thread;
        size_t current_read_rows = 0;
        std::mutex exception_during_read_lock;
        std::exception_ptr exception_during_read;
    };

}

Pipe ExecutablePoolDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    LOG_TRACE(log, "loadIds {} size = {}", toString(), ids.size());

    auto block = blockForIds(dict_struct, ids);
    return getStreamForBlock(block);
}

Pipe ExecutablePoolDictionarySource::loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows)
{
    LOG_TRACE(log, "loadKeys {} size = {}", toString(), requested_rows.size());

    auto block = blockForKeys(dict_struct, key_columns, requested_rows);
    return getStreamForBlock(block);
}

Pipe ExecutablePoolDictionarySource::getStreamForBlock(const Block & block)
{
    std::unique_ptr<ShellCommand> process;
    bool result = process_pool->tryBorrowObject(process, [this]()
    {
        bool terminate_in_destructor = true;
        ShellCommandDestructorStrategy strategy { terminate_in_destructor, configuration.command_termination_timeout };
        auto shell_command = ShellCommand::execute(configuration.command, false, strategy);
        return shell_command;
    }, configuration.max_command_execution_time * 10000);

    if (!result)
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED,
            "Could not get process from pool, max command execution timeout exceeded ({}) seconds",
            configuration.max_command_execution_time);

    size_t rows_to_read = block.rows();
    auto format = FormatFactory::instance().getInput(configuration.format, process->out, sample_block, context, rows_to_read);

    Pipe pipe(std::make_unique<PoolSourceWithBackgroundThread>(
        process_pool, std::move(process), Pipe(std::move(format)), rows_to_read, log,
        [block, this](WriteBufferFromFile & out) mutable
        {
            auto output_stream = context->getOutputStream(configuration.format, out, block.cloneEmpty());
            formatBlock(output_stream, block);
        }));

    if (configuration.implicit_key)
        pipe.addTransform(std::make_shared<TransformWithAdditionalColumns>(block, pipe.getHeader()));

    return pipe;
}

bool ExecutablePoolDictionarySource::isModified() const
{
    return true;
}

bool ExecutablePoolDictionarySource::supportsSelectiveLoad() const
{
    return true;
}

bool ExecutablePoolDictionarySource::hasUpdateField() const
{
    return false;
}

DictionarySourcePtr ExecutablePoolDictionarySource::clone() const
{
    return std::make_unique<ExecutablePoolDictionarySource>(*this);
}

std::string ExecutablePoolDictionarySource::toString() const
{
    return "ExecutablePool size: " + std::to_string(configuration.pool_size) + " command: " + configuration.command;
}

void registerDictionarySourceExecutablePool(DictionarySourceFactory & factory)
{
    auto create_table_source = [=](const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & config_prefix,
                                 Block & sample_block,
                                 ContextPtr context,
                                 const std::string & /* default_database */,
                                 bool created_from_ddl) -> DictionarySourcePtr
    {
        if (dict_struct.has_expressions)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Dictionary source of type `executable_pool` does not support attribute expressions");

        /// Executable dictionaries may execute arbitrary commands.
        /// It's OK for dictionaries created by administrator from xml-file, but
        /// maybe dangerous for dictionaries created from DDL-queries.
        if (created_from_ddl && context->getApplicationType() != Context::ApplicationType::LOCAL)
            throw Exception(ErrorCodes::DICTIONARY_ACCESS_DENIED, "Dictionaries with executable pool dictionary source are not allowed to be created from DDL query");

        auto context_local_copy = copyContextAndApplySettings(config_prefix, context, config);

        /** Currently parallel parsing input format cannot read exactly max_block_size rows from input,
         *  so it will be blocked on ReadBufferFromFileDescriptor because this file descriptor represent pipe that does not have eof.
         */
        auto settings_no_parallel_parsing = context_local_copy->getSettings();
        settings_no_parallel_parsing.input_format_parallel_parsing = false;
        context_local_copy->setSettings(settings_no_parallel_parsing);

        String settings_config_prefix = config_prefix + ".executable_pool";

        size_t max_command_execution_time = config.getUInt64(settings_config_prefix + ".max_command_execution_time", 10);

        size_t max_execution_time_seconds = static_cast<size_t>(context->getSettings().max_execution_time.totalSeconds());
        if (max_execution_time_seconds != 0 && max_command_execution_time > max_execution_time_seconds)
            max_command_execution_time = max_execution_time_seconds;

        ExecutablePoolDictionarySource::Configuration configuration
        {
            .command = config.getString(settings_config_prefix + ".command"),
            .format = config.getString(settings_config_prefix + ".format"),
            .pool_size = config.getUInt64(settings_config_prefix + ".size"),
            .command_termination_timeout = config.getUInt64(settings_config_prefix + ".command_termination_timeout", 10),
            .max_command_execution_time = max_command_execution_time,
            .implicit_key = config.getBool(settings_config_prefix + ".implicit_key", false),
        };

        return std::make_unique<ExecutablePoolDictionarySource>(dict_struct, configuration, sample_block, context_local_copy);
    };

    factory.registerSource("executable_pool", create_table_source);
}

}
