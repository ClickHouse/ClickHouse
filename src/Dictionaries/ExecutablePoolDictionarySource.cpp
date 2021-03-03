#include "ExecutablePoolDictionarySource.h"

#include <functional>
#include <ext/scope_guard.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Interpreters/Context.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/copyData.h>
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
}

ExecutablePoolDictionarySource::ExecutablePoolDictionarySource(
    const DictionaryStructure & dict_struct_,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    Block & sample_block_,
    const Context & context_)
    : log(&Poco::Logger::get("ExecutablePoolDictionarySource"))
    , dict_struct{dict_struct_}
    , implicit_key{config.getBool(config_prefix + ".implicit_key", false)}
    , command{config.getString(config_prefix + ".command")}
    , update_field{config.getString(config_prefix + ".update_field", "")}
    , format{config.getString(config_prefix + ".format")}
    , pool_size(config.getUInt64(config_prefix + ".size"))
    , command_termination_timeout(config.getUInt64(config_prefix + ".command_termination_timeout", 10))
    , sample_block{sample_block_}
    , context(context_)
    /// If pool size == 0 then there is no size restrictions. Poco max size of semaphore is integer type.
    , process_pool(std::make_shared<ProcessPool>(pool_size == 0 ? std::numeric_limits<int>::max() : pool_size))
{
    /// Remove keys from sample_block for implicit_key dictionary because
    /// these columns will not be returned from source
    /// Implicit key means that the source script will return only values,
    /// and the correspondence to the requested keys is determined implicitly - by the order of rows in the result.
    if (implicit_key)
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
    , implicit_key{other.implicit_key}
    , command{other.command}
    , update_field{other.update_field}
    , format{other.format}
    , pool_size{other.pool_size}
    , command_termination_timeout{other.command_termination_timeout}
    , sample_block{other.sample_block}
    , context(other.context)
    , process_pool(std::make_shared<ProcessPool>(pool_size))
{
}

BlockInputStreamPtr ExecutablePoolDictionarySource::loadAll()
{
    throw Exception("ExecutablePoolDictionarySource with implicit_key does not support loadAll method", ErrorCodes::UNSUPPORTED_METHOD);
}

BlockInputStreamPtr ExecutablePoolDictionarySource::loadUpdatedAll()
{
    throw Exception("ExecutablePoolDictionarySource with implicit_key does not support loadAll method", ErrorCodes::UNSUPPORTED_METHOD);
}

namespace
{
    /** A stream, that runs child process and sends data to its stdin in background thread,
      *  and receives data from its stdout.
      */
    class PoolBlockInputStreamWithBackgroundThread final : public IBlockInputStream
    {
    public:
        PoolBlockInputStreamWithBackgroundThread(
            std::shared_ptr<ProcessPool> process_pool_,
            std::unique_ptr<ShellCommand> && command_,
            BlockInputStreamPtr && stream_,
            size_t read_rows_,
            Poco::Logger * log_,
            std::function<void(WriteBufferFromFile &)> && send_data_)
            : process_pool(process_pool_)
            , command(std::move(command_))
            , stream(std::move(stream_))
            , rows_to_read(read_rows_)
            , log(log_)
            , send_data(std::move(send_data_))
            , thread([this]
            {
                try
                {
                    send_data(command->in);
                }
                catch (const std::exception & ex)
                {
                    LOG_ERROR(log, "Error during write into process input stream: ({})", ex.what());
                    error_during_write = true;
                }
            })
        {}

        ~PoolBlockInputStreamWithBackgroundThread() override
        {
            if (thread.joinable())
                thread.join();

            if (command)
                process_pool->returnObject(std::move(command));
        }

        Block getHeader() const override
        {
            return stream->getHeader();
        }

    private:
        Block readImpl() override
        {
            if (error_during_write)
            {
                command = nullptr;
                return Block();
            }

            if (current_read_rows == rows_to_read)
                return Block();

            Block block;

            try
            {
                block = stream->read();
                current_read_rows += block.rows();
            }
            catch (const std::exception & ex)
            {
                LOG_ERROR(log, "Error during read from process output stream: ({})", ex.what());
                command = nullptr;
            }

            return block;
        }

        void readPrefix() override
        {
            if (error_during_write)
                return;

            stream->readPrefix();
        }

        void readSuffix() override
        {
            if (error_during_write)
                command = nullptr;
            else
                stream->readSuffix();

            if (thread.joinable())
                thread.join();
        }

        String getName() const override { return "PoolWithBackgroundThread"; }

        std::shared_ptr<ProcessPool> process_pool;
        std::unique_ptr<ShellCommand> command;
        BlockInputStreamPtr stream;
        size_t rows_to_read;
        Poco::Logger * log;
        std::function<void(WriteBufferFromFile &)> send_data;
        ThreadFromGlobalPool thread;
        size_t current_read_rows = 0;
        std::atomic<bool> error_during_write = false;
    };

}

BlockInputStreamPtr ExecutablePoolDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    LOG_TRACE(log, "loadIds {} size = {}", toString(), ids.size());

    auto block = blockForIds(dict_struct, ids);
    return getStreamForBlock(block);
}

BlockInputStreamPtr ExecutablePoolDictionarySource::loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows)
{
    LOG_TRACE(log, "loadKeys {} size = {}", toString(), requested_rows.size());

    auto block = blockForKeys(dict_struct, key_columns, requested_rows);
    return getStreamForBlock(block);
}

BlockInputStreamPtr ExecutablePoolDictionarySource::getStreamForBlock(const Block & block)
{
    std::cerr << "ExecutablePoolDictionarySource::getStreamForBlock borrow object start " << std::endl;
    std::cerr << "Borrowed objects size " << process_pool->borrowedObjectsSize() << " allocated objects size " << process_pool->allocatedObjectsSize() << std::endl;

    std::unique_ptr<ShellCommand> process = process_pool->tryBorrowObject([this]()
    {
        bool terminate_in_destructor = true;
        ShellCommandDestructorStrategy strategy { terminate_in_destructor, command_termination_timeout };
        auto shell_command = ShellCommand::execute(command, false, strategy);
        return shell_command;
    }, 5000);

    size_t rows_to_read = block.rows();
    auto read_stream = context.getInputFormat(format, process->out, sample_block, rows_to_read);

    auto stream = std::make_unique<PoolBlockInputStreamWithBackgroundThread>(
        process_pool, std::move(process), std::move(read_stream), rows_to_read, log,
        [block, this](WriteBufferFromFile & out) mutable
        {
            auto output_stream = context.getOutputStream(format, out, block.cloneEmpty());
            formatBlock(output_stream, block);
        });

    if (implicit_key)
        return std::make_shared<BlockInputStreamWithAdditionalColumns>(block, std::move(stream));
    else
        return std::shared_ptr<PoolBlockInputStreamWithBackgroundThread>(stream.release());
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
    return !update_field.empty();
}

DictionarySourcePtr ExecutablePoolDictionarySource::clone() const
{
    return std::make_unique<ExecutablePoolDictionarySource>(*this);
}

std::string ExecutablePoolDictionarySource::toString() const
{
    return "ExecutablePool size: " + std::to_string(pool_size) + " command: " + command;
}

void registerDictionarySourceExecutablePool(DictionarySourceFactory & factory)
{
    auto create_table_source = [=](const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & config_prefix,
                                 Block & sample_block,
                                 const Context & context,
                                 const std::string & /* default_database */,
                                 bool check_config) -> DictionarySourcePtr
    {
        if (dict_struct.has_expressions)
            throw Exception{"Dictionary source of type `executable_pool` does not support attribute expressions", ErrorCodes::LOGICAL_ERROR};

        /// Executable dictionaries may execute arbitrary commands.
        /// It's OK for dictionaries created by administrator from xml-file, but
        /// maybe dangerous for dictionaries created from DDL-queries.
        if (check_config)
            throw Exception("Dictionaries with executable pool dictionary source is not allowed created from DDL are not allowed", ErrorCodes::DICTIONARY_ACCESS_DENIED);

        Context context_local_copy = copyContextAndApplySettings(config_prefix, context, config);

        auto settings_no_parallel_parsing = context_local_copy.getSettings();
        settings_no_parallel_parsing.input_format_parallel_parsing = false;

        context_local_copy.setSettings(settings_no_parallel_parsing);

        return std::make_unique<ExecutablePoolDictionarySource>(
            dict_struct, config, config_prefix + ".executable_pool",
            sample_block, context_local_copy);
    };

    factory.registerSource("executable_pool", create_table_source);
}

}
