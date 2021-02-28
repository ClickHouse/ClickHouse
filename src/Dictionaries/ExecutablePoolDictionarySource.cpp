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
    extern const int BAD_ARGUMENTS;
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
    , sample_block{sample_block_}
    , context(context_)
    , process_pool(std::make_shared<ProcessPool>(pool_size))
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

    if (pool_size == 0)
        throw Exception("ExecutablePoolDictionarySource cannot have pool of size 0", ErrorCodes::BAD_ARGUMENTS);

    for (size_t i = 0; i < pool_size; ++i)
        process_pool->emplace(ShellCommand::execute(command, false, true));
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
    , sample_block{other.sample_block}
    , context(other.context)
    , process_pool(std::make_shared<ProcessPool>(pool_size))
{
    for (size_t i = 0; i < pool_size; ++i)
        process_pool->emplace(ShellCommand::execute(command, false, true));
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
            std::shared_ptr<ProcessPool> processes_pool_,
            BlockInputStreamPtr && stream_,
            std::unique_ptr<ShellCommand> && command_,
            size_t read_rows_,
            std::function<void(WriteBufferFromFile &)> && send_data_)
            : processes_pool(processes_pool_)
            , stream(std::move(stream_))
            , command(std::move(command_))
            , rows_to_read(read_rows_)
            , send_data(std::move(send_data_))
            , thread([this] { send_data(command->in); })
        {}

        ~PoolBlockInputStreamWithBackgroundThread() override
        {
            if (thread.joinable())
                thread.join();

            if (command)
                processes_pool->emplace(std::move(command));
        }

        Block getHeader() const override
        {
            return stream->getHeader();
        }

    private:
        Block readImpl() override
        {
            if (current_read_rows == rows_to_read)
                return Block();

            auto block = stream->read();
            current_read_rows += block.rows();

            return block;
        }

        void readPrefix() override
        {
            stream->readPrefix();
        }

        void readSuffix() override
        {
            stream->readSuffix();

            if (thread.joinable())
                thread.join();

            processes_pool->emplace(std::move(command));
            command = nullptr;
        }

        String getName() const override { return "PoolWithBackgroundThread"; }

        std::shared_ptr<ProcessPool> processes_pool;
        BlockInputStreamPtr stream;
        std::unique_ptr<ShellCommand> command;
        size_t rows_to_read;
        std::function<void(WriteBufferFromFile &)> send_data;
        ThreadFromGlobalPool thread;
        size_t current_read_rows = 0;
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
    std::unique_ptr<ShellCommand> process;
    process_pool->pop(process);

    size_t rows_to_read = block.rows();
    auto read_stream = context.getInputFormat(format, process->out, sample_block, rows_to_read);

    auto stream = std::make_unique<PoolBlockInputStreamWithBackgroundThread>(
        process_pool, std::move(read_stream), std::move(process), rows_to_read,
        [block, this](WriteBufferFromFile & out) mutable
        {
            auto output_stream = context.getOutputStream(format, out, block.cloneEmpty());
            formatBlock(output_stream, block);
            std::cerr << "Write block to process " << std::endl;
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
