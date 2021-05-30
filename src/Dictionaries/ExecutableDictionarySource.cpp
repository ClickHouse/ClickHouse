#include "ExecutableDictionarySource.h"

#include <functional>
#include <ext/scope_guard.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/OwningBlockInputStream.h>
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
static const UInt64 max_block_size = 8192;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int DICTIONARY_ACCESS_DENIED;
    extern const int UNSUPPORTED_METHOD;
}

namespace
{
    /// Owns ShellCommand and calls wait for it.
    class ShellCommandOwningBlockInputStream : public OwningBlockInputStream<ShellCommand>
    {
    private:
        Poco::Logger * log;
    public:
        ShellCommandOwningBlockInputStream(Poco::Logger * log_, const BlockInputStreamPtr & impl, std::unique_ptr<ShellCommand> command_)
            : OwningBlockInputStream(std::move(impl), std::move(command_)), log(log_)
        {
        }

        void readSuffix() override
        {
            OwningBlockInputStream<ShellCommand>::readSuffix();

            std::string err;
            readStringUntilEOF(err, own->err);
            if (!err.empty())
                LOG_ERROR(log, "Having stderr: {}", err);

            own->wait();
        }
    };

}

ExecutableDictionarySource::ExecutableDictionarySource(
    const DictionaryStructure & dict_struct_,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    Block & sample_block_,
    ContextPtr context_)
    : log(&Poco::Logger::get("ExecutableDictionarySource"))
    , dict_struct{dict_struct_}
    , implicit_key{config.getBool(config_prefix + ".implicit_key", false)}
    , command{config.getString(config_prefix + ".command")}
    , update_field{config.getString(config_prefix + ".update_field", "")}
    , format{config.getString(config_prefix + ".format")}
    , sample_block{sample_block_}
    , context(context_)
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

ExecutableDictionarySource::ExecutableDictionarySource(const ExecutableDictionarySource & other)
    : log(&Poco::Logger::get("ExecutableDictionarySource"))
    , update_time{other.update_time}
    , dict_struct{other.dict_struct}
    , implicit_key{other.implicit_key}
    , command{other.command}
    , update_field{other.update_field}
    , format{other.format}
    , sample_block{other.sample_block}
    , context(Context::createCopy(other.context))
{
}

BlockInputStreamPtr ExecutableDictionarySource::loadAll()
{
    if (implicit_key)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "ExecutableDictionarySource with implicit_key does not support loadAll method");

    LOG_TRACE(log, "loadAll {}", toString());
    auto process = ShellCommand::execute(command);
    auto input_stream = context->getInputFormat(format, process->out, sample_block, max_block_size);
    return std::make_shared<ShellCommandOwningBlockInputStream>(log, input_stream, std::move(process));
}

BlockInputStreamPtr ExecutableDictionarySource::loadUpdatedAll()
{
    if (implicit_key)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "ExecutableDictionarySource with implicit_key does not support loadUpdatedAll method");

    time_t new_update_time = time(nullptr);
    SCOPE_EXIT(update_time = new_update_time);

    std::string command_with_update_field = command;
    if (update_time)
        command_with_update_field += " " + update_field + " " + DB::toString(LocalDateTime(update_time - 1));

    LOG_TRACE(log, "loadUpdatedAll {}", command_with_update_field);
    auto process = ShellCommand::execute(command_with_update_field);
    auto input_stream = context->getInputFormat(format, process->out, sample_block, max_block_size);
    return std::make_shared<ShellCommandOwningBlockInputStream>(log, input_stream, std::move(process));
}

namespace
{
    /** A stream, that runs child process and sends data to its stdin in background thread,
      *  and receives data from its stdout.
      */
    class BlockInputStreamWithBackgroundThread final : public IBlockInputStream
    {
    public:
        BlockInputStreamWithBackgroundThread(
            ContextPtr context,
            const std::string & format,
            const Block & sample_block,
            const std::string & command_str,
            Poco::Logger * log_,
            std::function<void(WriteBufferFromFile &)> && send_data_)
            : log(log_),
            command(ShellCommand::execute(command_str)),
            send_data(std::move(send_data_)),
            thread([this] { send_data(command->in); })
        {
            stream = context->getInputFormat(format, command->out, sample_block, max_block_size);
        }

        ~BlockInputStreamWithBackgroundThread() override
        {
            if (thread.joinable())
                thread.join();
        }

        Block getHeader() const override
        {
            return stream->getHeader();
        }

    private:
        Block readImpl() override
        {
            return stream->read();
        }

        void readPrefix() override
        {
            stream->readPrefix();
        }

        void readSuffix() override
        {
            stream->readSuffix();

            std::string err;
            readStringUntilEOF(err, command->err);
            if (!err.empty())
                LOG_ERROR(log, "Having stderr: {}", err);

            if (thread.joinable())
                thread.join();

            command->wait();
        }

        String getName() const override { return "WithBackgroundThread"; }

        Poco::Logger * log;
        BlockInputStreamPtr stream;
        std::unique_ptr<ShellCommand> command;
        std::function<void(WriteBufferFromFile &)> send_data;
        ThreadFromGlobalPool thread;
    };
}

BlockInputStreamPtr ExecutableDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    LOG_TRACE(log, "loadIds {} size = {}", toString(), ids.size());

    auto block = blockForIds(dict_struct, ids);
    return getStreamForBlock(block);
}

BlockInputStreamPtr ExecutableDictionarySource::loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows)
{
    LOG_TRACE(log, "loadKeys {} size = {}", toString(), requested_rows.size());

    auto block = blockForKeys(dict_struct, key_columns, requested_rows);
    return getStreamForBlock(block);
}

BlockInputStreamPtr ExecutableDictionarySource::getStreamForBlock(const Block & block)
{
    auto stream = std::make_unique<BlockInputStreamWithBackgroundThread>(
        context, format, sample_block, command, log,
        [block, this](WriteBufferFromFile & out) mutable
        {
            auto output_stream = context->getOutputStream(format, out, block.cloneEmpty());
            formatBlock(output_stream, block);
            out.close();
        });

    if (implicit_key)
        return std::make_shared<BlockInputStreamWithAdditionalColumns>(block, std::move(stream));
    else
        return std::shared_ptr<BlockInputStreamWithBackgroundThread>(stream.release());
}

bool ExecutableDictionarySource::isModified() const
{
    return true;
}

bool ExecutableDictionarySource::supportsSelectiveLoad() const
{
    return true;
}

bool ExecutableDictionarySource::hasUpdateField() const
{
    return !update_field.empty();
}

DictionarySourcePtr ExecutableDictionarySource::clone() const
{
    return std::make_unique<ExecutableDictionarySource>(*this);
}

std::string ExecutableDictionarySource::toString() const
{
    return "Executable: " + command;
}

void registerDictionarySourceExecutable(DictionarySourceFactory & factory)
{
    auto create_table_source = [=](const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & config_prefix,
                                 Block & sample_block,
                                 ContextPtr context,
                                 const std::string & /* default_database */,
                                 bool check_config) -> DictionarySourcePtr
    {
        if (dict_struct.has_expressions)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Dictionary source of type `executable` does not support attribute expressions");

        /// Executable dictionaries may execute arbitrary commands.
        /// It's OK for dictionaries created by administrator from xml-file, but
        /// maybe dangerous for dictionaries created from DDL-queries.
        if (check_config)
            throw Exception(ErrorCodes::DICTIONARY_ACCESS_DENIED, "Dictionaries with executable dictionary source are not allowed to be created from DDL query");

        auto context_local_copy = copyContextAndApplySettings(config_prefix, context, config);

        return std::make_unique<ExecutableDictionarySource>(
            dict_struct, config, config_prefix + ".executable",
            sample_block, context_local_copy);
    };
    factory.registerSource("executable", create_table_source);
}

}
