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
    const Context & context_)
    : log(&Poco::Logger::get("ExecutableDictionarySource"))
    , dict_struct{dict_struct_}
    , command{config.getString(config_prefix + ".command")}
    , update_field{config.getString(config_prefix + ".update_field", "")}
    , format{config.getString(config_prefix + ".format")}
    , sample_block{sample_block_}
    , context(context_)
{
}

ExecutableDictionarySource::ExecutableDictionarySource(const ExecutableDictionarySource & other)
    : log(&Poco::Logger::get("ExecutableDictionarySource"))
    , update_time{other.update_time}
    , dict_struct{other.dict_struct}
    , command{other.command}
    , update_field{other.update_field}
    , format{other.format}
    , sample_block{other.sample_block}
    , context(other.context)
{
}

BlockInputStreamPtr ExecutableDictionarySource::loadAll()
{
    LOG_TRACE(log, "loadAll {}", toString());
    auto process = ShellCommand::execute(command);
    auto input_stream = context.getInputFormat(format, process->out, sample_block, max_block_size);
    return std::make_shared<ShellCommandOwningBlockInputStream>(log, input_stream, std::move(process));
}

BlockInputStreamPtr ExecutableDictionarySource::loadUpdatedAll()
{
    time_t new_update_time = time(nullptr);
    SCOPE_EXIT(update_time = new_update_time);

    std::string command_with_update_field = command;
    if (update_time)
        command_with_update_field += " " + update_field + " " + DB::toString(LocalDateTime(update_time - 1));

    LOG_TRACE(log, "loadUpdatedAll {}", command_with_update_field);
    auto process = ShellCommand::execute(command_with_update_field);
    auto input_stream = context.getInputFormat(format, process->out, sample_block, max_block_size);
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
            const Context & context,
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
            stream = context.getInputFormat(format, command->out, sample_block, max_block_size);
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

    return std::make_shared<BlockInputStreamWithBackgroundThread>(
        context, format, sample_block, command, log,
        [&ids, this](WriteBufferFromFile & out) mutable
        {
            auto output_stream = context.getOutputFormat(format, out, sample_block);
            formatIDs(output_stream, ids);
            out.close();
        });
}

BlockInputStreamPtr ExecutableDictionarySource::loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows)
{
    LOG_TRACE(log, "loadKeys {} size = {}", toString(), requested_rows.size());

    return std::make_shared<BlockInputStreamWithBackgroundThread>(
        context, format, sample_block, command, log,
        [key_columns, &requested_rows, this](WriteBufferFromFile & out) mutable
        {
            auto output_stream = context.getOutputFormat(format, out, sample_block);
            formatKeys(dict_struct, output_stream, key_columns, requested_rows);
            out.close();
        });
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
                                 const Context & context,
                                 const std::string & /* default_database */,
                                 bool check_config) -> DictionarySourcePtr
    {
        if (dict_struct.has_expressions)
            throw Exception{"Dictionary source of type `executable` does not support attribute expressions", ErrorCodes::LOGICAL_ERROR};

        /// Executable dictionaries may execute arbitrary commands.
        /// It's OK for dictionaries created by administrator from xml-file, but
        /// maybe dangerous for dictionaries created from DDL-queries.
        if (check_config)
            throw Exception("Dictionaries with Executable dictionary source is not allowed", ErrorCodes::DICTIONARY_ACCESS_DENIED);

        Context context_local_copy = copyContextAndApplySettings(config_prefix, context, config);

        return std::make_unique<ExecutableDictionarySource>(
            dict_struct, config, config_prefix + ".executable",
            sample_block, context_local_copy);
    };
    factory.registerSource("executable", create_table_source);
}

}
