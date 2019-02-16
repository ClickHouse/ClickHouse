#include "ExecutableDictionarySource.h"

#include <future>
#include <thread>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/OwningBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Common/ShellCommand.h>
#include <Common/ThreadPool.h>
#include <common/logger_useful.h>
#include "DictionarySourceFactory.h"
#include "DictionarySourceHelpers.h"
#include "DictionaryStructure.h"


namespace DB
{
static const UInt64 max_block_size = 8192;


namespace
{
    /// Owns ShellCommand and calls wait for it.
    class ShellCommandOwningBlockInputStream : public OwningBlockInputStream<ShellCommand>
    {
    public:
        ShellCommandOwningBlockInputStream(const BlockInputStreamPtr & impl, std::unique_ptr<ShellCommand> own_)
            : OwningBlockInputStream(std::move(impl), std::move(own_))
        {
        }

        void readSuffix() override
        {
            OwningBlockInputStream<ShellCommand>::readSuffix();
            own->wait();
        }
    };

}


ExecutableDictionarySource::ExecutableDictionarySource(
    const DictionaryStructure & dict_struct_,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    Block & sample_block,
    const Context & context)
    : log(&Logger::get("ExecutableDictionarySource"))
    , update_time{std::chrono::system_clock::from_time_t(0)}
    , dict_struct{dict_struct_}
    , command{config.getString(config_prefix + ".command")}
    , update_field{config.getString(config_prefix + ".update_field", "")}
    , format{config.getString(config_prefix + ".format")}
    , sample_block{sample_block}
    , context(context)
{
}

ExecutableDictionarySource::ExecutableDictionarySource(const ExecutableDictionarySource & other)
    : log(&Logger::get("ExecutableDictionarySource"))
    , update_time{other.update_time}
    , dict_struct{other.dict_struct}
    , command{other.command}
    , update_field{other.update_field}
    , format{other.format}
    , sample_block{other.sample_block}
    , context(other.context)
{
}

std::string ExecutableDictionarySource::getUpdateFieldAndDate()
{
    if (update_time != std::chrono::system_clock::from_time_t(0))
    {
        auto tmp_time = update_time;
        update_time = std::chrono::system_clock::now();
        time_t hr_time = std::chrono::system_clock::to_time_t(tmp_time) - 1;
        char buffer[80];
        struct tm * timeinfo;
        timeinfo = localtime(&hr_time);
        strftime(buffer, 80, "\"%Y-%m-%d %H:%M:%S\"", timeinfo);
        std::string str_time(buffer);
        return command + " " + update_field + " " + str_time;
        ///Example case: command -T "2018-02-12 12:44:04"
        ///should return all entries after mentioned date
        ///if executable is eligible to return entries according to date.
        ///Where "-T" is passed as update_field.
    }
    else
    {
        std::string str_time("\"0000-00-00 00:00:00\""); ///for initial load
        return command + " " + update_field + " " + str_time;
    }
}

BlockInputStreamPtr ExecutableDictionarySource::loadAll()
{
    LOG_TRACE(log, "loadAll " + toString());
    auto process = ShellCommand::execute(command);
    auto input_stream = context.getInputFormat(format, process->out, sample_block, max_block_size);
    return std::make_shared<ShellCommandOwningBlockInputStream>(input_stream, std::move(process));
}

BlockInputStreamPtr ExecutableDictionarySource::loadUpdatedAll()
{
    std::string command_update = getUpdateFieldAndDate();
    LOG_TRACE(log, "loadUpdatedAll " + command_update);
    auto process = ShellCommand::execute(command_update);
    auto input_stream = context.getInputFormat(format, process->out, sample_block, max_block_size);
    return std::make_shared<ShellCommandOwningBlockInputStream>(input_stream, std::move(process));
}

namespace
{
    /** A stream, that also runs and waits for background thread
  * (that will feed data into pipe to be read from the other side of the pipe).
  */
    class BlockInputStreamWithBackgroundThread final : public IBlockInputStream
    {
    public:
        BlockInputStreamWithBackgroundThread(
            const BlockInputStreamPtr & stream_, std::unique_ptr<ShellCommand> && command_, std::packaged_task<void()> && task_)
            : stream{stream_}, command{std::move(command_)}, task(std::move(task_)), thread([this] {
                task();
                command->in.close();
            })
        {
            children.push_back(stream);
        }

        ~BlockInputStreamWithBackgroundThread() override
        {
            if (thread.joinable())
            {
                try
                {
                    readSuffix();
                }
                catch (...)
                {
                    tryLogCurrentException(__PRETTY_FUNCTION__);
                }
            }
        }

        Block getHeader() const override { return stream->getHeader(); }

    private:
        Block readImpl() override { return stream->read(); }

        void readSuffix() override
        {
            IBlockInputStream::readSuffix();
            if (!wait_called)
            {
                wait_called = true;
                command->wait();
            }
            thread.join();
            /// To rethrow an exception, if any.
            task.get_future().get();
        }

        String getName() const override { return "WithBackgroundThread"; }

        BlockInputStreamPtr stream;
        std::unique_ptr<ShellCommand> command;
        std::packaged_task<void()> task;
        ThreadFromGlobalPool thread;
        bool wait_called = false;
    };

}


BlockInputStreamPtr ExecutableDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    LOG_TRACE(log, "loadIds " << toString() << " size = " << ids.size());
    auto process = ShellCommand::execute(command);

    auto output_stream = context.getOutputFormat(format, process->in, sample_block);
    auto input_stream = context.getInputFormat(format, process->out, sample_block, max_block_size);

    return std::make_shared<BlockInputStreamWithBackgroundThread>(
        input_stream, std::move(process), std::packaged_task<void()>([output_stream, &ids]() mutable { formatIDs(output_stream, ids); }));
}

BlockInputStreamPtr ExecutableDictionarySource::loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows)
{
    LOG_TRACE(log, "loadKeys " << toString() << " size = " << requested_rows.size());
    auto process = ShellCommand::execute(command);

    auto output_stream = context.getOutputFormat(format, process->in, sample_block);
    auto input_stream = context.getInputFormat(format, process->out, sample_block, max_block_size);

    return std::make_shared<BlockInputStreamWithBackgroundThread>(
        input_stream, std::move(process), std::packaged_task<void()>([output_stream, key_columns, &requested_rows, this]() mutable
        {
            formatKeys(dict_struct, output_stream, key_columns, requested_rows);
        }));
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
    if (update_field.empty())
        return false;
    else
        return true;
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
    auto createTableSource = [=](const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & config_prefix,
                                 Block & sample_block,
                                 Context & context) -> DictionarySourcePtr
    {
        if (dict_struct.has_expressions)
            throw Exception{"Dictionary source of type `executable` does not support attribute expressions", ErrorCodes::LOGICAL_ERROR};

        return std::make_unique<ExecutableDictionarySource>(dict_struct, config, config_prefix + ".executable", sample_block, context);
    };
    factory.registerSource("executable", createTableSource);
}

}
