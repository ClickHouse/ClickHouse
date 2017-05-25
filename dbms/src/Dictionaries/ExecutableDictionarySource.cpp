#include <thread>
#include <future>

#include <Dictionaries/ExecutableDictionarySource.h>

#include <Common/ShellCommand.h>
#include <Interpreters/Context.h>
#include <DataStreams/OwningBlockInputStream.h>
#include <Dictionaries/DictionarySourceHelpers.h>

#include <DataStreams/IBlockOutputStream.h>
#include <DataTypes/DataTypesNumber.h>

#include <common/logger_useful.h>


namespace DB
{

static const size_t max_block_size = 8192;


ExecutableDictionarySource::ExecutableDictionarySource(const DictionaryStructure & dict_struct_,
    const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix,
    Block & sample_block, const Context & context)
    : log(&Logger::get("ExecutableDictionarySource")),
    dict_struct{dict_struct_},
    command{config.getString(config_prefix + ".command")},
    format{config.getString(config_prefix + ".format")},
    sample_block{sample_block},
    context(context)
{
}

ExecutableDictionarySource::ExecutableDictionarySource(const ExecutableDictionarySource & other)
    : log(&Logger::get("ExecutableDictionarySource")),
    dict_struct{other.dict_struct},
    command{other.command},
    format{other.format},
    sample_block{other.sample_block},
    context(other.context)
{
}

BlockInputStreamPtr ExecutableDictionarySource::loadAll()
{
    LOG_TRACE(log, "loadAll " + toString());
    auto process = ShellCommand::execute(command);
    auto input_stream = context.getInputFormat(format, process->out, sample_block, max_block_size);
    return std::make_shared<OwningBlockInputStream<ShellCommand>>(input_stream, std::move(process));
}


/** A stream, that also runs and waits for background thread
  * (that will feed data into pipe to be read from the other side of the pipe).
  */
class BlockInputStreamWithBackgroundThread : public IProfilingBlockInputStream
{
public:
    BlockInputStreamWithBackgroundThread(
        const BlockInputStreamPtr & stream_, std::unique_ptr<ShellCommand> && command_,
        std::packaged_task<void()> && task_)
        : stream{stream_}, command{std::move(command_)}, task(std::move(task_)),
        thread([this]{ task(); command->in.close(); })
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

private:
    Block readImpl() override { return stream->read(); }

    void readSuffix() override
    {
        thread.join();
        /// To rethrow an exception, if any.
        task.get_future().get();
    }

    String getName() const override { return "WithBackgroundThread"; }
    String getID() const override {  return "WithBackgroundThread(" + stream->getID() + ")"; }

    BlockInputStreamPtr stream;
    std::unique_ptr<ShellCommand> command;
    std::packaged_task<void()> task;
    std::thread thread;
};


BlockInputStreamPtr ExecutableDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    LOG_TRACE(log, "loadIds " << toString() << " size = " << ids.size());
    auto process = ShellCommand::execute(command);

    auto output_stream = context.getOutputFormat(format, process->in, sample_block);
    auto input_stream = context.getInputFormat(format, process->out, sample_block, max_block_size);

    return std::make_shared<BlockInputStreamWithBackgroundThread>(
        input_stream, std::move(process), std::packaged_task<void()>(
        [output_stream, &ids, this]() mutable
        {
            formatIDs(output_stream, ids);
        }));
}

BlockInputStreamPtr ExecutableDictionarySource::loadKeys(
    const Columns & key_columns, const std::vector<std::size_t> & requested_rows)
{
    LOG_TRACE(log, "loadKeys " << toString() << " size = " << requested_rows.size());
    auto process = ShellCommand::execute(command);

    auto output_stream = context.getOutputFormat(format, process->in, sample_block);
    auto input_stream = context.getInputFormat(format, process->out, sample_block, max_block_size);

    return std::make_shared<BlockInputStreamWithBackgroundThread>(
        input_stream, std::move(process), std::packaged_task<void()>(
        [output_stream, key_columns, &requested_rows, this]() mutable
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

DictionarySourcePtr ExecutableDictionarySource::clone() const
{
    return std::make_unique<ExecutableDictionarySource>(*this);
}

std::string ExecutableDictionarySource::toString() const
{
    return "Executable: " + command;
}

}
