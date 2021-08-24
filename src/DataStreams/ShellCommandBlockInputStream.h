#pragma once

#include <memory>

#include <Common/ShellCommand.h>
#include <common/logger_useful.h>
#include <DataStreams/OwningBlockInputStream.h>
#include <IO/ReadHelpers.h>

namespace DB
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
        UInt64 max_block_size,
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
