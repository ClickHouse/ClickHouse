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

}
