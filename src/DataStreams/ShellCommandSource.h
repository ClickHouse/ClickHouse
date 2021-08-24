#pragma once

#include <memory>

#include <common/logger_useful.h>
#include <Common/ShellCommand.h>
#include <Common/ThreadPool.h>
#include <IO/ReadHelpers.h>
#include <Formats/FormatFactory.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Executors/PullingPipelineExecutor.h>


namespace DB
{

/// Owns ShellCommand and calls wait for it.
class ShellCommandOwningTransform final : public ISimpleTransform
{
public:
    ShellCommandOwningTransform(
        const Block & header,
        Poco::Logger * log_,
        std::unique_ptr<ShellCommand> command_)
        : ISimpleTransform(header, header, true)
        , command(std::move(command_))
        , log(log_)
    {
    }

    String getName() const override { return "ShellCommandOwningTransform"; }
    void transform(Chunk &) override {}

    Status prepare() override
    {
        auto status = ISimpleTransform::prepare();
        if (status == Status::Finished)
        {
            std::string err;
            readStringUntilEOF(err, command->err);
            if (!err.empty())
                LOG_ERROR(log, "Having stderr: {}", err);

            command->wait();
        }

        return status;
    }

private:
    std::unique_ptr<ShellCommand> command;
    Poco::Logger * log;
};

}
