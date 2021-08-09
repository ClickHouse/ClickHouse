#pragma once
#include <DataStreams/IBlockInputStream.h>
#include <Processors/Formats/IInputFormat.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class IInputFormat;
using InputFormatPtr = std::shared_ptr<IInputFormat>;

class InputStreamFromInputFormat : public IBlockInputStream
{
public:
    explicit InputStreamFromInputFormat(InputFormatPtr input_format_)
        : input_format(std::move(input_format_))
        , port(input_format->getPort().getHeader(), input_format.get())
    {
        connect(input_format->getPort(), port);
        port.setNeeded();
    }

    String getName() const override { return input_format->getName(); }
    Block getHeader() const override { return input_format->getPort().getHeader(); }

    void cancel(bool kill) override
    {
        input_format->cancel();
        IBlockInputStream::cancel(kill);
    }

    const BlockMissingValues & getMissingValues() const override { return input_format->getMissingValues(); }

protected:

    Block readImpl() override
    {
        while (true)
        {
            auto status = input_format->prepare();

            switch (status)
            {
                case IProcessor::Status::Ready:
                    input_format->work();
                    break;

                case IProcessor::Status::Finished:
                    return {};

                case IProcessor::Status::PortFull:
                    return input_format->getPort().getHeader().cloneWithColumns(port.pull().detachColumns());

                case IProcessor::Status::NeedData:
                case IProcessor::Status::Async:
                case IProcessor::Status::ExpandPipeline:
                    throw Exception("Source processor returned status " + IProcessor::statusToName(status), ErrorCodes::LOGICAL_ERROR);
            }
        }
    }

private:
    InputFormatPtr input_format;
    InputPort port;
};

}
