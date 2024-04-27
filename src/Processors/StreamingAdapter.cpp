#include <magic_enum.hpp>

#include <base/defines.h>
#include <base/types.h>

#include <Common/logger_useful.h>

#include <Processors/IProcessor.h>
#include <Processors/StreamingAdapter.h>

namespace DB
{

StreamingAdapter::StreamingAdapter(const Block & header_) : IProcessor(InputPorts(2, header_), OutputPorts(1, header_))
{
    input_storage_port = &inputs.front();
    input_subscription_port = &inputs.back();
    output_port = &outputs.front();
}

IProcessor::Status StreamingAdapter::prepare()
{
    LOG_DEBUG(log, "current state: {}", magic_enum::enum_name(state));

    if (isCancelled())
        return Status::Finished;

    if (state == StreamingState::Finished)
        return Status::Finished;

    if (state == StreamingState::ReadingFromStorage)
    {
        LOG_DEBUG(log, "reading from storage source");

        Status status = preparePair(input_storage_port, output_port);

        if (status == Status::Finished)
        {
            if (state == StreamingState::Finished)
                return Status::Finished;

            state = StreamingState::ReadingFromSubscription;
        }
        else
        {
            return status;
        }
    }

    chassert(state == StreamingState::ReadingFromSubscription);

    LOG_DEBUG(log, "reading from subscription source");

    return preparePair(input_subscription_port, output_port);
}

IProcessor::Status StreamingAdapter::preparePair(InputPort * input, OutputPort * output)
{
    chassert(state != StreamingState::Finished);

    /// check finished

    if (output->isFinished())
    {
        input_storage_port->close();
        input_subscription_port->close();

        state = StreamingState::Finished;

        return Status::Finished;
    }

    if (input->isFinished())
        return Status::Finished;

    /// Check can output.

    if (!output->canPush())
    {
        input->setNotNeeded();
        return Status::PortFull;
    }

    /// Check can input.

    if (!input->hasData())
    {
        input->setNeeded();
        return Status::NeedData;
    }

    /// Move data.

    Chunk chunk = input->pull(true);
    output->push(std::move(chunk));

    /// Now, we pulled from input. It must be empty.

    return Status::NeedData;
}

}
