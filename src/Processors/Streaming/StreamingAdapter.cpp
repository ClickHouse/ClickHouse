#include <memory>
#include <base/defines.h>
#include <base/types.h>

#include <Common/logger_useful.h>

#include <Processors/IProcessor.h>
#include <Processors/Streaming/StreamingAdapter.h>
#include <Poco/Logger.h>

namespace DB
{

StreamingAdapter::StreamingAdapter(const Block & header_, SequencerPtr sequencer_, ReadingSourceOptions reading_sources_)
    : IProcessor(InputPorts(2, header_), OutputPorts(1, header_))
    , sequencer{std::move(sequencer_)}
    , reading_sources{std::move(reading_sources_)}
{
    storage_ports = {&inputs.front()};
    subscription_ports = {&inputs.back()};
}

IProcessor::Status StreamingAdapter::prepare()
{
    if (isCancelled())
        return Status::Finished;

    if (isFinished())
    {
        closeAllPorts();
        return Status::Finished;
    }

    /// all output ports not finished

    if (reading_sources.isSet(ReadingSourceOption::Storage))
        readFromStorage();

    if (reading_sources.isSet(ReadingSourceOption::Subscription))
        readFromSubscription();

    return writeToOutputs();
}

void StreamingAdapter::work()
{
    sequencer->recalcState();
}

IProcessor::Status StreamingAdapter::pullData(InputPort * input)
{
    /// check finished

    if (input->isFinished())
        return Status::Finished;

    /// check input has data.

    if (!input->hasData())
    {
        input->setNeeded();
        return Status::NeedData;
    }

    /// add data to sequencer.

    Chunk chunk = input->pull(true);
    sequencer->addChunk(std::move(chunk));

    return Status::NeedData;
}

void StreamingAdapter::readFromStorage()
{
    chassert(reading_sources.isSet(ReadingSourceOption::Storage));

    size_t finished_count = 0;

    for (auto * input : storage_ports)
    {
        Status pull_status = pullData(input);

        if (pull_status == Status::Finished)
            finished_count += 1;
    }

    if (finished_count == storage_ports.size())
    {
        reading_sources.unSet(ReadingSourceOption::Storage);
        reading_sources.set(ReadingSourceOption::Subscription);
    }
}

void StreamingAdapter::readFromSubscription()
{
    chassert(reading_sources.isSet(ReadingSourceOption::Subscription));

    for (auto * input : subscription_ports)
    {
        Status pull_status = pullData(input);
        chassert(pull_status != Status::Finished);
    }
}

IProcessor::Status StreamingAdapter::writeToOutputs()
{
    size_t need_data_count = 0;
    size_t recalc_state_count = 0;
    size_t full_or_unneeded_count = 0;

    size_t index = 0;
    for (auto & output : outputs)
    {
        chassert(!output.isFinished());

        if (output.canPush())
        {
            auto result = sequencer->tryExtractNext(outputs.size(), index);

            if (auto * emit = std::get_if<Emit>(&result))
                output.push(std::move(emit->chunk));
            else if (std::holds_alternative<RecalcState>(result))
                recalc_state_count += 1;
            else
                need_data_count += 1;
        }
        else
            full_or_unneeded_count += 1;

        index += 1;
    }

    if (recalc_state_count > 0)
        return Status::Ready;

    if (full_or_unneeded_count == outputs.size())
        return Status::PortFull;

    if (need_data_count == 0)
        return Status::PortFull;

    return Status::NeedData;
}

bool StreamingAdapter::isFinished() const
{
    for (const auto & output : getOutputs())
        if (output.isFinished())
            return true;

    return false;
}

void StreamingAdapter::closeAllPorts()
{
    for (auto & input : getInputs())
        if (!input.isFinished())
            input.close();

    for (auto & output : getOutputs())
        if (!output.isFinished())
            output.finish();
}

}
