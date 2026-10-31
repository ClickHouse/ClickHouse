#include <Processors/Transforms/BalancingTransform.h>


namespace DB
{

BalancingTransform::BalancingTransform(
    SharedHeader header_,
    InsertMemoryThrottlePtr throttle_)
    : ISimpleTransform(header_, header_, false)
    , throttle(std::move(throttle_))
{
}

IProcessor::Status BalancingTransform::prepare()
{
    auto & input_port = getInputPort();
    auto & output_port = getOutputPort();

    if (output_port.isFinished())
    {
        input_port.close();
        return Status::Finished;
    }

    /// if we can't push to output, don't pull from input
    if (!output_port.canPush())
    {
        input_port.setNotNeeded();
        return Status::PortFull;
    }

    if (input_port.isFinished())
    {
        output_port.finish();
        return Status::Finished;
    }

    /// check throttling BEFORE pulling data
    /// this creates backpressure by making this stream appear busy
    if (throttle && throttle->isEnabled() && throttle->isThrottled())
    {
        input_port.setNotNeeded();
        return Status::PortFull;
    }

    /// request data from input
    input_port.setNeeded();
    if (!input_port.hasData())
        return Status::NeedData;

    /// Pass data through unchanged
    Chunk chunk = input_port.pull();
    output_port.push(std::move(chunk));
    return Status::Ready;
}

void BalancingTransform::transform(Chunk & /*chunk*/)
{
    /// all work is done in prepare()
}

}
