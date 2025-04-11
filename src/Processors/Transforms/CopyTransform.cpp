#include <Processors/Transforms/CopyTransform.h>

#include <Processors/Port.h>

namespace DB
{

CopyTransform::CopyTransform(const Block & header, size_t num_outputs)
    : IProcessor(InputPorts(1, header), OutputPorts(num_outputs, header))
    , was_output_processed(num_outputs, 0)
{
}

IProcessor::Status CopyTransform::prepare()
{
    Status status = Status::Ready;
    while (status == Status::Ready)
    {
        status = bool(data) ? prepareGenerate() : prepareConsume();
    }
    return status;
}

IProcessor::Status CopyTransform::prepareConsume()
{
    auto & input = getInputPort();

    /// Check all outputs are finished or ready to get data.

    bool all_finished = true;
    for (auto & output : outputs)
    {
        if (output.isFinished())
            continue;

        all_finished = false;
    }

    if (all_finished)
    {
        input.close();
        return Status::Finished;
    }

    /// Try get chunk from input.

    if (input.isFinished())
    {
        for (auto & output : outputs)
            output.finish();

        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    data = input.pullData();

    for (auto & was_processed : was_output_processed)
        was_processed = false;

    return Status::Ready;
}

IProcessor::Status CopyTransform::prepareGenerate()
{
    bool all_outputs_processed = true;

    size_t output_number = 0;
    for (auto & output : outputs)
    {
        auto & was_processed = was_output_processed[output_number];
        ++output_number;

        if (was_processed)
            continue;

        if (output.isFinished())
            continue;

        if (!output.canPush())
        {
            all_outputs_processed = false;
            continue;
        }

        if (data.exception)
            output.pushException(data.exception);
        else
            output.push(data.chunk.clone());

        was_processed = true;
    }

    if (all_outputs_processed)
    {
        data = {};
        return Status::Ready;
    }

    return Status::PortFull;
}

}
