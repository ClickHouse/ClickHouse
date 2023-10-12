#include <Processors/Transforms/PartialResultTransform.h>

namespace DB
{


PartialResultTransform::PartialResultTransform(const Block & header, UInt64 partial_result_limit_, UInt64 partial_result_duration_ms_)
    : PartialResultTransform(header, header, partial_result_limit_, partial_result_duration_ms_) {}

PartialResultTransform::PartialResultTransform(const Block & input_header, const Block & output_header, UInt64 partial_result_limit_, UInt64 partial_result_duration_ms_)
    : IProcessor({input_header}, {output_header})
    , input(inputs.front())
    , output(outputs.front())
    , partial_result_limit(partial_result_limit_)
    , partial_result_duration_ms(partial_result_duration_ms_)
    , watch(CLOCK_MONOTONIC)
    {}

IProcessor::Status PartialResultTransform::prepare()
{
    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (finished_getting_snapshots)
    {
        output.finish();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    /// If input data from previous partial result processor is finished then
    /// PartialResultTransform ready to create snapshots and send them as a partial result
    if (input.isFinished())
    {
        if (partial_result.snapshot_status == SnaphotStatus::Ready)
        {
            partial_result.snapshot_status = SnaphotStatus::NotReady;
            output.push(std::move(partial_result.chunk));
            return Status::PortFull;
        }

        return Status::Ready;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    partial_result.chunk = input.pull();
    transformPartialResult(partial_result.chunk);
    if (partial_result.chunk.getNumRows() > 0)
    {
        output.push(std::move(partial_result.chunk));
        return Status::PortFull;
    }

    return Status::NeedData;
}

void PartialResultTransform::work()
{
    if (partial_result_duration_ms < watch.elapsedMilliseconds())
    {
        partial_result = getRealProcessorSnapshot();
        if (partial_result.snapshot_status == SnaphotStatus::Stopped)
            finished_getting_snapshots = true;

        watch.restart();
    }
}

}
