#include <Processors/Transforms/BuildProbeJoinTransforms.h>

#include <Processors/Merges/Algorithms/MergeTreeReadInfo.h>
#include <Processors/Port.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
    extern const Event JoinBuildTableRowCount;
    extern const Event JoinProbeTableRowCount;
    extern const Event JoinResultRowCount;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

JoinBuildSideTransform::JoinBuildSideTransform(SharedHeader input_header)
    : IProcessor({std::move(input_header)}, {Block()})
{
}

IProcessor::Status JoinBuildSideTransform::prepare()
{
    auto & output = outputs.front();
    auto & input = inputs.front();

    /// A finished output means every probe closed its barrier input: nobody will read the
    /// shared state, so the build is abandoned without calling finishBuild.
    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    if (has_input)
        return Status::Ready;

    if (stop_reading)
        input.close();

    if (!input.isFinished())
    {
        input.setNeeded();
        if (!input.hasData())
            return Status::NeedData;

        input_chunk = input.pull(true);
        has_input = true;
        return Status::Ready;
    }

    if (!build_finished)
        return Status::Ready;

    /// finishBuild has published the shared state; closing the port releases the probes.
    output.finish();
    return Status::Finished;
}

void JoinBuildSideTransform::work()
{
    if (has_input)
    {
        has_input = false;
        /// A virtual row is an in-order scheduling hint (a peek at a future row), not data.
        if (isVirtualRow(input_chunk))
        {
            input_chunk = {};
            return;
        }
        ProfileEvents::increment(ProfileEvents::JoinBuildTableRowCount, input_chunk.getNumRows());
        if (!consumeBuildChunk(std::move(input_chunk)))
            stop_reading = true;
        input_chunk = {};
        return;
    }

    finishBuild();
    build_finished = true;
}

JoinProbeSideTransform::JoinProbeSideTransform(SharedHeader input_header, SharedHeader output_header)
    : IProcessor({std::move(input_header), Block()}, {std::move(output_header)})
{
}

IProcessor::Status JoinProbeSideTransform::prepare()
{
    auto & output = outputs.front();
    auto & data_input = inputs.front();
    auto & barrier_input = inputs.back();

    if (output.isFinished())
    {
        data_input.close();
        barrier_input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        data_input.setNotNeeded();
        return Status::PortFull;
    }

    if (output_chunk)
    {
        output.push(std::move(*output_chunk));
        output_chunk.reset();
        return Status::PortFull;
    }

    if (!barrier_released)
    {
        if (!barrier_input.isFinished())
        {
            barrier_input.setNeeded();
            if (barrier_input.hasData())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "No data is expected on the barrier input of JoinProbeSideTransform");

            data_input.setNotNeeded();
            return Status::NeedData;
        }
        /// The barrier closed: work() calls onBarrierReleased exactly once.
        return Status::Ready;
    }

    if (has_input || producing)
        return Status::Ready;

    if (data_input.isFinished())
    {
        output.finish();
        return Status::Finished;
    }

    data_input.setNeeded();
    if (!data_input.hasData())
        return Status::NeedData;

    input_chunk = data_input.pull(true);
    has_input = input_chunk.hasRows() || isVirtualRow(input_chunk);
    return Status::Ready;
}

void JoinProbeSideTransform::work()
{
    if (!barrier_released)
    {
        onBarrierReleased();
        barrier_released = true;
        return;
    }

    if (has_input)
    {
        has_input = false;
        /// Forward the virtual-row marker with the output header (no data), so downstream
        /// in-order consumers still see it.
        if (isVirtualRow(input_chunk))
        {
            auto block = outputs.front().getHeader().cloneEmpty();
            output_chunk = Chunk(block.getColumns(), 0);
            output_chunk->setChunkInfos(input_chunk.getChunkInfos());
            input_chunk = {};
            return;
        }
        ProfileEvents::increment(ProfileEvents::JoinProbeTableRowCount, input_chunk.getNumRows());
        consumeProbeChunk(std::move(input_chunk));
        input_chunk = {};
        producing = true;
    }

    if (producing)
    {
        output_chunk = produceChunk();
        if (output_chunk)
            ProfileEvents::increment(ProfileEvents::JoinResultRowCount, output_chunk->getNumRows());
        else
            producing = false;
    }
}

}
