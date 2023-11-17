#include <Processors/Transforms/ReadFromMergeTreeDependencyTransform.h>

#include <QueryPipeline/RemoteQueryExecutor.h>
#include "Processors/Port.h"

namespace DB
{

ReadFromMergeTreeDependencyTransform::ReadFromMergeTreeDependencyTransform(const Block & header, UUID uuid_)
    : IProcessor(InputPorts(1, header), OutputPorts(1, header))
    , uuid(uuid_)
    , data_port(&inputs.front())
{
}

void ReadFromMergeTreeDependencyTransform::connectToScheduler(OutputPort & output_port)
{
    inputs.emplace_back(Block{}, this);
    dependency_port = &inputs.back();
    connect(output_port, *dependency_port);
}

UUID ReadFromMergeTreeDependencyTransform::getParallelReplicasGroupUUID()
{
    return uuid;
}

IProcessor::Status ReadFromMergeTreeDependencyTransform::prepare()
{
    Status status = Status::Ready;

    while (status == Status::Ready)
    {
        status = !has_data ? prepareConsume()
                           : prepareGenerate();
    }

    return status;
}

IProcessor::Status ReadFromMergeTreeDependencyTransform::prepareConsume()
{
    auto & output_port = getOutputPort();

    /// Check all outputs are finished or ready to get data.
    if (output_port.isFinished())
    {
        data_port->close();
        dependency_port->close();
        return Status::Finished;
    }

    /// Try get chunk from input.
    if (data_port->isFinished())
    {
        if (dependency_port->hasData())
            dependency_port->pull(true);
        dependency_port->close();
        output_port.finish();
        return Status::Finished;
    }

    if (!dependency_port->isFinished())
    {
        dependency_port->setNeeded();
        if (!dependency_port->hasData())
            return Status::NeedData;
    }

    data_port->setNeeded();
    if (!data_port->hasData())
        return Status::NeedData;

    if (!dependency_port->isFinished())
        dependency_port->pull();

    chunk = data_port->pull();
    has_data = true;

    return Status::Ready;
}

IProcessor::Status ReadFromMergeTreeDependencyTransform::prepareGenerate()
{
    auto & output_port = getOutputPort();
    if (!output_port.isFinished() && output_port.canPush())
    {
        output_port.push(std::move(chunk));
        has_data = false;
        return Status::Ready;
    }

    if (output_port.isFinished())
    {
        data_port->close();
        dependency_port->close();
        return Status::Finished;
    }

    return Status::PortFull;
}

}
