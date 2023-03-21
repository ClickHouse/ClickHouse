#include <mutex>
#include <Processors/Transforms/ReadFromMergeTreeDependencyTransform.h>

#include <QueryPipeline/RemoteQueryExecutor.h>
#include "Processors/Port.h"

namespace DB
{

void ParallelReplicasScheduler::addTask(void * data, int fd)
{
    std::unique_lock lock(mutex);

    if (!queue.hasTask(data))
        queue.addTask(0, data, fd);
}

bool ParallelReplicasScheduler::hasSomething()
{
    std::unique_lock lock(mutex);

    if (queue.size() == 0)
        return true;

    return static_cast<bool>(queue.wait(lock, false));
}

void ParallelReplicasScheduler::finish()
{
    std::lock_guard lock(mutex);

    if (finished)
        return;

    queue.finish();
    finished = true;
}

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

void ReadFromMergeTreeDependencyTransform::connectToPoller(std::shared_ptr<ParallelReplicasScheduler> poller_)
{
    poller = poller_;
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

    /// The logic is the following:
    /// If we have an empty chunk - use it.
    /// If we dont' have an empty chunk - check whether some remote socket has some data in it.
    /// If it has data - we need to read from remote. Otherwise - we can read from local replica unconditionally.

    if (!dependency_port->isFinished())
    {
        dependency_port->setNeeded();
        if (!dependency_port->hasData())
        {
            if (poller->hasSomething())
                return Status::NeedData;
        }
        else
        {
            dependency_port->pull();
        }
    }

    data_port->setNeeded();
    if (!data_port->hasData())
        return Status::NeedData;

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

void ReadFromMergeTreeDependencyTransform::onCancel()
{
    if (poller)
        poller->finish();
}

}
