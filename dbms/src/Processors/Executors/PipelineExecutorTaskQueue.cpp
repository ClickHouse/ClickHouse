#include <Processors/Executors/PipelineExecutor.h>

namespace DB
{

bool PipelineExecutor::TaskQueue::push(ExecutionState * value)
{
    return container.push(value);
}

bool PipelineExecutor::TaskQueue::pop(ExecutionState *& value)
{
    return container.pop(value);
}

void PipelineExecutor::TaskQueue::reserve(size_t size)
{
    container.reserve(size);
}

void PipelineExecutor::TaskQueue::reserve_unsafe(size_t size)
{
    container.reserve_unsafe(size);
}

}
