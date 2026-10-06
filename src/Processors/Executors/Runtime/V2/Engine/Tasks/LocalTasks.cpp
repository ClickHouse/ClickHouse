#include <Processors/Executors/Runtime/V2/Engine/Tasks/LocalTasks.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Runtime::V2
{

void LocalTasks::pushBack(Task task)
{
    tasks.push_back(task);
}

Task LocalTasks::popFront()
{
    if (tasks.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "LocalTasks is empty");

    Task task = tasks.front();
    tasks.pop_front();
    return task;
}

Task LocalTasks::popBack()
{
    if (tasks.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "LocalTasks is empty");

    Task task = tasks.back();
    tasks.pop_back();
    return task;
}

bool LocalTasks::empty() const
{
    return tasks.empty();
}

size_t LocalTasks::size() const
{
    return tasks.size();
}

}

}
