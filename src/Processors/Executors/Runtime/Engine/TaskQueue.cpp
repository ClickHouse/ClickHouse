#include <Processors/Executors/Runtime/Engine/TaskQueue.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void TaskQueue::pushBack(Task task)
{
    tasks.push_back(task);
}

void TaskQueue::pushFront(Task task)
{
    tasks.push_front(task);
}

Task TaskQueue::popFront()
{
    if (tasks.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "TaskQueue is empty");

    Task task = tasks.front();
    tasks.pop_front();
    return task;
}

Task TaskQueue::popBack()
{
    if (tasks.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "TaskQueue is empty");

    Task task = tasks.back();
    tasks.pop_back();
    return task;
}

void TaskQueue::takeFront(TaskQueue & from, size_t count)
{
    if (&from == this)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "TaskQueue cannot take from itself");
    if (count > from.tasks.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "TaskQueue has {} tasks, {} requested", from.tasks.size(), count);

    tasks.insert(tasks.end(), from.tasks.begin(), from.tasks.begin() + count);
    from.tasks.erase(from.tasks.begin(), from.tasks.begin() + count);
}

void TaskQueue::takeAll(TaskQueue & from)
{
    takeFront(from, from.tasks.size());
}

bool TaskQueue::empty() const
{
    return tasks.empty();
}

size_t TaskQueue::size() const
{
    return tasks.size();
}

}
