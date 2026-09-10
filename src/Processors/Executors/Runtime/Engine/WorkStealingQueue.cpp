#include <Processors/Executors/Runtime/Engine/WorkStealingQueue.h>
#include <Common/Exception.h>

#include <algorithm>
#include <span>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void WorkStealingQueue::pushBack(Task task)
{
    tasks.push_back(task);
}

void WorkStealingQueue::pushFront(Task task)
{
    tasks.push_front(task);
}

Task WorkStealingQueue::popFront()
{
    if (tasks.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "WorkStealingQueue is empty");

    Task task = tasks.front();
    tasks.pop_front();
    return task;
}

Task WorkStealingQueue::popBack()
{
    if (tasks.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "WorkStealingQueue is empty");

    Task task = tasks.back();
    tasks.pop_back();
    return task;
}

size_t WorkStealingQueue::takeFirst(WorkStealingQueue & victim, size_t max_to_take)
{
    if (&victim == this)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "WorkStealingQueue cannot take from itself");

    size_t count = std::min((victim.tasks.size() + 1) / 2, max_to_take);
    std::span taken = std::span(victim.tasks).first(count);
    tasks.insert(tasks.end(), taken.begin(), taken.end());
    victim.tasks.erase(victim.tasks.begin(), victim.tasks.begin() + count);

    return count;
}

size_t WorkStealingQueue::takeAll(WorkStealingQueue & victim)
{
    if (&victim == this)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "WorkStealingQueue cannot take from itself");

    size_t count = victim.tasks.size();
    tasks.insert(tasks.end(), victim.tasks.begin(), victim.tasks.end());
    victim.tasks.clear();

    return count;
}

bool WorkStealingQueue::empty() const
{
    return tasks.empty();
}

size_t WorkStealingQueue::size() const
{
    return tasks.size();
}

}
