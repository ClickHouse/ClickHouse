#pragma once

#include <string>
#include <vector>
#include <mutex>
#include <unordered_map>

#include <Common/Exception.h>

namespace DB 
{


namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

using QueryId = std::string;
using Task = std::string;
using Tasks = std::vector<Task>;
using TasksIterator = Tasks::iterator;

class S3NextTaskResolver
{
public:
    S3NextTaskResolver(QueryId query_id, Tasks && all_tasks)
        : id(query_id)
        , tasks(all_tasks)
        , current(tasks.begin())
    {}

    std::string next()
    {
        auto it = current;
        ++current;
        return it == tasks.end() ? "" : *it;
    }

    std::string getId()
    {
        return id;
    }

private:
    QueryId id;
    Tasks tasks;
    TasksIterator current;
};

using S3NextTaskResolverPtr = std::shared_ptr<S3NextTaskResolver>;

class TaskSupervisor
{
public:
    using QueryId = std::string;

    TaskSupervisor() = default;

    void registerNextTaskResolver(S3NextTaskResolverPtr resolver)
    {
        std::lock_guard lock(mutex);
        auto & target = dict[resolver->getId()];
        if (target)
            throw Exception(fmt::format("NextTaskResolver with name {} is already registered for query {}",
                target->getId(), resolver->getId()), ErrorCodes::LOGICAL_ERROR);
        target = std::move(resolver);
    }


    Task getNextTaskForId(const QueryId & id)
    {
        std::lock_guard lock(mutex);
        auto it = dict.find(id);
        if (it == dict.end())
            return "";
        auto answer = it->second->next();
        if (answer.empty())
            dict.erase(it); 
        return answer;
    }

private:
    using ResolverDict = std::unordered_map<QueryId, S3NextTaskResolverPtr>;
    ResolverDict dict;
    std::mutex mutex;
};


}
