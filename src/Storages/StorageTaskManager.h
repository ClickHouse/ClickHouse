#pragma once


#include <cmath>
#include <iterator>
#include <mutex>
#include <string>
#include <vector>
#include <memory>
#include <iostream>
#include <unordered_map>
#include <shared_mutex>


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


class NextTaskResolverBase
{
public:
    virtual ~NextTaskResolverBase() = default;
    virtual std::string next() = 0;
    virtual std::string getName() = 0;
    virtual std::string getId() = 0;
};

using NextTaskResolverBasePtr = std::unique_ptr<NextTaskResolverBase>;

class S3NextTaskResolver : public NextTaskResolverBase
{
public:
    S3NextTaskResolver(QueryId query_id, Tasks && all_tasks)
        : id(query_id)
        , tasks(all_tasks)
        , current(tasks.begin())
    {}

    ~S3NextTaskResolver() override = default;

    std::string next() override
    {
        auto it = current;
        ++current;
        return it == tasks.end() ? "" : *it;
    }
    
    std::string getName() override
    {
        return "S3NextTaskResolverBase";
    }

    std::string getId() override
    {
        return id;
    }

private:
    QueryId id;
    Tasks tasks;
    TasksIterator current;
};

class TaskSupervisor
{
public:
    using QueryId = std::string;

    TaskSupervisor() = default;

    static TaskSupervisor & instance()
    {
        static TaskSupervisor task_manager;
        return task_manager;
    }

    void registerNextTaskResolver(NextTaskResolverBasePtr resolver)
    {
        std::lock_guard lock(rwlock);
        auto & target = dict[resolver->getId()];
        if (target)
            throw Exception(fmt::format("NextTaskResolver with name {} is already registered for query {}",
                target->getId(), resolver->getId()), ErrorCodes::LOGICAL_ERROR);
        target = std::move(resolver);
    }


    Task getNextTaskForId(const QueryId & id)
    {
        std::lock_guard lock(rwlock);
        auto it = dict.find(id);
        if (it == dict.end())
            return "";
        auto answer = it->second->next();
        if (answer.empty())
            dict.erase(it); 
        return answer;
    }

private:
    using ResolverDict = std::unordered_map<QueryId, NextTaskResolverBasePtr>;
    ResolverDict dict;
    std::shared_mutex rwlock;
};


}
