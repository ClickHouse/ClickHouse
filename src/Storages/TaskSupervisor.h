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

struct ReadTaskResolver
{
    ReadTaskResolver(String name_, std::function<String()> callback_)
        : name(name_), callback(callback_) {}
    String name;
    std::function<String()> callback;
};

using ReadTaskResolverPtr = std::unique_ptr<ReadTaskResolver>;

class TaskSupervisor
{
public:
    using QueryId = std::string;

    TaskSupervisor() = default;

    void registerNextTaskResolver(ReadTaskResolverPtr resolver)
    {
        std::lock_guard lock(mutex);
        auto & target = dict[resolver->name];
        if (target)
            throw Exception(fmt::format("NextTaskResolver with name {} is already registered for query {}",
                target->name, resolver->name), ErrorCodes::LOGICAL_ERROR);
        target = std::move(resolver);
    }


    Task getNextTaskForId(const QueryId & id)
    {
        std::lock_guard lock(mutex);
        auto it = dict.find(id);
        if (it == dict.end())
            return "";
        auto answer = it->second->callback();
        if (answer.empty())
            dict.erase(it); 
        return answer;
    }

private:
    using ResolverDict = std::unordered_map<QueryId, ReadTaskResolverPtr>;
    ResolverDict dict;
    std::mutex mutex;
};


}
