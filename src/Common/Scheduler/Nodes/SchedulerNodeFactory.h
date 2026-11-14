#pragma once

#include <Common/ErrorCodes.h>
#include <Common/Exception.h>

#include <Common/Scheduler/ISchedulerNode.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <boost/noncopyable.hpp>

#include <memory>
#include <mutex>
#include <unordered_map>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_SCHEDULER_NODE;
}

class SchedulerNodeFactory : private boost::noncopyable
{
public:
    static SchedulerNodeFactory & instance()
    {
        static SchedulerNodeFactory ret;
        return ret;
    }

    SchedulerNodePtr get(const String & name, EventQueue * event_queue, const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
    {
        std::lock_guard lock{mutex};
        if (auto iter = methods.find(name); iter != methods.end())
            return iter->second(event_queue, config, config_prefix);
        throw Exception(ErrorCodes::INVALID_SCHEDULER_NODE, "Unknown scheduler node type: {}", name);
    }

    template <class TDerived>
    void registerMethod(const String & name)
    {
        std::lock_guard lock{mutex};
        methods[name] = [] (EventQueue * event_queue, const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
        {
            return std::make_shared<TDerived>(event_queue, config, config_prefix);
        };
    }

private:
    std::mutex mutex;
    using Method = std::function<SchedulerNodePtr(EventQueue * event_queue, const Poco::Util::AbstractConfiguration & config, const String & config_prefix)>;
    std::unordered_map<String, Method> methods;
};

}
