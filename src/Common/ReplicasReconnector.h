#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context.h>
#include <boost/noncopyable.hpp>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <utility>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_INITIALIZED;
    extern const int LOGICAL_ERROR;
}

class ReplicasReconnector : private boost::noncopyable
{
public:
    using Reconnector = std::function<bool()>;
    using ReconnectorsList = std::list<Reconnector>;

    ReplicasReconnector(const ReplicasReconnector &) = delete;

    ~ReplicasReconnector()
    {
        emergency_stop = true;
        task_handle->deactivate();
        instance_ptr(nullptr, true);
    }

    [[nodiscard]]
    static std::unique_ptr<ReplicasReconnector> init(ContextPtr context)
    {
        std::unique_ptr<ReplicasReconnector> ret(new ReplicasReconnector(context));
        instance_ptr(ret.get());
        return ret;
    }

    static ReplicasReconnector & instance()
    {
        ReplicasReconnector * ptr = instance_ptr();
        if (!ptr)
            throw Exception(ErrorCodes::NOT_INITIALIZED, "Replicas reconnector is not initialized.");
        return *ptr;
    }

    void add(const Reconnector & reconnector)
    {
        std::lock_guard lock(mutex);
        reconnectors.push_back(reconnector);
        task_handle->activateAndSchedule();
    }

private:
    ReconnectorsList reconnectors;
    std::mutex mutex;
    std::atomic_bool emergency_stop{false};
    BackgroundSchedulePoolTaskHolder task_handle;

    static ReplicasReconnector * instance_ptr(ReplicasReconnector * ptr = nullptr, bool deinitialize = false)
    {
        static ReplicasReconnector * instance_ptr = nullptr;

        if (deinitialize)
        {
            instance_ptr = nullptr;
            return instance_ptr;
        }

        if (instance_ptr)
        {
            if (ptr)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Replicas reconnector is already initialized.");
        }
        else
            instance_ptr = ptr;

        return instance_ptr;
    }

    explicit ReplicasReconnector(ContextPtr context)
        : task_handle(context->getSchedulePool().createTask("ReplicasReconnector", [this]{ run(); }))
    {
    }

    void run()
    {
        std::unique_lock lock(mutex);

        for (auto it = reconnectors.cbegin(); !emergency_stop && it != reconnectors.end();)
        {
            lock.unlock();
            bool res = (*it)();
            lock.lock();

            if (res)
                ++it;
            else
                it = reconnectors.erase(it);
        }

        if (!reconnectors.empty())
            task_handle->scheduleAfter(5000);
    }
};

}
