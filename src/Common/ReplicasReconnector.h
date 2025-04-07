#pragma once

#include <Core/ServerSettings.h>
#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context.h>
#include <Common/logger_useful.h>
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

namespace ServerSetting
{
    extern const ServerSettingsUInt64 dictionary_background_reconnect_interval;
}

class ReplicasReconnector : private boost::noncopyable
{
public:
    using Reconnector = std::function<bool(UInt64)>;
    using ReconnectorsList = std::list<Reconnector>;

    ReplicasReconnector(const ReplicasReconnector &) = delete;

    ~ReplicasReconnector()
    {
        emergency_stop = true;
        task_handle->deactivate();
        instance_ptr = nullptr;
    }

    [[nodiscard]]
    static std::unique_ptr<ReplicasReconnector> init(ContextPtr context)
    {
        if (instance_ptr)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Replicas reconnector is already initialized.");

        std::unique_ptr<ReplicasReconnector> ret(new ReplicasReconnector(context));
        instance_ptr = ret.get();
        return ret;
    }

    static ReplicasReconnector & instance()
    {
        if (!instance_ptr)
            throw Exception(ErrorCodes::NOT_INITIALIZED, "Replicas reconnector is not initialized.");

        return *instance_ptr;
    }

    void add(const Reconnector & reconnector)
    {
        std::lock_guard lock(mutex);
        reconnectors.push_back(reconnector);
        task_handle->activateAndSchedule();
    }

private:
    inline static ReplicasReconnector * instance_ptr = nullptr;
    ReconnectorsList reconnectors;
    std::mutex mutex;
    std::atomic_bool emergency_stop{false};
    BackgroundSchedulePoolTaskHolder task_handle;
    LoggerPtr log = nullptr;

    explicit ReplicasReconnector(ContextPtr context)
        : task_handle(context->getSchedulePool().createTask("ReplicasReconnector", [this]{ run(); }))
        , log(getLogger("ReplicasReconnector"))
    {
    }

    void run()
    {
        auto interval_milliseconds = Context::getGlobalContextInstance()->getServerSettings()[ServerSetting::dictionary_background_reconnect_interval];
        std::unique_lock lock(mutex);

        for (auto it = reconnectors.cbegin(); !emergency_stop && it != reconnectors.end();)
        {
            bool res = true;
            lock.unlock();

            try
            {
                res = (*it)(interval_milliseconds);
            }
            catch (...)
            {
                LOG_WARNING(log, "Failed reconnection routine.");
            }

            lock.lock();

            if (res)
                ++it;
            else
                it = reconnectors.erase(it);
        }

        if (!reconnectors.empty())
            task_handle->scheduleAfter(Context::getGlobalContextInstance()->getServerSettings()[ServerSetting::dictionary_background_reconnect_interval]);
    }
};

}
