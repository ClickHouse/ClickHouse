#include <Core/BackgroundSchedulePool.h>
#include <Core/ServerSettings.h>
#include <Common/ReplicasReconnector.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>


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

ReplicasReconnector::ReplicasReconnector(ContextPtr context)
    : task_handle(context->getSchedulePool().createTask("ReplicasReconnector", [this]{ run(); }))
    , log(getLogger("ReplicasReconnector"))
{
}

ReplicasReconnector::~ReplicasReconnector()
{
    emergency_stop = true;
    task_handle->deactivate();
    instance_ptr = nullptr;
}

std::unique_ptr<ReplicasReconnector> ReplicasReconnector::init(ContextPtr context)
{
    if (instance_ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Replicas reconnector is already initialized.");

    std::unique_ptr<ReplicasReconnector> ret(new ReplicasReconnector(context));
    instance_ptr = ret.get();
    return ret;
}

ReplicasReconnector & ReplicasReconnector::instance()
{
    if (!instance_ptr)
        throw Exception(ErrorCodes::NOT_INITIALIZED, "Replicas reconnector is not initialized.");

    return *instance_ptr;
}

void ReplicasReconnector::add(const Reconnector & reconnector)
{
    std::lock_guard lock(mutex);
    reconnectors.push_back(reconnector);
    task_handle->activateAndSchedule();
}

void ReplicasReconnector::run()
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

}
