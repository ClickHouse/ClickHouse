#include "DNSCacheUpdater.h"
#include <Common/DNSResolver.h>
#include <Interpreters/Context.h>
#include <Core/BackgroundSchedulePool.h>

namespace DB
{

DNSCacheUpdater::DNSCacheUpdater(Context & context_, Int32 update_period_seconds_)
    : context(context_),
    update_period_seconds(update_period_seconds_),
    pool(context_.getSchedulePool())
{
    task_handle = pool.createTask("DNSCacheUpdater", [this]{ run(); });
}

void DNSCacheUpdater::run()
{
    watch.restart();
    auto & resolver = DNSResolver::instance();

    /// Reload cluster config if IP of any host has been changed since last update.
    if (resolver.updateCache())
    {
        LOG_INFO(&Poco::Logger::get("DNSCacheUpdater"),
            "IPs of some hosts have been changed. Will reload cluster config.");
        try
        {
            context.reloadClusterConfig();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    auto interval_ms = std::max(0, update_period_seconds * 1000 - static_cast<Int32>(watch.elapsedMilliseconds()));
    task_handle->scheduleAfter(interval_ms);
}

void DNSCacheUpdater::start()
{
    task_handle->activateAndSchedule();
}

DNSCacheUpdater::~DNSCacheUpdater()
{
    task_handle->deactivate();
}

}
