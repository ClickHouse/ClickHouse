#include "DNSCacheUpdater.h"

#include <Interpreters/Context.h>
#include <Common/DNSResolver.h>


namespace DB
{

DNSCacheUpdater::DNSCacheUpdater(ContextPtr context_, Int32 update_period_seconds_, UInt32 max_consecutive_failures_)
    : WithContext(context_)
    , update_period_seconds(update_period_seconds_)
    , max_consecutive_failures(max_consecutive_failures_)
    , pool(getContext()->getSchedulePool())
{
    task_handle = pool.createTask("DNSCacheUpdater", [this]{ run(); });
}

void DNSCacheUpdater::run()
{
    auto & resolver = DNSResolver::instance();

    /// Reload cluster config if IP of any host has been changed since last update.
    if (resolver.updateCache(max_consecutive_failures))
    {
        LOG_INFO(&Poco::Logger::get("DNSCacheUpdater"), "IPs of some hosts have been changed. Will reload cluster config.");
        try
        {
            getContext()->reloadClusterConfig();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    /** DNS resolution may take a while and by this reason, actual update period will be longer than update_period_seconds.
      * We intentionally allow this "drift" for two reasons:
      * - automatically throttle when DNS requests take longer time;
      * - add natural randomization on huge clusters - avoid sending all requests at the same moment of time from different servers.
      */
    task_handle->scheduleAfter(size_t(update_period_seconds) * 1000);
}

void DNSCacheUpdater::start()
{
    LOG_INFO(&Poco::Logger::get("DNSCacheUpdater"), "Update period {} seconds", update_period_seconds);
    task_handle->activateAndSchedule();
}

DNSCacheUpdater::~DNSCacheUpdater()
{
    task_handle->deactivate();
}

}
