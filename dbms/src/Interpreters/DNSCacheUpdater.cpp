#include "DNSCacheUpdater.h"
#include <Common/DNSResolver.h>
#include <Interpreters/Context.h>
#include <Core/BackgroundSchedulePool.h>
#include <Common/ProfileEvents.h>
#include <Poco/Net/NetException.h>
#include <common/logger_useful.h>


namespace ProfileEvents
{
    extern Event NetworkErrors;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
    extern const int ALL_CONNECTION_TRIES_FAILED;
}


/// Call it inside catch section
/// Returns true if it is a network error
static bool isNetworkError()
{
    try
    {
        throw;
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::TIMEOUT_EXCEEDED || e.code() == ErrorCodes::ALL_CONNECTION_TRIES_FAILED)
            return true;
    }
    catch (Poco::Net::DNSException &)
    {
        return true;
    }
    catch (Poco::TimeoutException &)
    {
        return true;
    }
    catch (...)
    {
        /// Do nothing
    }

    return false;
}


DNSCacheUpdater::DNSCacheUpdater(Context & context_)
    : context(context_), pool(context_.getSchedulePool())
{
    task_handle = pool.createTask("DNSCacheUpdater", [this]{ run(); });
}

void DNSCacheUpdater::run()
{
    auto num_current_network_exceptions = ProfileEvents::global_counters[ProfileEvents::NetworkErrors].load(std::memory_order_relaxed);
    if (num_current_network_exceptions >= last_num_network_erros + min_errors_to_update_cache)
    {
        try
        {
            LOG_INFO(&Poco::Logger::get("DNSCacheUpdater"), "Updating DNS cache");

            DNSResolver::instance().dropCache();
            context.reloadClusterConfig();

            last_num_network_erros = num_current_network_exceptions;
            task_handle->scheduleAfter(min_update_period_seconds * 1000);
            return;
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    task_handle->scheduleAfter(10 * 1000);
}

bool DNSCacheUpdater::incrementNetworkErrorEventsIfNeeded()
{
    if (isNetworkError())
    {
        ProfileEvents::increment(ProfileEvents::NetworkErrors);
        return true;
    }

    return false;
}

}

