#include "DNSCacheUpdater.h"
#include <Common/DNSCache.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/BackgroundProcessingPool.h>
#include <Common/ProfileEvents.h>
#include <Poco/Net/NetException.h>
#include <common/logger_useful.h>


namespace ProfileEvents
{
    extern Event NetworkErrors;
}


namespace DB
{

using BackgroundProcessingPoolTaskInfo = BackgroundProcessingPool::TaskInfo;

namespace ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
    extern const int ALL_CONNECTION_TRIES_FAILED;
}


DNSCacheUpdater::DNSCacheUpdater(Context & context_)
    : context(context_), pool(context_.getBackgroundPool())
{
    task_handle = pool.addTask([this] () { return run(); });
}

bool DNSCacheUpdater::run()
{
    /// TODO: Ensusre that we get global counter (not thread local)
    auto num_current_network_exceptions = ProfileEvents::counters[ProfileEvents::NetworkErrors].load(std::memory_order_relaxed);

    if (num_current_network_exceptions >= last_num_network_erros + min_errors_to_update_cache
        && time(nullptr) > last_update_time + min_update_period_seconds)
    {
        try
        {
            LOG_INFO(&Poco::Logger::get("DNSCacheUpdater"), "Updating DNS cache");

            DNSCache::instance().drop();
            context.reloadClusterConfig();

            last_num_network_erros = num_current_network_exceptions;
            last_update_time = time(nullptr);

            return true;
        }
        catch (...)
        {
            /// Do not increment ProfileEvents::NetworkErrors twice
            if (isNetworkError())
                return false;

            throw;
        }
    }

    /// According to BackgroundProcessingPool logic, if task has done work, it could be executed again immediately.
    return false;
}

DNSCacheUpdater::~DNSCacheUpdater()
{
    if (task_handle)
        pool.removeTask(task_handle);
    task_handle.reset();
}


bool DNSCacheUpdater::incrementNetworkErrors()
{
    if (isNetworkError())
    {
        ProfileEvents::increment(ProfileEvents::NetworkErrors);
        return true;
    }

    return false;
}

bool DNSCacheUpdater::isNetworkError()
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
    catch (Poco::Net::DNSException & e)
    {
        return true;
    }
    catch (Poco::TimeoutException & e)
    {
        return true;
    }
    catch (...)
    {
        /// Do nothing
    }

    return false;
}


}

