#include <Backups/BackupConcurrencyCheck.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CONCURRENT_ACCESS_NOT_SUPPORTED;
}


BackupConcurrencyCheck::BackupConcurrencyCheck(
    bool is_restore_,
    bool on_cluster_,
    const String & zookeeper_path_,
    bool allow_concurrency_,
    BackupConcurrencyCounters & counters_)
    : is_restore(is_restore_), on_cluster(on_cluster_), zookeeper_path(zookeeper_path_), counters(counters_)
{
    std::lock_guard lock{counters.mutex};

    if (!allow_concurrency_)
    {
        bool found_concurrent_operation = false;
        if (is_restore)
        {
            size_t num_local_restores = counters.local_restores;
            size_t num_on_cluster_restores = counters.on_cluster_restores.size();
            if (on_cluster)
            {
                if (!counters.on_cluster_restores.contains(zookeeper_path))
                    ++num_on_cluster_restores;
            }
            else
            {
                ++num_local_restores;
            }
            found_concurrent_operation = (num_local_restores + num_on_cluster_restores > 1);
        }
        else
        {
            size_t num_local_backups = counters.local_backups;
            size_t num_on_cluster_backups = counters.on_cluster_backups.size();
            if (on_cluster)
            {
                if (!counters.on_cluster_backups.contains(zookeeper_path))
                    ++num_on_cluster_backups;
            }
            else
            {
                ++num_local_backups;
            }
            found_concurrent_operation = (num_local_backups + num_on_cluster_backups > 1);
        }

        if (found_concurrent_operation)
            throwConcurrentOperationNotAllowed(is_restore);
    }

    if (on_cluster)
    {
        if (is_restore)
            ++counters.on_cluster_restores[zookeeper_path];
        else
            ++counters.on_cluster_backups[zookeeper_path];
    }
    else
    {
        if (is_restore)
            ++counters.local_restores;
        else
            ++counters.local_backups;
    }
}


BackupConcurrencyCheck::~BackupConcurrencyCheck()
{
    std::lock_guard lock{counters.mutex};

    if (on_cluster)
    {
        if (is_restore)
        {
            auto it = counters.on_cluster_restores.find(zookeeper_path);
            if (it != counters.on_cluster_restores.end())
            {
                if (!--it->second)
                    counters.on_cluster_restores.erase(it);
            }
        }
        else
        {
            auto it = counters.on_cluster_backups.find(zookeeper_path);
            if (it != counters.on_cluster_backups.end())
            {
                if (!--it->second)
                    counters.on_cluster_backups.erase(it);
            }
        }
    }
    else
    {
        if (is_restore)
            --counters.local_restores;
        else
            --counters.local_backups;
    }
}


void BackupConcurrencyCheck::throwConcurrentOperationNotAllowed(bool is_restore)
{
    throw Exception(
        ErrorCodes::CONCURRENT_ACCESS_NOT_SUPPORTED,
        "Concurrent {} are not allowed, turn on setting '{}'",
        is_restore ? "restores" : "backups",
        is_restore ? "allow_concurrent_restores" : "allow_concurrent_backups");
}


BackupConcurrencyCounters::BackupConcurrencyCounters() = default;


BackupConcurrencyCounters::~BackupConcurrencyCounters()
{
    if (local_backups > 0 || local_restores > 0 || !on_cluster_backups.empty() || !on_cluster_restores.empty())
        LOG_ERROR(getLogger(__PRETTY_FUNCTION__), "Some backups or restores are processing");
}

}
