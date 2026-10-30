#pragma once

#include <base/defines.h>

#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>


namespace DB
{

/// The privileges checked for a query, reported in `system.query_log` as `used_privileges` and `missing_privileges`.
struct QueryPrivilegesInfo
{
    QueryPrivilegesInfo() = default;

    QueryPrivilegesInfo(const QueryPrivilegesInfo & rhs)
    {
        std::lock_guard<std::mutex> lock(rhs.mutex);
        used_privileges = rhs.used_privileges;
        missing_privileges = rhs.missing_privileges;
    }

    QueryPrivilegesInfo(QueryPrivilegesInfo && rhs) = delete;

    void add(const std::string & privilege, bool granted)
    {
        std::lock_guard lock(mutex);
        if (granted)
            used_privileges.emplace(privilege);
        else
            missing_privileges.emplace(privilege);
    }

    std::unordered_set<std::string> used_privileges TSA_GUARDED_BY(mutex);
    std::unordered_set<std::string> missing_privileges TSA_GUARDED_BY(mutex);

    mutable std::mutex mutex;
};

using QueryPrivilegesInfoPtr = std::shared_ptr<QueryPrivilegesInfo>;

}
