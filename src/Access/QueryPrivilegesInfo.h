#pragma once

#include <base/defines.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>


namespace DB
{

struct AccessRightsElement;
class AccessRightsElements;

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

    /// The record of access checks is used by `SHOW GRANTS ... FOR QUERY`: when enabled, every access check
    /// except grant option checks is kept, whether it passed or not. The copy constructor does not copy it.
    void startRecordingAccess();
    [[nodiscard]] bool isRecordingAccess() const { return recording_access.load(std::memory_order_relaxed); }
    void recordAccess(const AccessRightsElement & element);
    [[nodiscard]] AccessRightsElements getRecordedAccess() const;

    std::unordered_set<std::string> used_privileges TSA_GUARDED_BY(mutex);
    std::unordered_set<std::string> missing_privileges TSA_GUARDED_BY(mutex);

    mutable std::mutex mutex;

private:
    std::atomic<bool> recording_access = false;
    std::shared_ptr<AccessRightsElements> recorded_access TSA_GUARDED_BY(mutex);
};

using QueryPrivilegesInfoPtr = std::shared_ptr<QueryPrivilegesInfo>;

}
