#include <Access/QueryPrivilegesInfo.h>
#include <Access/Common/AccessRightsElement.h>


namespace DB
{

void QueryPrivilegesInfo::startRecordingAccess()
{
    std::lock_guard lock(mutex);
    if (!recorded_access)
        recorded_access = std::make_shared<AccessRightsElements>();
    recording_access.store(true, std::memory_order_relaxed);
}

void QueryPrivilegesInfo::recordAccess(const AccessRightsElement & element)
{
    std::lock_guard lock(mutex);
    if (recorded_access)
        recorded_access->push_back(element);
}

AccessRightsElements QueryPrivilegesInfo::getRecordedAccess() const
{
    std::lock_guard lock(mutex);
    if (!recorded_access)
        return {};
    return *recorded_access;
}

}
