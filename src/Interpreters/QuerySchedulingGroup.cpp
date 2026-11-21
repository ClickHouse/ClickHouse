#include <Interpreters/QuerySchedulingGroup.h>

#include <Interpreters/QuerySlot.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Core/UUID.h>

#include <algorithm>

namespace ProfileEvents
{
    extern const Event ConcurrentQuerySlotsJoined;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int QUERY_SLOT_ACQUISITION_TIMEOUT;
}

QuerySchedulingGroup::QuerySchedulingGroup(String workload_, const ClassifierSettings & settings_, ClassifierPtr classifier_)
    : id(UUIDHelpers::generateV4())
    , workload(std::move(workload_))
    , settings(settings_)
    , classifier(std::move(classifier_))
{
}

std::shared_ptr<QuerySlot> QuerySchedulingGroup::acquireQuerySlot(ResourceLink link, std::chrono::steady_clock::time_point admission_deadline)
{
    // Members are admitted one at a time: a member that comes while another one waits for admission
    // waits too and then joins the slot it got, so a group never holds two slots.
    std::unique_lock lock{admission_mutex, std::defer_lock};
    if (admission_deadline == std::chrono::steady_clock::time_point::max())
        lock.lock();
    else if (!lock.try_lock_until(admission_deadline))
        throw Exception(ErrorCodes::QUERY_SLOT_ACQUISITION_TIMEOUT,
            "Timed out waiting to acquire a query slot for workload scheduling (exceeded workload_admission_timeout_ms)");

    if (auto slot = query_slot.lock())
    {
        ProfileEvents::increment(ProfileEvents::ConcurrentQuerySlotsJoined);
        return slot;
    }

    auto slot = std::make_shared<QuerySlot>(link, admission_deadline);
    query_slot = slot;
    return slot;
}

DistributedQuerySchedulingGroups & DistributedQuerySchedulingGroups::instance()
{
    static DistributedQuerySchedulingGroups instance;
    return instance;
}

void DistributedQuerySchedulingGroups::add(const String & group_id, const QuerySchedulingGroupPtr & group)
{
    std::lock_guard lock{mutex};
    addLocked(group_id, group);
}

QuerySchedulingGroupPtr DistributedQuerySchedulingGroups::join(
    const String & group_id, const std::function<QuerySchedulingGroupPtr(const QuerySchedulingGroupPtr &)> & make_group)
{
    std::lock_guard lock{mutex};
    QuerySchedulingGroupPtr registered;
    if (auto it = groups.find(group_id); it != groups.end())
        registered = it->second.lock();
    auto group = make_group(registered);
    if (!registered)
        addLocked(group_id, group);
    return group;
}

void DistributedQuerySchedulingGroups::addLocked(const String & group_id, const QuerySchedulingGroupPtr & group)
{
    // Drop the entries of finished queries; there are as many entries as distributed queries running here.
    std::erase_if(groups, [](const auto & entry) { return entry.second.expired(); });
    groups[group_id] = group;
}

}
