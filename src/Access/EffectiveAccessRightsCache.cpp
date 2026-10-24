#include <Access/EffectiveAccessRightsCache.h>
#include <algorithm>
#include <utility>


namespace DB
{

EffectiveAccessRightsCache::Handle::Handle(std::shared_ptr<Group> group_) : group(std::move(group_))
{
    group->contexts.fetch_add(1, std::memory_order_relaxed);
}


EffectiveAccessRightsCache::Handle & EffectiveAccessRightsCache::Handle::operator =(Handle && other) noexcept
{
    if (this != &other)
    {
        release();
        group = std::move(other.group);
    }
    return *this;
}


EffectiveAccessRightsCache::Handle::~Handle()
{
    release();
}


void EffectiveAccessRightsCache::Handle::release()
{
    if (group)
    {
        group->contexts.fetch_sub(1, std::memory_order_relaxed);
        group.reset();
    }
}


bool EffectiveAccessRightsCache::Handle::isShared() const
{
    /// This is only an optimization decision. Concurrent registration or removal may miss a reuse
    /// opportunity; snapshot validation under the group lock always determines the returned rights.
    return group && group->contexts.load(std::memory_order_relaxed) > 1;
}


std::optional<EffectiveAccessRightsCache::Result> EffectiveAccessRightsCache::Handle::find(
    const UserPtr & user,
    const std::shared_ptr<const EnabledRolesInfo> & roles_info,
    const ImplicitExpansionSettings & settings) const
{
    if (!isShared())
        return std::nullopt;

    std::lock_guard lock{group->mutex};
    if (group->settings != settings)
        return std::nullopt;

    auto stored_user = group->user.lock();
    auto stored_roles_info = group->roles_info.lock();
    auto stored_access = group->access.lock();
    auto stored_access_with_implicit = group->access_with_implicit.lock();

    if ((stored_user == user) && (stored_roles_info == roles_info) && stored_access && stored_access_with_implicit)
        return Result{std::move(stored_access), std::move(stored_access_with_implicit)};

    return std::nullopt;
}


void EffectiveAccessRightsCache::Handle::store(
    const UserPtr & user,
    const std::shared_ptr<const EnabledRolesInfo> & roles_info,
    const ImplicitExpansionSettings & settings,
    const std::shared_ptr<const AccessRights> & access,
    const std::shared_ptr<const AccessRights> & access_with_implicit) const
{
    if (!isShared())
        return;

    std::lock_guard lock{group->mutex};
    group->user = user;
    group->roles_info = roles_info;
    group->settings = settings;
    group->access = access;
    group->access_with_implicit = access_with_implicit;
}


EffectiveAccessRightsCache::Handle EffectiveAccessRightsCache::registerContext(const UUID & user_id)
{
    auto it = index.find(user_id);
    std::shared_ptr<Group> group;
    if (it != index.end())
        group = it->second->group.lock();

    if (!group)
    {
        group = std::make_shared<Group>();
        if (it == index.end())
        {
            /// Allocate before publishing the iterator. `splice` does not allocate or throw.
            Entries new_entry;
            new_entry.push_back(Entry{user_id, group});
            index.emplace(user_id, new_entry.begin());
            entries.splice(entries.end(), new_entry);
        }
        else
            it->second->group = group;
    }

    sweepExpiredEntries();
    return Handle{std::move(group)};
}


void EffectiveAccessRightsCache::sweepExpiredEntries()
{
    for (size_t remaining = std::min(SWEEP_BUDGET, entries.size()); remaining; --remaining)
    {
        if (next_entry_to_sweep == entries.end())
            next_entry_to_sweep = entries.begin();

        auto entry = next_entry_to_sweep++;
        if (entry->group.expired())
        {
            index.erase(entry->user_id);
            entries.erase(entry);
        }
    }
}

}
