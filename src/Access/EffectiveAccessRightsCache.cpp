#include <Access/EffectiveAccessRightsCache.h>


namespace DB
{

bool EffectiveAccessRightsCache::isExpired(const Entry & entry)
{
    return entry.access.expired() || entry.access_with_implicit.expired();
}


std::optional<EffectiveAccessRightsCache::Result> EffectiveAccessRightsCache::find(
    const UUID & user_id,
    const UserPtr & user,
    const std::shared_ptr<const EnabledRolesInfo> & roles_info,
    const ImplicitExpansionSettings & settings)
{
    std::lock_guard lock{mutex};
    auto it = entries.find(user_id);
    if (it == entries.end())
        return std::nullopt;

    for (const Entry & entry : it->second)
    {
        auto stored_user = entry.user.lock();
        auto stored_roles_info = entry.roles_info.lock();
        auto stored_access = entry.access.lock();
        auto stored_access_with_implicit = entry.access_with_implicit.lock();

        if ((stored_user == user)
            && (stored_roles_info == roles_info)
            && (entry.settings == settings)
            && stored_access
            && stored_access_with_implicit)
        {
            return Result{std::move(stored_access), std::move(stored_access_with_implicit)};
        }
    }

    return std::nullopt;
}


void EffectiveAccessRightsCache::store(
    const UUID & user_id,
    const UserPtr & user,
    const std::shared_ptr<const EnabledRolesInfo> & roles_info,
    const ImplicitExpansionSettings & settings,
    const std::shared_ptr<const AccessRights> & access,
    const std::shared_ptr<const AccessRights> & access_with_implicit)
{
    std::lock_guard lock{mutex};

    if (++stores_since_sweep >= SWEEP_INTERVAL)
    {
        stores_since_sweep = 0;
        sweepExpiredEntries();
    }

    std::vector<Entry> & user_entries = entries[user_id];

    /// Drop dead entries of this user so its vector cannot grow unboundedly.
    std::erase_if(user_entries, isExpired);

    for (Entry & entry : user_entries)
    {
        auto stored_user = entry.user.lock();
        auto stored_roles_info = entry.roles_info.lock();

        /// Match by the snapshots only: if the settings have changed since this entry was
        /// stored, `find` missed it because of them, and the entry must be replaced, not duplicated.
        if ((stored_user == user) && (stored_roles_info == roles_info))
        {
            entry.settings = settings;
            entry.access = access;
            entry.access_with_implicit = access_with_implicit;
            return;
        }
    }

    Entry new_entry;
    new_entry.user = user;
    new_entry.roles_info = roles_info;
    new_entry.settings = settings;
    new_entry.access = access;
    new_entry.access_with_implicit = access_with_implicit;
    user_entries.push_back(std::move(new_entry));
}


void EffectiveAccessRightsCache::sweepExpiredEntries()
{
    for (auto it = entries.begin(); it != entries.end();)
    {
        auto & user_entries = it->second;
        std::erase_if(user_entries, isExpired);
        if (user_entries.empty())
            it = entries.erase(it);
        else
            ++it;
    }
}

}
