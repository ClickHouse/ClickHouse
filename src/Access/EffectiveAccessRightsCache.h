#pragma once

#include <Access/ImplicitExpansionSettings.h>
#include <Core/UUID.h>
#include <base/defines.h>
#include <atomic>
#include <cstddef>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>


namespace DB
{

struct User;
using UserPtr = std::shared_ptr<const User>;
struct EnabledRolesInfo;
class AccessRights;

/// Registers compatible access contexts while holding the existing `EnabledRoles` subscription lock.
/// Each enabled-role set has its own registry, and each user has a separate result slot.
/// A context keeps its registration across permission updates. A sole context bypasses the slot;
/// multiple contexts can reuse results without taking a global cache lock.
class EffectiveAccessRightsCache
{
    struct Group;

public:
    explicit EffectiveAccessRightsCache(std::mutex & mutex_) : mutex(mutex_)
    {
    }
    EffectiveAccessRightsCache(const EffectiveAccessRightsCache &) = delete;
    EffectiveAccessRightsCache & operator =(const EffectiveAccessRightsCache &) = delete;
    EffectiveAccessRightsCache(EffectiveAccessRightsCache &&) = delete;
    EffectiveAccessRightsCache & operator =(EffectiveAccessRightsCache &&) = delete;

    struct Result
    {
        std::shared_ptr<const AccessRights> access;
        std::shared_ptr<const AccessRights> access_with_implicit;
    };

    /// One registration per unrestricted `ContextAccess`, independent of temporary result references.
    class Handle
    {
    public:
        Handle() = default;
        Handle(Handle && other) noexcept = default;
        Handle & operator =(Handle && other) noexcept;
        ~Handle();

        bool isShared() const;

        /// Requires the exact immutable snapshots and settings. Address reuse cannot produce a hit.
        std::optional<Result> find(const UserPtr & user,
            const std::shared_ptr<const EnabledRolesInfo> & roles_info,
            const ImplicitExpansionSettings & settings) const;

        void store(const UserPtr & user,
            const std::shared_ptr<const EnabledRolesInfo> & roles_info,
            const ImplicitExpansionSettings & settings,
            const std::shared_ptr<const AccessRights> & access,
            const std::shared_ptr<const AccessRights> & access_with_implicit) const;

    private:
        friend class EffectiveAccessRightsCache;
        explicit Handle(std::shared_ptr<Group> group_);
        void release();

        std::shared_ptr<Group> group;
    };

    /// The owner-provided mutex must outlive this registry. Acquire it through this accessor
    /// so TSA can match the held capability to the registration requirement.
    std::mutex & getMutex() const TSA_RETURN_CAPABILITY(mutex)
    {
        return mutex;
    }

    /// Requires the owner-provided mutex, which is the `EnabledRoles` subscription lock in production.
    /// Registration adds no lock acquisition; returned handles synchronize their group independently.
    Handle registerContext(const UUID & user_id) TSA_REQUIRES(mutex);

private:
    struct Group
    {
        std::atomic<size_t> contexts = 0;
        std::mutex mutex;
        std::weak_ptr<const User> user TSA_GUARDED_BY(mutex);
        std::weak_ptr<const EnabledRolesInfo> roles_info TSA_GUARDED_BY(mutex);
        ImplicitExpansionSettings settings TSA_GUARDED_BY(mutex);
        std::weak_ptr<const AccessRights> access TSA_GUARDED_BY(mutex);
        std::weak_ptr<const AccessRights> access_with_implicit TSA_GUARDED_BY(mutex);
    };

    struct Entry
    {
        UUID user_id;
        std::weak_ptr<Group> group;
    };

    using Entries = std::list<Entry>;

    void sweepExpiredEntries() TSA_REQUIRES(mutex);
    /// Each registration adds at most one entry. Scan two to give expired-entry cleanup
    /// extra progress while bounding each registration's work; live entries are only visited.
    static constexpr size_t SWEEP_BUDGET = 2;

    /// The registry is protected by its owner's subscription lock; handles only access their group.
    std::mutex & mutex;
    Entries entries TSA_GUARDED_BY(mutex);
    std::unordered_map<UUID, Entries::iterator> index TSA_GUARDED_BY(mutex);
    Entries::iterator next_entry_to_sweep TSA_GUARDED_BY(mutex) = entries.end();
};

}
