#pragma once

#include <Access/IAccessEntity.h>
#include <Access/Common/QuotaDefs.h>
#include <Access/RolesOrUsersSet.h>
#include <chrono>


namespace DB
{

/** Quota for resources consumption for specific interval.
  * Used to limit resource usage by user.
  * Quota is applied "softly" - could be slightly exceed, because it is checked usually only on each block of processed data.
  * Accumulated values are not persisted and are lost on server restart.
  * Quota is local to server,
  *  but for distributed queries, accumulated values for read rows and bytes
  *  are collected from all participating servers and accumulated locally.
  */
struct Quota : public IAccessEntity
{
    /// Amount of resources available to consume for each duration.
    struct Limits
    {
        std::optional<QuotaValue> max[static_cast<size_t>(QuotaType::MAX)];
        std::chrono::seconds duration = std::chrono::seconds::zero();

        /// Intervals can be randomized (to avoid DoS if intervals for many users end at one time).
        bool randomize_interval = false;

        friend bool operator ==(const Limits & lhs, const Limits & rhs);
        friend bool operator !=(const Limits & lhs, const Limits & rhs) { return !(lhs == rhs); }
    };

    std::vector<Limits> all_limits;

    /// Key to share quota consumption.
    /// Users with the same key share the same amount of resource.
    QuotaKeyType key_type = QuotaKeyType::NONE;

    /// Which roles or users should use this quota.
    RolesOrUsersSet to_roles;

    bool equal(const IAccessEntity & other) const override;
    std::shared_ptr<IAccessEntity> clone() const override { return cloneImpl<Quota>(); }
    static constexpr const auto TYPE = AccessEntityType::QUOTA;
    AccessEntityType getType() const override { return TYPE; }

    std::vector<UUID> findDependencies() const override;
    void doReplaceDependencies(const std::unordered_map<UUID, UUID> & old_to_new_ids) override;
    bool isBackupAllowed() const override { return true; }
};

using QuotaPtr = std::shared_ptr<const Quota>;
}
