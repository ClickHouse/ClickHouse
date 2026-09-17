#pragma once

#include <Access/EnabledRowPolicies.h>
#include <base/scope_guard.h>
#include <mutex>
#include <map>
#include <unordered_map>


namespace DB
{
class AccessControl;
struct RolesOrUsersSet;
struct RowPolicy;
using RowPolicyPtr = std::shared_ptr<const RowPolicy>;

/// Stores read and parsed row policies.
class RowPolicyCache
{
public:
    explicit RowPolicyCache(const AccessControl & access_control_);
    ~RowPolicyCache();

    std::shared_ptr<const EnabledRowPolicies> getEnabledRowPolicies(const UUID & user_id, const boost::container::flat_set<UUID> & enabled_roles);

private:
    struct PolicyInfo
    {
        explicit PolicyInfo(const RowPolicyPtr & policy_) { setPolicy(policy_); }
        void setPolicy(const RowPolicyPtr & policy_);

        bool isForDatabase() const { return policy->isForDatabase(); }
        RowPolicyPtr policy;
        const RolesOrUsersSet * roles = nullptr;
        std::shared_ptr<const std::pair<String, String>> database_and_table_name;
        ASTPtr parsed_filters[static_cast<size_t>(RowPolicyFilterType::MAX)];
    };

    void ensureAllRowPoliciesRead() TSA_REQUIRES(mutex);
    void rowPolicyAddedOrChanged(const UUID & policy_id, const RowPolicyPtr & new_policy) TSA_REQUIRES(mutex);
    void rowPolicyRemoved(const UUID & policy_id) TSA_REQUIRES(mutex);
    void mixFiltersIfNeeded();
    /// Takes no lock and reads only `policies` (not `all_policies`), so it can rebuild on a snapshot
    /// off the `mutex`. `const` to keep it from mutating cache state off-lock.
    void mixFiltersFor(EnabledRowPolicies & enabled, const std::unordered_map<UUID, PolicyInfo> & policies, bool users_without_row_policies_can_read_rows) const;

    const AccessControl & access_control;
    std::unordered_map<UUID, PolicyInfo> all_policies TSA_GUARDED_BY(mutex);
    bool all_policies_read TSA_GUARDED_BY(mutex) = false;
    /// Set while applying a batch of changes; the rebuild is coalesced to once per notification batch.
    bool need_mix_filters TSA_GUARDED_BY(mutex) = false;
    scope_guard subscription;
    std::map<EnabledRowPolicies::Params, std::weak_ptr<EnabledRowPolicies>> enabled_row_policies TSA_GUARDED_BY(mutex);
    std::mutex mutex;
};

}
