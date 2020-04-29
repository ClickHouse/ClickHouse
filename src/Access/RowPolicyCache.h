#pragma once

#include <Access/EnabledRowPolicies.h>
#include <ext/scope_guard.h>
#include <mutex>
#include <map>
#include <unordered_map>


namespace DB
{
class AccessControlManager;

/// Stores read and parsed row policies.
class RowPolicyCache
{
public:
    RowPolicyCache(const AccessControlManager & access_control_manager_);
    ~RowPolicyCache();

    std::shared_ptr<const EnabledRowPolicies> getEnabledRowPolicies(const UUID & user_id, const boost::container::flat_set<UUID> & enabled_roles);

private:
    using ParsedConditions = EnabledRowPolicies::ParsedConditions;

    struct PolicyInfo
    {
        PolicyInfo(const RowPolicyPtr & policy_) { setPolicy(policy_); }
        void setPolicy(const RowPolicyPtr & policy_);

        RowPolicyPtr policy;
        const ExtendedRoleSet * roles = nullptr;
        ParsedConditions parsed_conditions;
    };

    void ensureAllRowPoliciesRead();
    void rowPolicyAddedOrChanged(const UUID & policy_id, const RowPolicyPtr & new_policy);
    void rowPolicyRemoved(const UUID & policy_id);
    void mixConditions();
    void mixConditionsFor(EnabledRowPolicies & enabled);

    const AccessControlManager & access_control_manager;
    std::unordered_map<UUID, PolicyInfo> all_policies;
    bool all_policies_read = false;
    ext::scope_guard subscription;
    std::map<EnabledRowPolicies::Params, std::weak_ptr<EnabledRowPolicies>> enabled_row_policies;
    std::mutex mutex;
};

}
