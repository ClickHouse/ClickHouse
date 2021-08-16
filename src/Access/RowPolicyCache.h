#pragma once

#include <Access/EnabledRowPolicies.h>
#include <common/scope_guard.h>
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
    struct PolicyInfo
    {
        PolicyInfo(const RowPolicyPtr & policy_) { setPolicy(policy_); }
        void setPolicy(const RowPolicyPtr & policy_);

        RowPolicyPtr policy;
        const RolesOrUsersSet * roles = nullptr;
        std::shared_ptr<const std::pair<String, String>> database_and_table_name;
        ASTPtr parsed_conditions[RowPolicy::MAX_CONDITION_TYPE];
    };

    void ensureAllRowPoliciesRead();
    void rowPolicyAddedOrChanged(const UUID & policy_id, const RowPolicyPtr & new_policy);
    void rowPolicyRemoved(const UUID & policy_id);
    void mixConditions();
    void mixConditionsFor(EnabledRowPolicies & enabled);

    const AccessControlManager & access_control_manager;
    std::unordered_map<UUID, PolicyInfo> all_policies;
    bool all_policies_read = false;
    scope_guard subscription;
    std::map<EnabledRowPolicies::Params, std::weak_ptr<EnabledRowPolicies>> enabled_row_policies;
    std::mutex mutex;
};

}
