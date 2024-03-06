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

        RowPolicyPtr policy;
        const RolesOrUsersSet * roles = nullptr;
        std::shared_ptr<const std::pair<String, String>> database_and_table_name;
        ASTPtr parsed_filters[static_cast<size_t>(RowPolicyFilterType::MAX)];
    };

    void ensureAllRowPoliciesRead();
    void rowPolicyAddedOrChanged(const UUID & policy_id, const RowPolicyPtr & new_policy);
    void rowPolicyRemoved(const UUID & policy_id);
    void mixFilters();
    void mixFiltersFor(EnabledRowPolicies & enabled);

    const AccessControl & access_control;
    std::unordered_map<UUID, PolicyInfo> all_policies;
    bool all_policies_read = false;
    scope_guard subscription;
    std::map<EnabledRowPolicies::Params, std::weak_ptr<EnabledRowPolicies>> enabled_row_policies;
    std::mutex mutex;
};

}
