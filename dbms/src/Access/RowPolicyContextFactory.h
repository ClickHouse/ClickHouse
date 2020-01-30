#pragma once

#include <Access/RowPolicyContext.h>
#include <Access/IAccessStorage.h>
#include <mutex>
#include <unordered_map>
#include <unordered_set>


namespace DB
{
class AccessControlManager;


/// Stores read and parsed row policies.
class RowPolicyContextFactory
{
public:
    RowPolicyContextFactory(const AccessControlManager & access_control_manager_);
    ~RowPolicyContextFactory();

    RowPolicyContextPtr createContext(const String & user_name);

private:
    using ParsedConditions = RowPolicyContext::ParsedConditions;

    struct PolicyInfo
    {
        PolicyInfo(const RowPolicyPtr & policy_) { setPolicy(policy_); }
        void setPolicy(const RowPolicyPtr & policy_);
        bool canUseWithContext(const RowPolicyContext & context) const;

        RowPolicyPtr policy;
        std::unordered_set<String> roles;
        bool all_roles = false;
        std::unordered_set<String> except_roles;
        ParsedConditions parsed_conditions;
    };

    void ensureAllRowPoliciesRead();
    void rowPolicyAddedOrChanged(const UUID & policy_id, const RowPolicyPtr & new_policy);
    void rowPolicyRemoved(const UUID & policy_id);
    void mixConditionsForAllContexts();
    void mixConditionsForContext(RowPolicyContext & context);

    const AccessControlManager & access_control_manager;
    std::unordered_map<UUID, PolicyInfo> all_policies;
    bool all_policies_read = false;
    IAccessStorage::SubscriptionPtr subscription;
    std::vector<std::weak_ptr<RowPolicyContext>> contexts;
    std::mutex mutex;
};

}
