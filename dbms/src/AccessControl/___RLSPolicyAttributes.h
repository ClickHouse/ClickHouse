#pragma once

#include <AccessControl/IAccessAttributes.h>
#include <Core/UUID.h>
#include <unordered_set>

namespace DB
{
/// Row-level security policy.
struct RLSPolicyAttributes : public IAccessAttributes
{
    String database;
    String table;

    enum class PolicyType
    {
        PERMISSIVE,
        RESTRICTIVE,
    };
    PolicyType policy_type = PolicyType::PERMISSIVE;

    String select_filter;
    String insert_filter;

    std::unordered_set<UUID> apply_to_roles;

    RLSPolicyAttributes() : IAccessAttributes(Type::RLS_POLICY) {}
    std::shared_ptr<IAccessAttributes> clone() const override;
    bool isEqual(const IAccessAttributes & other) const override;
protected:
    void copyTo(RLSPolicyAttributes & dest) const;
};

using RLSPolicyAttributesPtr = std::shared_ptr<const RLSPolicyAttributes>;
}
