#pragma once

#include <Account/IAccessAttributes.h>


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

    RLSPolicyAttributes() : IAccessAttributes(Type::RLS_POLICY) {}
    std::shared_ptr<IAccessAttributes> clone() const override;
    bool isEqual(const IAccessAttributes & other) const override;
protected:
    void copyTo(RLSPolicyAttributes & dest) const;
};

using RLSPolicyAttributesPtr = std::shared_ptr<const RLSPolicyAttributes>;
}
