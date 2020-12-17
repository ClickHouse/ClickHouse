#pragma once

#include <Access/RowPolicy.h>
#include <common/types.h>
#include <Core/UUID.h>
#include <boost/smart_ptr/atomic_shared_ptr.hpp>
#include <unordered_map>
#include <memory>


namespace DB
{
class IAST;
using ASTPtr = std::shared_ptr<IAST>;


/// Provides fast access to row policies' conditions for a specific user and tables.
class EnabledRowPolicies
{
public:
    struct Params
    {
        UUID user_id;
        boost::container::flat_set<UUID> enabled_roles;

        auto toTuple() const { return std::tie(user_id, enabled_roles); }
        friend bool operator ==(const Params & lhs, const Params & rhs) { return lhs.toTuple() == rhs.toTuple(); }
        friend bool operator !=(const Params & lhs, const Params & rhs) { return !(lhs == rhs); }
        friend bool operator <(const Params & lhs, const Params & rhs) { return lhs.toTuple() < rhs.toTuple(); }
        friend bool operator >(const Params & lhs, const Params & rhs) { return rhs < lhs; }
        friend bool operator <=(const Params & lhs, const Params & rhs) { return !(rhs < lhs); }
        friend bool operator >=(const Params & lhs, const Params & rhs) { return !(lhs < rhs); }
    };

    ~EnabledRowPolicies();

    using ConditionType = RowPolicy::ConditionType;

    /// Returns prepared filter for a specific table and operations.
    /// The function can return nullptr, that means there is no filters applied.
    /// The returned filter can be a combination of the filters defined by multiple row policies.
    ASTPtr getCondition(const String & database, const String & table_name, ConditionType type) const;
    ASTPtr getCondition(const String & database, const String & table_name, ConditionType type, const ASTPtr & extra_condition) const;

private:
    friend class RowPolicyCache;
    EnabledRowPolicies(const Params & params_);

    struct MixedConditionKey
    {
        std::string_view database;
        std::string_view table_name;
        ConditionType condition_type;

        auto toTuple() const { return std::tie(database, table_name, condition_type); }
        friend bool operator==(const MixedConditionKey & left, const MixedConditionKey & right) { return left.toTuple() == right.toTuple(); }
        friend bool operator!=(const MixedConditionKey & left, const MixedConditionKey & right) { return left.toTuple() != right.toTuple(); }
    };

    struct Hash
    {
        size_t operator()(const MixedConditionKey & key) const;
    };

    struct MixedCondition
    {
        ASTPtr ast;
        std::shared_ptr<const std::pair<String, String>> database_and_table_name;
    };
    using MapOfMixedConditions = std::unordered_map<MixedConditionKey, MixedCondition, Hash>;

    const Params params;
    mutable boost::atomic_shared_ptr<const MapOfMixedConditions> map_of_mixed_conditions;
};

}
