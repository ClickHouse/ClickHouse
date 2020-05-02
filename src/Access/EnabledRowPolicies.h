#pragma once

#include <Access/RowPolicy.h>
#include <Core/Types.h>
#include <Core/UUID.h>
#include <boost/smart_ptr/atomic_shared_ptr.hpp>
#include <memory>
#include <unordered_map>


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

    /// Returns IDs of all the policies used by the current user.
    std::vector<UUID> getCurrentPolicyIDs() const;

    /// Returns IDs of the policies used by a concrete table.
    std::vector<UUID> getCurrentPolicyIDs(const String & database, const String & table_name) const;

private:
    friend class RowPolicyCache;
    EnabledRowPolicies(const Params & params_);

    using DatabaseAndTableName = std::pair<String, String>;
    using DatabaseAndTableNameRef = std::pair<std::string_view, std::string_view>;
    struct Hash
    {
        size_t operator()(const DatabaseAndTableNameRef & database_and_table_name) const;
    };
    using ParsedConditions = std::array<ASTPtr, RowPolicy::MAX_CONDITION_TYPE>;
    struct MixedConditions
    {
        std::unique_ptr<DatabaseAndTableName> database_and_table_name_keeper;
        ParsedConditions mixed_conditions;
        std::vector<UUID> policy_ids;
    };
    using MapOfMixedConditions = std::unordered_map<DatabaseAndTableNameRef, MixedConditions, Hash>;

    const Params params;
    mutable boost::atomic_shared_ptr<const MapOfMixedConditions> map_of_mixed_conditions;
};

}
