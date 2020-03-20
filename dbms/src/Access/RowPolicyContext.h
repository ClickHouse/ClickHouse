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
class RowPolicyContext
{
public:
    /// Default constructor makes a row policy usage context which restricts nothing.
    RowPolicyContext();

    ~RowPolicyContext();

    using ConditionIndex = RowPolicy::ConditionIndex;

    /// Returns prepared filter for a specific table and operations.
    /// The function can return nullptr, that means there is no filters applied.
    /// The returned filter can be a combination of the filters defined by multiple row policies.
    ASTPtr getCondition(const String & database, const String & table_name, ConditionIndex index) const;

    /// Combines two conditions into one by using the logical AND operator.
    static ASTPtr combineConditionsUsingAnd(const ASTPtr & lhs, const ASTPtr & rhs);

    /// Returns IDs of all the policies used by the current user.
    std::vector<UUID> getCurrentPolicyIDs() const;

    /// Returns IDs of the policies used by a concrete table.
    std::vector<UUID> getCurrentPolicyIDs(const String & database, const String & table_name) const;

private:
    friend class RowPolicyContextFactory;
    friend struct ext::shared_ptr_helper<RowPolicyContext>;
    RowPolicyContext(UUID user_id_, std::vector<UUID> enabled_roles_); /// RowPolicyContext should be created by RowPolicyContextFactory.

    using DatabaseAndTableName = std::pair<String, String>;
    using DatabaseAndTableNameRef = std::pair<std::string_view, std::string_view>;
    struct Hash
    {
        size_t operator()(const DatabaseAndTableNameRef & database_and_table_name) const;
    };
    static constexpr size_t MAX_CONDITION_INDEX = RowPolicy::MAX_CONDITION_INDEX;
    using ParsedConditions = std::array<ASTPtr, MAX_CONDITION_INDEX>;
    struct MixedConditions
    {
        std::unique_ptr<DatabaseAndTableName> database_and_table_name_keeper;
        ParsedConditions mixed_conditions;
        std::vector<UUID> policy_ids;
    };
    using MapOfMixedConditions = std::unordered_map<DatabaseAndTableNameRef, MixedConditions, Hash>;

    const UUID user_id;
    const std::vector<UUID> enabled_roles;
    mutable boost::atomic_shared_ptr<const MapOfMixedConditions> map_of_mixed_conditions;
};


using RowPolicyContextPtr = std::shared_ptr<const RowPolicyContext>;
}
