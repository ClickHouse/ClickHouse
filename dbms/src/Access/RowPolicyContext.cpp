#include <Access/RowPolicyContext.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTExpressionList.h>
#include <boost/smart_ptr/make_shared.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/copy.hpp>


namespace DB
{
size_t RowPolicyContext::Hash::operator()(const DatabaseAndTableNameRef & database_and_table_name) const
{
    return std::hash<std::string_view>{}(database_and_table_name.first) - std::hash<std::string_view>{}(database_and_table_name.second);
}


RowPolicyContext::RowPolicyContext()
    : map_of_mixed_conditions(boost::make_shared<MapOfMixedConditions>())
{
}


RowPolicyContext::~RowPolicyContext() = default;


RowPolicyContext::RowPolicyContext(UUID user_id_, std::vector<UUID> enabled_roles_)
    : user_id(std::move(user_id_)), enabled_roles(std::move(enabled_roles_))
{}


ASTPtr RowPolicyContext::getCondition(const String & database, const String & table_name, ConditionIndex index) const
{
    /// We don't lock `mutex` here.
    auto loaded = map_of_mixed_conditions.load();
    auto it = loaded->find({database, table_name});
    if (it == loaded->end())
        return {};
    return it->second.mixed_conditions[index];
}


ASTPtr RowPolicyContext::combineConditionsUsingAnd(const ASTPtr & lhs, const ASTPtr & rhs)
{
    if (!lhs)
        return rhs;
    if (!rhs)
        return lhs;
    auto function = std::make_shared<ASTFunction>();
    auto exp_list = std::make_shared<ASTExpressionList>();
    function->name = "and";
    function->arguments = exp_list;
    function->children.push_back(exp_list);
    exp_list->children.push_back(lhs);
    exp_list->children.push_back(rhs);
    return function;
}


std::vector<UUID> RowPolicyContext::getCurrentPolicyIDs() const
{
    /// We don't lock `mutex` here.
    auto loaded = map_of_mixed_conditions.load();
    std::vector<UUID> policy_ids;
    for (const auto & mixed_conditions : *loaded | boost::adaptors::map_values)
        boost::range::copy(mixed_conditions.policy_ids, std::back_inserter(policy_ids));
    return policy_ids;
}


std::vector<UUID> RowPolicyContext::getCurrentPolicyIDs(const String & database, const String & table_name) const
{
    /// We don't lock `mutex` here.
    auto loaded = map_of_mixed_conditions.load();
    auto it = loaded->find({database, table_name});
    if (it == loaded->end())
        return {};
    return it->second.policy_ids;
}
}
