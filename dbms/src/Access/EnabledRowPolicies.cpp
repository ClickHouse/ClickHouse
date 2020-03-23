#include <Access/EnabledRowPolicies.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTExpressionList.h>
#include <boost/smart_ptr/make_shared.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/copy.hpp>


namespace DB
{
size_t EnabledRowPolicies::Hash::operator()(const DatabaseAndTableNameRef & database_and_table_name) const
{
    return std::hash<std::string_view>{}(database_and_table_name.first) - std::hash<std::string_view>{}(database_and_table_name.second);
}


EnabledRowPolicies::EnabledRowPolicies(const Params & params_)
    : params(params_)
{
}

EnabledRowPolicies::~EnabledRowPolicies() = default;


ASTPtr EnabledRowPolicies::getCondition(const String & database, const String & table_name, ConditionType type) const
{
    /// We don't lock `mutex` here.
    auto loaded = map_of_mixed_conditions.load();
    auto it = loaded->find({database, table_name});
    if (it == loaded->end())
        return {};
    return it->second.mixed_conditions[type];
}


ASTPtr EnabledRowPolicies::getCondition(const String & database, const String & table_name, ConditionType type, const ASTPtr & extra_condition) const
{
    ASTPtr main_condition = getCondition(database, table_name, type);
    if (!main_condition)
        return extra_condition;
    if (!extra_condition)
        return main_condition;
    auto function = std::make_shared<ASTFunction>();
    auto exp_list = std::make_shared<ASTExpressionList>();
    function->name = "and";
    function->arguments = exp_list;
    function->children.push_back(exp_list);
    exp_list->children.push_back(main_condition);
    exp_list->children.push_back(extra_condition);
    return function;
}


std::vector<UUID> EnabledRowPolicies::getCurrentPolicyIDs() const
{
    /// We don't lock `mutex` here.
    auto loaded = map_of_mixed_conditions.load();
    std::vector<UUID> policy_ids;
    for (const auto & mixed_conditions : *loaded | boost::adaptors::map_values)
        boost::range::copy(mixed_conditions.policy_ids, std::back_inserter(policy_ids));
    return policy_ids;
}


std::vector<UUID> EnabledRowPolicies::getCurrentPolicyIDs(const String & database, const String & table_name) const
{
    /// We don't lock `mutex` here.
    auto loaded = map_of_mixed_conditions.load();
    auto it = loaded->find({database, table_name});
    if (it == loaded->end())
        return {};
    return it->second.policy_ids;
}

}
