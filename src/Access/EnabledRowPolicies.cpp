#include <Access/EnabledRowPolicies.h>
#include <Parsers/makeASTForLogicalFunction.h>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/copy.hpp>


namespace DB
{
size_t EnabledRowPolicies::Hash::operator()(const MixedConditionKey & key) const
{
    return std::hash<std::string_view>{}(key.database) - std::hash<std::string_view>{}(key.table_name) + static_cast<size_t>(key.condition_type);
}


EnabledRowPolicies::EnabledRowPolicies(const Params & params_)
    : params(params_)
{
}

EnabledRowPolicies::~EnabledRowPolicies() = default;


ASTPtr EnabledRowPolicies::getCondition(const String & database, const String & table_name, ConditionType condition_type) const
{
    /// We don't lock `mutex` here.
    auto loaded = map_of_mixed_conditions.load();
    auto it = loaded->find({database, table_name, condition_type});
    if (it == loaded->end())
        return {};

    auto condition = it->second.ast;

    bool value;
    if (tryGetLiteralBool(condition.get(), value) && value)
        return nullptr; /// The condition is always true, no need to check it.

    return condition;
}

ASTPtr EnabledRowPolicies::getCondition(const String & database, const String & table_name, ConditionType type, const ASTPtr & extra_condition) const
{
    ASTPtr condition = getCondition(database, table_name, type);
    if (condition && extra_condition)
        condition = makeASTForLogicalAnd({condition, extra_condition});
    else if (!condition)
        condition = extra_condition;

    bool value;
    if (tryGetLiteralBool(condition.get(), value) && value)
        return nullptr;  /// The condition is always true, no need to check it.

    return condition;
}

}
