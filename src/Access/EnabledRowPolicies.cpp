#include <Access/EnabledRowPolicies.h>
#include <Parsers/makeASTForLogicalFunction.h>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/copy.hpp>


namespace DB
{
size_t EnabledRowPolicies::Hash::operator()(const MixedFiltersKey & key) const
{
    return std::hash<std::string_view>{}(key.database) - std::hash<std::string_view>{}(key.table_name) + static_cast<size_t>(key.filter_type);
}


EnabledRowPolicies::EnabledRowPolicies() : params()
{
}

EnabledRowPolicies::EnabledRowPolicies(const Params & params_) : params(params_)
{
}

EnabledRowPolicies::~EnabledRowPolicies() = default;


ASTPtr EnabledRowPolicies::getFilter(const String & database, const String & table_name, RowPolicyFilterType filter_type) const
{
    /// We don't lock `mutex` here.
    auto loaded = mixed_filters.load();
    auto it = loaded->find({database, table_name, filter_type});
    if (it == loaded->end())
        return {};

    auto filter = it->second.ast;

    bool value;
    if (tryGetLiteralBool(filter.get(), value) && value)
        return nullptr; /// The condition is always true, no need to check it.

    return filter;
}

ASTPtr EnabledRowPolicies::getFilter(const String & database, const String & table_name, RowPolicyFilterType filter_type, const ASTPtr & combine_with_expr) const
{
    ASTPtr filter = getFilter(database, table_name, filter_type);
    if (filter && combine_with_expr)
        filter = makeASTForLogicalAnd({filter, combine_with_expr});
    else if (!filter)
        filter = combine_with_expr;

    bool value;
    if (tryGetLiteralBool(filter.get(), value) && value)
        return nullptr;  /// The condition is always true, no need to check it.

    return filter;
}

}
