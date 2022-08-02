#include <Access/EnabledRowPolicies.h>
#include <Parsers/makeASTForLogicalFunction.h>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/copy.hpp>


namespace DB
{

void RowPolicyFilter::optimize()
{
    bool value;
    if (tryGetLiteralBool(expression.get(), value) && value)
        expression.reset(); /// The condition is always true, no need to check it.
}

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


RowPolicyFilter EnabledRowPolicies::getFilter(const String & database, const String & table_name, RowPolicyFilterType filter_type) const
{
    /// We don't lock `mutex` here.
    auto loaded = mixed_filters.load();
    auto it = loaded->find({database, table_name, filter_type});
    if (it == loaded->end())
        return {};

    RowPolicyFilter filter = {it->second.ast, it->second.policies};
    filter.optimize();

    return filter;
}

RowPolicyFilter EnabledRowPolicies::getFilter(const String & database, const String & table_name, RowPolicyFilterType filter_type, const RowPolicyFilter & combine_with_filter) const
{
    RowPolicyFilter filter = getFilter(database, table_name, filter_type);
    if (filter.expression && combine_with_filter.expression)
    {
        filter.expression = makeASTForLogicalAnd({filter.expression, combine_with_filter.expression});
    }
    else if (!filter.expression)
    {
        filter.expression = combine_with_filter.expression;
    }

    std::copy(combine_with_filter.policies.begin(), combine_with_filter.policies.end(), std::back_inserter(filter.policies));
    filter.optimize();

    return filter;
}

}
