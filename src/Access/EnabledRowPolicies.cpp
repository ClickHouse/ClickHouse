#include <Access/EnabledRowPolicies.h>
#include <Parsers/makeASTForLogicalFunction.h>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/copy.hpp>


namespace DB
{

bool RowPolicyFilter::isAlwaysTrue() const
{
    bool value;
    return !expression || (tryGetLiteralBool(expression.get(), value) && value);
}

bool RowPolicyFilter::isAlwaysFalse() const
{
    bool value;
    return expression && (tryGetLiteralBool(expression.get(), value) && !value);
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


RowPolicyFilterPtr EnabledRowPolicies::getFilter(const String & database, const String & table_name, RowPolicyFilterType filter_type) const
{
    /// We don't lock `mutex` here.
    auto loaded = mixed_filters.load();
    auto it = loaded->find({database, table_name, filter_type});
    if (it == loaded->end())
    {   /// Look for a policy for database if a table policy not found
        it = loaded->find({database, RowPolicyName::ANY_TABLE_MARK, filter_type});
        if (it == loaded->end())
        {
            return {};
        }
    }

    return it->second;
}

RowPolicyFilterPtr EnabledRowPolicies::getFilter(const String & database, const String & table_name, RowPolicyFilterType filter_type, RowPolicyFilterPtr combine_with_filter) const
{
    RowPolicyFilterPtr filter = getFilter(database, table_name, filter_type);
    if (filter && combine_with_filter)
    {
        auto new_filter = std::make_shared<RowPolicyFilter>(*filter);

        if (filter->isAlwaysTrue())
        {
            new_filter->expression = combine_with_filter->expression;
        }
        else if (combine_with_filter->isAlwaysTrue())
        {
            new_filter->expression = filter->expression;
        }
        else
        {
            new_filter->expression = makeASTForLogicalAnd({filter->expression, combine_with_filter->expression});
        }

        std::copy(combine_with_filter->policies.begin(), combine_with_filter->policies.end(), std::back_inserter(new_filter->policies));
        filter = new_filter;
    }
    else if (!filter)
    {
        filter = combine_with_filter;
    }

    return filter;
}

}
