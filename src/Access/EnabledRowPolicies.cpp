#include <Access/EnabledRowPolicies.h>
#include <Parsers/makeASTForLogicalFunction.h>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/copy.hpp>

#include <Common/logger_useful.h>


namespace DB
{

bool RowPolicyFilter::empty() const
{
    bool value;
    return !expression || (tryGetLiteralBool(expression.get(), value) && value);
}

size_t EnabledRowPolicies::Hash::operator()(const MixedFiltersKey & key) const
{
    return std::hash<std::string_view>{}(key.database) - std::hash<std::string_view>{}(key.table_name) + static_cast<size_t>(key.filter_type);
}


// size_t EnabledRowPolicies::Hash::operator()(const MixedFiltersKey & key) const
// {
//     return std::hash<std::string_view>{}(key.database) + static_cast<size_t>(key.filter_type);
// }

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
    {


        for (auto it = loaded->begin(); it != loaded->end(); ++it)
        {
            LOG_TRACE((&Poco::Logger::get("EnabledRowPolicies::getFilter")), "  db: {}, table {}", it->first.database, it->first.table_name);

        }

    }




    auto it = loaded->find({database, table_name, filter_type});
    if (it == loaded->end())
    {
        it = loaded->find({database, "*", filter_type});
        if (it == loaded->end())
        {
            LOG_TRACE((&Poco::Logger::get("EnabledRowPolicies::getFilter")), "db: {}, table {} - not found ({} records)",
                database, table_name, loaded->size());
            return {};
        }
    }


    LOG_TRACE((&Poco::Logger::get("EnabledRowPolicies::getFilter")), "db: {}, table {} - found ({} records)",
        database, table_name, loaded->size());
    return it->second;
}

RowPolicyFilterPtr EnabledRowPolicies::getFilter(const String & database, const String & table_name, RowPolicyFilterType filter_type, RowPolicyFilterPtr combine_with_filter) const
{
    RowPolicyFilterPtr filter = getFilter(database, table_name, filter_type);
    if (filter && combine_with_filter)
    {
        auto new_filter = std::make_shared<RowPolicyFilter>(*filter);

        if (filter->empty())
        {
            new_filter->expression = combine_with_filter->expression;
        }
        else if (combine_with_filter->empty())
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
