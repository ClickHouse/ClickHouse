#include <Access/RowPolicyCache.h>
#include <Access/AccessControl.h>
#include <Access/EnabledRowPolicies.h>
#include <Access/RowPolicy.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Parsers/makeASTForLogicalFunction.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <base/range.h>
#include <boost/smart_ptr/make_shared.hpp>
#include <Core/Defines.h>


namespace DB
{
namespace
{
    /// Accumulates filters from multiple row policies and joins them using the AND logical operation.
    class FiltersMixer
    {
    public:
        void add(const ASTPtr & filter, bool is_restrictive)
        {
            if (is_restrictive)
                restrictions.push_back(filter);
            else
                permissions.push_back(filter);
        }

        ASTPtr getResult() &&
        {
            /// Process permissive filters.
            restrictions.push_back(makeASTForLogicalOr(std::move(permissions)));

            /// Process restrictive filters.
            auto result = makeASTForLogicalAnd(std::move(restrictions));

            bool value;
            if (tryGetLiteralBool(result.get(), value) && value)
                result = nullptr;  /// The condition is always true, no need to check it.

            return result;
        }

    private:
        ASTs permissions;
        ASTs restrictions;
    };
}


void RowPolicyCache::PolicyInfo::setPolicy(const RowPolicyPtr & policy_)
{
    policy = policy_;
    roles = &policy->to_roles;
    database_and_table_name = std::make_shared<std::pair<String, String>>(policy->getDatabase(), policy->getTableName());

    for (auto filter_type : collections::range(0, RowPolicyFilterType::MAX))
    {
        auto filter_type_i = static_cast<size_t>(filter_type);
        parsed_filters[filter_type_i] = nullptr;
        const String & filter = policy->filters[filter_type_i];
        if (filter.empty())
            continue;

        auto previous_range = std::pair(std::begin(policy->filters), std::begin(policy->filters) + filter_type_i);
        const auto * previous_it = std::find(previous_range.first, previous_range.second, filter);
        if (previous_it != previous_range.second)
        {
            /// The filter is already parsed before.
            parsed_filters[filter_type_i] = parsed_filters[previous_it - previous_range.first];
            continue;
        }

        /// Try to parse the filter.
        try
        {
            ParserExpression parser;
            parsed_filters[filter_type_i] = parseQuery(parser, filter, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
        }
        catch (...)
        {
            tryLogCurrentException(
                &Poco::Logger::get("RowPolicy"),
                String("Could not parse the condition ") + toString(filter_type) + " of row policy "
                    + backQuote(policy->getName()));
        }
    }
}


RowPolicyCache::RowPolicyCache(const AccessControl & access_control_)
    : access_control(access_control_)
{
}

RowPolicyCache::~RowPolicyCache() = default;


std::shared_ptr<const EnabledRowPolicies> RowPolicyCache::getEnabledRowPolicies(const UUID & user_id, const boost::container::flat_set<UUID> & enabled_roles)
{
    std::lock_guard lock{mutex};
    ensureAllRowPoliciesRead();

    EnabledRowPolicies::Params params;
    params.user_id = user_id;
    params.enabled_roles = enabled_roles;
    auto it = enabled_row_policies.find(params);
    if (it != enabled_row_policies.end())
    {
        auto from_cache = it->second.lock();
        if (from_cache)
            return from_cache;
        enabled_row_policies.erase(it);
    }

    auto res = std::shared_ptr<EnabledRowPolicies>(new EnabledRowPolicies(params));
    enabled_row_policies.emplace(std::move(params), res);
    mixFiltersFor(*res);
    return res;
}


void RowPolicyCache::ensureAllRowPoliciesRead()
{
    /// `mutex` is already locked.
    if (all_policies_read)
        return;
    all_policies_read = true;

    subscription = access_control.subscribeForChanges<RowPolicy>(
        [&](const UUID & id, const AccessEntityPtr & entity)
        {
            if (entity)
                rowPolicyAddedOrChanged(id, typeid_cast<RowPolicyPtr>(entity));
            else
                rowPolicyRemoved(id);
        });

    for (const UUID & id : access_control.findAll<RowPolicy>())
    {
        auto quota = access_control.tryRead<RowPolicy>(id);
        if (quota)
            all_policies.emplace(id, PolicyInfo(quota));
    }
}


void RowPolicyCache::rowPolicyAddedOrChanged(const UUID & policy_id, const RowPolicyPtr & new_policy)
{
    std::lock_guard lock{mutex};
    auto it = all_policies.find(policy_id);
    if (it == all_policies.end())
    {
        it = all_policies.emplace(policy_id, PolicyInfo(new_policy)).first;
    }
    else
    {
        if (it->second.policy == new_policy)
            return;
    }

    auto & info = it->second;
    info.setPolicy(new_policy);
    mixFilters();
}


void RowPolicyCache::rowPolicyRemoved(const UUID & policy_id)
{
    std::lock_guard lock{mutex};
    all_policies.erase(policy_id);
    mixFilters();
}


void RowPolicyCache::mixFilters()
{
    /// `mutex` is already locked.
    for (auto i = enabled_row_policies.begin(), e = enabled_row_policies.end(); i != e;)
    {
        auto elem = i->second.lock();
        if (!elem)
            i = enabled_row_policies.erase(i);
        else
        {
            mixFiltersFor(*elem);
            ++i;
        }
    }
}


void RowPolicyCache::mixFiltersFor(EnabledRowPolicies & enabled)
{
    /// `mutex` is already locked.

    using MixedFiltersMap = EnabledRowPolicies::MixedFiltersMap;
    using MixedFiltersKey = EnabledRowPolicies::MixedFiltersKey;
    using Hash = EnabledRowPolicies::Hash;

    struct MixerWithNames
    {
        FiltersMixer mixer;
        std::shared_ptr<const std::pair<String, String>> database_and_table_name;
    };

    std::unordered_map<MixedFiltersKey, MixerWithNames, Hash> mixers;

    for (const auto & [policy_id, info] : all_policies)
    {
        const auto & policy = *info.policy;
        bool match = info.roles->match(enabled.params.user_id, enabled.params.enabled_roles);
        MixedFiltersKey key;
        key.database = info.database_and_table_name->first;
        key.table_name = info.database_and_table_name->second;
        for (auto filter_type : collections::range(0, RowPolicyFilterType::MAX))
        {
            auto filter_type_i = static_cast<size_t>(filter_type);
            if (info.parsed_filters[filter_type_i])
            {
                key.filter_type = filter_type;
                auto & mixer = mixers[key];
                mixer.database_and_table_name = info.database_and_table_name;
                if (match)
                    mixer.mixer.add(info.parsed_filters[filter_type_i], policy.isRestrictive());
            }
        }
    }

    auto mixed_filters = boost::make_shared<MixedFiltersMap>();
    for (auto & [key, mixer] : mixers)
    {
        auto & mixed_filter = (*mixed_filters)[key];
        mixed_filter.database_and_table_name = mixer.database_and_table_name;
        mixed_filter.ast = std::move(mixer.mixer).getResult();
    }

    enabled.mixed_filters.store(mixed_filters);
}

}
