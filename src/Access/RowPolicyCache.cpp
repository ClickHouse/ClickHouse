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

#include <Common/logger_useful.h>


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

        ASTPtr getResult(bool users_without_row_policies_can_read_rows) &&
        {
            if (!permissions.empty() || !users_without_row_policies_can_read_rows)
            {
                /// Process permissive filters.
                restrictions.push_back(makeASTForLogicalOr(std::move(permissions)));
            }

            /// Process restrictive filters.
            ASTPtr result;
            if (!restrictions.empty())
                result = makeASTForLogicalAnd(std::move(restrictions));

            if (result)
            {
                bool value;
                if (tryGetLiteralBool(result.get(), value) && value)
                    result = nullptr; /// The condition is always true, no need to check it.
            }

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
        auto policy = access_control.tryRead<RowPolicy>(id);
        if (policy)
        {
            PolicyInfo policy_info(policy);
            if (policy_info.database_and_table_name->second == "*")
            {
                database_policies.emplace(id, std::move(policy_info));
            }
            else
            {
                table_policies.emplace(id, std::move(policy_info));
            }
        }
    }
}


void RowPolicyCache::rowPolicyAddedOrChanged(const UUID & policy_id, const RowPolicyPtr & new_policy)
{
    std::lock_guard lock{mutex};
    bool found = true;

    auto it = table_policies.find(policy_id);
    if (it == table_policies.end())
    {
        it = database_policies.find(policy_id);
        if (it == database_policies.end())
        {
            PolicyMap & policy_map = new_policy->isDatabase() ? database_policies : table_policies;
            it = policy_map.emplace(policy_id, PolicyInfo(new_policy)).first;
            found = false;
        }
    }

    if (found && it->second.policy == new_policy)
    {
        return;
    }

    auto & info = it->second;
    info.setPolicy(new_policy);
    mixFilters();
}


void RowPolicyCache::rowPolicyRemoved(const UUID & policy_id)
{
    std::lock_guard lock{mutex};
    auto it = database_policies.find(policy_id);
    if (it != database_policies.end())
    {
        database_policies.erase(it);
    }
    else
    {
        table_policies.erase(policy_id);
    }
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
        std::vector<RowPolicyPtr> policies;
    };

    std::unordered_map<MixedFiltersKey, MixerWithNames, Hash> table_mixers;
    std::unordered_map<MixedFiltersKey, MixerWithNames, Hash> database_mixers;


    for (const auto & [policy_id, info] : database_policies)
    {
        const auto & policy = *info.policy;
        bool match = info.roles->match(enabled.params.user_id, enabled.params.enabled_roles);
        for (auto filter_type : collections::range(0, RowPolicyFilterType::MAX))
        {
            auto filter_type_i = static_cast<size_t>(filter_type);
            if (info.parsed_filters[filter_type_i])
            {
                MixedFiltersKey key{info.database_and_table_name->first,
                    info.database_and_table_name->second,
                    filter_type};
                LOG_TRACE((&Poco::Logger::get("mixFiltersFor")), "db: {} : {}", key.database, key.table_name);

                auto & mixer = database_mixers[key];  //  getting database level mixer
                mixer.database_and_table_name = info.database_and_table_name;
                if (match)
                {
                    mixer.mixer.add(info.parsed_filters[filter_type_i], policy.isRestrictive());
                    mixer.policies.push_back(info.policy);
                }
            }
        }
    }


    for (const auto & [policy_id, info] : table_policies)
    {
        const auto & policy = *info.policy;
        bool match = info.roles->match(enabled.params.user_id, enabled.params.enabled_roles);
        for (auto filter_type : collections::range(0, RowPolicyFilterType::MAX))
        {
            auto filter_type_i = static_cast<size_t>(filter_type);
            if (info.parsed_filters[filter_type_i])
            {
                MixedFiltersKey key{info.database_and_table_name->first,
                    info.database_and_table_name->second,
                    filter_type};
                LOG_TRACE((&Poco::Logger::get("mixFiltersFor")), "table: {} : {}", key.database, key.table_name);
                auto table_it = table_mixers.find(key);
                if (table_it == table_mixers.end())
                {
                    LOG_TRACE((&Poco::Logger::get("mixFiltersFor")), "table: not found, looking for db");
                    MixedFiltersKey database_key = key;
                    database_key.table_name = "*";

                    auto database_it = database_mixers.find(database_key);

                    if (database_it == database_mixers.end())
                    {
                        LOG_TRACE((&Poco::Logger::get("mixFiltersFor")), "table: not found, database not found");
                        table_it = table_mixers.try_emplace(key).first;
                    }
                    else
                    {
                        LOG_TRACE((&Poco::Logger::get("mixFiltersFor")), "table: not found, database found");
                        table_it = table_mixers.insert({key, database_it->second}).first;
                    }
                }

                auto & mixer = table_it->second; // table_mixers[key];    getting table level mixer
                mixer.database_and_table_name = info.database_and_table_name;
                if (match)
                {
                    mixer.mixer.add(info.parsed_filters[filter_type_i], policy.isRestrictive());
                    mixer.policies.push_back(info.policy);
                }
            }
        }
    }

    auto mixed_filters = boost::make_shared<MixedFiltersMap>();

    for (auto mixer_map_ptr :  { &table_mixers, &database_mixers})
    {
        for (auto & [key, mixer] : *mixer_map_ptr)
        {
            auto mixed_filter = std::make_shared<RowPolicyFilter>();
            mixed_filter->database_and_table_name = std::move(mixer.database_and_table_name);
            mixed_filter->expression = std::move(mixer.mixer).getResult(access_control.isEnabledUsersWithoutRowPoliciesCanReadRows());
            mixed_filter->policies = std::move(mixer.policies);
            mixed_filters->emplace(key, std::move(mixed_filter));
        }
    }


    enabled.mixed_filters.store(mixed_filters);
}

}
