#include <Access/RowPolicyCache.h>
#include <Access/AccessControl.h>
#include <Access/EnabledRowPolicies.h>
#include <Access/RowPolicy.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Parsers/makeASTForLogicalFunction.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <base/range.h>
#include <boost/smart_ptr/make_shared.hpp>
#include <Core/Defines.h>


namespace ProfileEvents
{
    extern const Event RowPolicyCacheRecalculations;
    extern const Event RowPolicyCacheRecalculationMicroseconds;
}

namespace DB
{
namespace
{
    /// Helper to accumulate filters from multiple row policies and join them together
    ///   by AND or OR logical operations.
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
                bool value = false;
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
        const auto previous_it = std::find(previous_range.first, previous_range.second, filter);
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
            parsed_filters[filter_type_i] = parseQuery(parser, filter, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
        }
        catch (...)
        {
            tryLogCurrentException(
                getLogger("RowPolicy"),
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

    subscription = access_control.subscribeForChanges<RowPolicy>(
        [this](const std::vector<AccessChangesNotifier::Change> & changes)
        {
            std::lock_guard lock{mutex};
            for (const auto & change : changes)
            {
                if (change.entity)
                    rowPolicyAddedOrChanged(change.id, typeid_cast<RowPolicyPtr>(change.entity));
                else
                    rowPolicyRemoved(change.id);
            }
            mixFiltersIfNeeded();
        });

    /// Start clean: a previous attempt may have thrown mid-scan.
    all_policies.clear();
    for (const UUID & id : access_control.findAll<RowPolicy>())
    {
        auto policy = access_control.tryRead<RowPolicy>(id);
        if (policy)
        {
            all_policies.emplace(id, PolicyInfo(policy));
        }
    }

    /// Set only after the subscription and the initial read succeed.
    all_policies_read = true;
}


void RowPolicyCache::rowPolicyAddedOrChanged(const UUID & policy_id, const RowPolicyPtr & new_policy)
{
    /// `mutex` is already locked.
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
    need_mix_filters = true;
}


void RowPolicyCache::rowPolicyRemoved(const UUID & policy_id)
{
    /// `mutex` is already locked.
    all_policies.erase(policy_id);
    need_mix_filters = true;
}


void RowPolicyCache::mixFiltersIfNeeded()
{
    /// `mutex` is already locked.
    if (!need_mix_filters)
        return;
    /// Clear the flag only after a successful rebuild, so a throwing mixFilters() is retried next batch.
    mixFilters();
    need_mix_filters = false;
}


void RowPolicyCache::mixFilters()
{
    /// `mutex` is already locked.
    ProfileEvents::increment(ProfileEvents::RowPolicyCacheRecalculations);
    Stopwatch watch;
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

    const auto elapsed_ms = watch.elapsedMilliseconds();
    ProfileEvents::increment(ProfileEvents::RowPolicyCacheRecalculationMicroseconds, watch.elapsedMicroseconds());
    /// O(enabled sets * policies), under `mutex` that the ContextAccess build path also takes.
    if (elapsed_ms >= 1000)
        LOG_DEBUG(getLogger("RowPolicyCache"), "Re-mixed row policy filters for {} enabled set(s) over {} policies in {} ms", enabled_row_policies.size(), all_policies.size(), elapsed_ms);
    else
        LOG_TRACE(getLogger("RowPolicyCache"), "Re-mixed row policy filters for {} enabled set(s) over {} policies in {} ms", enabled_row_policies.size(), all_policies.size(), elapsed_ms);
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

    std::unordered_map<MixedFiltersKey, MixerWithNames, Hash> database_mixers;

    /// populate database_mixers using database-level policies
    ///  to aggregate (mix) rules per database
    for (const auto & [policy_id, info] : all_policies)
    {
        if (info.isForDatabase())
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

                    auto & mixer = database_mixers[key];
                    mixer.database_and_table_name = info.database_and_table_name;
                    if (match)
                    {
                        mixer.mixer.add(info.parsed_filters[filter_type_i], policy.isRestrictive());
                        mixer.policies.push_back(info.policy);
                    }
                }
            }
        }
    }

    std::unordered_map<MixedFiltersKey, MixerWithNames, Hash> table_mixers;

    /// populate table_mixers using database_mixers and table-level policies
    for (const auto & [policy_id, info] : all_policies)
    {
        if (!info.isForDatabase())
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
                    auto table_it = table_mixers.find(key);
                    if (table_it == table_mixers.end())
                    {   /// no exact match - create new mixer
                        MixedFiltersKey database_key = key;
                        database_key.table_name = RowPolicyName::ANY_TABLE_MARK;

                        auto database_it = database_mixers.find(database_key);

                        if (database_it == database_mixers.end())
                        {
                            table_it = table_mixers.try_emplace(key).first;
                        }
                        else
                        {
                            /// table policies are based on database ones
                            table_it = table_mixers.insert({key, database_it->second}).first;
                        }
                    }

                    auto & mixer = table_it->second; ///  getting table level mixer
                    mixer.database_and_table_name = info.database_and_table_name;
                    if (match)
                    {
                        mixer.mixer.add(info.parsed_filters[filter_type_i], policy.isRestrictive());
                        mixer.policies.push_back(info.policy);
                    }
                }
            }
        }
    }

    auto mixed_filters = boost::make_shared<MixedFiltersMap>();

    /// Retrieve aggregated policies from mixers
    ///  if a table has a policy for this particular table, we have all needed information in table_mixers
    ///    (policies for the database are already applied)
    ///  otherwise we would look for a policy for database using RowPolicy::ANY_TABLE_MARK
    /// Consider restrictive policies a=1 for db.t, b=2 for db.* and c=3 for db.*
    ///   We are going to have two items in mixed_filters:
    ///     1. a=1 AND b=2 AND c=3   for db.t (comes from table_mixers, where it had been created with the help of database_mixers)
    ///     2. b=2 AND c=3  for db.* (comes directly from database_mixers)
    for (auto * mixer_map_ptr : {&table_mixers, &database_mixers})
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
