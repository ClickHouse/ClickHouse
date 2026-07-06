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
    mixFiltersFor(*res, all_policies, access_control.isEnabledUsersWithoutRowPoliciesCanReadRows());
    return res;
}


void RowPolicyCache::ensureAllRowPoliciesRead()
{
    if (all_policies_read)
        return;

    subscription = access_control.subscribeForChanges<RowPolicy>(
        [this](const std::vector<AccessChangesNotifier::Change> & changes)
        {
            {
                std::lock_guard lock{mutex};
                for (const auto & change : changes)
                {
                    if (change.entity)
                        rowPolicyAddedOrChanged(change.id, typeid_cast<RowPolicyPtr>(change.entity));
                    else
                        rowPolicyRemoved(change.id);
                }
            }
            /// Off `mutex` - see mixFiltersIfNeeded.
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
    /// Rebuilding is O(enabled sets * policies) and slow with lots of policies, so do it off `mutex`
    /// to not block getEnabledRowPolicies (i.e. logins). Safe because it always runs single-threaded
    /// from AccessChangesNotifier::sendNotifications, on an immutable snapshot, publishing via atomic store.
    std::unordered_map<UUID, PolicyInfo> policies_snapshot;
    std::vector<std::shared_ptr<EnabledRowPolicies>> targets;
    bool users_without_row_policies_can_read_rows = false;
    {
        std::lock_guard lock{mutex};
        if (!need_mix_filters)
            return;
        policies_snapshot = all_policies;
        targets.reserve(enabled_row_policies.size());
        for (auto i = enabled_row_policies.begin(), e = enabled_row_policies.end(); i != e;)
        {
            if (auto elem = i->second.lock())
            {
                targets.push_back(std::move(elem));
                ++i;
            }
            else
                i = enabled_row_policies.erase(i);
        }
        users_without_row_policies_can_read_rows = access_control.isEnabledUsersWithoutRowPoliciesCanReadRows();
        /// Cleared only after the snapshot is built (so a throwing snapshot is retried), but under
        /// `mutex` so a change during the unlocked rebuild re-sets it and is picked up next batch.
        need_mix_filters = false;
    }

    ProfileEvents::increment(ProfileEvents::RowPolicyCacheRecalculations);
    Stopwatch watch;
    try
    {
        for (const auto & target : targets)
            mixFiltersFor(*target, policies_snapshot, users_without_row_policies_can_read_rows);
    }
    catch (...)
    {
        /// Rebuild failed: request a retry on the next batch (some sets may be left stale).
        std::lock_guard lock{mutex};
        need_mix_filters = true;
        throw;
    }

    const auto elapsed_ms = watch.elapsedMilliseconds();
    ProfileEvents::increment(ProfileEvents::RowPolicyCacheRecalculationMicroseconds, watch.elapsedMicroseconds());
    if (elapsed_ms >= 1000)
        LOG_DEBUG(getLogger("RowPolicyCache"), "Re-mixed row policy filters for {} enabled set(s) over {} policies in {} ms (off-lock)", targets.size(), policies_snapshot.size(), elapsed_ms);
    else
        LOG_TRACE(getLogger("RowPolicyCache"), "Re-mixed row policy filters for {} enabled set(s) over {} policies in {} ms (off-lock)", targets.size(), policies_snapshot.size(), elapsed_ms);
}


void RowPolicyCache::mixFiltersFor(EnabledRowPolicies & enabled, const std::unordered_map<UUID, PolicyInfo> & policies, bool users_without_row_policies_can_read_rows) const
{
    /// Reads only `policies` and atomically publishes into `enabled`; takes no lock.

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
    for (const auto & [policy_id, info] : policies)
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
    for (const auto & [policy_id, info] : policies)
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
            mixed_filter->expression = std::move(mixer.mixer).getResult(users_without_row_policies_can_read_rows);
            mixed_filter->policies = std::move(mixer.policies);

            /// The key's string_view-s must borrow from the value's own pair (which the filter
            /// owns), not from `key` built at first insertion: several policies can share one
            /// (database, table) key while the value's pair is reassigned to the last of them,
            /// so a key borrowing another policy's pair would dangle once that policy is dropped.
            const auto & names = *mixed_filter->database_and_table_name;
            MixedFiltersKey owned_key{names.first, names.second, key.filter_type};
            mixed_filters->emplace(owned_key, std::move(mixed_filter));
        }
    }

    enabled.mixed_filters.store(mixed_filters);
}

}
