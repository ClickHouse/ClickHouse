#include <Access/RowPolicyCache.h>
#include <Access/EnabledRowPolicies.h>
#include <Access/AccessControlManager.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Parsers/makeASTForLogicalFunction.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <ext/range.h>
#include <boost/smart_ptr/make_shared.hpp>
#include <Core/Defines.h>


namespace DB
{
namespace
{
    using ConditionType = RowPolicy::ConditionType;
    constexpr auto MAX_CONDITION_TYPE = RowPolicy::MAX_CONDITION_TYPE;


    /// Accumulates conditions from multiple row policies and joins them using the AND logical operation.
    class ConditionsMixer
    {
    public:
        void add(const ASTPtr & condition, bool is_restrictive)
        {
            if (is_restrictive)
                restrictions.push_back(condition);
            else
                permissions.push_back(condition);
        }

        ASTPtr getResult() &&
        {
            /// Process permissive conditions.
            restrictions.push_back(makeASTForLogicalOr(std::move(permissions)));

            /// Process restrictive conditions.
            auto condition = makeASTForLogicalAnd(std::move(restrictions));

            bool value;
            if (tryGetLiteralBool(condition.get(), value) && value)
                condition = nullptr;  /// The condition is always true, no need to check it.

            return condition;
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

    for (auto type : ext::range(0, MAX_CONDITION_TYPE))
    {
        parsed_conditions[type] = nullptr;
        const String & condition = policy->conditions[type];
        if (condition.empty())
            continue;

        auto previous_range = std::pair(std::begin(policy->conditions), std::begin(policy->conditions) + type);
        const auto * previous_it = std::find(previous_range.first, previous_range.second, condition);
        if (previous_it != previous_range.second)
        {
            /// The condition is already parsed before.
            parsed_conditions[type] = parsed_conditions[previous_it - previous_range.first];
            continue;
        }

        /// Try to parse the condition.
        try
        {
            ParserExpression parser;
            parsed_conditions[type] = parseQuery(parser, condition, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
        }
        catch (...)
        {
            tryLogCurrentException(
                &Poco::Logger::get("RowPolicy"),
                String("Could not parse the condition ") + RowPolicy::conditionTypeToString(type) + " of row policy "
                    + backQuote(policy->getFullName()));
        }
    }
}


RowPolicyCache::RowPolicyCache(const AccessControlManager & access_control_manager_)
    : access_control_manager(access_control_manager_)
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
    mixConditionsFor(*res);
    return res;
}


void RowPolicyCache::ensureAllRowPoliciesRead()
{
    /// `mutex` is already locked.
    if (all_policies_read)
        return;
    all_policies_read = true;

    subscription = access_control_manager.subscribeForChanges<RowPolicy>(
        [&](const UUID & id, const AccessEntityPtr & entity)
        {
            if (entity)
                rowPolicyAddedOrChanged(id, typeid_cast<RowPolicyPtr>(entity));
            else
                rowPolicyRemoved(id);
        });

    for (const UUID & id : access_control_manager.findAll<RowPolicy>())
    {
        auto quota = access_control_manager.tryRead<RowPolicy>(id);
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
    mixConditions();
}


void RowPolicyCache::rowPolicyRemoved(const UUID & policy_id)
{
    std::lock_guard lock{mutex};
    all_policies.erase(policy_id);
    mixConditions();
}


void RowPolicyCache::mixConditions()
{
    /// `mutex` is already locked.
    for (auto i = enabled_row_policies.begin(), e = enabled_row_policies.end(); i != e;)
    {
        auto elem = i->second.lock();
        if (!elem)
            i = enabled_row_policies.erase(i);
        else
        {
            mixConditionsFor(*elem);
            ++i;
        }
    }
}


void RowPolicyCache::mixConditionsFor(EnabledRowPolicies & enabled)
{
    /// `mutex` is already locked.
    struct Mixers
    {
        ConditionsMixer mixers[MAX_CONDITION_TYPE];
        std::vector<UUID> policy_ids;
    };
    using MapOfMixedConditions = EnabledRowPolicies::MapOfMixedConditions;
    using DatabaseAndTableName = EnabledRowPolicies::DatabaseAndTableName;
    using DatabaseAndTableNameRef = EnabledRowPolicies::DatabaseAndTableNameRef;
    using Hash = EnabledRowPolicies::Hash;

    std::unordered_map<DatabaseAndTableName, Mixers, Hash> map_of_mixers;

    for (const auto & [policy_id, info] : all_policies)
    {
        const auto & policy = *info.policy;
        auto & mixers = map_of_mixers[std::pair{policy.getDatabase(), policy.getTableName()}];
        if (info.roles->match(enabled.params.user_id, enabled.params.enabled_roles))
        {
            mixers.policy_ids.push_back(policy_id);
            for (auto type : ext::range(0, MAX_CONDITION_TYPE))
                if (info.parsed_conditions[type])
                    mixers.mixers[type].add(info.parsed_conditions[type], policy.isRestrictive());
        }
    }

    auto map_of_mixed_conditions = boost::make_shared<MapOfMixedConditions>();
    for (auto & [database_and_table_name, mixers] : map_of_mixers)
    {
        auto database_and_table_name_keeper = std::make_unique<DatabaseAndTableName>();
        database_and_table_name_keeper->first = database_and_table_name.first;
        database_and_table_name_keeper->second = database_and_table_name.second;
        auto & mixed_conditions = (*map_of_mixed_conditions)[DatabaseAndTableNameRef{database_and_table_name_keeper->first,
                                                                                     database_and_table_name_keeper->second}];
        mixed_conditions.database_and_table_name_keeper = std::move(database_and_table_name_keeper);
        mixed_conditions.policy_ids = std::move(mixers.policy_ids);
        for (auto type : ext::range(0, MAX_CONDITION_TYPE))
            mixed_conditions.mixed_conditions[type] = std::move(mixers.mixers[type]).getResult();
    }

    enabled.map_of_mixed_conditions.store(map_of_mixed_conditions);
}

}
