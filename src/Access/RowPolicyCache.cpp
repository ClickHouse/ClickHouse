#include <Access/RowPolicyCache.h>
#include <Access/EnabledRowPolicies.h>
#include <Access/AccessControlManager.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <ext/range.h>
#include <boost/smart_ptr/make_shared.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm_ext/erase.hpp>


namespace DB
{
namespace
{
    bool tryGetLiteralBool(const IAST & ast, bool & value)
    {
        try
        {
            if (const ASTLiteral * literal = ast.as<ASTLiteral>())
            {
                value = !literal->value.isNull() && applyVisitor(FieldVisitorConvertToNumber<bool>(), literal->value);
                return true;
            }
            return false;
        }
        catch (...)
        {
            return false;
        }
    }

    ASTPtr applyFunctionAND(ASTs arguments)
    {
        bool const_arguments = true;
        boost::range::remove_erase_if(arguments, [&](const ASTPtr & argument) -> bool
        {
            bool b;
            if (!tryGetLiteralBool(*argument, b))
                return false;
            const_arguments &= b;
            return true;
        });

        if (!const_arguments)
            return std::make_shared<ASTLiteral>(Field{UInt8(0)});
        if (arguments.empty())
            return std::make_shared<ASTLiteral>(Field{UInt8(1)});
        if (arguments.size() == 1)
            return arguments[0];

        auto function = std::make_shared<ASTFunction>();
        auto exp_list = std::make_shared<ASTExpressionList>();
        function->name = "and";
        function->arguments = exp_list;
        function->children.push_back(exp_list);
        exp_list->children = std::move(arguments);
        return function;
    }


    ASTPtr applyFunctionOR(ASTs arguments)
    {
        bool const_arguments = false;
        boost::range::remove_erase_if(arguments, [&](const ASTPtr & argument) -> bool
        {
            bool b;
            if (!tryGetLiteralBool(*argument, b))
                return false;
            const_arguments |= b;
            return true;
        });

        if (const_arguments)
            return std::make_shared<ASTLiteral>(Field{UInt8(1)});
        if (arguments.empty())
            return std::make_shared<ASTLiteral>(Field{UInt8(0)});
        if (arguments.size() == 1)
            return arguments[0];

        auto function = std::make_shared<ASTFunction>();
        auto exp_list = std::make_shared<ASTExpressionList>();
        function->name = "or";
        function->arguments = exp_list;
        function->children.push_back(exp_list);
        exp_list->children = std::move(arguments);
        return function;
    }


    using ConditionType = RowPolicy::ConditionType;
    constexpr size_t MAX_CONDITION_TYPE = RowPolicy::MAX_CONDITION_TYPE;


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
            restrictions.push_back(applyFunctionOR(std::move(permissions)));

            /// Process restrictive conditions.
            return applyFunctionAND(std::move(restrictions));
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

    for (auto type : ext::range_with_static_cast<ConditionType>(0, MAX_CONDITION_TYPE))
    {
        parsed_conditions[type] = nullptr;
        const String & condition = policy->conditions[type];
        if (condition.empty())
            continue;

        auto previous_range = std::pair(std::begin(policy->conditions), std::begin(policy->conditions) + type);
        auto previous_it = std::find(previous_range.first, previous_range.second, condition);
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
            parsed_conditions[type] = parseQuery(parser, condition, 0);
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


std::shared_ptr<const EnabledRowPolicies> RowPolicyCache::getEnabledRowPolicies(const UUID & user_id, const std::vector<UUID> & enabled_roles)
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
    std::erase_if(
        enabled_row_policies,
        [&](const std::pair<EnabledRowPolicies::Params, std::weak_ptr<EnabledRowPolicies>> & pr)
        {
            auto elem = pr.second.lock();
            if (!elem)
                return true; // remove from the `enabled_row_policies` map.
            mixConditionsFor(*elem);
            return false; // keep in the `enabled_row_policies` map.
        });
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
