#include <Access/RowPolicyContextFactory.h>
#include <Access/RowPolicyContext.h>
#include <Access/AccessControlManager.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <ext/range.h>
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


    using ConditionIndex = RowPolicy::ConditionIndex;
    static constexpr size_t MAX_CONDITION_INDEX = RowPolicy::MAX_CONDITION_INDEX;


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
            if (!permissions.empty())
                restrictions.push_back(applyFunctionOR(std::move(permissions)));

            /// Process restrictive conditions.
            if (!restrictions.empty())
                return applyFunctionAND(std::move(restrictions));
            return nullptr;
        }

    private:
        ASTs permissions;
        ASTs restrictions;
    };
}


void RowPolicyContextFactory::PolicyInfo::setPolicy(const RowPolicyPtr & policy_)
{
    policy = policy_;

    boost::range::copy(policy->roles, std::inserter(roles, roles.end()));
    all_roles = policy->all_roles;
    boost::range::copy(policy->except_roles, std::inserter(except_roles, except_roles.end()));

    for (auto index : ext::range_with_static_cast<ConditionIndex>(0, MAX_CONDITION_INDEX))
    {
        parsed_conditions[index] = nullptr;
        const String & condition = policy->conditions[index];
        if (condition.empty())
            continue;

        auto previous_range = std::pair(std::begin(policy->conditions), std::begin(policy->conditions) + index);
        auto previous_it = std::find(previous_range.first, previous_range.second, condition);
        if (previous_it != previous_range.second)
        {
            /// The condition is already parsed before.
            parsed_conditions[index] = parsed_conditions[previous_it - previous_range.first];
            continue;
        }

        /// Try to parse the condition.
        try
        {
            ParserExpression parser;
            parsed_conditions[index] = parseQuery(parser, condition, 0);
        }
        catch (...)
        {
            tryLogCurrentException(
                &Poco::Logger::get("RowPolicy"),
                String("Could not parse the condition ") + RowPolicy::conditionIndexToString(index) + " of row policy "
                    + backQuote(policy->getFullName()));
        }
    }
}


bool RowPolicyContextFactory::PolicyInfo::canUseWithContext(const RowPolicyContext & context) const
{
    if (roles.count(context.user_name))
        return true;

    if (all_roles && !except_roles.count(context.user_name))
        return true;

    return false;
}


RowPolicyContextFactory::RowPolicyContextFactory(const AccessControlManager & access_control_manager_)
    : access_control_manager(access_control_manager_)
{
}

RowPolicyContextFactory::~RowPolicyContextFactory() = default;


RowPolicyContextPtr RowPolicyContextFactory::createContext(const String & user_name)
{
    std::lock_guard lock{mutex};
    ensureAllRowPoliciesRead();
    auto context = ext::shared_ptr_helper<RowPolicyContext>::create(user_name);
    contexts.push_back(context);
    mixConditionsForContext(*context);
    return context;
}


void RowPolicyContextFactory::ensureAllRowPoliciesRead()
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


void RowPolicyContextFactory::rowPolicyAddedOrChanged(const UUID & policy_id, const RowPolicyPtr & new_policy)
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
    mixConditionsForAllContexts();
}


void RowPolicyContextFactory::rowPolicyRemoved(const UUID & policy_id)
{
    std::lock_guard lock{mutex};
    all_policies.erase(policy_id);
    mixConditionsForAllContexts();
}


void RowPolicyContextFactory::mixConditionsForAllContexts()
{
    /// `mutex` is already locked.
    boost::range::remove_erase_if(
        contexts,
        [&](const std::weak_ptr<RowPolicyContext> & weak)
        {
            auto context = weak.lock();
            if (!context)
                return true; // remove from the `contexts` list.
            mixConditionsForContext(*context);
            return false; // keep in the `contexts` list.
        });
}


void RowPolicyContextFactory::mixConditionsForContext(RowPolicyContext & context)
{
    /// `mutex` is already locked.
    struct Mixers
    {
        ConditionsMixer mixers[MAX_CONDITION_INDEX];
        std::vector<UUID> policy_ids;
    };
    using MapOfMixedConditions = RowPolicyContext::MapOfMixedConditions;
    using DatabaseAndTableName = RowPolicyContext::DatabaseAndTableName;
    using DatabaseAndTableNameRef = RowPolicyContext::DatabaseAndTableNameRef;
    using Hash = RowPolicyContext::Hash;

    std::unordered_map<DatabaseAndTableName, Mixers, Hash> map_of_mixers;

    for (const auto & [policy_id, info] : all_policies)
    {
        if (info.canUseWithContext(context))
        {
            const auto & policy = *info.policy;
            auto & mixers = map_of_mixers[std::pair{policy.getDatabase(), policy.getTableName()}];
            mixers.policy_ids.push_back(policy_id);
            for (auto index : ext::range(0, MAX_CONDITION_INDEX))
                if (info.parsed_conditions[index])
                    mixers.mixers[index].add(info.parsed_conditions[index], policy.isRestrictive());
        }
    }

    auto map_of_mixed_conditions = std::make_shared<MapOfMixedConditions>();
    for (auto & [database_and_table_name, mixers] : map_of_mixers)
    {
        auto database_and_table_name_keeper = std::make_unique<DatabaseAndTableName>();
        database_and_table_name_keeper->first = database_and_table_name.first;
        database_and_table_name_keeper->second = database_and_table_name.second;
        auto & mixed_conditions = (*map_of_mixed_conditions)[DatabaseAndTableNameRef{database_and_table_name_keeper->first,
                                                                                     database_and_table_name_keeper->second}];
        mixed_conditions.database_and_table_name_keeper = std::move(database_and_table_name_keeper);
        mixed_conditions.policy_ids = std::move(mixers.policy_ids);
        for (auto index : ext::range(0, MAX_CONDITION_INDEX))
            mixed_conditions.mixed_conditions[index] = std::move(mixers.mixers[index]).getResult();
    }

    std::atomic_store(&context.atomic_map_of_mixed_conditions, std::shared_ptr<const MapOfMixedConditions>{map_of_mixed_conditions});
}

}
