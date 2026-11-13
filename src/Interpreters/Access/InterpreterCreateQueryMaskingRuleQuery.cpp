#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/Access/InterpreterCreateQueryMaskingRuleQuery.h>

#include <Access/AccessControl.h>
#include <Access/Common/AccessFlags.h>
#include <Access/QueryMaskingRule.h>
#include <Common/SensitiveDataMasker.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Parsers/Access/ASTCreateQueryMaskingRuleQuery.h>


namespace DB
{

void InterpreterCreateQueryMaskingRuleQuery::updateQueryMaskingRuleFromQuery(QueryMaskingRule & rule, const ASTCreateQueryMaskingRuleQuery & query)
{
    if (!query.new_name.empty())
        rule.setName(query.new_name);
    else
        rule.setName(query.name);

    if (query.regexp)
        rule.regexp = *query.regexp;

    if (query.replace)
        rule.replace = *query.replace;

    SensitiveDataMasker::validateRule(rule.formatTypeWithName(), rule.regexp, rule.replace);
}


BlockIO InterpreterCreateQueryMaskingRuleQuery::execute()
{
    const auto updated_query_ptr = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    auto & query = updated_query_ptr->as<ASTCreateQueryMaskingRuleQuery &>();

    auto & access_control = getContext()->getAccessControl();
    getContext()->checkAccess(AccessType::QUERY_MASKING_RULE_ADMIN);

    if (!query.cluster.empty())
        return executeDDLQueryOnCluster(updated_query_ptr, getContext());

    IAccessStorage * storage = &access_control;
    MultipleAccessStorage::StoragePtr storage_ptr;

    if (!query.storage_name.empty())
    {
        storage_ptr = access_control.getStorageByName(query.storage_name);
        storage = storage_ptr.get();
    }

    if (query.alter)
    {
        auto update_func = [&](const AccessEntityPtr & entity, const UUID &) -> AccessEntityPtr
        {
            auto updated_rule = typeid_cast<std::shared_ptr<QueryMaskingRule>>(entity->clone());
            updateQueryMaskingRuleFromQuery(*updated_rule, query);
            return updated_rule;
        };
        if (query.if_exists)
        {
            if (auto id = storage->find<QueryMaskingRule>(query.name))
                access_control.tryUpdate(*id, update_func);
        }
        else
            access_control.update(storage->getID<QueryMaskingRule>(query.name), update_func);
    }
    else
    {
        auto new_rule = std::make_shared<QueryMaskingRule>();
        updateQueryMaskingRuleFromQuery(*new_rule, query);

        if (!query.storage_name.empty())
            access_control.insertInto(query.storage_name, {new_rule}, query.or_replace, !query.if_not_exists);
        else if (query.if_not_exists)
            access_control.tryInsert(new_rule);
        else if (query.or_replace)
            access_control.insertOrReplace(new_rule);
        else
            access_control.insert(new_rule);
    }

    return {};
}

void registerInterpreterCreateQueryMaskingRuleQuery(InterpreterFactory & factory);
void registerInterpreterCreateQueryMaskingRuleQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCreateQueryMaskingRuleQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterCreateQueryMaskingRuleQuery", create_fn);
}

}
